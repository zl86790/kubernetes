/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package noderesources

import (
	"context"
	"fmt"
	clocktesting "k8s.io/utils/clock/testing"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	draapi "k8s.io/dynamic-resource-allocation/api"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/resourceslice/tracker"
)

func TestDeviceMatchCacheLazyValidation(t *testing.T) {
	store := cache.NewStore(cache.MetaNamespaceKeyFunc)
	c := &DRACaches{resourceSliceStore: store}
	external := &resourceapi.ResourceSlice{ObjectMeta: metav1.ObjectMeta{Name: "slice", UID: "uid", Generation: 10}}
	if err := store.Add(external); err != nil {
		t.Fatal(err)
	}
	old := &draapi.ResourceSlice{ObjectMeta: external.ObjectMeta}
	oldResults := c.matchResultsForSlice(old)
	updated := external.DeepCopy()
	updated.Generation++
	if err := store.Update(updated); err != nil {
		t.Fatal(err)
	}
	// No event is delivered; access validation must reject the old entry itself.
	if c.matchResultsForSlice(old) == oldResults {
		t.Fatal("lazy validation reused an obsolete entry")
	}
	if c.deviceMatchCache.size() != 0 {
		t.Fatal("obsolete entry retained after validation")
	}
	latest := &draapi.ResourceSlice{ObjectMeta: updated.ObjectMeta}
	current := c.matchResultsForSlice(latest)
	for range 100 {
		if got := c.matchResultsForSlice(old); got.generation != 10 {
			t.Fatal("old snapshot received newer results")
		}
	}
	c.gcDeviceMatchCache()
	if value, _ := c.deviceMatchCache.Load(latest.UID); value != current {
		t.Fatal("old snapshot or GC removed newer results")
	}
	if c.matchResultsForSlice(latest) != current {
		t.Fatal("stable generation lost cached results")
	}
}

func populateDeviceMatchGC(t *testing.T, c *DRACaches, count int) []*resourceapi.ResourceSlice {
	t.Helper()
	external := make([]*resourceapi.ResourceSlice, count)
	for i := range count {
		external[i] = &resourceapi.ResourceSlice{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("slice-%d", i), UID: types.UID(fmt.Sprintf("uid-%d", i)), Generation: 10}}
		if err := c.resourceSliceStore.Add(external[i]); err != nil {
			t.Fatal(err)
		}
		c.matchResultsForSlice(&draapi.ResourceSlice{ObjectMeta: external[i].ObjectMeta})
	}
	return external
}

func TestDeviceMatchCacheGCTraversal(t *testing.T) {
	c := &DRACaches{resourceSliceStore: cache.NewStore(cache.MetaNamespaceKeyFunc), gcClock: clocktesting.NewFakeClock(time.Now())}
	external := populateDeviceMatchGC(t, c, 20000)
	stats := c.gcDeviceMatchCache()
	if stats.inspected != gcBatchSize || stats.reclaimed != 0 {
		t.Fatalf("valid batch: %+v", stats)
	}
	c.deviceMatchCache.mu.Lock()
	cursor := c.deviceMatchCache.cursor.Value
	c.deviceMatchCache.mu.Unlock()
	if cursor != external[gcBatchSize].UID {
		t.Fatalf("cursor = %v, want %s", cursor, external[gcBatchSize].UID)
	}
	// Simulate missed events and no subsequent Score access.
	for _, slice := range external {
		if err := c.resourceSliceStore.Delete(slice); err != nil {
			t.Fatal(err)
		}
	}
	inspected, batches := 0, 0
	for c.deviceMatchCache.size() != 0 {
		stats = c.gcDeviceMatchCache()
		if stats.inspected > gcBatchSize || stats.inspected == 0 || stats.reclaimed != stats.inspected {
			t.Fatalf("stale batch: %+v", stats)
		}
		inspected += stats.inspected
		batches++
		if batches > 40 {
			t.Fatal("GC did not make traversal progress")
		}
	}
	if inspected != 20000 || batches != 40 {
		t.Fatalf("inspected=%d batches=%d, want 20000 and 40", inspected, batches)
	}
	c.deviceMatchCache.mu.Lock()
	defer c.deviceMatchCache.mu.Unlock()
	if c.deviceMatchCache.cursor != nil || c.deviceMatchCache.entries.Len() != 0 {
		t.Fatal("empty cache retained traversal metadata")
	}
}

func TestDeviceMatchCacheGCChangedSlice(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprintf("recreate=%t", recreate), func(t *testing.T) {
			c := &DRACaches{resourceSliceStore: cache.NewStore(cache.MetaNamespaceKeyFunc), gcClock: clocktesting.NewFakeClock(time.Now())}
			original := populateDeviceMatchGC(t, c, 1)[0]
			changed := original.DeepCopy()
			if recreate {
				changed.UID = "new-uid"
			} else {
				changed.Generation++
			}
			if err := c.resourceSliceStore.Update(changed); err != nil {
				t.Fatal(err)
			}
			// No event or read identifies this stale UID/version before independent GC.
			stats := c.gcDeviceMatchCache()
			if stats.inspected != 1 || stats.reclaimed != 1 || c.deviceMatchCache.size() != 0 {
				t.Fatalf("GC missed changed slice: stats=%+v indexed=%d", stats, c.deviceMatchCache.size())
			}
		})
	}
}

type deviceMatchSlowStore struct {
	cache.Store
	clock *clocktesting.FakeClock
}

func (s deviceMatchSlowStore) GetByKey(key string) (any, bool, error) {
	s.clock.Step(2 * time.Millisecond)
	return s.Store.GetByKey(key)
}

func TestDeviceMatchCacheGCTimeBudget(t *testing.T) {
	gcClock := clocktesting.NewFakeClock(time.Now())
	store := cache.NewStore(cache.MetaNamespaceKeyFunc)
	c := &DRACaches{resourceSliceStore: store, gcClock: gcClock}
	external := populateDeviceMatchGC(t, c, 10)
	c.resourceSliceStore = deviceMatchSlowStore{Store: store, clock: gcClock}
	stats := c.gcDeviceMatchCache()
	if stats.inspected != 3 || stats.elapsed != 6*time.Millisecond {
		t.Fatalf("budgeted batch: %+v", stats)
	}
	c.deviceMatchCache.mu.Lock()
	cursor := c.deviceMatchCache.cursor.Value
	c.deviceMatchCache.mu.Unlock()
	if cursor != external[3].UID {
		t.Fatalf("cursor after time budget = %v, want %s", cursor, external[3].UID)
	}
}

func TestDeviceMatchCacheGCIndexMutations(t *testing.T) {
	c := &DRACaches{resourceSliceStore: cache.NewStore(cache.MetaNamespaceKeyFunc)}
	external := populateDeviceMatchGC(t, c, 3)
	c.deviceMatchCache.next()
	old, _ := c.deviceMatchCache.Load(external[1].UID)
	c.deviceMatchCache.CompareAndDelete(external[1].UID, old)
	newUID := types.UID("inserted")
	c.deviceMatchCache.Store(newUID, &sliceMatchResults{generation: 10})
	old, _ = c.deviceMatchCache.Load(external[2].UID)
	replacement := &sliceMatchResults{generation: 11, name: external[2].Name}
	if !c.deviceMatchCache.CompareAndSwap(external[2].UID, old, replacement) {
		t.Fatal("replace failed")
	}
	for _, want := range []types.UID{external[2].UID, newUID, external[0].UID} {
		key, _, ok := c.deviceMatchCache.next()
		if !ok || key != want {
			t.Fatalf("next key=%v, want %s", key, want)
		}
	}
	if c.deviceMatchCache.size() != 3 {
		t.Fatal("index cardinality differs from cache")
	}
}

type deviceMatchBlockingStore struct {
	cache.Store
	read   chan struct{}
	resume chan struct{}
	once   sync.Once
}

func (s *deviceMatchBlockingStore) GetByKey(key string) (any, bool, error) {
	obj, exists, err := s.Store.GetByKey(key)
	s.once.Do(func() { close(s.read); <-s.resume })
	return obj, exists, err
}

func TestDeviceMatchCacheGCConcurrentReplacement(t *testing.T) {
	base := cache.NewStore(cache.MetaNamespaceKeyFunc)
	c := &DRACaches{resourceSliceStore: base}
	external := populateDeviceMatchGC(t, c, 1)[0]
	old, _ := c.deviceMatchCache.Load(external.UID)
	updated := external.DeepCopy()
	updated.Generation++
	if err := base.Update(updated); err != nil {
		t.Fatal(err)
	}
	store := &deviceMatchBlockingStore{Store: base, read: make(chan struct{}), resume: make(chan struct{})}
	c.resourceSliceStore = store
	done := make(chan deviceMatchGCStats, 1)
	go func() { done <- c.gcDeviceMatchCache() }()
	<-store.read
	newer := &sliceMatchResults{generation: updated.Generation, name: updated.Name}
	if !c.deviceMatchCache.CompareAndSwap(external.UID, old, newer) {
		t.Fatal("new generation was not published")
	}
	close(store.resume)
	stats := <-done
	if stats.reclaimed != 0 {
		t.Fatalf("old GC candidate deleted newer results: %+v", stats)
	}
	if value, _ := c.deviceMatchCache.Load(external.UID); value != newer {
		t.Fatal("newer entry was lost")
	}
	if c.deviceMatchCache.size() != 1 {
		t.Fatal("replacement lost its traversal index")
	}
}

func TestDeviceMatchCacheDeleteBeforePublication(t *testing.T) {
	base := cache.NewStore(cache.MetaNamespaceKeyFunc)
	store := &deviceMatchBlockingStore{Store: base, read: make(chan struct{}), resume: make(chan struct{})}
	c := &DRACaches{resourceSliceStore: store}
	external := &resourceapi.ResourceSlice{ObjectMeta: metav1.ObjectMeta{Name: "slice", UID: "uid", Generation: 10}}
	if err := base.Add(external); err != nil {
		t.Fatal(err)
	}
	done := make(chan *sliceMatchResults, 1)
	go func() { done <- c.matchResultsForSlice(&draapi.ResourceSlice{ObjectMeta: external.ObjectMeta}) }()
	<-store.read
	if err := base.Delete(external); err != nil {
		t.Fatal(err)
	}
	c.invalidateDeviceMatchCache(external)
	close(store.resume)
	if result := <-done; result.generation != 10 {
		t.Fatal("deleted snapshot could not finish correctly")
	}
	if c.deviceMatchCache.size() != 0 {
		t.Fatal("publication after Delete resurrected cache/index state")
	}
}

func TestDeviceMatchCacheConcurrentCleanup(t *testing.T) {
	store := cache.NewStore(cache.MetaNamespaceKeyFunc)
	c := &DRACaches{resourceSliceStore: store}
	external := populateDeviceMatchGC(t, c, 1)[0]
	old := &draapi.ResourceSlice{ObjectMeta: external.ObjectMeta}
	latest := old.DeepCopy()
	latest.Generation++
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range 8 {
		workers.Go(func() {
			<-start
			for j := range 1000 {
				snapshot := old
				if (i+j)%2 != 0 {
					snapshot = latest
				}
				if got := c.matchResultsForSlice(snapshot); got.generation != snapshot.Generation {
					t.Errorf("snapshot generation %d received %d", snapshot.Generation, got.generation)
				}
			}
		})
	}
	workers.Go(func() {
		<-start
		for range 1000 {
			updated := external.DeepCopy()
			updated.Generation++
			if err := store.Update(updated); err != nil {
				t.Error(err)
				return
			}
			c.invalidateDeviceMatchCache(external)
		}
	})
	workers.Go(func() {
		<-start
		for range 1000 {
			c.gcDeviceMatchCache()
		}
	})
	close(start)
	workers.Wait()
	current := c.matchResultsForSlice(latest)
	c.gcDeviceMatchCache()
	if value, _ := c.deviceMatchCache.Load(latest.UID); value != current {
		t.Fatal("concurrent GC removed current generation")
	}
	if err := store.Delete(external); err != nil {
		t.Fatal(err)
	}
	c.invalidateDeviceMatchCache(&resourceapi.ResourceSlice{ObjectMeta: latest.ObjectMeta})
	for range 100 {
		c.matchResultsForSlice(old)
		c.matchResultsForSlice(latest)
	}
	if c.deviceMatchCache.size() != 0 {
		t.Fatal("worker resurrected a deleted UID or its index")
	}
}

type deviceMatchLifecycleInformer struct {
	cache.SharedIndexInformer
	removed chan struct{}
}

func (i *deviceMatchLifecycleInformer) RemoveEventHandler(reg cache.ResourceEventHandlerRegistration) error {
	err := i.SharedIndexInformer.RemoveEventHandler(reg)
	close(i.removed)
	return err
}

func TestDeviceMatchCacheIdleCleanupAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		informer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &resourceapi.ResourceSlice{}, 0, cache.Indexers{})
		caches := []*DRACaches{{}, {}}
		subscribers := []*deviceMatchLifecycleInformer{
			{SharedIndexInformer: informer, removed: make(chan struct{})},
			{SharedIndexInformer: informer, removed: make(chan struct{})},
		}
		cancels := make([]context.CancelFunc, len(caches))
		for i, c := range caches {
			profileCtx, stop := context.WithCancel(ctx)
			cancels[i] = stop
			if err := c.watchResourceSlices(profileCtx, subscribers[i]); err != nil {
				t.Fatal(err)
			}
			populateDeviceMatchGC(t, c, 1)
		}
		if err := informer.GetStore().Delete(&resourceapi.ResourceSlice{ObjectMeta: metav1.ObjectMeta{Name: "slice-0"}}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		time.Sleep(gcInterval - time.Second)
		synctest.Wait()
		for _, c := range caches {
			if c.deviceMatchCache.size() != 1 {
				t.Fatal("GC ran before the 60-second interval")
			}
		}
		time.Sleep(time.Second)
		synctest.Wait()
		for _, c := range caches {
			if c.deviceMatchCache.size() != 0 {
				t.Fatal("idle GC missed an entry without an event")
			}
		}
		for i, c := range caches {
			cancels[i]()
			synctest.Wait()
			select {
			case <-subscribers[i].removed:
			default:
				t.Fatal("shutdown did not remove handler")
			}
			select {
			case <-c.gcDone:
			default:
				t.Fatal("GC worker did not stop")
			}
			if i == 0 {
				select {
				case <-caches[1].gcDone:
					t.Fatal("canceling one profile stopped another GC worker")
				default:
				}
			}
		}
	})
}

func TestDeviceMatchCacheDeviceTaintRule(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	external := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "slice", UID: "uid", Generation: 10},
		Spec:       resourceapi.ResourceSliceSpec{Driver: "test-driver", Devices: []resourceapi.Device{{Name: "gpu", Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"enabled": {BoolValue: new(true)}}}}},
	}
	client := fake.NewClientset(external)
	factory := informers.NewSharedInformerFactory(client, 0)
	internalInformer := draapi.NewInformerForResourceSlice(factory)
	tr, err := tracker.StartTracker(ctx, tracker.Options{
		EnableDeviceTaintRules: true,
		SliceLister:            draapi.NewResourceSliceLister(internalInformer.GetIndexer()),
		SliceInformer:          internalInformer,
		TaintInformer:          factory.Resource().V1().DeviceTaintRules(),
		KubeClient:             client,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Stop()
	scorer := &resourceAllocationScorer{DRACaches: DRACaches{celCache: cel.NewCache(10, cel.Features{})}}
	rawInformer := factory.Resource().V1().ResourceSlices().Informer()
	if err := scorer.watchResourceSlices(ctx, rawInformer); err != nil {
		t.Fatal(err)
	}
	factory.Start(ctx.Done())
	defer func() { cancel(); factory.Shutdown() }()
	if !cache.WaitForCacheSync(ctx.Done(), rawInformer.HasSynced, tr.HasSynced) {
		t.Fatal("ResourceSlice tracker did not sync")
	}
	slices, err := tr.ListPatchedResourceSlices()
	if err != nil || len(slices) != 1 {
		t.Fatalf("initial tracker slices=%d err=%v", len(slices), err)
	}
	class := &resourceapi.DeviceClass{Spec: resourceapi.DeviceClassSpec{Selectors: []resourceapi.DeviceSelector{{CEL: &resourceapi.CELDeviceSelector{Expression: `device.attributes["test-driver"].enabled`}}}}}
	if match, err := scorer.deviceMatchesClass(ctx, slices[0], slices[0].Spec.Devices[0], class); err != nil || !match {
		t.Fatalf("initial match=%v err=%v", match, err)
	}
	original := scorer.matchResultsForSlice(slices[0])
	rule := &resourceapi.DeviceTaintRule{
		ObjectMeta: metav1.ObjectMeta{Name: "rule"},
		Spec:       resourceapi.DeviceTaintRuleSpec{DeviceSelector: &resourceapi.DeviceTaintSelector{}, Taint: resourceapi.DeviceTaint{Key: "test", Effect: resourceapi.DeviceTaintEffectNoSchedule}},
	}
	if _, err := client.ResourceV1().DeviceTaintRules().Create(ctx, rule, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		var err error
		slices, err = tr.ListPatchedResourceSlices()
		return len(slices) == 1 && len(slices[0].Spec.Devices[0].Taints) == 1, err
	}); err != nil {
		t.Fatal("DeviceTaintRule was not applied:", err)
	}
	if match, err := scorer.deviceMatchesClass(ctx, slices[0], slices[0].Spec.Devices[0], class); err != nil || !match {
		t.Fatalf("tainted match=%v err=%v", match, err)
	}
	if scorer.matchResultsForSlice(slices[0]) != original {
		t.Fatal("taint-only change invalidated unchanged CEL inputs")
	}
}

type deviceMatchRegistrationFailureInformer struct {
	cache.SharedIndexInformer
}

func (i deviceMatchRegistrationFailureInformer) AddEventHandler(cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return nil, fmt.Errorf("injected registration failure")
}

func TestDeviceMatchCacheRegistrationFailure(t *testing.T) {
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &resourceapi.ResourceSlice{}, 0, cache.Indexers{})
	c := &DRACaches{}
	err := c.watchResourceSlices(context.Background(), deviceMatchRegistrationFailureInformer{SharedIndexInformer: informer})
	if err == nil || err.Error() != "register ResourceSlice match cache invalidation handler: injected registration failure" {
		t.Fatalf("registration error = %v", err)
	}
}

func (c *deviceMatchCache) Store(key, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.values.Load(key); ok {
		element := old.(*sliceMatchResults).element
		old.(*sliceMatchResults).element = nil
		value.(*sliceMatchResults).element = element
	} else {
		value.(*sliceMatchResults).element = c.entries.PushBack(key)
	}
	c.values.Store(key, value)
}

func (c *deviceMatchCache) Range(f func(key, value any) bool) { c.values.Range(f) }
