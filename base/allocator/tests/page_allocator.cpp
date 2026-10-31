/// Deterministic tests of `PageAllocatorShard` / `PageAllocator` / `ExtentOps` (`pa.c`, `pac.c`, `extent.c`) on a private shard
/// with its own base and extent map: the batched retained allocation and its `page_allocator_mapped` accounting, eager
/// coalescing of large dirty extents, decay to retained, in-place expand/shrink, guarded extents (two-sided and the
/// bump allocator), the oversize purge shortcut, settings, stats and destroy. The values are page-size independent.
/// See page_allocator_oracle.cpp for the randomized comparison with jemalloc.

#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/ExtentOps.h>
#include <allocator/Options.h>
#include <allocator/PageAllocator.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>

#include "Test.h"

#include <cstdlib>
#include <cstring>
#include <new>

using namespace jemalloc;

namespace
{

void bootOnce()
{
    static bool booted = false;
    if (!booted)
    {
        REQUIRE(!pages::boot());
        booted = true;
    }
}

struct Shard
{
    Base * base = nullptr;
    ExtentMap * extent_map = nullptr;
    PageAllocatorShardStats * stats = nullptr;
    PageAllocatorShard * shard = nullptr;

    Shard(unsigned idx, ssize_t dirty_ms, ssize_t muzzy_ms, size_t oversize_threshold)
    {
        bootOnce();
        base = Base::create(nullptr, idx, &extent_hooks_default_extent_hooks, true);
        REQUIRE(base != nullptr);
        void * extent_map_memory = std::aligned_alloc(64, (sizeof(ExtentMap) + 63) / 64 * 64);
        std::memset(extent_map_memory, 0, sizeof(ExtentMap));
        extent_map = new (extent_map_memory) ExtentMap();
        REQUIRE(!extent_map->init(base, true));
        stats = new PageAllocatorShardStats();
        void * shard_memory = std::aligned_alloc(64, (sizeof(PageAllocatorShard) + 63) / 64 * 64);
        std::memset(shard_memory, 0, sizeof(PageAllocatorShard));
        shard = new (shard_memory) PageAllocatorShard();
        Nanoseconds now;
        now.initUpdate();
        REQUIRE(!shard->init(nullptr, extent_map, base, idx, stats, nullptr, now, oversize_threshold, dirty_ms, muzzy_ms));
    }

    PageAllocator & pageAllocator() { return shard->page_allocator; }

    Extent * alloc(size_t size, bool slab = false, bool guarded = false, bool zero = false)
    {
        bool deferred = false;
        Extent * e = shard->alloc(nullptr, size, PAGE, slab, slab ? 0 : SIZE_CLASS_NUM_BINS, zero, guarded, &deferred);
        CHECK(!deferred);
        return e;
    }

    void deallocate(Extent * e)
    {
        bool deferred = false;
        shard->deallocate(nullptr, e, &deferred);
        CHECK(deferred);
    }

    void decayAll(bool dirty, bool fully)
    {
        Decay & d = dirty ? pageAllocator().decay_dirty : pageAllocator().decay_muzzy;
        DecayStats & s = dirty ? stats->page_allocator_stats.decay_dirty : stats->page_allocator_stats.decay_muzzy;
        ExtentCache & c = dirty ? pageAllocator().extent_cache_dirty : pageAllocator().extent_cache_muzzy;
        d.mutex.lock(nullptr);
        pageAllocator().decayAll(nullptr, &d, &s, &c, fully);
        d.mutex.unlock(nullptr);
    }

    size_t retainedPages() const { return shard->page_allocator.extent_cache_retained.numPagesGet(); }

    void destroy()
    {
        decayAll(true, true);
        decayAll(false, true);
        shard->destroy(nullptr);
    }
};

/// The size of the first mapping of the retained growth: 2 MiB.
constexpr size_t FIRST_GROW = size_t(2) << 20;

}

TEST(PageAllocator, BatchedSize)
{
    /// Rounded up to the classic size class, but not beyond the next huge page boundary.
    CHECK_EQ(pageAllocatorAllocRetainedBatchedSize(5 * PAGE), 5 * PAGE);
    CHECK_EQ(pageAllocatorAllocRetainedBatchedSize(9 * PAGE), 10 * PAGE);
    CHECK_EQ(pageAllocatorAllocRetainedBatchedSize(17 * PAGE), 20 * PAGE);
    CHECK_EQ(pageAllocatorAllocRetainedBatchedSize(33 * PAGE), 40 * PAGE);
    CHECK_EQ(
        pageAllocatorAllocRetainedBatchedSize(HUGE_PAGE + PAGE),
        minOf(size_classes::sizeToUsableSizeComputeUsingDelta(HUGE_PAGE + PAGE), 2 * HUGE_PAGE));
    CHECK_EQ(pageAllocatorAllocRetainedBatchedSize(SIZE_CLASS_LARGE_MAX_CLASS + PAGE), SIZE_CLASS_LARGE_MAX_CLASS + PAGE);
}

TEST(PageAllocator, Lifecycle)
{
    Shard s(1, 5000, 0, size_t(64) << 20);
    background_thread_enabled_state.store(false);

    /// 9 pages: a 10-page chunk is taken from the (newly grown) retained cache, the extra page goes to the dirty cache.
    Extent * e = s.alloc(9 * PAGE);
    REQUIRE(e != nullptr);
    CHECK_EQ(e->size(), 9 * PAGE);
    CHECK_EQ(e->serialNumber(), uint64_t(0));
    CHECK(e->isHead());
    CHECK_EQ(e->state(), extent_state_active);
    CHECK_EQ(e->sizeClassIdx(), SIZE_CLASS_NUM_BINS);
    CHECK_EQ(e->arenaIdx(), 1u);
    CHECK_EQ(s.shard->numActiveGet(), size_t(9));
    CHECK_EQ(s.shard->numDirtyGet(), size_t(1));
    CHECK_EQ(s.pageAllocator().mapped(), 10 * PAGE);
    CHECK_EQ(s.retainedPages(), (FIRST_GROW - 10 * PAGE) / PAGE);
    CHECK_EQ(s.pageAllocator().extent_serial_number_next.load(), size_t(1));
    /// The first growth is 2 MiB; the next one is the next page size class.
    CHECK_EQ(s.pageAllocator().exponential_grow.next, size_classes::pageSizeToPageSizeClassIdx(FIRST_GROW) + 1);

    /// The extent is mapped (boundary pages) with its szind.
    FullAllocContext context{};
    CHECK(!s.extent_map->fullAllocContextTryLookup(nullptr, e->addr(), &context));
    CHECK(context.extent == e && context.size_class_idx == SIZE_CLASS_NUM_BINS && !context.slab);
    std::memset(e->addr(), 0x5a, e->size());

    /// Deallocating a large extent coalesces it eagerly with the dirty trail.
    s.deallocate(e);
    CHECK_EQ(s.shard->numActiveGet(), size_t(0));
    CHECK_EQ(s.shard->numDirtyGet(), size_t(10));
    CHECK(s.pageAllocator().extent_cache_dirty.extent_set.lruFirst() == e);
    CHECK_EQ(e->size(), 10 * PAGE);
    CHECK_EQ(e->state(), extent_state_dirty);

    /// Reallocation reuses the dirty extent (first fit), splitting it again.
    Extent * e2 = s.alloc(9 * PAGE);
    CHECK(e2 == e);
    CHECK_EQ(s.pageAllocator().mapped(), 10 * PAGE);
    s.deallocate(e2);

    /// Purging everything moves the pages to the retained cache, which coalesces back into the whole mapping.
    s.decayAll(true, false);
    CHECK_EQ(s.shard->numDirtyGet(), size_t(0));
    CHECK_EQ(s.retainedPages(), FIRST_GROW / PAGE);
    CHECK_EQ(
        s.pageAllocator().extent_cache_retained.numExtentsGet(
            size_classes::pageSizeToPageSizeClassIdx(size_classes::pageSizeQuantizeFloor(FIRST_GROW))),
        size_t(1));
    CHECK_EQ(s.pageAllocator().mapped(), size_t(0));
    CHECK_EQ(s.stats->page_allocator_stats.decay_dirty.num_purge.read(), uint64_t(1));
    CHECK_EQ(s.stats->page_allocator_stats.decay_dirty.num_madvises.read(), uint64_t(1));
    CHECK_EQ(s.stats->page_allocator_stats.decay_dirty.purged.read(), uint64_t(10));

    /// Stats merge.
    PageAllocatorShardStats merged;
    static PageAllocatorExtentStats extent_stats[SIZE_CLASS_NUM_PAGE_SIZES];
    size_t resident = 0;
    s.shard->statsMerge(nullptr, &merged, extent_stats, &resident);
    CHECK_EQ(merged.page_allocator_stats.retained, FIRST_GROW);
    CHECK_EQ(merged.page_allocator_stats.decay_dirty.purged.read(), uint64_t(10));
    CHECK_EQ(resident, size_t(0));
    CHECK_EQ(
        extent_stats[size_classes::pageSizeToPageSizeClassIdx(size_classes::pageSizeQuantizeFloor(FIRST_GROW))].num_retained, size_t(1));
    CHECK_EQ(
        extent_stats[size_classes::pageSizeToPageSizeClassIdx(size_classes::pageSizeQuantizeFloor(FIRST_GROW))].retained_bytes, FIRST_GROW);
    size_t num_active = 0;
    size_t num_dirty = 0;
    size_t num_muzzy = 0;
    s.shard->basicStatsMerge(&num_active, &num_dirty, &num_muzzy);
    CHECK(num_active == 0 && num_dirty == 0 && num_muzzy == 0);

    MutexProfilingData mutex_data[mutex_profiling_num_arena_mutexes];
    s.shard->mutexStatsRead(nullptr, mutex_data);
    CHECK_GT(mutex_data[arena_profiling_mutex_extents_dirty].num_lock_ops, uint64_t(0));
    CHECK_EQ(mutex_data[arena_profiling_mutex_huge_page_shard].num_lock_ops, uint64_t(0));

    /// Fork hooks in the arena's order.
    s.shard->prefork0(nullptr);
    s.shard->prefork2(nullptr);
    s.shard->prefork3(nullptr);
    s.shard->prefork4(nullptr);
    s.shard->prefork5(nullptr);
    s.shard->postforkParent(nullptr);

    s.destroy();
    CHECK_EQ(s.retainedPages(), size_t(0));
}

TEST(PageAllocator, ExpandShrink)
{
    Shard s(2, 5000, 0, size_t(64) << 20);
    /// 10 pages is a size class: no trail.
    Extent * e = s.alloc(10 * PAGE);
    REQUIRE(e != nullptr);
    CHECK_EQ(s.shard->numDirtyGet(), size_t(0));
    CHECK_EQ(s.pageAllocator().mapped(), 10 * PAGE);

    /// Expanding takes the forward neighbor from the retained cache.
    bool deferred = false;
    CHECK(!s.shard->expand(nullptr, e, 10 * PAGE, 12 * PAGE, SIZE_CLASS_NUM_BINS + 1, true, &deferred));
    CHECK(!deferred);
    CHECK_EQ(e->size(), 12 * PAGE);
    CHECK_EQ(e->sizeClassIdx(), SIZE_CLASS_NUM_BINS + 1);
    CHECK_EQ(s.shard->numActiveGet(), size_t(12));
    CHECK_EQ(s.pageAllocator().mapped(), 12 * PAGE);
    /// The zeroed expansion.
    const unsigned char * p = static_cast<const unsigned char *>(e->addr());
    CHECK(p[10 * PAGE] == 0 && p[12 * PAGE - 1] == 0);

    /// Shrinking returns the trail to the dirty cache.
    CHECK(!s.shard->shrink(nullptr, e, 12 * PAGE, 4 * PAGE, SIZE_CLASS_NUM_BINS, &deferred));
    CHECK(deferred);
    CHECK_EQ(e->size(), 4 * PAGE);
    CHECK_EQ(s.shard->numActiveGet(), size_t(4));
    CHECK_EQ(s.shard->numDirtyGet(), size_t(8));

    /// Expanding again takes the dirty neighbor (no new mapping).
    deferred = false;
    CHECK(!s.shard->expand(nullptr, e, 4 * PAGE, 6 * PAGE, SIZE_CLASS_NUM_BINS, false, &deferred));
    CHECK_EQ(s.shard->numDirtyGet(), size_t(6));
    CHECK_EQ(s.pageAllocator().mapped(), 12 * PAGE);

    /// An active neighbor blocks the expansion (and no new mapping is made for in-place expansion with `retain`).
    Extent * f = s.alloc(6 * PAGE);
    REQUIRE(f != nullptr);
    CHECK_EQ(reinterpret_cast<uintptr_t>(f->addr()), reinterpret_cast<uintptr_t>(e->past()));
    CHECK(s.shard->expand(nullptr, e, 6 * PAGE, 7 * PAGE, SIZE_CLASS_NUM_BINS, false, &deferred));
    CHECK_EQ(e->size(), 6 * PAGE);
    CHECK_EQ(f->state(), extent_state_active);

    s.deallocate(f);
    s.deallocate(e);
    s.destroy();
}

TEST(PageAllocator, Guarded)
{
    Shard s(3, 5000, 0, size_t(64) << 20);

    /// A large guarded extent: two guard pages around it, unguarded eagerly on dalloc.
    size_t size = SIZE_CLASS_LARGE_MIN_CLASS;
    Extent * e = s.alloc(size, /* slab */ false, /* guarded */ true);
    REQUIRE(e != nullptr);
    CHECK(e->guarded());
    CHECK_EQ(e->size(), size);
    CHECK_EQ(s.shard->numActiveGet(), size / PAGE);
    std::memset(e->addr(), 1, size);
    s.deallocate(e);
    CHECK(!e->guarded());
    CHECK_EQ(e->size(), size + 2 * PAGE);
    CHECK_EQ(s.pageAllocator().extent_cache_dirty.guarded_extent_set.numPagesGet(), size_t(0));

    /// A guarded slab comes from the bump allocator (right guard only) and is cached guarded.
    Extent * slab = s.alloc(2 * PAGE, /* slab */ true, /* guarded */ true);
    REQUIRE(slab != nullptr);
    CHECK(slab->guarded() && slab->slab());
    CHECK_EQ(slab->size(), 2 * PAGE);
    REQUIRE(s.pageAllocator().sanitizer_bump_alloc.current_region != nullptr);
    CHECK_EQ(s.pageAllocator().sanitizer_bump_alloc.current_region->size(), SANITIZER_BUMP_ALLOC_RETAINED_ALLOC_SIZE - 3 * PAGE);
    std::memset(slab->addr(), 2, 2 * PAGE);
    s.deallocate(slab);
    CHECK(slab->guarded());
    CHECK_EQ(s.pageAllocator().extent_cache_dirty.guarded_extent_set.numPagesGet(), size_t(2));
    /// Exact-fit reuse of the cached guarded slab.
    Extent * again = s.alloc(2 * PAGE, true, true);
    CHECK(again == slab);
    CHECK_EQ(s.pageAllocator().extent_cache_dirty.guarded_extent_set.numPagesGet(), size_t(0));
    /// Guarded extents cannot be resized in place.
    bool deferred = false;
    CHECK(s.shard->shrink(nullptr, again, 2 * PAGE, PAGE, 0, &deferred));
    s.deallocate(again);

    /// Purging evicts the guarded extent (after the non-guarded ones); it stays guarded in the retained cache (and is
    /// unguarded by `extentDestroyWrapper`).
    s.decayAll(true, true);
    CHECK_EQ(s.pageAllocator().extent_cache_dirty.numPagesGet(), size_t(0));
    CHECK_EQ(s.pageAllocator().extent_cache_retained.guarded_extent_set.numPagesGet(), size_t(2));
    s.destroy();
    CHECK_EQ(s.pageAllocator().extent_cache_retained.numPagesGet(), size_t(0));
}

TEST(PageAllocator, OversizeShortcut)
{
    Shard s(4, 5000, 0, 16 * PAGE);
    background_thread_enabled_state.store(false);
    Extent * e = s.alloc(20 * PAGE);
    REQUIRE(e != nullptr);
    s.deallocate(e);
    /// Purged directly to retained.
    CHECK_EQ(s.shard->numDirtyGet(), size_t(0));
    CHECK_EQ(s.stats->page_allocator_stats.decay_dirty.num_madvises.read(), uint64_t(1));
    CHECK_EQ(s.stats->page_allocator_stats.decay_dirty.purged.read(), uint64_t(20));
    CHECK_EQ(s.stats->page_allocator_stats.decay_dirty.num_purge.read(), uint64_t(0));
    CHECK_EQ(s.pageAllocator().mapped(), size_t(0));

    /// Not with background threads.
    background_thread_enabled_state.store(true);
    e = s.alloc(20 * PAGE);
    s.deallocate(e);
    CHECK_EQ(s.shard->numDirtyGet(), size_t(20));
    background_thread_enabled_state.store(false);

    /// Not when decay is disabled.
    CHECK(!s.shard->decayMsSet(nullptr, extent_state_dirty, -1, PAGE_ALLOCATOR_PURGE_NEVER));
    e = s.alloc(20 * PAGE);
    s.deallocate(e);
    CHECK_EQ(s.shard->numDirtyGet(), size_t(20));
    s.destroy();
}

TEST(PageAllocator, DecaySettings)
{
    Shard s(5, 0, 0, size_t(64) << 20);
    CHECK_EQ(s.shard->decayMsGet(extent_state_dirty), ssize_t(0));
    CHECK(s.shard->dontDecayMuzzy());

    Extent * e = s.alloc(10 * PAGE);
    s.deallocate(e);
    CHECK_EQ(s.shard->numDirtyGet(), size_t(10));
    /// Immediate decay purges everything on the next check, whatever the eagerness.
    PageAllocator & page_allocator = s.pageAllocator();
    page_allocator.decay_dirty.mutex.lock(nullptr);
    CHECK(!page_allocator.maybeDecayPurge(
        nullptr,
        &page_allocator.decay_dirty,
        &s.stats->page_allocator_stats.decay_dirty,
        &page_allocator.extent_cache_dirty,
        PAGE_ALLOCATOR_PURGE_NEVER));
    page_allocator.decay_dirty.mutex.unlock(nullptr);
    CHECK_EQ(s.shard->numDirtyGet(), size_t(0));
    CHECK_EQ(s.shard->timeUntilDeferredWork(nullptr), DECAY_UNBOUNDED_TIME_TO_PURGE);

    /// Invalid values are rejected.
    CHECK(s.shard->decayMsSet(nullptr, extent_state_dirty, -2, PAGE_ALLOCATOR_PURGE_NEVER));
    /// Gradual decay: nothing is purged before the epoch advances.
    CHECK(!s.shard->decayMsSet(nullptr, extent_state_dirty, 1000000, PAGE_ALLOCATOR_PURGE_ALWAYS));
    CHECK_EQ(s.shard->decayMsGet(extent_state_dirty), ssize_t(1000000));
    e = s.alloc(10 * PAGE);
    s.deallocate(e);
    page_allocator.decay_dirty.mutex.lock(nullptr);
    CHECK(!page_allocator.maybeDecayPurge(
        nullptr,
        &page_allocator.decay_dirty,
        &s.stats->page_allocator_stats.decay_dirty,
        &page_allocator.extent_cache_dirty,
        PAGE_ALLOCATOR_PURGE_ON_EPOCH_ADVANCE));
    page_allocator.decay_dirty.mutex.unlock(nullptr);
    CHECK_EQ(s.shard->numDirtyGet(), size_t(10));
    /// The dirty pages were not recorded in a past epoch yet: the next check is a full decay period away.
    uint64_t t = s.shard->timeUntilDeferredWork(nullptr);
    CHECK_EQ(t, page_allocator.decay_dirty.interval.ns() * SMOOTHSTEP_NUM_STEPS);

    /// Muzzy decay: dirty pages go to the muzzy cache first (purged lazily), then to retained.
    CHECK(!s.shard->decayMsSet(nullptr, extent_state_muzzy, 1000000, PAGE_ALLOCATOR_PURGE_NEVER));
    CHECK(!s.shard->dontDecayMuzzy());
    s.decayAll(true, false);
    CHECK_EQ(s.shard->numDirtyGet(), size_t(0));
    CHECK_EQ(s.shard->numMuzzyGet(), size_t(10));
    CHECK_EQ(s.stats->page_allocator_stats.decay_dirty.purged.read(), uint64_t(20));
    /// Allocation also searches the muzzy cache.
    e = s.alloc(10 * PAGE);
    CHECK_EQ(s.shard->numMuzzyGet(), size_t(0));
    s.deallocate(e);
    s.decayAll(true, false);
    s.decayAll(false, false);
    CHECK_EQ(s.shard->numMuzzyGet(), size_t(0));
    CHECK_EQ(s.stats->page_allocator_stats.decay_muzzy.purged.read(), uint64_t(10));

    /// The retained grow limit.
    size_t old_limit = 0;
    CHECK(!page_allocator.retainGrowLimitGetSet(nullptr, &old_limit, nullptr));
    CHECK_EQ(old_limit, size_classes::pageSizeClassIdxToSize(size_classes::pageSizeToPageSizeClassIdx(SIZE_CLASS_LARGE_MAX_CLASS)));
    size_t new_limit = size_t(4) << 20;
    CHECK(!page_allocator.retainGrowLimitGetSet(nullptr, &old_limit, &new_limit));
    CHECK(!page_allocator.retainGrowLimitGetSet(nullptr, &old_limit, nullptr));
    CHECK_EQ(old_limit, size_t(4) << 20);
    /// Growth stops at the limit (unless forced by the request size).
    Extent * big = s.alloc(size_t(4) << 20);
    REQUIRE(big != nullptr);
    CHECK_EQ(page_allocator.exponential_grow.next, size_classes::pageSizeToPageSizeClassIdx(size_t(4) << 20));
    s.deallocate(big);
    s.destroy();
}

TEST(PageAllocator, CoalesceLimitQuirk)
{
    /// `extent_record` coalesces a large dirty extent with neighbors only up to `size << log2_extent_max_active_fit`;
    /// a rejected neighbor stays in the `merging` state (jemalloc compatibility, fork patch 3c14707b).
    /// The pinned numbers depend on the geometry; they were taken with HUGEPAGE = PAGE * PAGE / 8 (Linux x86_64,
    /// aarch64, riscv64). With 64 KiB pages and 2 MiB huge pages (ppc64le) the extents differ (the same as in jemalloc,
    /// checked against the reference under qemu).
    if constexpr (LOG2_HUGE_PAGE != 2 * LOG2_PAGE - 3)
        return;
    Shard s(6, 5000, 0, SIZE_MAX);
    /// 1028 pages: a 1280-page chunk; the 252-page tail goes to the dirty cache.
    Extent * h = s.alloc(1028 * PAGE);
    REQUIRE(h != nullptr);
    CHECK_EQ(s.shard->numDirtyGet(), size_t(252));
    /// Shrinking to 4 pages: the 1024-page tail is coalesced with the dirty neighbor (within 1024 << 6).
    bool deferred = false;
    REQUIRE(!s.shard->shrink(nullptr, h, 1028 * PAGE, 4 * PAGE, SIZE_CLASS_NUM_BINS, &deferred));
    Extent * big = s.pageAllocator().extent_cache_dirty.extent_set.lruFirst();
    REQUIRE(big != nullptr);
    CHECK_EQ(big->size(), 1276 * PAGE);
    CHECK(s.pageAllocator().extent_cache_dirty.extent_set.lru.next(big) == nullptr);
    CHECK_EQ(reinterpret_cast<uintptr_t>(big->addr()), reinterpret_cast<uintptr_t>(h->past()));

    /// The 4-page extent may grow only up to 4 << 6 pages: the big neighbor is acquired, rejected, and not released.
    s.deallocate(h);
    CHECK_EQ(h->state(), extent_state_dirty);
    CHECK_EQ(h->size(), 4 * PAGE);
    CHECK_EQ(big->state(), extent_state_merging);
    CHECK_EQ(s.shard->numDirtyGet(), size_t(1280));

    /// The merging extent is still in the set and can be allocated.
    Extent * again = s.alloc(1024 * PAGE);
    CHECK(again == big);
    CHECK_EQ(big->state(), extent_state_active);
    CHECK_EQ(s.shard->numDirtyGet(), size_t(256));
    s.deallocate(again);
    s.destroy();
}
