/// The thread cache on top of real arenas (arena 0 and a manual arena, with `b0`, the global extent map and the page
/// allocator): tcache creation and the arena association, the fill counts (`num_fill_min` / `num_fill_max`), flushes on a
/// full bin, the time-gated GC (flush counts, fill count adaptation, the locality heuristic with remote pointers),
/// `threadCacheFlush`, disabling / re-enabling, `thread.tcache.max`, `num_cached_max` writes, explicit tcaches, and the stats
/// merged into the arena bins. The TSD is a heap object in the nominal state (not the thread's TLS).

#include <allocator/ArenaInlines.h>
#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/ExtentMap.h>
#include <allocator/Frontend.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadCache.h>

#include "Test.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <vector>

using namespace jemalloc;

namespace
{

Arena * arena0 = nullptr;

void bootOnce()
{
    static bool booted = false;
    if (booted)
        return;
    booted = true;
    /// The allocator is "initialized" for the tcache (the tcache binds through `arenaChoose`), and no option forces the
    /// slow paths.
    malloc_init_state = malloc_init_initialized;
    malloc_slow = false;
    REQUIRE(!pages::boot());
    REQUIRE(!baseBoot(nullptr));
    REQUIRE(!arena_extent_map_global.init(base0Get(), true));
    REQUIRE(!arenaBoot(&default_size_class_data, base0Get(), false));
    REQUIRE(!threadCacheBoot(nullptr, base0Get()));
    num_arenas_auto = 1;
    manual_arena_base = 1;
    arena0 = arenaInit(nullptr, 0, &arena_config_default);
    REQUIRE(arena0 != nullptr);
    a0 = arena0;
    /// As `malloc_init_hard`: the background thread module is booted (disabled), `arenaInit` checks its thread slots.
    REQUIRE(!backgroundThreadBoot0());
    REQUIRE(!backgroundThreadBoot1(nullptr, base0Get()));
}

std::unique_ptr<ThreadState> makeThreadState()
{
    bootOnce();
    auto thread_state = std::make_unique<ThreadState>();
    thread_state->state.store(thread_state_nominal, std::memory_order_relaxed);
    thread_state->radix_tree_context.init();
    thread_state->prng_state = 42;
    REQUIRE(!threadCacheThreadStateDataInit(*thread_state));
    return thread_state;
}

Bin * bin0(Arena * arena, SizeClassIdx idx)
{
    return arenaGetBin(arena, idx, 0);
}

size_t threadCacheListLength(Arena * arena)
{
    size_t n = 0;
    arena->thread_cache_list.forEach([&](ThreadCacheSlow *) { ++n; });
    return n;
}

/// Makes the next GC event run (the 10 ms gate counts from `last_gc_time`).
void allowGC(ThreadState & thread_state)
{
    thread_state.threadCacheSlowGet()->last_gc_time = Nanoseconds::zero();
}

/// Makes the next GC event a no-op (the clock never goes below the current value).
void forbidGC(ThreadState & thread_state)
{
    thread_state.threadCacheSlowGet()->last_gc_time = Nanoseconds::fromNanoseconds(UINT64_MAX / 2);
}

}

TEST(ThreadCacheArena, CreateAndDestroy)
{
    auto thread_state = makeThreadState();
    ThreadCache * thread_cache = threadCacheGet(*thread_state);
    REQUIRE(thread_cache != nullptr);
    ThreadCacheSlow * slow = thread_state->threadCacheSlowGet();
    CHECK(thread_cache->thread_cache_slow == slow);
    CHECK(slow->thread_cache == thread_cache);
    CHECK(slow->arena == arena0);
    CHECK(thread_state->arena == arena0);
    CHECK_EQ(slow->thread_cache_num_bins, global_do_not_change_thread_cache_num_bins);
    CHECK_EQ(slow->next_gc_bin_large, SIZE_CLASS_NUM_BINS);
    CHECK_EQ(threadCacheListLength(arena0), size_t(1));

    /// The stack is one internal, page-aligned allocation from arena 0.
    size_t size;
    size_t alignment;
    cacheBinInfoComputeAlloc(threadCacheGetDefaultNumCachedMax(), slow->thread_cache_num_bins, size, alignment);
    CHECK_EQ(reinterpret_cast<uintptr_t>(slow->dynamic_alloc) % PAGE, uintptr_t(0));
    CHECK_EQ(arenaInternalGet(arena0), size_classes::alignedSizeToUsableSize(size, PAGE));
    CHECK_EQ(*static_cast<uintptr_t *>(slow->dynamic_alloc), cache_bin_preceding_junk);

    for (SizeClassIdx i = 0; i < THREAD_CACHE_NUM_BINS_MAX; ++i)
    {
        bool expect_enabled = i < slow->thread_cache_num_bins && threadCacheGetDefaultNumCachedMax()[i].num_cached_max > 0;
        CHECK_EQ(!threadCacheBinDisabled(i, &thread_cache->bins[i], slow), expect_enabled);
        CHECK_EQ(thread_cache->bins[i].numCachedMaxGetUnsafe(), threadCacheGetDefaultNumCachedMax()[i].num_cached_max);
        CacheBinSize n = 0;
        CHECK(!threadCacheBinNumCachedMaxRead(*thread_state, size_classes::indexToSize(i), n));
        CHECK_EQ(unsigned(n), expect_enabled ? unsigned(threadCacheGetDefaultNumCachedMax()[i].num_cached_max) : 0u);
    }
    CacheBinSize n = 0;
    CHECK(threadCacheBinNumCachedMaxRead(*thread_state, THREAD_CACHE_MAX_CLASS_LIMIT + 1, n));

    threadCacheCleanup(*thread_state);
    /// Cleanup keeps `thread_cache_enabled` (the TSD cleanup resets the state); the bins are zeroed.
    CHECK(thread_cache->bins[0].stillZeroInitialized());
    CHECK_EQ(threadCacheListLength(arena0), size_t(0));
    CHECK_EQ(arenaInternalGet(arena0), size_t(0));
    thread_state->thread_cache_enabled = false;
    thread_state->slowUpdate();
    arenaCleanup(*thread_state);
    internalArenaCleanup(*thread_state);
}

TEST(ThreadCacheArena, FillFlushAndStats)
{
    auto thread_state = makeThreadState();
    ThreadCache * thread_cache = threadCacheGet(*thread_state);
    ThreadCacheSlow * slow = thread_state->threadCacheSlowGet();
    const SizeClassIdx idx = 0;
    CacheBin * bin = &thread_cache->bins[idx];
    const unsigned num_cached_max = bin->numCachedMaxGet();
    CHECK_EQ(num_cached_max, 200u);
    Bin * arena_bin = bin0(arena0, idx);
    BinStats before = arena_bin->stats;

    /// First miss: nfill = 200 >> 1 = 100, nfill_min = 51. A fresh slab has more free regions than nfill_max, so the
    /// arena fills exactly nfill_min.
    std::vector<void *> ptrs;
    ptrs.push_back(threadCacheAllocSmall(*thread_state, nullptr, thread_cache, 8, idx, false, false));
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 50u);
    CHECK(slow->bin_refilled[idx]);
    CHECK_EQ(arena_bin->stats.num_fills - before.num_fills, uint64_t(1));
    CHECK_EQ(arena_bin->stats.num_allocations - before.num_allocations, uint64_t(51));
    CHECK_EQ(bin->thread_cache_stats.num_requests, uint64_t(1));
    for (int i = 0; i < 50; ++i)
        ptrs.push_back(threadCacheAllocSmall(*thread_state, nullptr, thread_cache, 8, idx, false, false));
    /// Regions come in ascending address order.
    for (size_t i = 1; i < ptrs.size(); ++i)
        CHECK_EQ(reinterpret_cast<uintptr_t>(ptrs[i]), reinterpret_cast<uintptr_t>(ptrs[i - 1]) + 8);
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 0u);
    CHECK_EQ(bin->thread_cache_stats.num_requests, uint64_t(51));

    /// The second fill merges the 51 requests into the bin stats.
    void * p = threadCacheAllocSmall(*thread_state, nullptr, thread_cache, 8, idx, true, false);
    CHECK_EQ(*static_cast<uint64_t *>(p), uint64_t(0));
    ptrs.push_back(p);
    CHECK_EQ(arena_bin->stats.num_fills - before.num_fills, uint64_t(2));
    CHECK_EQ(arena_bin->stats.num_requests - before.num_requests, uint64_t(51));
    CHECK_EQ(bin->thread_cache_stats.num_requests, uint64_t(1));
    while (bin->numCachedGetLocal() > 0)
        ptrs.push_back(threadCacheAllocSmall(*thread_state, nullptr, thread_cache, 8, idx, false, false));
    /// Allocate enough to overflow the bin on free.
    while (ptrs.size() < 260 || bin->numCachedGetLocal() > 0)
        ptrs.push_back(threadCacheAllocSmall(*thread_state, nullptr, thread_cache, 8, idx, false, false));
    uint64_t num_requests_before_flush = bin->thread_cache_stats.num_requests;
    uint64_t merged_before_flush = arena_bin->stats.num_requests;

    /// Free into the bin until it is full (200), then the next free flushes the bottom 100 (the oldest frees).
    for (size_t i = 0; i < 200; ++i)
        threadCacheDeallocateSmall(*thread_state, thread_cache, ptrs[i], idx, false);
    CHECK(bin->full());
    uint64_t num_flushes = arena_bin->stats.num_flushes;
    threadCacheDeallocateSmall(*thread_state, thread_cache, ptrs[200], idx, false);
    CHECK_EQ(arena_bin->stats.num_flushes - num_flushes, uint64_t(1));
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 101u);
    CHECK_EQ(arena_bin->stats.num_requests - merged_before_flush, num_requests_before_flush);
    CHECK_EQ(bin->thread_cache_stats.num_requests, uint64_t(0));
    /// The cached items are the most recently freed ones, newest on top.
    CHECK_EQ(bin->stack_head[0], ptrs[200]);
    CHECK_EQ(bin->stack_head[100], ptrs[100]);
    for (size_t i = 201; i < ptrs.size(); ++i)
        threadCacheDeallocateSmall(*thread_state, thread_cache, ptrs[i], idx, false);

    /// `threadCacheFlush` empties every enabled bin (and counts one flush per enabled bin, even if empty).
    uint64_t num_flushes1 = bin0(arena0, 1)->stats.num_flushes;
    threadCacheFlush(*thread_state);
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 0u);
    CHECK_EQ(bin0(arena0, 1)->stats.num_flushes - num_flushes1, uint64_t(1));
    CHECK_EQ(arena_bin->stats.current_regions, before.current_regions);

    threadCacheCleanup(*thread_state);
    arenaCleanup(*thread_state);
    internalArenaCleanup(*thread_state);
}

TEST(ThreadCacheArena, GC)
{
    auto thread_state = makeThreadState();
    ThreadCache * thread_cache = threadCacheGet(*thread_state);
    ThreadCacheSlow * slow = thread_state->threadCacheSlowGet();
    /// A size class no other test uses: all regions come from one fresh slab.
    const SizeClassIdx idx = 3;
    const size_t size = size_classes::indexToSize(idx);
    CacheBin * bin = &thread_cache->bins[idx];
    Bin * arena_bin = bin0(arena0, idx);
    REQUIRE(bin->numCachedMaxGet() == 200);
    REQUIRE(bin_infos[idx].num_regions >= 154);

    /// Two fills of nfill_min = 51 (a fresh slab has more than nfill_max = 100 free regions): 102 items, all cached
    /// after they are freed.
    std::vector<void *> ptrs;
    for (int i = 0; i < 102; ++i)
        ptrs.push_back(threadCacheAllocSmall(*thread_state, nullptr, thread_cache, size, idx, false, false));
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 0u);
    CHECK_EQ(arena_bin->stats.num_fills, uint64_t(2));
    for (void * p : ptrs)
        threadCacheDeallocateSmall(*thread_state, thread_cache, p, idx, false);
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 102u);

    /// The gate: a GC event within 10 ms of the last one does nothing.
    forbidGC(*thread_state);
    threadCacheGCEvent(*thread_state);
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 102u);

    /// The first GC: low water is 0 (the bin was emptied before the frees), the bin was refilled => fill count
    /// doubled (base stays at 1), refilled flag cleared, nothing flushed (all items are local), low water := 100.
    allowGC(*thread_state);
    CHECK(slow->bin_refilled[idx]);
    threadCacheGCEvent(*thread_state);
    CHECK(!slow->bin_refilled[idx]);
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 102u);
    CHECK_EQ(unsigned(bin->lowWaterGet()), 102u);
    CHECK_EQ(unsigned(slow->bin_fill_control_do_not_access_directly[idx].base), 1u);
    CHECK(slow->last_gc_time.ns() != 0);
    /// No other bin flushed anything: the small cursor went all the way around.
    CHECK_EQ(slow->next_gc_bin_small, 0u);
    CHECK_EQ(slow->next_gc_bin_large, SIZE_CLASS_NUM_BINS);

    /// Use 20 items: low water 82 => flush 82 - 82/4 = 62 (the bottom ones), fill count halved (base 2).
    std::vector<void *> used;
    for (int i = 0; i < 20; ++i)
        used.push_back(threadCacheAllocSmall(*thread_state, nullptr, thread_cache, size, idx, false, false));
    CHECK_EQ(unsigned(bin->lowWaterGet()), 82u);
    uint64_t num_flushes = arena_bin->stats.num_flushes;
    allowGC(*thread_state);
    threadCacheGCEvent(*thread_state);
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 20u);
    CHECK_EQ(arena_bin->stats.num_flushes - num_flushes, uint64_t(1));
    CHECK_EQ(unsigned(slow->bin_fill_control_do_not_access_directly[idx].base), 2u);
    CHECK_EQ(unsigned(bin->lowWaterGet()), 20u);
    /// One small bin flushed: the cursor still visits all bins (fewer than `THREAD_CACHE_GC_SMALL_NUM_BINS_MAX` flushed).
    CHECK_EQ(slow->next_gc_bin_small, 0u);

    /// Untouched for a period: low water 20 => flush 15, base 3.
    allowGC(*thread_state);
    threadCacheGCEvent(*thread_state);
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 5u);
    CHECK_EQ(unsigned(slow->bin_fill_control_do_not_access_directly[idx].base), 3u);

    /// Empty the bin; the refill now has nfill_max = 200 >> 3 = 25, nfill_min 13 (slabcur has more free regions than
    /// 25, so 13).
    for (int i = 0; i < 5; ++i)
        used.push_back(threadCacheAllocSmall(*thread_state, nullptr, thread_cache, size, idx, false, false));
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 0u);
    used.push_back(threadCacheAllocSmall(*thread_state, nullptr, thread_cache, size, idx, false, false));
    CHECK_EQ(unsigned(bin->numCachedGetLocal()) + 1, 13u);
    /// Burst: base 3, offset 1 => the next fill would use lg_div 2.
    CHECK_EQ(unsigned(slow->bin_fill_control_do_not_access_directly[idx].offset), 1u);
    while (bin->numCachedGetLocal() > 0)
        used.push_back(threadCacheAllocSmall(*thread_state, nullptr, thread_cache, size, idx, false, false));
    /// Refilled with low water 0 => base 2, offset reset.
    allowGC(*thread_state);
    threadCacheGCEvent(*thread_state);
    CHECK_EQ(unsigned(slow->bin_fill_control_do_not_access_directly[idx].base), 2u);
    CHECK_EQ(unsigned(slow->bin_fill_control_do_not_access_directly[idx].offset), 0u);

    for (void * p : used)
        threadCacheDeallocateSmall(*thread_state, thread_cache, p, idx, false);
    threadCacheCleanup(*thread_state);
    arenaCleanup(*thread_state);
    internalArenaCleanup(*thread_state);
}

TEST(ThreadCacheArena, GCRemotePointers)
{
    auto thread_state = makeThreadState();
    ThreadCache * thread_cache = threadCacheGet(*thread_state);
    const SizeClassIdx idx = 1;
    CacheBin * bin = &thread_cache->bins[idx];

    /// Local items: from the tcache (arena 0's current slab).
    std::vector<void *> local;
    for (int i = 0; i < 10; ++i)
        local.push_back(threadCacheAllocSmall(*thread_state, nullptr, thread_cache, 16, idx, false, false));
    /// Remote items: from a manual arena (far away in the address space).
    Arena * a1 = arenaInit(thread_state.get(), 1, &arena_config_default);
    REQUIRE(a1 != nullptr);
    std::vector<void *> remote;
    for (int i = 0; i < 4; ++i)
        remote.push_back(arenaMallocHard(thread_state.get(), a1, 16, idx, false, true));
    void * current_slab_addr = bin0(arena0, idx)->current_slab->addr();
    bool far = true;
    for (void * r : remote)
    {
        uintptr_t d = reinterpret_cast<uintptr_t>(r) > reinterpret_cast<uintptr_t>(current_slab_addr)
            ? reinterpret_cast<uintptr_t>(r) - reinterpret_cast<uintptr_t>(current_slab_addr)
            : reinterpret_cast<uintptr_t>(current_slab_addr) - reinterpret_cast<uintptr_t>(r);
        far = far && d > THREAD_CACHE_GC_NEIGHBOR_LIMIT;
    }
    if (!far)
    {
        std::fprintf(stderr, "skipped: the manual arena is within 2 MiB of arena 0\n");
        return;
    }

    /// Bin (top -> bottom): local[9..5], remote[3..2], local[4..0], remote[1..0]: interleaved.
    threadCacheDeallocateSmall(*thread_state, thread_cache, remote[0], idx, false);
    threadCacheDeallocateSmall(*thread_state, thread_cache, remote[1], idx, false);
    for (int i = 0; i < 5; ++i)
        threadCacheDeallocateSmall(*thread_state, thread_cache, local[i], idx, false);
    threadCacheDeallocateSmall(*thread_state, thread_cache, remote[2], idx, false);
    threadCacheDeallocateSmall(*thread_state, thread_cache, remote[3], idx, false);
    for (int i = 5; i < 10; ++i)
        threadCacheDeallocateSmall(*thread_state, thread_cache, local[i], idx, false);
    /// On top of the 41 left over from the fill (51 - 10).
    const unsigned num_cached = bin->numCachedGetLocal();
    CHECK_EQ(num_cached, 55u);

    /// Low water is 0 (the bin was empty at the last refill), so the intended flush is 0, but the heuristic still
    /// flushes the 4 remote pointers, keeping the local ones in their order.
    Bin * arena1_bin = bin0(a1, idx);
    uint64_t a1_num_deallocations = arena1_bin->stats.num_deallocations;
    allowGC(*thread_state);
    threadCacheGCEvent(*thread_state);
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), num_cached - 4);
    CHECK_EQ(arena1_bin->stats.num_deallocations - a1_num_deallocations, uint64_t(4));
    for (int i = 0; i < 10; ++i)
        CHECK_EQ(bin->stack_head[i], local[9 - i]);

    threadCacheCleanup(*thread_state);
    arenaCleanup(*thread_state);
    internalArenaCleanup(*thread_state);
}

TEST(ThreadCacheArena, LargeBinsAndThreadCacheMax)
{
    auto thread_state = makeThreadState();
    ThreadCacheSlow * slow = thread_state->threadCacheSlowGet();

    /// Enable all large bins; the per-bin settings carry over.
    CHECK(!threadThreadCacheMaxSet(*thread_state, THREAD_CACHE_MAX_CLASS_LIMIT));
    CHECK_EQ(slow->thread_cache_num_bins, THREAD_CACHE_NUM_BINS_MAX);
    ThreadCache * thread_cache = threadCacheGet(*thread_state);
    CHECK(slow->arena == arena0);
    CHECK_EQ(threadCacheListLength(arena0), size_t(1));
    const SizeClassIdx idx = SIZE_CLASS_NUM_BINS;
    CacheBin * bin = &thread_cache->bins[idx];
    CHECK(!threadCacheBinDisabled(idx, bin, slow));
    CHECK_EQ(unsigned(bin->numCachedMaxGet()), 20u);

    /// Large misses allocate one object at a time and do not count requests.
    size_t size = size_classes::indexToSize(idx);
    std::vector<void *> ptrs;
    for (int i = 0; i < 30; ++i)
        ptrs.push_back(threadCacheAllocLarge(*thread_state, nullptr, thread_cache, size, idx, false, false));
    CHECK_EQ(bin->thread_cache_stats.num_requests, uint64_t(0));
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 0u);
    ArenaStatsLarge & large_stats = arena0->stats.large_stats[idx - SIZE_CLASS_NUM_BINS];
    uint64_t large_flushes = large_stats.num_flushes.read();
    for (void * p : ptrs)
        threadCacheDeallocateLarge(*thread_state, thread_cache, p, idx, false);
    /// 20 cached, then the 21st free flushes 10, and 9 more fit.
    CHECK_EQ(large_stats.num_flushes.read() - large_flushes, uint64_t(1));
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 20u);

    /// Hits count requests.
    void * p = threadCacheAllocLarge(*thread_state, nullptr, thread_cache, size, idx, true, false);
    CHECK_EQ(bin->thread_cache_stats.num_requests, uint64_t(1));
    threadCacheDeallocateLarge(*thread_state, thread_cache, p, idx, false);

    /// Large GC: low water 19 (one alloc since the low water reset), ncached 19 -> rem = 19 - 19 + 19 / 4 = 4. Only
    /// one large bin per GC, after the small ones.
    thread_cache->bins[idx].lowWaterSet();
    void * kept = threadCacheAllocLarge(*thread_state, nullptr, thread_cache, size, idx, false, false);
    CHECK_EQ(unsigned(bin->lowWaterGet()), 19u);
    thread_state->threadCacheSlowGet()->last_gc_time = Nanoseconds::zero();
    threadCacheGCEvent(*thread_state);
    CHECK_EQ(unsigned(bin->numCachedGetLocal()), 4u);
    CHECK_EQ(slow->next_gc_bin_large, SIZE_CLASS_NUM_BINS + 1);
    threadCacheDeallocateLarge(*thread_state, thread_cache, kept, idx, false);

    /// Back to the default; the large bins are disabled again (but keep their `num_cached_max`).
    CHECK(!threadThreadCacheMaxSet(*thread_state, global_do_not_change_thread_cache_max_class));
    thread_cache = threadCacheGet(*thread_state);
    CHECK_EQ(slow->thread_cache_num_bins, global_do_not_change_thread_cache_num_bins);
    /// (With 4 KiB pages the default already caches every bin up to the limit.)
    CHECK_EQ(
        threadCacheBinDisabled(THREAD_CACHE_NUM_BINS_MAX - 1, &thread_cache->bins[THREAD_CACHE_NUM_BINS_MAX - 1], slow),
        global_do_not_change_thread_cache_num_bins < THREAD_CACHE_NUM_BINS_MAX);
    CHECK_EQ(unsigned(thread_cache->bins[THREAD_CACHE_NUM_BINS_MAX - 1].numCachedMaxGetUnsafe()), 20u);

    threadCacheCleanup(*thread_state);
    arenaCleanup(*thread_state);
    internalArenaCleanup(*thread_state);
}

TEST(ThreadCacheArena, EnableDisableAndNumCachedMaxWrite)
{
    auto thread_state = makeThreadState();
    ThreadCacheSlow * slow = thread_state->threadCacheSlowGet();
    const char * settings = "8-8:10|16-16:0";
    CHECK(!threadCacheBinsNumCachedMaxWrite(*thread_state, settings, strlen(settings)));
    CacheBinSize n = 0;
    CHECK(!threadCacheBinNumCachedMaxRead(*thread_state, 8, n));
    CHECK_EQ(unsigned(n), 10u);
    CHECK(!threadCacheBinNumCachedMaxRead(*thread_state, 16, n));
    CHECK_EQ(unsigned(n), 0u);
    ThreadCache * thread_cache = threadCacheGet(*thread_state);
    CHECK(threadCacheBinDisabled(1, &thread_cache->bins[1], slow));
    /// A disabled small bin goes to the arena directly.
    void * p = threadCacheAllocSmall(*thread_state, nullptr, thread_cache, 16, 1, false, false);
    CHECK(p != nullptr);
    threadCacheDeallocateSmall(*thread_state, thread_cache, p, 1, false);
    CHECK_EQ(unsigned(thread_cache->bins[1].numCachedGetInternal()), 0u);

    threadCacheEnabledSet(*thread_state, false);
    CHECK(!thread_state->thread_cache_enabled);
    CHECK(threadCacheGet(*thread_state) == nullptr);
    CHECK_EQ(thread_state->stateGet(), uint8_t(thread_state_nominal_slow));
    CHECK(!threadCacheBinNumCachedMaxRead(*thread_state, 8, n));
    CHECK_EQ(unsigned(n), 0u);
    CHECK_EQ(threadCacheListLength(arena0), size_t(0));

    /// Re-enabling uses the defaults again.
    threadCacheEnabledSet(*thread_state, true);
    CHECK(thread_state->thread_cache_enabled);
    CHECK_EQ(thread_state->stateGet(), uint8_t(thread_state_nominal));
    CHECK(!threadCacheBinNumCachedMaxRead(*thread_state, 8, n));
    CHECK_EQ(unsigned(n), unsigned(threadCacheGetDefaultNumCachedMax()[0].num_cached_max));
    CHECK_EQ(threadCacheListLength(arena0), size_t(1));

    threadCacheCleanup(*thread_state);
    arenaCleanup(*thread_state);
    internalArenaCleanup(*thread_state);
}

TEST(ThreadCacheArena, ExplicitThreadCaches)
{
    auto thread_state = makeThreadState();
    unsigned idx0 = 1000;
    unsigned idx1 = 1000;
    REQUIRE(!explicitThreadCachesCreate(*thread_state, base0Get(), idx0));
    REQUIRE(!explicitThreadCachesCreate(*thread_state, base0Get(), idx1));
    CHECK_EQ(idx0, 0u);
    CHECK_EQ(idx1, 1u);
    ThreadCache * t0 = explicitThreadCachesGet(*thread_state, idx0);
    CHECK(t0 != nullptr);
    /// The layout: [stacks][ThreadCache][ThreadCacheSlow] in one allocation.
    size_t stack_size;
    size_t alignment;
    cacheBinInfoComputeAlloc(threadCacheGetDefaultNumCachedMax(), global_do_not_change_thread_cache_num_bins, stack_size, alignment);
    CHECK_EQ(reinterpret_cast<std::byte *>(t0), static_cast<std::byte *>(t0->thread_cache_slow->dynamic_alloc) + stack_size);
    CHECK_EQ(reinterpret_cast<std::byte *>(t0->thread_cache_slow), reinterpret_cast<std::byte *>(t0) + sizeof(ThreadCache));
    /// Associated with the internal arena of the thread (plus the thread's own tcache).
    CHECK(t0->thread_cache_slow->arena == arena0);
    CHECK_EQ(threadCacheListLength(arena0), size_t(3));

    void * p = threadCacheAllocSmall(*thread_state, nullptr, t0, 32, 2, false, false);
    threadCacheDeallocateSmall(*thread_state, t0, p, 2, false);

    /// Flush: the slot needs re-initialization, and the next get creates a fresh tcache.
    explicitThreadCachesFlush(*thread_state, idx0);
    CHECK(explicit_thread_caches[idx0].thread_cache == EXPLICIT_THREAD_CACHES_ELEMENT_NEED_REINIT);
    CHECK_EQ(threadCacheListLength(arena0), size_t(2));
    ThreadCache * t0b = explicitThreadCachesGet(*thread_state, idx0);
    CHECK(t0b != nullptr && t0b != EXPLICIT_THREAD_CACHES_ELEMENT_NEED_REINIT);
    CHECK_EQ(unsigned(t0b->bins[2].numCachedGetLocal()), 0u);

    /// Destroy: the slot is reused LIFO.
    explicitThreadCachesDestroy(*thread_state, idx1);
    explicitThreadCachesDestroy(*thread_state, idx0);
    unsigned idx2 = 1000;
    REQUIRE(!explicitThreadCachesCreate(*thread_state, base0Get(), idx2));
    CHECK_EQ(idx2, idx0);
    unsigned idx3 = 1000;
    REQUIRE(!explicitThreadCachesCreate(*thread_state, base0Get(), idx3));
    CHECK_EQ(idx3, idx1);
    explicitThreadCachesDestroy(*thread_state, idx2);
    explicitThreadCachesDestroy(*thread_state, idx3);
    CHECK_EQ(threadCacheListLength(arena0), size_t(1));

    threadCacheCleanup(*thread_state);
    arenaCleanup(*thread_state);
    internalArenaCleanup(*thread_state);
}
