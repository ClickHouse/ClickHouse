/// The thread cache on top of real arenas (arena 0 and a manual arena, with `b0`, the global extent map and the page
/// allocator): tcache creation and the arena association, the fill counts (`nfill_min` / `nfill_max`), flushes on a
/// full bin, the time-gated GC (flush counts, fill count adaptation, the locality heuristic with remote pointers),
/// `tcacheFlush`, disabling / re-enabling, `thread.tcache.max`, `ncached_max` writes, explicit tcaches, and the stats
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

Arena * a0arena = nullptr;

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
    REQUIRE(!arena_emap_global.init(b0get(), true));
    REQUIRE(!arenaBoot(&default_sc_data, b0get(), false));
    REQUIRE(!tcacheBoot(nullptr, b0get()));
    narenas_auto = 1;
    manual_arena_base = 1;
    a0arena = arenaInit(nullptr, 0, &arena_config_default);
    REQUIRE(a0arena != nullptr);
    a0 = a0arena;
    /// As `malloc_init_hard`: the background thread module is booted (disabled), `arenaInit` checks its thread slots.
    REQUIRE(!backgroundThreadBoot0());
    REQUIRE(!backgroundThreadBoot1(nullptr, b0get()));
}

std::unique_ptr<ThreadState> makeTsd()
{
    bootOnce();
    auto tsd = std::make_unique<ThreadState>();
    tsd->state.store(tsd_state_nominal, std::memory_order_relaxed);
    tsd->rtree_ctx.init();
    tsd->prng_state = 42;
    REQUIRE(!tcacheTsdDataInit(*tsd));
    return tsd;
}

Bin * bin0(Arena * arena, szind_t ind)
{
    return arenaGetBin(arena, ind, 0);
}

size_t tcacheListLength(Arena * arena)
{
    size_t n = 0;
    arena->tcache_ql.forEach([&](ThreadCacheSlow *) { ++n; });
    return n;
}

/// Makes the next GC event run (the 10 ms gate counts from `last_gc_time`).
void allowGc(ThreadState & tsd)
{
    tsd.tcacheSlowGet()->last_gc_time = NsTime::zero();
}

/// Makes the next GC event a no-op (the clock never goes below the current value).
void forbidGc(ThreadState & tsd)
{
    tsd.tcacheSlowGet()->last_gc_time = NsTime::fromNs(UINT64_MAX / 2);
}

}

TEST(ThreadCacheArena, CreateAndDestroy)
{
    auto tsd = makeTsd();
    ThreadCache * tcache = tcacheGet(*tsd);
    REQUIRE(tcache != nullptr);
    ThreadCacheSlow * slow = tsd->tcacheSlowGet();
    CHECK(tcache->tcache_slow == slow);
    CHECK(slow->tcache == tcache);
    CHECK(slow->arena == a0arena);
    CHECK(tsd->arena == a0arena);
    CHECK_EQ(slow->tcache_nbins, global_do_not_change_tcache_nbins);
    CHECK_EQ(slow->next_gc_bin_large, SC_NBINS);
    CHECK_EQ(tcacheListLength(a0arena), size_t(1));

    /// The stack is one internal, page-aligned allocation from arena 0.
    size_t size;
    size_t alignment;
    cacheBinInfoComputeAlloc(tcacheGetDefaultNcachedMax(), slow->tcache_nbins, size, alignment);
    CHECK_EQ(reinterpret_cast<uintptr_t>(slow->dyn_alloc) % PAGE, uintptr_t(0));
    CHECK_EQ(arenaInternalGet(a0arena), sz::sa2u(size, PAGE));
    CHECK_EQ(*static_cast<uintptr_t *>(slow->dyn_alloc), cache_bin_preceding_junk);

    for (szind_t i = 0; i < TCACHE_NBINS_MAX; ++i)
    {
        bool expect_enabled = i < slow->tcache_nbins && tcacheGetDefaultNcachedMax()[i].ncached_max > 0;
        CHECK_EQ(!tcacheBinDisabled(i, &tcache->bins[i], slow), expect_enabled);
        CHECK_EQ(tcache->bins[i].ncachedMaxGetUnsafe(), tcacheGetDefaultNcachedMax()[i].ncached_max);
        cache_bin_sz_t n = 0;
        CHECK(!tcacheBinNcachedMaxRead(*tsd, sz::indexToSize(i), n));
        CHECK_EQ(unsigned(n), expect_enabled ? unsigned(tcacheGetDefaultNcachedMax()[i].ncached_max) : 0u);
    }
    cache_bin_sz_t n = 0;
    CHECK(tcacheBinNcachedMaxRead(*tsd, TCACHE_MAXCLASS_LIMIT + 1, n));

    tcacheCleanup(*tsd);
    /// Cleanup keeps `tcache_enabled` (the TSD cleanup resets the state); the bins are zeroed.
    CHECK(tcache->bins[0].stillZeroInitialized());
    CHECK_EQ(tcacheListLength(a0arena), size_t(0));
    CHECK_EQ(arenaInternalGet(a0arena), size_t(0));
    tsd->tcache_enabled = false;
    tsd->slowUpdate();
    arenaCleanup(*tsd);
    iarenaCleanup(*tsd);
}

TEST(ThreadCacheArena, FillFlushAndStats)
{
    auto tsd = makeTsd();
    ThreadCache * tcache = tcacheGet(*tsd);
    ThreadCacheSlow * slow = tsd->tcacheSlowGet();
    const szind_t ind = 0;
    CacheBin * bin = &tcache->bins[ind];
    const unsigned ncached_max = bin->ncachedMaxGet();
    CHECK_EQ(ncached_max, 200u);
    Bin * abin = bin0(a0arena, ind);
    BinStats before = abin->stats;

    /// First miss: nfill = 200 >> 1 = 100, nfill_min = 51. A fresh slab has more free regions than nfill_max, so the
    /// arena fills exactly nfill_min.
    std::vector<void *> ptrs;
    ptrs.push_back(tcacheAllocSmall(*tsd, nullptr, tcache, 8, ind, false, false));
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 50u);
    CHECK(slow->bin_refilled[ind]);
    CHECK_EQ(abin->stats.nfills - before.nfills, uint64_t(1));
    CHECK_EQ(abin->stats.nmalloc - before.nmalloc, uint64_t(51));
    CHECK_EQ(bin->tstats.nrequests, uint64_t(1));
    for (int i = 0; i < 50; ++i)
        ptrs.push_back(tcacheAllocSmall(*tsd, nullptr, tcache, 8, ind, false, false));
    /// Regions come in ascending address order.
    for (size_t i = 1; i < ptrs.size(); ++i)
        CHECK_EQ(reinterpret_cast<uintptr_t>(ptrs[i]), reinterpret_cast<uintptr_t>(ptrs[i - 1]) + 8);
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 0u);
    CHECK_EQ(bin->tstats.nrequests, uint64_t(51));

    /// The second fill merges the 51 requests into the bin stats.
    void * p = tcacheAllocSmall(*tsd, nullptr, tcache, 8, ind, true, false);
    CHECK_EQ(*static_cast<uint64_t *>(p), uint64_t(0));
    ptrs.push_back(p);
    CHECK_EQ(abin->stats.nfills - before.nfills, uint64_t(2));
    CHECK_EQ(abin->stats.nrequests - before.nrequests, uint64_t(51));
    CHECK_EQ(bin->tstats.nrequests, uint64_t(1));
    while (bin->ncachedGetLocal() > 0)
        ptrs.push_back(tcacheAllocSmall(*tsd, nullptr, tcache, 8, ind, false, false));
    /// Allocate enough to overflow the bin on free.
    while (ptrs.size() < 260 || bin->ncachedGetLocal() > 0)
        ptrs.push_back(tcacheAllocSmall(*tsd, nullptr, tcache, 8, ind, false, false));
    uint64_t nrequests_before_flush = bin->tstats.nrequests;
    uint64_t merged_before_flush = abin->stats.nrequests;

    /// Free into the bin until it is full (200), then the next free flushes the bottom 100 (the oldest frees).
    for (size_t i = 0; i < 200; ++i)
        tcacheDallocSmall(*tsd, tcache, ptrs[i], ind, false);
    CHECK(bin->full());
    uint64_t nflushes = abin->stats.nflushes;
    tcacheDallocSmall(*tsd, tcache, ptrs[200], ind, false);
    CHECK_EQ(abin->stats.nflushes - nflushes, uint64_t(1));
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 101u);
    CHECK_EQ(abin->stats.nrequests - merged_before_flush, nrequests_before_flush);
    CHECK_EQ(bin->tstats.nrequests, uint64_t(0));
    /// The cached items are the most recently freed ones, newest on top.
    CHECK_EQ(bin->stack_head[0], ptrs[200]);
    CHECK_EQ(bin->stack_head[100], ptrs[100]);
    for (size_t i = 201; i < ptrs.size(); ++i)
        tcacheDallocSmall(*tsd, tcache, ptrs[i], ind, false);

    /// `tcacheFlush` empties every enabled bin (and counts one flush per enabled bin, even if empty).
    uint64_t nflushes1 = bin0(a0arena, 1)->stats.nflushes;
    tcacheFlush(*tsd);
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 0u);
    CHECK_EQ(bin0(a0arena, 1)->stats.nflushes - nflushes1, uint64_t(1));
    CHECK_EQ(abin->stats.curregs, before.curregs);

    tcacheCleanup(*tsd);
    arenaCleanup(*tsd);
    iarenaCleanup(*tsd);
}

TEST(ThreadCacheArena, Gc)
{
    auto tsd = makeTsd();
    ThreadCache * tcache = tcacheGet(*tsd);
    ThreadCacheSlow * slow = tsd->tcacheSlowGet();
    /// A size class no other test uses: all regions come from one fresh slab.
    const szind_t ind = 3;
    const size_t size = sz::indexToSize(ind);
    CacheBin * bin = &tcache->bins[ind];
    Bin * abin = bin0(a0arena, ind);
    REQUIRE(bin->ncachedMaxGet() == 200);
    REQUIRE(bin_infos[ind].nregs >= 154);

    /// Two fills of nfill_min = 51 (a fresh slab has more than nfill_max = 100 free regions): 102 items, all cached
    /// after they are freed.
    std::vector<void *> ptrs;
    for (int i = 0; i < 102; ++i)
        ptrs.push_back(tcacheAllocSmall(*tsd, nullptr, tcache, size, ind, false, false));
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 0u);
    CHECK_EQ(abin->stats.nfills, uint64_t(2));
    for (void * p : ptrs)
        tcacheDallocSmall(*tsd, tcache, p, ind, false);
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 102u);

    /// The gate: a GC event within 10 ms of the last one does nothing.
    forbidGc(*tsd);
    tcacheGcEvent(*tsd);
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 102u);

    /// The first GC: low water is 0 (the bin was emptied before the frees), the bin was refilled => fill count
    /// doubled (base stays at 1), refilled flag cleared, nothing flushed (all items are local), low water := 100.
    allowGc(*tsd);
    CHECK(slow->bin_refilled[ind]);
    tcacheGcEvent(*tsd);
    CHECK(!slow->bin_refilled[ind]);
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 102u);
    CHECK_EQ(unsigned(bin->lowWaterGet()), 102u);
    CHECK_EQ(unsigned(slow->bin_fill_ctl_do_not_access_directly[ind].base), 1u);
    CHECK(slow->last_gc_time.ns() != 0);
    /// No other bin flushed anything: the small cursor went all the way around.
    CHECK_EQ(slow->next_gc_bin_small, 0u);
    CHECK_EQ(slow->next_gc_bin_large, SC_NBINS);

    /// Use 20 items: low water 82 => flush 82 - 82/4 = 62 (the bottom ones), fill count halved (base 2).
    std::vector<void *> used;
    for (int i = 0; i < 20; ++i)
        used.push_back(tcacheAllocSmall(*tsd, nullptr, tcache, size, ind, false, false));
    CHECK_EQ(unsigned(bin->lowWaterGet()), 82u);
    uint64_t nflushes = abin->stats.nflushes;
    allowGc(*tsd);
    tcacheGcEvent(*tsd);
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 20u);
    CHECK_EQ(abin->stats.nflushes - nflushes, uint64_t(1));
    CHECK_EQ(unsigned(slow->bin_fill_ctl_do_not_access_directly[ind].base), 2u);
    CHECK_EQ(unsigned(bin->lowWaterGet()), 20u);
    /// One small bin flushed: the cursor still visits all bins (fewer than `TCACHE_GC_SMALL_NBINS_MAX` flushed).
    CHECK_EQ(slow->next_gc_bin_small, 0u);

    /// Untouched for a period: low water 20 => flush 15, base 3.
    allowGc(*tsd);
    tcacheGcEvent(*tsd);
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 5u);
    CHECK_EQ(unsigned(slow->bin_fill_ctl_do_not_access_directly[ind].base), 3u);

    /// Empty the bin; the refill now has nfill_max = 200 >> 3 = 25, nfill_min 13 (slabcur has more free regions than
    /// 25, so 13).
    for (int i = 0; i < 5; ++i)
        used.push_back(tcacheAllocSmall(*tsd, nullptr, tcache, size, ind, false, false));
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 0u);
    used.push_back(tcacheAllocSmall(*tsd, nullptr, tcache, size, ind, false, false));
    CHECK_EQ(unsigned(bin->ncachedGetLocal()) + 1, 13u);
    /// Burst: base 3, offset 1 => the next fill would use lg_div 2.
    CHECK_EQ(unsigned(slow->bin_fill_ctl_do_not_access_directly[ind].offset), 1u);
    while (bin->ncachedGetLocal() > 0)
        used.push_back(tcacheAllocSmall(*tsd, nullptr, tcache, size, ind, false, false));
    /// Refilled with low water 0 => base 2, offset reset.
    allowGc(*tsd);
    tcacheGcEvent(*tsd);
    CHECK_EQ(unsigned(slow->bin_fill_ctl_do_not_access_directly[ind].base), 2u);
    CHECK_EQ(unsigned(slow->bin_fill_ctl_do_not_access_directly[ind].offset), 0u);

    for (void * p : used)
        tcacheDallocSmall(*tsd, tcache, p, ind, false);
    tcacheCleanup(*tsd);
    arenaCleanup(*tsd);
    iarenaCleanup(*tsd);
}

TEST(ThreadCacheArena, GcRemotePointers)
{
    auto tsd = makeTsd();
    ThreadCache * tcache = tcacheGet(*tsd);
    const szind_t ind = 1;
    CacheBin * bin = &tcache->bins[ind];

    /// Local items: from the tcache (arena 0's current slab).
    std::vector<void *> local;
    for (int i = 0; i < 10; ++i)
        local.push_back(tcacheAllocSmall(*tsd, nullptr, tcache, 16, ind, false, false));
    /// Remote items: from a manual arena (far away in the address space).
    Arena * a1 = arenaInit(tsd.get(), 1, &arena_config_default);
    REQUIRE(a1 != nullptr);
    std::vector<void *> remote;
    for (int i = 0; i < 4; ++i)
        remote.push_back(arenaMallocHard(tsd.get(), a1, 16, ind, false, true));
    void * slabcur_addr = bin0(a0arena, ind)->slabcur->addr();
    bool far = true;
    for (void * r : remote)
    {
        uintptr_t d = reinterpret_cast<uintptr_t>(r) > reinterpret_cast<uintptr_t>(slabcur_addr)
            ? reinterpret_cast<uintptr_t>(r) - reinterpret_cast<uintptr_t>(slabcur_addr)
            : reinterpret_cast<uintptr_t>(slabcur_addr) - reinterpret_cast<uintptr_t>(r);
        far = far && d > TCACHE_GC_NEIGHBOR_LIMIT;
    }
    if (!far)
    {
        std::fprintf(stderr, "skipped: the manual arena is within 2 MiB of arena 0\n");
        return;
    }

    /// Bin (top -> bottom): local[9..5], remote[3..2], local[4..0], remote[1..0]: interleaved.
    tcacheDallocSmall(*tsd, tcache, remote[0], ind, false);
    tcacheDallocSmall(*tsd, tcache, remote[1], ind, false);
    for (int i = 0; i < 5; ++i)
        tcacheDallocSmall(*tsd, tcache, local[i], ind, false);
    tcacheDallocSmall(*tsd, tcache, remote[2], ind, false);
    tcacheDallocSmall(*tsd, tcache, remote[3], ind, false);
    for (int i = 5; i < 10; ++i)
        tcacheDallocSmall(*tsd, tcache, local[i], ind, false);
    /// On top of the 41 left over from the fill (51 - 10).
    const unsigned ncached = bin->ncachedGetLocal();
    CHECK_EQ(ncached, 55u);

    /// Low water is 0 (the bin was empty at the last refill), so the intended flush is 0, but the heuristic still
    /// flushes the 4 remote pointers, keeping the local ones in their order.
    Bin * a1bin = bin0(a1, ind);
    uint64_t a1_ndalloc = a1bin->stats.ndalloc;
    allowGc(*tsd);
    tcacheGcEvent(*tsd);
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), ncached - 4);
    CHECK_EQ(a1bin->stats.ndalloc - a1_ndalloc, uint64_t(4));
    for (int i = 0; i < 10; ++i)
        CHECK_EQ(bin->stack_head[i], local[9 - i]);

    tcacheCleanup(*tsd);
    arenaCleanup(*tsd);
    iarenaCleanup(*tsd);
}

TEST(ThreadCacheArena, LargeBinsAndTcacheMax)
{
    auto tsd = makeTsd();
    ThreadCacheSlow * slow = tsd->tcacheSlowGet();

    /// Enable all large bins; the per-bin settings carry over.
    CHECK(!threadTcacheMaxSet(*tsd, TCACHE_MAXCLASS_LIMIT));
    CHECK_EQ(slow->tcache_nbins, TCACHE_NBINS_MAX);
    ThreadCache * tcache = tcacheGet(*tsd);
    CHECK(slow->arena == a0arena);
    CHECK_EQ(tcacheListLength(a0arena), size_t(1));
    const szind_t ind = SC_NBINS;
    CacheBin * bin = &tcache->bins[ind];
    CHECK(!tcacheBinDisabled(ind, bin, slow));
    CHECK_EQ(unsigned(bin->ncachedMaxGet()), 20u);

    /// Large misses allocate one object at a time and do not count requests.
    size_t size = sz::indexToSize(ind);
    std::vector<void *> ptrs;
    for (int i = 0; i < 30; ++i)
        ptrs.push_back(tcacheAllocLarge(*tsd, nullptr, tcache, size, ind, false, false));
    CHECK_EQ(bin->tstats.nrequests, uint64_t(0));
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 0u);
    ArenaStatsLarge & lstats = a0arena->stats.lstats[ind - SC_NBINS];
    uint64_t lflushes = lstats.nflushes.read();
    for (void * p : ptrs)
        tcacheDallocLarge(*tsd, tcache, p, ind, false);
    /// 20 cached, then the 21st free flushes 10, and 9 more fit.
    CHECK_EQ(lstats.nflushes.read() - lflushes, uint64_t(1));
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 20u);

    /// Hits count requests.
    void * p = tcacheAllocLarge(*tsd, nullptr, tcache, size, ind, true, false);
    CHECK_EQ(bin->tstats.nrequests, uint64_t(1));
    tcacheDallocLarge(*tsd, tcache, p, ind, false);

    /// Large GC: low water 19 (one alloc since the low water reset), ncached 19 -> rem = 19 - 19 + 19 / 4 = 4. Only
    /// one large bin per GC, after the small ones.
    tcache->bins[ind].lowWaterSet();
    void * kept = tcacheAllocLarge(*tsd, nullptr, tcache, size, ind, false, false);
    CHECK_EQ(unsigned(bin->lowWaterGet()), 19u);
    tsd->tcacheSlowGet()->last_gc_time = NsTime::zero();
    tcacheGcEvent(*tsd);
    CHECK_EQ(unsigned(bin->ncachedGetLocal()), 4u);
    CHECK_EQ(slow->next_gc_bin_large, SC_NBINS + 1);
    tcacheDallocLarge(*tsd, tcache, kept, ind, false);

    /// Back to the default; the large bins are disabled again (but keep their `ncached_max`).
    CHECK(!threadTcacheMaxSet(*tsd, global_do_not_change_tcache_maxclass));
    tcache = tcacheGet(*tsd);
    CHECK_EQ(slow->tcache_nbins, global_do_not_change_tcache_nbins);
    /// (With 4 KiB pages the default already caches every bin up to the limit.)
    CHECK_EQ(tcacheBinDisabled(TCACHE_NBINS_MAX - 1, &tcache->bins[TCACHE_NBINS_MAX - 1], slow), global_do_not_change_tcache_nbins < TCACHE_NBINS_MAX);
    CHECK_EQ(unsigned(tcache->bins[TCACHE_NBINS_MAX - 1].ncachedMaxGetUnsafe()), 20u);

    tcacheCleanup(*tsd);
    arenaCleanup(*tsd);
    iarenaCleanup(*tsd);
}

TEST(ThreadCacheArena, EnableDisableAndNcachedMaxWrite)
{
    auto tsd = makeTsd();
    ThreadCacheSlow * slow = tsd->tcacheSlowGet();
    const char * settings = "8-8:10|16-16:0";
    CHECK(!tcacheBinsNcachedMaxWrite(*tsd, settings, strlen(settings)));
    cache_bin_sz_t n = 0;
    CHECK(!tcacheBinNcachedMaxRead(*tsd, 8, n));
    CHECK_EQ(unsigned(n), 10u);
    CHECK(!tcacheBinNcachedMaxRead(*tsd, 16, n));
    CHECK_EQ(unsigned(n), 0u);
    ThreadCache * tcache = tcacheGet(*tsd);
    CHECK(tcacheBinDisabled(1, &tcache->bins[1], slow));
    /// A disabled small bin goes to the arena directly.
    void * p = tcacheAllocSmall(*tsd, nullptr, tcache, 16, 1, false, false);
    CHECK(p != nullptr);
    tcacheDallocSmall(*tsd, tcache, p, 1, false);
    CHECK_EQ(unsigned(tcache->bins[1].ncachedGetInternal()), 0u);

    tcacheEnabledSet(*tsd, false);
    CHECK(!tsd->tcache_enabled);
    CHECK(tcacheGet(*tsd) == nullptr);
    CHECK_EQ(tsd->stateGet(), uint8_t(tsd_state_nominal_slow));
    CHECK(!tcacheBinNcachedMaxRead(*tsd, 8, n));
    CHECK_EQ(unsigned(n), 0u);
    CHECK_EQ(tcacheListLength(a0arena), size_t(0));

    /// Re-enabling uses the defaults again.
    tcacheEnabledSet(*tsd, true);
    CHECK(tsd->tcache_enabled);
    CHECK_EQ(tsd->stateGet(), uint8_t(tsd_state_nominal));
    CHECK(!tcacheBinNcachedMaxRead(*tsd, 8, n));
    CHECK_EQ(unsigned(n), unsigned(tcacheGetDefaultNcachedMax()[0].ncached_max));
    CHECK_EQ(tcacheListLength(a0arena), size_t(1));

    tcacheCleanup(*tsd);
    arenaCleanup(*tsd);
    iarenaCleanup(*tsd);
}

TEST(ThreadCacheArena, ExplicitTcaches)
{
    auto tsd = makeTsd();
    unsigned ind0 = 1000;
    unsigned ind1 = 1000;
    REQUIRE(!tcachesCreate(*tsd, b0get(), ind0));
    REQUIRE(!tcachesCreate(*tsd, b0get(), ind1));
    CHECK_EQ(ind0, 0u);
    CHECK_EQ(ind1, 1u);
    ThreadCache * t0 = tcachesGet(*tsd, ind0);
    CHECK(t0 != nullptr);
    /// The layout: [stacks][ThreadCache][ThreadCacheSlow] in one allocation.
    size_t stack_size;
    size_t alignment;
    cacheBinInfoComputeAlloc(tcacheGetDefaultNcachedMax(), global_do_not_change_tcache_nbins, stack_size, alignment);
    CHECK_EQ(reinterpret_cast<std::byte *>(t0), static_cast<std::byte *>(t0->tcache_slow->dyn_alloc) + stack_size);
    CHECK_EQ(reinterpret_cast<std::byte *>(t0->tcache_slow), reinterpret_cast<std::byte *>(t0) + sizeof(ThreadCache));
    /// Associated with the internal arena of the thread (plus the thread's own tcache).
    CHECK(t0->tcache_slow->arena == a0arena);
    CHECK_EQ(tcacheListLength(a0arena), size_t(3));

    void * p = tcacheAllocSmall(*tsd, nullptr, t0, 32, 2, false, false);
    tcacheDallocSmall(*tsd, t0, p, 2, false);

    /// Flush: the slot needs re-initialization, and the next get creates a fresh tcache.
    tcachesFlush(*tsd, ind0);
    CHECK(tcaches[ind0].tcache == TCACHES_ELM_NEED_REINIT);
    CHECK_EQ(tcacheListLength(a0arena), size_t(2));
    ThreadCache * t0b = tcachesGet(*tsd, ind0);
    CHECK(t0b != nullptr && t0b != TCACHES_ELM_NEED_REINIT);
    CHECK_EQ(unsigned(t0b->bins[2].ncachedGetLocal()), 0u);

    /// Destroy: the slot is reused LIFO.
    tcachesDestroy(*tsd, ind1);
    tcachesDestroy(*tsd, ind0);
    unsigned ind2 = 1000;
    REQUIRE(!tcachesCreate(*tsd, b0get(), ind2));
    CHECK_EQ(ind2, ind0);
    unsigned ind3 = 1000;
    REQUIRE(!tcachesCreate(*tsd, b0get(), ind3));
    CHECK_EQ(ind3, ind1);
    tcachesDestroy(*tsd, ind2);
    tcachesDestroy(*tsd, ind3);
    CHECK_EQ(tcacheListLength(a0arena), size_t(1));

    tcacheCleanup(*tsd);
    arenaCleanup(*tsd);
    iarenaCleanup(*tsd);
}
