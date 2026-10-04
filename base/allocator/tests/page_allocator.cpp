/// Deterministic tests of `PaShard` / `PageAllocator` / `ExtentOps` (`pa.c`, `pac.c`, `extent.c`) on a private shard
/// with its own base and extent map: the batched retained allocation and its `pac_mapped` accounting, eager
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
    ExtentMap * emap = nullptr;
    PaShardStats * stats = nullptr;
    PaShard * shard = nullptr;

    Shard(unsigned ind, ssize_t dirty_ms, ssize_t muzzy_ms, size_t oversize_threshold)
    {
        bootOnce();
        base = Base::create(nullptr, ind, &ehooks_default_extent_hooks, true);
        REQUIRE(base != nullptr);
        void * emap_memory = std::aligned_alloc(64, (sizeof(ExtentMap) + 63) / 64 * 64);
        std::memset(emap_memory, 0, sizeof(ExtentMap));
        emap = new (emap_memory) ExtentMap();
        REQUIRE(!emap->init(base, true));
        stats = new PaShardStats();
        void * shard_memory = std::aligned_alloc(64, (sizeof(PaShard) + 63) / 64 * 64);
        std::memset(shard_memory, 0, sizeof(PaShard));
        shard = new (shard_memory) PaShard();
        NsTime now;
        now.initUpdate();
        REQUIRE(!shard->init(nullptr, emap, base, ind, stats, nullptr, now, oversize_threshold, dirty_ms, muzzy_ms));
    }

    PageAllocator & pac() { return shard->pac; }

    Extent * alloc(size_t size, bool slab = false, bool guarded = false, bool zero = false)
    {
        bool deferred = false;
        Extent * e = shard->alloc(nullptr, size, PAGE, slab, slab ? 0 : SC_NBINS, zero, guarded, &deferred);
        CHECK(!deferred);
        return e;
    }

    void dalloc(Extent * e)
    {
        bool deferred = false;
        shard->dalloc(nullptr, e, &deferred);
        CHECK(deferred);
    }

    void decayAll(bool dirty, bool fully)
    {
        Decay & d = dirty ? pac().decay_dirty : pac().decay_muzzy;
        DecayStats & s = dirty ? stats->pac_stats.decay_dirty : stats->pac_stats.decay_muzzy;
        ExtentCache & c = dirty ? pac().ecache_dirty : pac().ecache_muzzy;
        d.mtx.lock(nullptr);
        pac().decayAll(nullptr, &d, &s, &c, fully);
        d.mtx.unlock(nullptr);
    }

    size_t retainedPages() const { return shard->pac.ecache_retained.npagesGet(); }

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
    CHECK_EQ(pacAllocRetainedBatchedSize(5 * PAGE), 5 * PAGE);
    CHECK_EQ(pacAllocRetainedBatchedSize(9 * PAGE), 10 * PAGE);
    CHECK_EQ(pacAllocRetainedBatchedSize(17 * PAGE), 20 * PAGE);
    CHECK_EQ(pacAllocRetainedBatchedSize(33 * PAGE), 40 * PAGE);
    CHECK_EQ(pacAllocRetainedBatchedSize(HUGEPAGE + PAGE), minOf(sz::s2uComputeUsingDelta(HUGEPAGE + PAGE), 2 * HUGEPAGE));
    CHECK_EQ(pacAllocRetainedBatchedSize(SC_LARGE_MAXCLASS + PAGE), SC_LARGE_MAXCLASS + PAGE);
}

TEST(PageAllocator, Lifecycle)
{
    Shard s(1, 5000, 0, size_t(64) << 20);
    background_thread_enabled_state.store(false);

    /// 9 pages: a 10-page chunk is taken from the (newly grown) retained cache, the extra page goes to the dirty cache.
    Extent * e = s.alloc(9 * PAGE);
    REQUIRE(e != nullptr);
    CHECK_EQ(e->size(), 9 * PAGE);
    CHECK_EQ(e->sn(), uint64_t(0));
    CHECK(e->isHead());
    CHECK_EQ(e->state(), extent_state_active);
    CHECK_EQ(e->szind(), SC_NBINS);
    CHECK_EQ(e->arenaInd(), 1u);
    CHECK_EQ(s.shard->nactiveGet(), size_t(9));
    CHECK_EQ(s.shard->ndirtyGet(), size_t(1));
    CHECK_EQ(s.pac().mapped(), 10 * PAGE);
    CHECK_EQ(s.retainedPages(), (FIRST_GROW - 10 * PAGE) / PAGE);
    CHECK_EQ(s.pac().extent_sn_next.load(), size_t(1));
    /// The first growth is 2 MiB; the next one is the next page size class.
    CHECK_EQ(s.pac().exp_grow.next, sz::psz2ind(FIRST_GROW) + 1);

    /// The extent is mapped (boundary pages) with its szind.
    FullAllocContext ctx{};
    CHECK(!s.emap->fullAllocCtxTryLookup(nullptr, e->addr(), &ctx));
    CHECK(ctx.edata == e && ctx.szind == SC_NBINS && !ctx.slab);
    std::memset(e->addr(), 0x5a, e->size());

    /// Deallocating a large extent coalesces it eagerly with the dirty trail.
    s.dalloc(e);
    CHECK_EQ(s.shard->nactiveGet(), size_t(0));
    CHECK_EQ(s.shard->ndirtyGet(), size_t(10));
    CHECK(s.pac().ecache_dirty.eset.lruFirst() == e);
    CHECK_EQ(e->size(), 10 * PAGE);
    CHECK_EQ(e->state(), extent_state_dirty);

    /// Reallocation reuses the dirty extent (first fit), splitting it again.
    Extent * e2 = s.alloc(9 * PAGE);
    CHECK(e2 == e);
    CHECK_EQ(s.pac().mapped(), 10 * PAGE);
    s.dalloc(e2);

    /// Purging everything moves the pages to the retained cache, which coalesces back into the whole mapping.
    s.decayAll(true, false);
    CHECK_EQ(s.shard->ndirtyGet(), size_t(0));
    CHECK_EQ(s.retainedPages(), FIRST_GROW / PAGE);
    CHECK_EQ(s.pac().ecache_retained.nextentsGet(sz::psz2ind(sz::pszQuantizeFloor(FIRST_GROW))), size_t(1));
    CHECK_EQ(s.pac().mapped(), size_t(0));
    CHECK_EQ(s.stats->pac_stats.decay_dirty.npurge.read(), uint64_t(1));
    CHECK_EQ(s.stats->pac_stats.decay_dirty.nmadvise.read(), uint64_t(1));
    CHECK_EQ(s.stats->pac_stats.decay_dirty.purged.read(), uint64_t(10));

    /// Stats merge.
    PaShardStats merged;
    static PacExtentStats estats[SC_NPSIZES];
    size_t resident = 0;
    s.shard->statsMerge(nullptr, &merged, estats, &resident);
    CHECK_EQ(merged.pac_stats.retained, FIRST_GROW);
    CHECK_EQ(merged.pac_stats.decay_dirty.purged.read(), uint64_t(10));
    CHECK_EQ(resident, size_t(0));
    CHECK_EQ(estats[sz::psz2ind(sz::pszQuantizeFloor(FIRST_GROW))].nretained, size_t(1));
    CHECK_EQ(estats[sz::psz2ind(sz::pszQuantizeFloor(FIRST_GROW))].retained_bytes, FIRST_GROW);
    size_t nactive = 0;
    size_t ndirty = 0;
    size_t nmuzzy = 0;
    s.shard->basicStatsMerge(&nactive, &ndirty, &nmuzzy);
    CHECK(nactive == 0 && ndirty == 0 && nmuzzy == 0);

    MutexProfData mutex_data[mutex_prof_num_arena_mutexes];
    s.shard->mtxStatsRead(nullptr, mutex_data);
    CHECK_GT(mutex_data[arena_prof_mutex_extents_dirty].n_lock_ops, uint64_t(0));
    CHECK_EQ(mutex_data[arena_prof_mutex_hpa_shard].n_lock_ops, uint64_t(0));

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
    CHECK_EQ(s.shard->ndirtyGet(), size_t(0));
    CHECK_EQ(s.pac().mapped(), 10 * PAGE);

    /// Expanding takes the forward neighbor from the retained cache.
    bool deferred = false;
    CHECK(!s.shard->expand(nullptr, e, 10 * PAGE, 12 * PAGE, SC_NBINS + 1, true, &deferred));
    CHECK(!deferred);
    CHECK_EQ(e->size(), 12 * PAGE);
    CHECK_EQ(e->szind(), SC_NBINS + 1);
    CHECK_EQ(s.shard->nactiveGet(), size_t(12));
    CHECK_EQ(s.pac().mapped(), 12 * PAGE);
    /// The zeroed expansion.
    const unsigned char * p = static_cast<const unsigned char *>(e->addr());
    CHECK(p[10 * PAGE] == 0 && p[12 * PAGE - 1] == 0);

    /// Shrinking returns the trail to the dirty cache.
    CHECK(!s.shard->shrink(nullptr, e, 12 * PAGE, 4 * PAGE, SC_NBINS, &deferred));
    CHECK(deferred);
    CHECK_EQ(e->size(), 4 * PAGE);
    CHECK_EQ(s.shard->nactiveGet(), size_t(4));
    CHECK_EQ(s.shard->ndirtyGet(), size_t(8));

    /// Expanding again takes the dirty neighbor (no new mapping).
    deferred = false;
    CHECK(!s.shard->expand(nullptr, e, 4 * PAGE, 6 * PAGE, SC_NBINS, false, &deferred));
    CHECK_EQ(s.shard->ndirtyGet(), size_t(6));
    CHECK_EQ(s.pac().mapped(), 12 * PAGE);

    /// An active neighbor blocks the expansion (and no new mapping is made for in-place expansion with `retain`).
    Extent * f = s.alloc(6 * PAGE);
    REQUIRE(f != nullptr);
    CHECK_EQ(reinterpret_cast<uintptr_t>(f->addr()), reinterpret_cast<uintptr_t>(e->past()));
    CHECK(s.shard->expand(nullptr, e, 6 * PAGE, 7 * PAGE, SC_NBINS, false, &deferred));
    CHECK_EQ(e->size(), 6 * PAGE);
    CHECK_EQ(f->state(), extent_state_active);

    s.dalloc(f);
    s.dalloc(e);
    s.destroy();
}

TEST(PageAllocator, Guarded)
{
    Shard s(3, 5000, 0, size_t(64) << 20);

    /// A large guarded extent: two guard pages around it, unguarded eagerly on dalloc.
    size_t size = SC_LARGE_MINCLASS;
    Extent * e = s.alloc(size, /* slab */ false, /* guarded */ true);
    REQUIRE(e != nullptr);
    CHECK(e->guarded());
    CHECK_EQ(e->size(), size);
    CHECK_EQ(s.shard->nactiveGet(), size / PAGE);
    std::memset(e->addr(), 1, size);
    s.dalloc(e);
    CHECK(!e->guarded());
    CHECK_EQ(e->size(), size + 2 * PAGE);
    CHECK_EQ(s.pac().ecache_dirty.guarded_eset.npagesGet(), size_t(0));

    /// A guarded slab comes from the bump allocator (right guard only) and is cached guarded.
    Extent * slab = s.alloc(2 * PAGE, /* slab */ true, /* guarded */ true);
    REQUIRE(slab != nullptr);
    CHECK(slab->guarded() && slab->slab());
    CHECK_EQ(slab->size(), 2 * PAGE);
    REQUIRE(s.pac().sba.curr_reg != nullptr);
    CHECK_EQ(s.pac().sba.curr_reg->size(), SBA_RETAINED_ALLOC_SIZE - 3 * PAGE);
    std::memset(slab->addr(), 2, 2 * PAGE);
    s.dalloc(slab);
    CHECK(slab->guarded());
    CHECK_EQ(s.pac().ecache_dirty.guarded_eset.npagesGet(), size_t(2));
    /// Exact-fit reuse of the cached guarded slab.
    Extent * again = s.alloc(2 * PAGE, true, true);
    CHECK(again == slab);
    CHECK_EQ(s.pac().ecache_dirty.guarded_eset.npagesGet(), size_t(0));
    /// Guarded extents cannot be resized in place.
    bool deferred = false;
    CHECK(s.shard->shrink(nullptr, again, 2 * PAGE, PAGE, 0, &deferred));
    s.dalloc(again);

    /// Purging evicts the guarded extent (after the non-guarded ones); it stays guarded in the retained cache (and is
    /// unguarded by `extentDestroyWrapper`).
    s.decayAll(true, true);
    CHECK_EQ(s.pac().ecache_dirty.npagesGet(), size_t(0));
    CHECK_EQ(s.pac().ecache_retained.guarded_eset.npagesGet(), size_t(2));
    s.destroy();
    CHECK_EQ(s.pac().ecache_retained.npagesGet(), size_t(0));
}

TEST(PageAllocator, OversizeShortcut)
{
    Shard s(4, 5000, 0, 16 * PAGE);
    background_thread_enabled_state.store(false);
    Extent * e = s.alloc(20 * PAGE);
    REQUIRE(e != nullptr);
    s.dalloc(e);
    /// Purged directly to retained.
    CHECK_EQ(s.shard->ndirtyGet(), size_t(0));
    CHECK_EQ(s.stats->pac_stats.decay_dirty.nmadvise.read(), uint64_t(1));
    CHECK_EQ(s.stats->pac_stats.decay_dirty.purged.read(), uint64_t(20));
    CHECK_EQ(s.stats->pac_stats.decay_dirty.npurge.read(), uint64_t(0));
    CHECK_EQ(s.pac().mapped(), size_t(0));

    /// Not with background threads.
    background_thread_enabled_state.store(true);
    e = s.alloc(20 * PAGE);
    s.dalloc(e);
    CHECK_EQ(s.shard->ndirtyGet(), size_t(20));
    background_thread_enabled_state.store(false);

    /// Not when decay is disabled.
    CHECK(!s.shard->decayMsSet(nullptr, extent_state_dirty, -1, PAC_PURGE_NEVER));
    e = s.alloc(20 * PAGE);
    s.dalloc(e);
    CHECK_EQ(s.shard->ndirtyGet(), size_t(20));
    s.destroy();
}

TEST(PageAllocator, DecaySettings)
{
    Shard s(5, 0, 0, size_t(64) << 20);
    CHECK_EQ(s.shard->decayMsGet(extent_state_dirty), ssize_t(0));
    CHECK(s.shard->dontDecayMuzzy());

    Extent * e = s.alloc(10 * PAGE);
    s.dalloc(e);
    CHECK_EQ(s.shard->ndirtyGet(), size_t(10));
    /// Immediate decay purges everything on the next check, whatever the eagerness.
    PageAllocator & pac = s.pac();
    pac.decay_dirty.mtx.lock(nullptr);
    CHECK(!pac.maybeDecayPurge(nullptr, &pac.decay_dirty, &s.stats->pac_stats.decay_dirty, &pac.ecache_dirty, PAC_PURGE_NEVER));
    pac.decay_dirty.mtx.unlock(nullptr);
    CHECK_EQ(s.shard->ndirtyGet(), size_t(0));
    CHECK_EQ(s.shard->timeUntilDeferredWork(nullptr), DECAY_UNBOUNDED_TIME_TO_PURGE);

    /// Invalid values are rejected.
    CHECK(s.shard->decayMsSet(nullptr, extent_state_dirty, -2, PAC_PURGE_NEVER));
    /// Gradual decay: nothing is purged before the epoch advances.
    CHECK(!s.shard->decayMsSet(nullptr, extent_state_dirty, 1000000, PAC_PURGE_ALWAYS));
    CHECK_EQ(s.shard->decayMsGet(extent_state_dirty), ssize_t(1000000));
    e = s.alloc(10 * PAGE);
    s.dalloc(e);
    pac.decay_dirty.mtx.lock(nullptr);
    CHECK(!pac.maybeDecayPurge(nullptr, &pac.decay_dirty, &s.stats->pac_stats.decay_dirty, &pac.ecache_dirty, PAC_PURGE_ON_EPOCH_ADVANCE));
    pac.decay_dirty.mtx.unlock(nullptr);
    CHECK_EQ(s.shard->ndirtyGet(), size_t(10));
    /// The dirty pages were not recorded in a past epoch yet: the next check is a full decay period away.
    uint64_t t = s.shard->timeUntilDeferredWork(nullptr);
    CHECK_EQ(t, pac.decay_dirty.interval.ns() * SMOOTHSTEP_NSTEPS);

    /// Muzzy decay: dirty pages go to the muzzy cache first (purged lazily), then to retained.
    CHECK(!s.shard->decayMsSet(nullptr, extent_state_muzzy, 1000000, PAC_PURGE_NEVER));
    CHECK(!s.shard->dontDecayMuzzy());
    s.decayAll(true, false);
    CHECK_EQ(s.shard->ndirtyGet(), size_t(0));
    CHECK_EQ(s.shard->nmuzzyGet(), size_t(10));
    CHECK_EQ(s.stats->pac_stats.decay_dirty.purged.read(), uint64_t(20));
    /// Allocation also searches the muzzy cache.
    e = s.alloc(10 * PAGE);
    CHECK_EQ(s.shard->nmuzzyGet(), size_t(0));
    s.dalloc(e);
    s.decayAll(true, false);
    s.decayAll(false, false);
    CHECK_EQ(s.shard->nmuzzyGet(), size_t(0));
    CHECK_EQ(s.stats->pac_stats.decay_muzzy.purged.read(), uint64_t(10));

    /// The retained grow limit.
    size_t old_limit = 0;
    CHECK(!pac.retainGrowLimitGetSet(nullptr, &old_limit, nullptr));
    CHECK_EQ(old_limit, sz::pind2sz(sz::psz2ind(SC_LARGE_MAXCLASS)));
    size_t new_limit = size_t(4) << 20;
    CHECK(!pac.retainGrowLimitGetSet(nullptr, &old_limit, &new_limit));
    CHECK(!pac.retainGrowLimitGetSet(nullptr, &old_limit, nullptr));
    CHECK_EQ(old_limit, size_t(4) << 20);
    /// Growth stops at the limit (unless forced by the request size).
    Extent * big = s.alloc(size_t(4) << 20);
    REQUIRE(big != nullptr);
    CHECK_EQ(pac.exp_grow.next, sz::psz2ind(size_t(4) << 20));
    s.dalloc(big);
    s.destroy();
}

TEST(PageAllocator, CoalesceLimitQuirk)
{
    /// `extent_record` coalesces a large dirty extent with neighbors only up to `size << lg_extent_max_active_fit`;
    /// a rejected neighbor stays in the `merging` state (jemalloc compatibility, fork patch 3c14707b).
    /// The pinned numbers depend on the geometry; they were taken with HUGEPAGE = PAGE * PAGE / 8 (Linux x86_64,
    /// aarch64, riscv64). With 64 KiB pages and 2 MiB huge pages (ppc64le) the extents differ (the same as in jemalloc,
    /// checked against the reference under qemu).
    if constexpr (LG_HUGEPAGE != 2 * LG_PAGE - 3)
        return;
    Shard s(6, 5000, 0, SIZE_MAX);
    /// 1028 pages: a 1280-page chunk; the 252-page tail goes to the dirty cache.
    Extent * h = s.alloc(1028 * PAGE);
    REQUIRE(h != nullptr);
    CHECK_EQ(s.shard->ndirtyGet(), size_t(252));
    /// Shrinking to 4 pages: the 1024-page tail is coalesced with the dirty neighbor (within 1024 << 6).
    bool deferred = false;
    REQUIRE(!s.shard->shrink(nullptr, h, 1028 * PAGE, 4 * PAGE, SC_NBINS, &deferred));
    Extent * big = s.pac().ecache_dirty.eset.lruFirst();
    REQUIRE(big != nullptr);
    CHECK_EQ(big->size(), 1276 * PAGE);
    CHECK(s.pac().ecache_dirty.eset.lru.next(big) == nullptr);
    CHECK_EQ(reinterpret_cast<uintptr_t>(big->addr()), reinterpret_cast<uintptr_t>(h->past()));

    /// The 4-page extent may grow only up to 4 << 6 pages: the big neighbor is acquired, rejected, and not released.
    s.dalloc(h);
    CHECK_EQ(h->state(), extent_state_dirty);
    CHECK_EQ(h->size(), 4 * PAGE);
    CHECK_EQ(big->state(), extent_state_merging);
    CHECK_EQ(s.shard->ndirtyGet(), size_t(1280));

    /// The merging extent is still in the set and can be allocated.
    Extent * again = s.alloc(1024 * PAGE);
    CHECK(again == big);
    CHECK_EQ(big->state(), extent_state_active);
    CHECK_EQ(s.shard->ndirtyGet(), size_t(256));
    s.dalloc(again);
    s.destroy();
}
