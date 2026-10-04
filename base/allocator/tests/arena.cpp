/// Scripted single-threaded scenarios on arenas created with `Arena::create` (`arena_new`), with a `ThreadState`
/// object as the tsd: the bin layout, slab and region selection order, small/large allocation and deallocation, tcache
/// fill/flush through `CacheBinPtrArray`, in-place reallocation, stats, reset/destroy, the huge arena and thread
/// binding. The exact equivalence with jemalloc is checked by arena_oracle.cpp; these tests pin page-size independent
/// behavior and run with every page size.

#include <allocator/ArenaInlines.h>
#include <allocator/Arena.h>
#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

#include "Test.h"

#include <cstring>
#include <vector>

using namespace jemalloc;

namespace
{

constinit ThreadState tsd;

void bootOnce()
{
    static bool booted = false;
    if (booted)
        return;
    booted = true;

    REQUIRE(!pages::boot());
    szBoot(default_sc_data, opt.cache_oblivious);
    REQUIRE(!baseBoot(nullptr));
    REQUIRE(!arena_emap_global.init(b0get(), /* zeroed */ true));
    REQUIRE(!arenaBoot(&default_sc_data, b0get(), false));
    REQUIRE(!arenas_lock.init("arenas", MutexRank::ARENAS, MutexLockOrder::RankExclusive));

    /// As `malloc_init_hard_a0_locked`: one auto arena; then as `malloc_init_narenas` with the huge arena.
    narenas_auto = 1;
    manual_arena_base = narenas_auto + 1;
    a0 = arenaInit(nullptr, 0, &arena_config_default);
    REQUIRE(a0 != nullptr);
    narenasTotalSet(narenas_auto);
    if (arenaInitHuge(nullptr, a0))
        narenasTotalInc();
    manual_arena_base = narenasTotalGet();
    /// The background thread module (disabled by default: `arenaInit` only checks the state of the thread slot).
    REQUIRE(!backgroundThreadBoot0());
    REQUIRE(!backgroundThreadBoot1(nullptr, b0get()));
    REQUIRE(!backgroundThreadEnabled());

    tsd.state.store(tsd_state_nominal_slow, std::memory_order_relaxed);
}

Arena * newManualArena()
{
    bootOnce();
    Arena * arena = arenaInit(&tsd, narenasTotalGet(), &arena_config_default);
    REQUIRE(arena != nullptr);
    return arena;
}

struct Totals
{
    ArenaStats astats;
    BinStatsData bstats[SC_NBINS];
    ArenaStatsLarge lstats[SC_NSIZES - SC_NBINS];
    PacExtentStats estats[SC_NPSIZES];
    unsigned nthreads = 0;
    const char * dss = nullptr;
    ssize_t dirty_decay_ms = 0;
    ssize_t muzzy_decay_ms = 0;
    size_t nactive = 0;
    size_t ndirty = 0;
    size_t nmuzzy = 0;
};

Totals * stats(Arena * arena)
{
    static Totals * t = nullptr;
    if (t == nullptr)
        t = static_cast<Totals *>(std::aligned_alloc(64, (sizeof(Totals) + 63) / 64 * 64));
    std::memset(static_cast<void *>(t), 0, sizeof(Totals));
    arenaStatsMerge(
        &tsd,
        arena,
        &t->nthreads,
        &t->dss,
        &t->dirty_decay_ms,
        &t->muzzy_decay_ms,
        &t->nactive,
        &t->ndirty,
        &t->nmuzzy,
        &t->astats,
        t->bstats,
        t->lstats,
        t->estats);
    return t;
}

}

TEST(Arena, Layout)
{
    bootOnce();
    CHECK_EQ(arena_bin_offsets[0], uint32_t(sizeof(Arena)));
    unsigned total = 0;
    for (unsigned i = 0; i < SC_NBINS; ++i)
    {
        CHECK_EQ(arena_bin_offsets[i], uint32_t(sizeof(Arena) + total * sizeof(Bin)));
        total += bin_infos[i].n_shards;
    }
    CHECK_EQ(arena_nbins_total, total);
    CHECK_EQ(sizeof(Arena) % CACHELINE, 0u);
    CHECK_EQ(alignof(Arena), CACHELINE);
    for (unsigned i = 0; i < SC_NBINS; ++i)
        CHECK_EQ(arena_binind_div_info[i].compute(bin_infos[i].reg_size * 7), 7u);
}

TEST(Arena, Creation)
{
    Arena * arena = newManualArena();
    unsigned ind = arenaIndGet(arena);
    CHECK(!arenaIsAuto(arena));
    CHECK(arenaGet(&tsd, ind, false) == arena);
    CHECK(arena->base->indGet() == ind);

    char name[ARENA_NAME_LEN];
    arenaNameGet(arena, name);
    char expected[ARENA_NAME_LEN];
    std::snprintf(expected, sizeof(expected), "manual_%u", ind);
    CHECK_EQ(std::strcmp(name, expected), 0);
    arenaNameSet(arena, "a very long name that does not fit into the arena name buffer");
    arenaNameGet(arena, name);
    CHECK_EQ(std::strlen(name), ARENA_NAME_LEN - 1);

    char a0_name[ARENA_NAME_LEN];
    arenaNameGet(a0, a0_name);
    CHECK_EQ(std::strcmp(a0_name, "auto_0"), 0);
    CHECK(arenaIsAuto(a0));

    CHECK_EQ(arenaDecayMsGet(arena, extent_state_dirty), opt.dirty_decay_ms);
    CHECK_EQ(arenaDecayMsGet(arena, extent_state_muzzy), opt.muzzy_decay_ms);
    CHECK(arenaDssPrecGet(arena) == DSS_PREC_DEFAULT);
    CHECK(!arenaDssPrecSet(arena, DssPrec::Primary));
    CHECK(arenaDssPrecGet(arena) == DssPrec::Primary);
    CHECK(!arenaDssPrecSet(arena, DSS_PREC_DEFAULT));

    Totals * t = stats(arena);
    CHECK_EQ(t->nthreads, 0u);
    CHECK_EQ(std::strcmp(t->dss, "secondary"), 0);
    CHECK_EQ(t->nactive, 0u);
    /// The arena (with its bins) is allocated from its own base.
    CHECK(t->astats.base >= sizeof(Arena) + sizeof(Bin) * arena_nbins_total);
}

TEST(Arena, SmallRegionOrder)
{
    Arena * arena = newManualArena();
    const szind_t binind = 0;
    const BinInfo & info = bin_infos[binind];
    std::vector<void *> ptrs;
    for (unsigned i = 0; i < info.nregs; ++i)
    {
        void * p = arenaMallocHard(&tsd, arena, 1, binind, false, true);
        REQUIRE(p != nullptr);
        ptrs.push_back(p);
    }
    /// The regions of the first slab are returned in address order.
    for (unsigned i = 1; i < info.nregs; ++i)
        CHECK_EQ(reinterpret_cast<uintptr_t>(ptrs[i]) - reinterpret_cast<uintptr_t>(ptrs[0]), i * info.reg_size);
    Totals * t = stats(arena);
    CHECK_EQ(t->bstats[binind].stats_data.nmalloc, uint64_t(info.nregs));
    CHECK_EQ(t->bstats[binind].stats_data.curregs, size_t(info.nregs));
    CHECK_EQ(t->bstats[binind].stats_data.curslabs, 1u);
    CHECK_EQ(t->nactive, info.slab_size / PAGE);

    /// The slab is full: the next region comes from a new slab.
    void * q = arenaMallocHard(&tsd, arena, 1, binind, false, true);
    CHECK(q != nullptr);
    t = stats(arena);
    CHECK_EQ(t->bstats[binind].stats_data.nslabs, 2u);
    CHECK_EQ(t->bstats[binind].stats_data.curslabs, 2u);

    /// Free regions 5 and 3 of the (older) first slab: it becomes the current slab again (the oldest/lowest non-full
    /// slab is preferred), and the lowest free region is returned first.
    arenaDallocNoTcache(&tsd, ptrs[5]);
    arenaDallocNoTcache(&tsd, ptrs[3]);
    CHECK(arenaMallocHard(&tsd, arena, 1, binind, false, true) == ptrs[3]);
    CHECK(arenaMallocHard(&tsd, arena, 1, binind, false, true) == ptrs[5]);
    Bin * bin = arenaGetBin(arena, binind, 0);
    CHECK(bin->slabcur != nullptr && bin->slabcur->addr() == ptrs[0]);

    /// Free everything: both slabs are released.
    arenaDallocNoTcache(&tsd, q);
    for (void * p : ptrs)
        arenaSdallocNoTcache(&tsd, p, 1);
    t = stats(arena);
    CHECK_EQ(t->bstats[binind].stats_data.curregs, 0u);
    CHECK_EQ(t->bstats[binind].stats_data.curslabs, 0u);
    CHECK_EQ(t->bstats[binind].stats_data.ndalloc, t->bstats[binind].stats_data.nmalloc);
    CHECK_EQ(t->nactive, 0u);
    CHECK(t->ndirty >= 2 * info.slab_size / PAGE || arenaDecayMsGet(arena, extent_state_dirty) == 0);
}

TEST(Arena, Large)
{
    Arena * arena = newManualArena();
    size_t size = SC_LARGE_MINCLASS + 1;
    szind_t ind = sz::sizeToIndex(size);
    void * p = arenaMallocHard(&tsd, arena, size, ind, true, false);
    REQUIRE(p != nullptr);
    size_t usize = sz::s2u(size);
    CHECK_EQ(arenaSalloc(&tsd, p), usize);
    CHECK_EQ(arenaVsalloc(&tsd, p), usize);
    CHECK(arenaAalloc(&tsd, p) == arena);
    for (size_t i = 0; i < usize; i += 4096)
        CHECK_EQ(static_cast<unsigned char *>(p)[i], 0);
    /// Cache-oblivious placement: a cacheline-aligned offset within the first page.
    Extent * edata = arena_emap_global.edataLookup(&tsd, p);
    size_t offset = reinterpret_cast<uintptr_t>(p) - reinterpret_cast<uintptr_t>(edata->base());
    CHECK_EQ(offset % CACHELINE, 0u);
    CHECK(offset < PAGE);
    CHECK_EQ(edata->size(), usize + sz_large_pad);
    /// Manual arenas track their large allocations.
    CHECK(arena->large.first() == edata);

    Totals * t = stats(arena);
    CHECK_EQ(t->lstats[ind - SC_NBINS].nmalloc.readUnsynchronized(), 1u);
    CHECK_EQ(t->lstats[ind - SC_NBINS].curlextents, 1u);
    CHECK_EQ(t->astats.allocated_large, usize);

    /// In-place growth and shrinking.
    size_t newsize = 0;
    CHECK(!arenaRallocNoMove(&tsd, p, usize, usize + PAGE, 0, false, &newsize));
    CHECK_EQ(newsize, sz::s2u(usize + PAGE));
    CHECK(!arenaRallocNoMove(&tsd, p, newsize, usize, 0, false, &newsize));
    CHECK_EQ(newsize, usize);
    /// Large -> small cannot be done in place.
    CHECK(arenaRallocNoMove(&tsd, p, usize, 8, 0, false, &newsize));

    arenaDallocNoTcache(&tsd, p);
    CHECK(arena->large.empty());
    t = stats(arena);
    CHECK_EQ(t->astats.allocated_large, 0u);
    CHECK_EQ(t->astats.nmalloc_large, t->astats.ndalloc_large);
}

TEST(Arena, SmallRallocNoMove)
{
    Arena * arena = newManualArena();
    void * p = arenaMallocHard(&tsd, arena, 20, sz::sizeToIndex(20), false, true);
    REQUIRE(p != nullptr);
    size_t usize = arenaSalloc(&tsd, p);
    size_t newsize = 0;
    /// The same size class: in place.
    CHECK(!arenaRallocNoMove(&tsd, p, usize, usize - 1, 0, false, &newsize));
    CHECK_EQ(newsize, usize);
    /// Growing beyond the class: must move.
    CHECK(arenaRallocNoMove(&tsd, p, usize, usize + 1, 0, false, &newsize));
    /// Shrinking into a smaller class (usize_max < oldsize): must move; with extra reaching the old size: in place.
    CHECK(arenaRallocNoMove(&tsd, p, usize, 8, 0, false, &newsize));
    CHECK(!arenaRallocNoMove(&tsd, p, usize, 8, usize - 8, false, &newsize));
    void * q = arenaRalloc(&tsd, arena, p, usize, 1000, 0, false, true, nullptr);
    REQUIRE(q != nullptr);
    CHECK(q != p);
    CHECK_EQ(arenaSalloc(&tsd, q), sz::s2u(1000));
    arenaDallocNoTcache(&tsd, q);
}

TEST(Arena, FillFlush)
{
    Arena * arena = newManualArena();
    const szind_t binind = 2;
    const BinInfo & info = bin_infos[binind];
    std::vector<void *> ptrs(info.nregs * 3);

    CacheBinPtrArray arr{cache_bin_sz_t(ptrs.size())};
    arr.ptr = ptrs.data();
    CacheBinStats merge{};
    merge.nrequests = 17;
    /// With an empty bin, the fill takes a fresh slab and uses it up while the total does not exceed `nfill_max`.
    cache_bin_sz_t nfill_max = cache_bin_sz_t(info.nregs + 5);
    cache_bin_sz_t filled = arenaPtrArrayFillSmall(&tsd, arena, binind, &arr, 10, nfill_max, merge);
    CHECK_EQ(unsigned(filled), info.nregs);
    for (unsigned i = 1; i < filled; ++i)
        CHECK_EQ(reinterpret_cast<uintptr_t>(ptrs[i]) - reinterpret_cast<uintptr_t>(ptrs[0]), i * info.reg_size);

    /// The next fill needs a new slab and takes only `nfill_min` from it when the whole slab would exceed the max.
    arr.ptr = ptrs.data() + filled;
    cache_bin_sz_t filled2 = arenaPtrArrayFillSmall(&tsd, arena, binind, &arr, 10, 12, merge);
    CHECK_EQ(unsigned(filled2), 10u);

    Totals * t = stats(arena);
    const BinStats & bs = t->bstats[binind].stats_data;
    CHECK_EQ(bs.nfills, 2u);
    CHECK_EQ(bs.nrequests, 34u);
    CHECK_EQ(bs.nmalloc, uint64_t(filled + filled2));
    CHECK_EQ(bs.curregs, size_t(filled + filled2));
    CHECK_EQ(bs.nslabs, 2u);

    /// Flush everything (stats merged into this arena).
    CacheBinPtrArray flush{cache_bin_sz_t(filled + filled2)};
    flush.ptr = ptrs.data();
    CacheBinStats flush_stats{};
    flush_stats.nrequests = 5;
    arenaPtrArrayFlush(tsd, binind, &flush, unsigned(filled + filled2), true, arena, flush_stats);
    t = stats(arena);
    CHECK_EQ(t->bstats[binind].stats_data.curregs, 0u);
    CHECK_EQ(t->bstats[binind].stats_data.curslabs, 0u);
    CHECK_EQ(t->bstats[binind].stats_data.nflushes, 1u);
    CHECK_EQ(t->bstats[binind].stats_data.nrequests, 39u);
    CHECK_EQ(t->nactive, 0u);

    /// A flush whose objects belong to another arena still merges the stats into `stats_arena`.
    Arena * other = newManualArena();
    void * p = arenaMallocHard(&tsd, other, 1, 0, false, true);
    CacheBinPtrArray one{1};
    one.ptr = &p;
    arenaPtrArrayFlush(tsd, 0, &one, 1, true, arena, flush_stats);
    t = stats(arena);
    CHECK_EQ(t->bstats[0].stats_data.nflushes, 1u);
    CHECK_EQ(t->bstats[0].stats_data.nrequests, 5u);
    t = stats(other);
    CHECK_EQ(t->bstats[0].stats_data.nflushes, 0u);
    CHECK_EQ(t->bstats[0].stats_data.ndalloc, 1u);
}

TEST(Arena, FlushLarge)
{
    Arena * arena = newManualArena();
    size_t usize = SC_LARGE_MINCLASS;
    szind_t ind = sz::sizeToIndex(usize);
    std::vector<void *> ptrs;
    for (int i = 0; i < 300; ++i)
        ptrs.push_back(arenaMallocHard(&tsd, arena, usize, ind, false, false));
    CacheBinPtrArray arr{cache_bin_sz_t(ptrs.size())};
    arr.ptr = ptrs.data();
    CacheBinStats merge{};
    merge.nrequests = 3;
    /// More than `CACHE_BIN_NFLUSH_BATCH_MAX` pointers: processed in batches, the stats are merged once.
    arenaPtrArrayFlush(tsd, ind, &arr, unsigned(ptrs.size()), false, arena, merge);
    CHECK(arena->large.empty());
    Totals * t = stats(arena);
    const ArenaStatsLarge & l = t->lstats[ind - SC_NBINS];
    CHECK_EQ(l.ndalloc.readUnsynchronized(), 300u);
    CHECK_EQ(l.nflushes.readUnsynchronized(), 1u);
    CHECK_EQ(l.nrequests.readUnsynchronized(), 300u + 3u);
    CHECK_EQ(l.curlextents, 0u);
}

TEST(Arena, FillSmallFresh)
{
    Arena * arena = newManualArena();
    const szind_t binind = 1;
    const BinInfo & info = bin_infos[binind];
    std::vector<void *> ptrs(info.nregs * 2 + 3);
    size_t n = arenaFillSmallFresh(&tsd, arena, binind, ptrs.data(), ptrs.size(), true);
    CHECK_EQ(n, ptrs.size());
    Totals * t = stats(arena);
    CHECK_EQ(t->bstats[binind].stats_data.nslabs, 3u);
    CHECK_EQ(t->bstats[binind].stats_data.curregs, ptrs.size());
    /// Manual arena: the two full slabs are tracked, the partial one goes to the non-full heap (`bin_lower_slab` with no
    /// current slab).
    Bin * bin = arenaGetBin(arena, binind, 0);
    size_t nfull = 0;
    for (Extent * e = bin->slabs_full.first(); e != nullptr; e = bin->slabs_full.next(e))
        ++nfull;
    CHECK_EQ(nfull, 2u);
    CHECK(bin->slabcur == nullptr);
    Extent * partial = bin->slabs_nonfull.first();
    REQUIRE(partial != nullptr);
    CHECK_EQ(partial->nfree(), info.nregs - 3);
    for (void * p : ptrs)
        arenaDallocNoTcache(&tsd, p);
    CHECK(bin->slabs_full.empty());
}

TEST(Arena, ResetDestroy)
{
    Arena * arena = newManualArena();
    unsigned ind = arenaIndGet(arena);
    for (int i = 0; i < 1000; ++i)
        arenaMallocHard(&tsd, arena, size_t(1) + size_t(i) % 3000, sz::sizeToIndex(size_t(1) + size_t(i) % 3000), false, true);
    for (int i = 0; i < 10; ++i)
        arenaMallocHard(&tsd, arena, SC_LARGE_MINCLASS * size_t(i + 1), sz::sizeToIndex(SC_LARGE_MINCLASS * size_t(i + 1)), false, false);
    CHECK(stats(arena)->nactive > 0);
    arenaReset(tsd, arena);
    Totals * t = stats(arena);
    CHECK_EQ(t->nactive, 0u);
    CHECK(arena->large.empty());
    for (unsigned i = 0; i < SC_NBINS; ++i)
        CHECK_EQ(t->bstats[i].stats_data.curregs, 0u);
    arenaDecay(&tsd, arena, false, true);
    t = stats(arena);
    CHECK_EQ(t->ndirty, 0u);
    CHECK_EQ(t->nmuzzy, 0u);
    arenaDestroy(tsd, arena);
    CHECK(arenaGet(&tsd, ind, false) == nullptr);
}

TEST(Arena, HugeArena)
{
    bootOnce();
    if (huge_arena_ind == 0)
        return; /// Disabled by the configuration.
    CHECK_EQ(oversize_threshold, opt.oversize_threshold);
    CHECK_EQ(a0->pa_shard.pac.oversize_threshold.load(), oversize_threshold);
    CHECK_EQ(manual_arena_base, huge_arena_ind + 1);
    Arena * huge = arenaChooseHuge(tsd);
    REQUIRE(huge != nullptr);
    CHECK_EQ(arenaIndGet(huge), huge_arena_ind);
    CHECK(arenaIsAuto(huge));
    char name[ARENA_NAME_LEN];
    arenaNameGet(huge, name);
    CHECK_EQ(std::strcmp(name, "auto_oversize"), 0);
    /// Without background threads the huge arena purges eagerly.
    CHECK_EQ(arenaDecayMsGet(huge, extent_state_dirty), 0);
    CHECK(arenaChooseHuge(tsd) == huge);
}

TEST(Arena, Binding)
{
    bootOnce();
    ThreadState & t = tsd;
    CHECK(t.arena == nullptr);
    /// One auto arena: the thread is bound to arena 0 for both kinds of allocations.
    unsigned before = arenaNthreadsGet(a0, false);
    unsigned before_internal = arenaNthreadsGet(a0, true);
    CHECK(arenaChoose(t, nullptr) == a0);
    CHECK(t.arena == a0);
    CHECK(t.iarena == a0);
    CHECK(arenaIchoose(t, nullptr) == a0);
    CHECK_EQ(arenaNthreadsGet(a0, false), before + 1);
    CHECK_EQ(arenaNthreadsGet(a0, true), before_internal + 1);
    for (unsigned i = 0; i < SC_NBINS; ++i)
        CHECK_EQ(unsigned(t.binshards.binshard[i]), 0u);

    /// Huge requests from auto arenas go to the huge arena; explicit arenas are never redirected.
    if (huge_arena_ind != 0)
    {
        CHECK(arenaChooseMaybeHuge(t, nullptr, oversize_threshold) == arenaGet(&t, huge_arena_ind, false));
        CHECK(arenaChooseMaybeHuge(t, nullptr, oversize_threshold - 1) == a0);
        CHECK(arenaChooseMaybeHuge(t, a0, oversize_threshold) == a0);
    }

    Arena * manual = newManualArena();
    arenaMigrate(t, a0, manual);
    CHECK(t.arena == manual);
    CHECK_EQ(arenaNthreadsGet(manual, false), 1u);
    /// Threads bound to a manual arena are not redirected to the huge arena.
    CHECK(arenaChooseMaybeHuge(t, nullptr, SC_LARGE_MAXCLASS) == manual);

    arenaCleanup(t);
    iarenaCleanup(t);
    CHECK(t.arena == nullptr);
    CHECK(t.iarena == nullptr);
    CHECK_EQ(arenaNthreadsGet(manual, false), 0u);
    CHECK_EQ(arenaNthreadsGet(a0, true), before_internal);
}

TEST(Arena, DecayMs)
{
    Arena * arena = newManualArena();
    CHECK(!arenaDecayMsSet(&tsd, arena, extent_state_dirty, 0));
    CHECK_EQ(arenaDecayMsGet(arena, extent_state_dirty), 0);
    CHECK(arenaDecayMsSet(&tsd, arena, extent_state_dirty, -2));
    /// With immediate dirty decay, freed slabs are purged right away.
    void * p = arenaMallocHard(&tsd, arena, 8, 0, false, true);
    arenaDallocNoTcache(&tsd, p);
    CHECK_EQ(stats(arena)->ndirty, 0u);

    ssize_t old = arenaDirtyDecayMsDefaultGet();
    CHECK(arenaDirtyDecayMsDefaultSet(-2));
    CHECK(!arenaDirtyDecayMsDefaultSet(1234));
    CHECK_EQ(arenaDirtyDecayMsDefaultGet(), 1234);
    CHECK(!arenaDirtyDecayMsDefaultSet(old));
    CHECK(!arenaMuzzyDecayMsDefaultSet(arenaMuzzyDecayMsDefaultGet()));

    size_t old_limit = 0;
    size_t new_limit = size_t(1) << 30;
    CHECK(!arenaRetainGrowLimitGetSet(tsd, arena, &old_limit, &new_limit));
    size_t check_limit = 0;
    CHECK(!arenaRetainGrowLimitGetSet(tsd, arena, &check_limit, nullptr));
    CHECK(check_limit <= new_limit);
}

TEST(Arena, BootstrapAllocation)
{
    bootOnce();
    /// `a0ialloc` needs `mallocInitA0` (Init); here only the deallocation side of internal accounting is checked
    /// through `arenaInternal*`.
    size_t before = arenaInternalGet(a0);
    arenaInternalAdd(a0, 100);
    CHECK_EQ(arenaInternalGet(a0), before + 100);
    arenaInternalSub(a0, 100);
    CHECK_EQ(arenaInternalGet(a0), before);
}
