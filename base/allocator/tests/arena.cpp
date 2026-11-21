/// Scripted single-threaded scenarios on arenas created with `Arena::create` (`arena_new`), with a `ThreadState`
/// object as the tsd: the bin layout, slab and region selection order, small/large allocation and deallocation, tcache
/// fill/flush through `CacheBinPtrArray`, in-place reallocation, stats, reset/destroy, the huge arena and thread
/// binding. The exact equivalence with jemalloc is checked by arena_oracle.cpp; these tests pin page-size independent
/// behavior and run with every page size.

#include <allocator/Arena.h>
#include <allocator/ArenaInlines.h>
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

constinit ThreadState thread_state;

void bootOnce()
{
    static bool booted = false;
    if (booted)
        return;
    booted = true;

    REQUIRE(!pages::boot());
    sizeBoot(default_size_class_data, options.cache_oblivious);
    REQUIRE(!baseBoot(nullptr));
    REQUIRE(!arena_extent_map_global.init(base0Get(), /* zeroed */ true));
    REQUIRE(!arenaBoot(&default_size_class_data, base0Get(), false));
    REQUIRE(!arenas_lock.init("arenas", MutexRank::ARENAS, MutexLockOrder::RankExclusive));

    /// As `malloc_init_hard_a0_locked`: one auto arena; then as `malloc_init_narenas` with the huge arena.
    num_arenas_auto = 1;
    manual_arena_base = num_arenas_auto + 1;
    a0 = arenaInit(nullptr, 0, &arena_config_default);
    REQUIRE(a0 != nullptr);
    numArenasTotalSet(num_arenas_auto);
    if (arenaInitHuge(nullptr, a0))
        numArenasTotalIncrement();
    manual_arena_base = numArenasTotalGet();
    /// The background thread module (disabled by default: `arenaInit` only checks the state of the thread slot).
    REQUIRE(!backgroundThreadBoot0());
    REQUIRE(!backgroundThreadBoot1(nullptr, base0Get()));
    REQUIRE(!backgroundThreadEnabled());

    thread_state.state.store(thread_state_nominal_slow, std::memory_order_relaxed);
}

Arena * newManualArena()
{
    bootOnce();
    Arena * arena = arenaInit(&thread_state, numArenasTotalGet(), &arena_config_default);
    REQUIRE(arena != nullptr);
    return arena;
}

struct Totals
{
    ArenaStats arena_stats;
    BinStatsData bin_stats[SIZE_CLASS_NUM_BINS];
    ArenaStatsLarge large_stats[SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS];
    PageAllocatorExtentStats extent_stats[SIZE_CLASS_NUM_PAGE_SIZES];
    unsigned num_threads = 0;
    const char * sbrk = nullptr;
    ssize_t dirty_decay_ms = 0;
    ssize_t muzzy_decay_ms = 0;
    size_t num_active = 0;
    size_t num_dirty = 0;
    size_t num_muzzy = 0;
};

Totals * stats(Arena * arena)
{
    static Totals * t = nullptr;
    if (t == nullptr)
        t = static_cast<Totals *>(std::aligned_alloc(64, (sizeof(Totals) + 63) / 64 * 64));
    std::memset(static_cast<void *>(t), 0, sizeof(Totals));
    arenaStatsMerge(
        &thread_state,
        arena,
        &t->num_threads,
        &t->sbrk,
        &t->dirty_decay_ms,
        &t->muzzy_decay_ms,
        &t->num_active,
        &t->num_dirty,
        &t->num_muzzy,
        &t->arena_stats,
        t->bin_stats,
        t->large_stats,
        t->extent_stats);
    return t;
}

}

TEST(Arena, Layout)
{
    bootOnce();
    CHECK_EQ(arena_bin_offsets[0], uint32_t(sizeof(Arena)));
    unsigned total = 0;
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        CHECK_EQ(arena_bin_offsets[i], uint32_t(sizeof(Arena) + total * sizeof(Bin)));
        total += bin_infos[i].num_shards;
    }
    CHECK_EQ(arena_num_bins_total, total);
    CHECK_EQ(sizeof(Arena) % CACHE_LINE, 0u);
    CHECK_EQ(alignof(Arena), CACHE_LINE);
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
        CHECK_EQ(arena_bin_idx_division_info[i].compute(bin_infos[i].region_size * 7), 7u);
}

TEST(Arena, Creation)
{
    Arena * arena = newManualArena();
    unsigned idx = arenaIdxGet(arena);
    CHECK(!arenaIsAuto(arena));
    CHECK(arenaGet(&thread_state, idx, false) == arena);
    CHECK(arena->base->idxGet() == idx);

    char name[ARENA_NAME_LEN];
    arenaNameGet(arena, name);
    char expected[ARENA_NAME_LEN];
    std::snprintf(expected, sizeof(expected), "manual_%u", idx);
    CHECK_EQ(std::strcmp(name, expected), 0);
    arenaNameSet(arena, "a very long name that does not fit into the arena name buffer");
    arenaNameGet(arena, name);
    CHECK_EQ(std::strlen(name), ARENA_NAME_LEN - 1);

    char a0_name[ARENA_NAME_LEN];
    arenaNameGet(a0, a0_name);
    CHECK_EQ(std::strcmp(a0_name, "auto_0"), 0);
    CHECK(arenaIsAuto(a0));

    CHECK_EQ(arenaDecayMsGet(arena, extent_state_dirty), options.dirty_decay_ms);
    CHECK_EQ(arenaDecayMsGet(arena, extent_state_muzzy), options.muzzy_decay_ms);
    CHECK(arenaSbrkPrecedenceGet(arena) == SBRK_PRECEDENCE_DEFAULT);
    CHECK(!arenaSbrkPrecedenceSet(arena, SbrkPrecedence::Primary));
    CHECK(arenaSbrkPrecedenceGet(arena) == SbrkPrecedence::Primary);
    CHECK(!arenaSbrkPrecedenceSet(arena, SBRK_PRECEDENCE_DEFAULT));

    Totals * t = stats(arena);
    CHECK_EQ(t->num_threads, 0u);
    CHECK_EQ(std::strcmp(t->sbrk, "secondary"), 0);
    CHECK_EQ(t->num_active, 0u);
    /// The arena (with its bins) is allocated from its own base.
    CHECK(t->arena_stats.base >= sizeof(Arena) + sizeof(Bin) * arena_num_bins_total);
}

TEST(Arena, SmallRegionOrder)
{
    Arena * arena = newManualArena();
    const SizeClassIdx bin_idx = 0;
    const BinInfo & info = bin_infos[bin_idx];
    std::vector<void *> ptrs;
    for (unsigned i = 0; i < info.num_regions; ++i)
    {
        void * p = arenaMallocHard(&thread_state, arena, 1, bin_idx, false, true);
        REQUIRE(p != nullptr);
        ptrs.push_back(p);
    }
    /// The regions of the first slab are returned in address order.
    for (unsigned i = 1; i < info.num_regions; ++i)
        CHECK_EQ(reinterpret_cast<uintptr_t>(ptrs[i]) - reinterpret_cast<uintptr_t>(ptrs[0]), i * info.region_size);
    Totals * t = stats(arena);
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.num_allocations, uint64_t(info.num_regions));
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.current_regions, size_t(info.num_regions));
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.current_slabs, 1u);
    CHECK_EQ(t->num_active, info.slab_size / PAGE);

    /// The slab is full: the next region comes from a new slab.
    void * q = arenaMallocHard(&thread_state, arena, 1, bin_idx, false, true);
    CHECK(q != nullptr);
    t = stats(arena);
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.num_slabs, 2u);
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.current_slabs, 2u);

    /// Free regions 5 and 3 of the (older) first slab: it becomes the current slab again (the oldest/lowest non-full
    /// slab is preferred), and the lowest free region is returned first.
    arenaDeallocateNoThreadCache(&thread_state, ptrs[5]);
    arenaDeallocateNoThreadCache(&thread_state, ptrs[3]);
    CHECK(arenaMallocHard(&thread_state, arena, 1, bin_idx, false, true) == ptrs[3]);
    CHECK(arenaMallocHard(&thread_state, arena, 1, bin_idx, false, true) == ptrs[5]);
    Bin * bin = arenaGetBin(arena, bin_idx, 0);
    CHECK(bin->current_slab != nullptr && bin->current_slab->addr() == ptrs[0]);

    /// Free everything: both slabs are released.
    arenaDeallocateNoThreadCache(&thread_state, q);
    for (void * p : ptrs)
        arenaSizedDeallocateNoThreadCache(&thread_state, p, 1);
    t = stats(arena);
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.current_regions, 0u);
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.current_slabs, 0u);
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.num_deallocations, t->bin_stats[bin_idx].stats_data.num_allocations);
    CHECK_EQ(t->num_active, 0u);
    CHECK(t->num_dirty >= 2 * info.slab_size / PAGE || arenaDecayMsGet(arena, extent_state_dirty) == 0);
}

TEST(Arena, Large)
{
    Arena * arena = newManualArena();
    size_t size = SIZE_CLASS_LARGE_MIN_CLASS + 1;
    SizeClassIdx idx = size_classes::sizeToIndex(size);
    void * p = arenaMallocHard(&thread_state, arena, size, idx, true, false);
    REQUIRE(p != nullptr);
    size_t usable_size = size_classes::sizeToUsableSize(size);
    CHECK_EQ(arenaAllocationSize(&thread_state, p), usable_size);
    CHECK_EQ(arenaAllocationSizeIfOwned(&thread_state, p), usable_size);
    CHECK(arenaOfPointer(&thread_state, p) == arena);
    for (size_t i = 0; i < usable_size; i += 4096)
        CHECK_EQ(static_cast<unsigned char *>(p)[i], 0);
    /// Cache-oblivious placement: a cacheline-aligned offset within the first page.
    Extent * extent = arena_extent_map_global.extentLookup(&thread_state, p);
    size_t offset = reinterpret_cast<uintptr_t>(p) - reinterpret_cast<uintptr_t>(extent->base());
    CHECK_EQ(offset % CACHE_LINE, 0u);
    CHECK(offset < PAGE);
    CHECK_EQ(extent->size(), usable_size + large_pad);
    /// Manual arenas track their large allocations.
    CHECK(arena->large.first() == extent);

    Totals * t = stats(arena);
    CHECK_EQ(t->large_stats[idx - SIZE_CLASS_NUM_BINS].num_allocations.readUnsynchronized(), 1u);
    CHECK_EQ(t->large_stats[idx - SIZE_CLASS_NUM_BINS].current_large_extents, 1u);
    CHECK_EQ(t->arena_stats.allocated_large, usable_size);

    /// In-place growth and shrinking.
    size_t new_size = 0;
    CHECK(!arenaReallocateNoMove(&thread_state, p, usable_size, usable_size + PAGE, 0, false, &new_size));
    CHECK_EQ(new_size, size_classes::sizeToUsableSize(usable_size + PAGE));
    CHECK(!arenaReallocateNoMove(&thread_state, p, new_size, usable_size, 0, false, &new_size));
    CHECK_EQ(new_size, usable_size);
    /// Large -> small cannot be done in place.
    CHECK(arenaReallocateNoMove(&thread_state, p, usable_size, 8, 0, false, &new_size));

    arenaDeallocateNoThreadCache(&thread_state, p);
    CHECK(arena->large.empty());
    t = stats(arena);
    CHECK_EQ(t->arena_stats.allocated_large, 0u);
    CHECK_EQ(t->arena_stats.num_allocations_large, t->arena_stats.num_deallocations_large);
}

TEST(Arena, SmallReallocateNoMove)
{
    Arena * arena = newManualArena();
    void * p = arenaMallocHard(&thread_state, arena, 20, size_classes::sizeToIndex(20), false, true);
    REQUIRE(p != nullptr);
    size_t usable_size = arenaAllocationSize(&thread_state, p);
    size_t new_size = 0;
    /// The same size class: in place.
    CHECK(!arenaReallocateNoMove(&thread_state, p, usable_size, usable_size - 1, 0, false, &new_size));
    CHECK_EQ(new_size, usable_size);
    /// Growing beyond the class: must move.
    CHECK(arenaReallocateNoMove(&thread_state, p, usable_size, usable_size + 1, 0, false, &new_size));
    /// Shrinking into a smaller class (usize_max < oldsize): must move; with extra reaching the old size: in place.
    CHECK(arenaReallocateNoMove(&thread_state, p, usable_size, 8, 0, false, &new_size));
    CHECK(!arenaReallocateNoMove(&thread_state, p, usable_size, 8, usable_size - 8, false, &new_size));
    void * q = arenaReallocate(&thread_state, arena, p, usable_size, 1000, 0, false, true, nullptr);
    REQUIRE(q != nullptr);
    CHECK(q != p);
    CHECK_EQ(arenaAllocationSize(&thread_state, q), size_classes::sizeToUsableSize(1000));
    arenaDeallocateNoThreadCache(&thread_state, q);
}

TEST(Arena, FillFlush)
{
    Arena * arena = newManualArena();
    const SizeClassIdx bin_idx = 2;
    const BinInfo & info = bin_infos[bin_idx];
    std::vector<void *> ptrs(info.num_regions * 3);

    CacheBinPtrArray array{CacheBinSize(ptrs.size())};
    array.ptr = ptrs.data();
    CacheBinStats merge{};
    merge.num_requests = 17;
    /// With an empty bin, the fill takes a fresh slab and uses it up while the total does not exceed `num_fill_max`.
    CacheBinSize num_fill_max = CacheBinSize(info.num_regions + 5);
    CacheBinSize filled = arenaPtrArrayFillSmall(&thread_state, arena, bin_idx, &array, 10, num_fill_max, merge);
    CHECK_EQ(unsigned(filled), info.num_regions);
    for (unsigned i = 1; i < filled; ++i)
        CHECK_EQ(reinterpret_cast<uintptr_t>(ptrs[i]) - reinterpret_cast<uintptr_t>(ptrs[0]), i * info.region_size);

    /// The next fill needs a new slab and takes only `num_fill_min` from it when the whole slab would exceed the max.
    array.ptr = ptrs.data() + filled;
    CacheBinSize filled2 = arenaPtrArrayFillSmall(&thread_state, arena, bin_idx, &array, 10, 12, merge);
    CHECK_EQ(unsigned(filled2), 10u);

    Totals * t = stats(arena);
    const BinStats & bin_stats_data = t->bin_stats[bin_idx].stats_data;
    CHECK_EQ(bin_stats_data.num_fills, 2u);
    CHECK_EQ(bin_stats_data.num_requests, 34u);
    CHECK_EQ(bin_stats_data.num_allocations, uint64_t(filled + filled2));
    CHECK_EQ(bin_stats_data.current_regions, size_t(filled + filled2));
    CHECK_EQ(bin_stats_data.num_slabs, 2u);

    /// Flush everything (stats merged into this arena).
    CacheBinPtrArray flush{CacheBinSize(filled + filled2)};
    flush.ptr = ptrs.data();
    CacheBinStats flush_stats{};
    flush_stats.num_requests = 5;
    arenaPtrArrayFlush(thread_state, bin_idx, &flush, unsigned(filled + filled2), true, arena, flush_stats);
    t = stats(arena);
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.current_regions, 0u);
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.current_slabs, 0u);
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.num_flushes, 1u);
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.num_requests, 39u);
    CHECK_EQ(t->num_active, 0u);

    /// A flush whose objects belong to another arena still merges the stats into `stats_arena`.
    Arena * other = newManualArena();
    void * p = arenaMallocHard(&thread_state, other, 1, 0, false, true);
    CacheBinPtrArray one{1};
    one.ptr = &p;
    arenaPtrArrayFlush(thread_state, 0, &one, 1, true, arena, flush_stats);
    t = stats(arena);
    CHECK_EQ(t->bin_stats[0].stats_data.num_flushes, 1u);
    CHECK_EQ(t->bin_stats[0].stats_data.num_requests, 5u);
    t = stats(other);
    CHECK_EQ(t->bin_stats[0].stats_data.num_flushes, 0u);
    CHECK_EQ(t->bin_stats[0].stats_data.num_deallocations, 1u);
}

TEST(Arena, FlushLarge)
{
    Arena * arena = newManualArena();
    size_t usable_size = SIZE_CLASS_LARGE_MIN_CLASS;
    SizeClassIdx idx = size_classes::sizeToIndex(usable_size);
    std::vector<void *> ptrs;
    for (int i = 0; i < 300; ++i)
        ptrs.push_back(arenaMallocHard(&thread_state, arena, usable_size, idx, false, false));
    CacheBinPtrArray array{CacheBinSize(ptrs.size())};
    array.ptr = ptrs.data();
    CacheBinStats merge{};
    merge.num_requests = 3;
    /// More than `CACHE_BIN_NUM_FLUSH_BATCH_MAX` pointers: processed in batches, the stats are merged once.
    arenaPtrArrayFlush(thread_state, idx, &array, unsigned(ptrs.size()), false, arena, merge);
    CHECK(arena->large.empty());
    Totals * t = stats(arena);
    const ArenaStatsLarge & l = t->large_stats[idx - SIZE_CLASS_NUM_BINS];
    CHECK_EQ(l.num_deallocations.readUnsynchronized(), 300u);
    CHECK_EQ(l.num_flushes.readUnsynchronized(), 1u);
    CHECK_EQ(l.num_requests.readUnsynchronized(), 300u + 3u);
    CHECK_EQ(l.current_large_extents, 0u);
}

TEST(Arena, FillSmallFresh)
{
    Arena * arena = newManualArena();
    const SizeClassIdx bin_idx = 1;
    const BinInfo & info = bin_infos[bin_idx];
    std::vector<void *> ptrs(info.num_regions * 2 + 3);
    size_t n = arenaFillSmallFresh(&thread_state, arena, bin_idx, ptrs.data(), ptrs.size(), true);
    CHECK_EQ(n, ptrs.size());
    Totals * t = stats(arena);
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.num_slabs, 3u);
    CHECK_EQ(t->bin_stats[bin_idx].stats_data.current_regions, ptrs.size());
    /// Manual arena: the two full slabs are tracked, the partial one goes to the non-full heap (`bin_lower_slab` with no
    /// current slab).
    Bin * bin = arenaGetBin(arena, bin_idx, 0);
    size_t num_full = 0;
    for (Extent * e = bin->slabs_full.first(); e != nullptr; e = bin->slabs_full.next(e))
        ++num_full;
    CHECK_EQ(num_full, 2u);
    CHECK(bin->current_slab == nullptr);
    Extent * partial = bin->slabs_non_full.first();
    REQUIRE(partial != nullptr);
    CHECK_EQ(partial->numFree(), info.num_regions - 3);
    for (void * p : ptrs)
        arenaDeallocateNoThreadCache(&thread_state, p);
    CHECK(bin->slabs_full.empty());
}

TEST(Arena, ResetDestroy)
{
    Arena * arena = newManualArena();
    unsigned idx = arenaIdxGet(arena);
    for (int i = 0; i < 1000; ++i)
        arenaMallocHard(
            &thread_state, arena, size_t(1) + size_t(i) % 3000, size_classes::sizeToIndex(size_t(1) + size_t(i) % 3000), false, true);
    for (int i = 0; i < 10; ++i)
        arenaMallocHard(
            &thread_state,
            arena,
            SIZE_CLASS_LARGE_MIN_CLASS * size_t(i + 1),
            size_classes::sizeToIndex(SIZE_CLASS_LARGE_MIN_CLASS * size_t(i + 1)),
            false,
            false);
    CHECK(stats(arena)->num_active > 0);
    arenaReset(thread_state, arena);
    Totals * t = stats(arena);
    CHECK_EQ(t->num_active, 0u);
    CHECK(arena->large.empty());
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
        CHECK_EQ(t->bin_stats[i].stats_data.current_regions, 0u);
    arenaDecay(&thread_state, arena, false, true);
    t = stats(arena);
    CHECK_EQ(t->num_dirty, 0u);
    CHECK_EQ(t->num_muzzy, 0u);
    arenaDestroy(thread_state, arena);
    CHECK(arenaGet(&thread_state, idx, false) == nullptr);
}

TEST(Arena, HugeArena)
{
    bootOnce();
    if (huge_arena_idx == 0)
        return; /// Disabled by the configuration.
    CHECK_EQ(oversize_threshold, options.oversize_threshold);
    CHECK_EQ(a0->page_allocator_shard.page_allocator.oversize_threshold.load(), oversize_threshold);
    CHECK_EQ(manual_arena_base, huge_arena_idx + 1);
    Arena * huge = arenaChooseHuge(thread_state);
    REQUIRE(huge != nullptr);
    CHECK_EQ(arenaIdxGet(huge), huge_arena_idx);
    CHECK(arenaIsAuto(huge));
    char name[ARENA_NAME_LEN];
    arenaNameGet(huge, name);
    CHECK_EQ(std::strcmp(name, "auto_oversize"), 0);
    /// Without background threads the huge arena purges eagerly.
    CHECK_EQ(arenaDecayMsGet(huge, extent_state_dirty), 0);
    CHECK(arenaChooseHuge(thread_state) == huge);
}

TEST(Arena, Binding)
{
    bootOnce();
    ThreadState & t = thread_state;
    CHECK(t.arena == nullptr);
    /// One auto arena: the thread is bound to arena 0 for both kinds of allocations.
    unsigned before = arenaNumThreadsGet(a0, false);
    unsigned before_internal = arenaNumThreadsGet(a0, true);
    CHECK(arenaChoose(t, nullptr) == a0);
    CHECK(t.arena == a0);
    CHECK(t.internal_arena == a0);
    CHECK(arenaChooseInternal(t, nullptr) == a0);
    CHECK_EQ(arenaNumThreadsGet(a0, false), before + 1);
    CHECK_EQ(arenaNumThreadsGet(a0, true), before_internal + 1);
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
        CHECK_EQ(unsigned(t.bin_shards.bin_shard[i]), 0u);

    /// Huge requests from auto arenas go to the huge arena; explicit arenas are never redirected.
    if (huge_arena_idx != 0)
    {
        CHECK(arenaChooseMaybeHuge(t, nullptr, oversize_threshold) == arenaGet(&t, huge_arena_idx, false));
        CHECK(arenaChooseMaybeHuge(t, nullptr, oversize_threshold - 1) == a0);
        CHECK(arenaChooseMaybeHuge(t, a0, oversize_threshold) == a0);
    }

    Arena * manual = newManualArena();
    arenaMigrate(t, a0, manual);
    CHECK(t.arena == manual);
    CHECK_EQ(arenaNumThreadsGet(manual, false), 1u);
    /// Threads bound to a manual arena are not redirected to the huge arena.
    CHECK(arenaChooseMaybeHuge(t, nullptr, SIZE_CLASS_LARGE_MAX_CLASS) == manual);

    arenaCleanup(t);
    internalArenaCleanup(t);
    CHECK(t.arena == nullptr);
    CHECK(t.internal_arena == nullptr);
    CHECK_EQ(arenaNumThreadsGet(manual, false), 0u);
    CHECK_EQ(arenaNumThreadsGet(a0, true), before_internal);
}

TEST(Arena, DecayMs)
{
    Arena * arena = newManualArena();
    CHECK(!arenaDecayMsSet(&thread_state, arena, extent_state_dirty, 0));
    CHECK_EQ(arenaDecayMsGet(arena, extent_state_dirty), 0);
    CHECK(arenaDecayMsSet(&thread_state, arena, extent_state_dirty, -2));
    /// With immediate dirty decay, freed slabs are purged right away.
    void * p = arenaMallocHard(&thread_state, arena, 8, 0, false, true);
    arenaDeallocateNoThreadCache(&thread_state, p);
    CHECK_EQ(stats(arena)->num_dirty, 0u);

    ssize_t old = arenaDirtyDecayMsDefaultGet();
    CHECK(arenaDirtyDecayMsDefaultSet(-2));
    CHECK(!arenaDirtyDecayMsDefaultSet(1234));
    CHECK_EQ(arenaDirtyDecayMsDefaultGet(), 1234);
    CHECK(!arenaDirtyDecayMsDefaultSet(old));
    CHECK(!arenaMuzzyDecayMsDefaultSet(arenaMuzzyDecayMsDefaultGet()));

    size_t old_limit = 0;
    size_t new_limit = size_t(1) << 30;
    CHECK(!arenaRetainGrowLimitGetSet(thread_state, arena, &old_limit, &new_limit));
    size_t check_limit = 0;
    CHECK(!arenaRetainGrowLimitGetSet(thread_state, arena, &check_limit, nullptr));
    CHECK(check_limit <= new_limit);
}

TEST(Arena, BootstrapAllocation)
{
    bootOnce();
    /// `arena0InternalAllocate` needs `mallocInitA0` (Init); here only the deallocation side of internal accounting is checked
    /// through `arenaInternal*`.
    size_t before = arenaInternalGet(a0);
    arenaInternalAdd(a0, 100);
    CHECK_EQ(arenaInternalGet(a0), before + 100);
    arenaInternalSub(a0, 100);
    CHECK_EQ(arenaInternalGet(a0), before);
}
