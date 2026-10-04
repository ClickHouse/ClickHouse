#pragma once

/// The arena: the bins of small size classes, large allocations, and the page allocator shard (`pa_shard`).
/// jemalloc: `arena.c`, `arena_structs.h`, `arena_stats.h`, `arena_types.h`, `arena_externs.h`, `arena_inlines_a.h`,
/// the tcache-independent part of `arena_inlines_b.h`, and `large.c` / `large_externs.h` (ArenaLarge.cpp).
///
/// Header layering (to break the cycle with the thread cache):
/// - Arena.h (this file): the data layout and all declarations; inline helpers that need neither the arena table nor
///   the tcache.
/// - Arenas.h: the global arena table (`arenas[]`, `narenas_auto`, `manual_arena_base`), `arenaGet`, `arenaIsAuto`,
///   `arenaGetFromEdata`, `arenaChoose*`, percpu arenas, bootstrap allocation helpers.
/// - ThreadCache.h (includes both of the above): the tcache.
/// - ArenaInlines.h (includes ThreadCache.h): the front-end dispatch helpers that call into the tcache
///   (`arenaMalloc`, `arenaDalloc`, `arenaSdalloc`, ...), and the ones built on the emap (`arenaSalloc`, ...).
///
/// The `Arena` object is followed in memory by its bins (`arena_bin_offsets`), and is allocated from its base (zeroed
/// memory). The functions mirror jemalloc's free functions (`arena_x(arena, ...)` -> `arenaX(arena, ...)`).

#include <allocator/Base.h>
#include <allocator/Bin.h>
#include <allocator/CacheBin.h>
#include <allocator/Common.h>
#include <allocator/Decay.h>
#include <allocator/Extent.h>
#include <allocator/ExtentHooks.h>
#include <allocator/IntrusiveList.h>
#include <allocator/Mutex.h>
#include <allocator/NsTime.h>
#include <allocator/PageAllocator.h>
#include <allocator/Prng.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadCacheData.h>
#include <allocator/ThreadState.h>

#include <atomic>
#include <cstdint>
#include <sys/types.h>

namespace jemalloc
{

struct ThreadCache;
struct ThreadCacheSlow;
class ProfThreadContext;

/// --- arena_types.h, arena_externs.h --------------------------------------------------------------------------------

/// Default decay times in milliseconds. jemalloc: DIRTY_DECAY_MS_DEFAULT, MUZZY_DECAY_MS_DEFAULT
inline constexpr ssize_t DIRTY_DECAY_MS_DEFAULT = 10 * 1000;
inline constexpr ssize_t MUZZY_DECAY_MS_DEFAULT = 0;

/// Maximum length of the arena name. jemalloc: ARENA_NAME_LEN
inline constexpr size_t ARENA_NAME_LEN = 32;

/// When `allocation_size >= oversize_threshold`, use the dedicated huge arena (unless an arena index is explicitly
/// specified). 0 disables the feature. jemalloc: OVERSIZE_THRESHOLD_DEFAULT
inline constexpr size_t OVERSIZE_THRESHOLD_DEFAULT = size_t(8) << 20;

/// When the amount of pages to be purged exceeds this amount, deferred purge should happen.
/// jemalloc: ARENA_DEFERRED_PURGE_NPAGES_THRESHOLD
inline constexpr uint64_t ARENA_DEFERRED_PURGE_NPAGES_THRESHOLD = 1024;

/// jemalloc: arena_config_t
struct ArenaConfig
{
    /// Extent hooks to be used for the arena (always the default table: custom hooks are not supported).
    const extent_hooks_t * extent_hooks;
    /// Use extent hooks for metadata (base) allocations when true.
    bool metadata_use_hooks;
};

/// jemalloc: arena_config_default
extern const ArenaConfig arena_config_default;

/// `arena_bin_offsets[binind]` is the offset (from the arena) of the first bin shard for size class `binind`.
/// jemalloc: arena_bin_offsets
extern constinit uint32_t arena_bin_offsets[SC_NBINS];

/// The total number of bin shards of an arena (sum of `bin_infos[i].n_shards`). jemalloc: nbins_total (static)
extern constinit unsigned arena_nbins_total;

/// The effective oversize threshold (`opt.oversize_threshold` validated by `arenaInitHuge`).
/// jemalloc: oversize_threshold
extern constinit size_t oversize_threshold;

/// a0 is used to handle huge requests before malloc init completes. After that, `huge_arena_ind` is updated to point
/// to the actual huge arena, which is the last one of the auto arenas.
/// jemalloc: huge_arena_ind
extern constinit unsigned huge_arena_ind;

/// --- Profiling info of an allocation (prof_structs.h; used by the arena/large prof hooks) --------------------------

/// jemalloc: prof_info_t
struct ProfInfo
{
    /// Time when the allocation was made.
    NsTime alloc_time;
    /// Points to the `ProfThreadContext` corresponding to the allocation.
    ProfThreadContext * alloc_tctx;
    /// Allocation request size.
    size_t alloc_size;
};

/// jemalloc: PROF_TCTX_SENTINEL
inline ProfThreadContext * const PROF_TCTX_SENTINEL = reinterpret_cast<ProfThreadContext *>(uintptr_t(1));

/// jemalloc: PROF_SAMPLE_ALIGNMENT
inline constexpr size_t PROF_SAMPLE_ALIGNMENT = PAGE;

/// jemalloc: prof_tctx_is_valid
JE_ALWAYS_INLINE bool profTctxIsValid(const ProfThreadContext * tctx)
{
    return tctx != nullptr && tctx != PROF_TCTX_SENTINEL;
}

/// --- arena_stats.h -------------------------------------------------------------------------------------------------

/// jemalloc: arena_stats_large_t
struct ArenaStatsLarge
{
    /// Total number of large allocation/deallocation requests served directly by the arena.
    LockedU64 nmalloc;
    LockedU64 ndalloc;
    /// Total large active bytes (allocated - deallocated) served directly by the arena.
    LockedU64 active_bytes;
    /// Number of allocation requests that correspond to this size class. This includes requests served by tcache,
    /// though tcache only periodically merges into this counter.
    LockedU64 nrequests; /// Partially derived.
    /// Number of tcache fills / flushes for large (similarly, periodically merged). Note that there is no large
    /// tcache batch-fill currently (i.e. only fill 1 at a time); however flush may be batched.
    LockedU64 nfills; /// Partially derived.
    LockedU64 nflushes; /// Partially derived.
    /// Current number of allocations of this size class.
    size_t curlextents = 0; /// Derived.
};

static_assert(sizeof(ArenaStatsLarge) == 56);

/// Arena stats. Fields marked "derived" are not directly maintained within the arena code; their values are derived
/// during stats merge requests. There is no stats mutex (`JEMALLOC_ATOMIC_U64`).
/// jemalloc: arena_stats_t
struct ArenaStats
{
    /// `resident` includes the base stats -- that's why it lives here and not in `PaShardStats`.
    size_t base = 0; /// Derived.
    size_t metadata_edata = 0; /// Derived.
    size_t metadata_rtree = 0; /// Derived.
    size_t resident = 0; /// Derived.
    size_t metadata_thp = 0; /// Derived.
    size_t mapped = 0; /// Derived.

    std::atomic<size_t> internal{0};

    size_t allocated_large = 0; /// Derived.
    uint64_t nmalloc_large = 0; /// Derived.
    uint64_t ndalloc_large = 0; /// Derived.
    uint64_t nfills_large = 0; /// Derived.
    uint64_t nflushes_large = 0; /// Derived.
    uint64_t nrequests_large = 0; /// Derived.

    /// The stats logically owned by the pa_shard in the same arena. This lives here only because it's convenient for
    /// the purposes of the ctl module -- it only knows about the single arena stats.
    PaShardStats pa_shard_stats;

    /// Number of bytes cached in tcache associated with this arena.
    size_t tcache_bytes = 0; /// Derived.
    size_t tcache_stashed_bytes = 0; /// Derived.

    MutexProfData mutex_prof_data[mutex_prof_num_arena_mutexes];

    /// One element for each large size class.
    ArenaStatsLarge lstats[SC_NSIZES - SC_NBINS];

    /// Arena uptime.
    NsTime uptime = NsTime::zero();
};

static_assert(offsetof(ArenaStats, internal) == 48);
static_assert(offsetof(ArenaStats, pa_shard_stats) == 104);
static_assert(offsetof(ArenaStats, mutex_prof_data) == 200);
static_assert(offsetof(ArenaStats, lstats) == 968);
static_assert(sizeof(ArenaStats) == 968 + 56 * (SC_NSIZES - SC_NBINS) + 8);

/// jemalloc: arena_stats_large_flush_nrequests_add
JE_ALWAYS_INLINE void arenaStatsLargeFlushNrequestsAdd(ThreadState * /*tsdn*/, ArenaStats * arena_stats, szind_t szind, uint64_t nrequests)
{
    ArenaStatsLarge & lstats = arena_stats->lstats[szind - SC_NBINS];
    lstats.nrequests.inc(nrequests);
    lstats.nflushes.inc(1);
}

/// --- arena_structs.h -----------------------------------------------------------------------------------------------

/// jemalloc: arena_t
class alignas(CACHELINE) Arena
{
public:
    constexpr Arena() = default;

    Arena(const Arena &) = delete;
    Arena & operator=(const Arena &) = delete;

    /// Creates the arena (and, for `ind != 0`, its base) and publishes it in `arenas[ind]`. Returns null on failure.
    /// jemalloc: arena_new
    static Arena * create(ThreadState * tsdn, unsigned ind, const ArenaConfig * config);

    /// The bins follow the structure (cacheline-aligned); use `arenaGetBin`.
    /// jemalloc: all_bins
    JE_ALWAYS_INLINE Bin * allBins() { return reinterpret_cast<Bin *>(reinterpret_cast<std::byte *>(this) + sizeof(Arena)); }

    /// Number of threads currently assigned to this arena. Each thread has two distinct assignments, one for
    /// application-serving allocation, and the other for internal metadata allocation. Internal metadata must not be
    /// allocated from arenas explicitly created via the `arenas.create` mallctl, because the `arena.<i>.reset`
    /// mallctl indiscriminately discards all allocations for the affected arena.
    ///   0: Application allocation.
    ///   1: Internal metadata allocation.
    std::atomic<unsigned> nthreads[2] = {};

    /// Next bin shard for binding new threads.
    std::atomic<unsigned> binshard_next{0};

    /// When percpu_arena is enabled, to amortize the cost of reading / updating the current CPU id, track the most
    /// recent thread accessing this arena, and only read CPU if there is a mismatch.
    ThreadState * last_thd = nullptr;

    /// Synchronization: internal.
    ArenaStats stats;

    /// Lists of tcaches and cache bin array descriptors for extant threads associated with this arena. Stats from
    /// these are merged incrementally, and at exit if `opt.stats_print` is enabled. Synchronization: `tcache_ql_mtx`.
    IntrusiveList<ThreadCacheSlow, &ThreadCacheSlow::link> tcache_ql;
    IntrusiveList<CacheBinArrayDescriptor, &CacheBinArrayDescriptor::link> cache_bin_array_descriptor_ql;
    /// "tcache_ql", `MutexRank::TCACHE_QL`.
    Mutex tcache_ql_mtx;

    /// Represents a `DssPrec`, but atomically.
    std::atomic<unsigned> dss_prec{0};

    /// Extant large allocations (only tracked for manual arenas). Synchronization: `large_mtx`.
    ExtentListActive large;
    /// Synchronizes all large allocation/update/deallocation. "arena_large", `MutexRank::ARENA_LARGE`.
    Mutex large_mtx;

    /// The page-level allocator shard this arena uses.
    PaShard pa_shard;

    /// A cached copy of `base->indGet()`. This can get accessed on hot paths; looking it up in base requires an
    /// extra pointer hop / cache miss.
    unsigned ind = 0;

    /// Base allocator, from which arena metadata are allocated. Synchronization: internal.
    Base * base = nullptr;

    /// Used to determine uptime. Read-only after initialization.
    NsTime create_time = NsTime::zero();

    /// The name of the arena.
    char name[ARENA_NAME_LEN] = {};
};

/// Measured from the C build (`stats.metadata` depends on the size of the arena allocation).
#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(Arena) == (LG_PAGE == 12 ? 80768 : (LG_PAGE == 14 ? 77952 : 75200)), "arena_t size (aarch64 glibc)");
static_assert(offsetof(Arena, stats) == 24);
static_assert(offsetof(Arena, pa_shard) == (LG_PAGE == 12 ? 12288 : (LG_PAGE == 14 ? 11840 : 11392)));
#endif

/// --- arena_inlines_a.h ---------------------------------------------------------------------------------------------

/// jemalloc: arena_ind_get
JE_ALWAYS_INLINE unsigned arenaIndGet(const Arena * arena)
{
    return arena->ind;
}

/// jemalloc: arena_internal_add
JE_ALWAYS_INLINE void arenaInternalAdd(Arena * arena, size_t size)
{
    arena->stats.internal.fetch_add(size, std::memory_order_relaxed);
}

/// jemalloc: arena_internal_sub
JE_ALWAYS_INLINE void arenaInternalSub(Arena * arena, size_t size)
{
    arena->stats.internal.fetch_sub(size, std::memory_order_relaxed);
}

/// jemalloc: arena_internal_get
JE_ALWAYS_INLINE size_t arenaInternalGet(Arena * arena)
{
    return arena->stats.internal.load(std::memory_order_relaxed);
}

/// --- arena_inlines_b.h (the parts that need neither the arena table nor the tcache) --------------------------------

/// jemalloc: arena_get_bin
JE_ALWAYS_INLINE Bin * arenaGetBin(Arena * arena, szind_t binind, unsigned binshard)
{
    Bin * shard0 = reinterpret_cast<Bin *>(reinterpret_cast<std::byte *>(arena) + arena_bin_offsets[binind]);
    return shard0 + binshard;
}

/// jemalloc: arena_get_ehooks
JE_ALWAYS_INLINE ExtentHooks * arenaGetEhooks(Arena * arena)
{
    return arena->base->ehooksGet();
}

/// jemalloc: arena_decay (declared here for `arenaDecayTicks`)
void arenaDecay(ThreadState * tsdn, Arena * arena, bool is_background_thread, bool all);

/// We use the `TickerGeom` to avoid having per-arena state in the tsd. Instead of having a countdown-until-decay timer
/// running for every arena in every thread, we flip a coin once per tick, whose probability of coming up heads is
/// 1/nticks; this is effectively the operation of the `TickerGeom`. Each arena has the same chance of a coinflip
/// coming up heads (1/ARENA_DECAY_NTICKS_PER_UPDATE), so we can use a single ticker for all of them.
/// jemalloc: arena_decay_ticks
JE_ALWAYS_INLINE void arenaDecayTicks(ThreadState * tsdn, Arena * arena, unsigned nticks)
{
    if (JE_UNLIKELY(tsdn == nullptr))
        return;
    ThreadState & tsd = *tsdn;
    if (JE_UNLIKELY(tsd.arena_decay_ticker.ticks(tsd.prngState(), int32_t(nticks), tsd.reentrancyLevel() > 0)))
        arenaDecay(tsdn, arena, false, false);
}

/// jemalloc: arena_decay_tick
JE_ALWAYS_INLINE void arenaDecayTick(ThreadState * tsdn, Arena * arena)
{
    arenaDecayTicks(tsdn, arena, 1);
}

/// Eagerly detect double free and sized dealloc bugs for large sizes (only with `config_opt_safety_checks`, which
/// is off). Returns true if the deallocation must be skipped.
/// jemalloc: large_dalloc_safety_checks
JE_ALWAYS_INLINE bool largeDallocSafetyChecks(Extent * edata, const void * ptr, size_t input_size)
{
    if constexpr (!config::opt_safety_checks)
    {
        return false;
    }
    else
    {
        if (JE_UNLIKELY(edata == nullptr || edata->state() != extent_state_active))
        {
            safetyCheckFail(
                "Invalid deallocation detected: pages being freed (%p) not currently active, possibly caused by double free bugs.",
                ptr);
            return true;
        }
        if (JE_UNLIKELY(input_size != edata->usize() || input_size > SC_LARGE_MAXCLASS))
        {
            safetyCheckFailSizedDealloc(/* current_dealloc */ true, ptr, /* true_size */ edata->usize(), input_size);
            return true;
        }
        return false;
    }
}

/// Randomizes the start of a large allocation within its first page (cache-oblivious large allocations). Uses the
/// thread's PRNG state (shared with the decay ticker: the stream is part of the observable behavior); without tsd,
/// a PRNG seeded with the address of a stack variable.
/// jemalloc: arena_cache_oblivious_randomize
JE_ALWAYS_INLINE void arenaCacheObliviousRandomize(ThreadState * tsdn, Arena * /*arena*/, Extent * edata, size_t alignment)
{
    JE_ASSERT(edata->base() == edata->addr());

    if (alignment < PAGE)
    {
        unsigned lg_range = LG_PAGE - lgFloor(cachelineCeiling(alignment));
        size_t r;
        if (tsdn != nullptr)
        {
            r = size_t(prngLgRangeU64(tsdn->prngState(), lg_range));
        }
        else
        {
            uint64_t stack_value = uint64_t(reinterpret_cast<uintptr_t>(&r));
            r = size_t(prngLgRangeU64(stack_value, lg_range));
        }
        uintptr_t random_offset = uintptr_t(r) << (LG_PAGE - lg_range);
        edata->setAddr(static_cast<std::byte *>(edata->addr()) + random_offset);
        JE_ASSERT(alignmentAddrToBase(edata->addr(), alignment) == edata->addr());
    }
}

/// --- arena.c -------------------------------------------------------------------------------------------------------

/// jemalloc: arena_new
Arena * arenaNew(ThreadState * tsdn, unsigned ind, const ArenaConfig * config);

/// Computes the decay defaults, the bin division magics and the bin offsets. `hpa` is ignored (HPA is dropped).
/// Returns true on error.
/// jemalloc: arena_boot
bool arenaBoot(const SizeClassData * sc_data, Base * base, bool hpa);

/// Sets up the oversize (huge) arena: reserves its index if `opt.oversize_threshold` is a valid large size, and
/// patches the threshold of arena 0 (created before the options were parsed). Returns whether it is enabled.
/// jemalloc: arena_init_huge
bool arenaInitHuge(ThreadState * tsdn, Arena * a0);

/// Returns the huge arena (creating it on demand).
/// jemalloc: arena_choose_huge
Arena * arenaChooseHuge(ThreadState & tsd);

/// jemalloc: arena_basic_stats_merge
void arenaBasicStatsMerge(
    ThreadState * tsdn,
    Arena * arena,
    unsigned * nthreads,
    const char ** dss,
    ssize_t * dirty_decay_ms,
    ssize_t * muzzy_decay_ms,
    size_t * nactive,
    size_t * ndirty,
    size_t * nmuzzy);

/// `bstats` has `SC_NBINS` elements, `lstats` has `SC_NSIZES - SC_NBINS`, `estats` has `SC_NPSIZES`. The HPA stats
/// output of jemalloc is dropped (they would only be merged if HPA was ever used).
/// jemalloc: arena_stats_merge
void arenaStatsMerge(
    ThreadState * tsdn,
    Arena * arena,
    unsigned * nthreads,
    const char ** dss,
    ssize_t * dirty_decay_ms,
    ssize_t * muzzy_decay_ms,
    size_t * nactive,
    size_t * ndirty,
    size_t * nmuzzy,
    ArenaStats * astats,
    BinStatsData * bstats,
    ArenaStatsLarge * lstats,
    PacExtentStats * estats);

/// React to deferred work generated by a PAI function.
/// jemalloc: arena_handle_deferred_work
void arenaHandleDeferredWork(ThreadState * tsdn, Arena * arena);

/// jemalloc: arena_extent_alloc_large
Extent * arenaExtentAllocLarge(ThreadState * tsdn, Arena * arena, size_t usize, size_t alignment, bool zero);

/// jemalloc: arena_extent_dalloc_large_prep
void arenaExtentDallocLargePrep(ThreadState * tsdn, Arena * arena, Extent * edata);

/// jemalloc: arena_extent_ralloc_large_shrink
void arenaExtentRallocLargeShrink(ThreadState * tsdn, Arena * arena, Extent * edata, size_t oldusize);

/// jemalloc: arena_extent_ralloc_large_expand
void arenaExtentRallocLargeExpand(ThreadState * tsdn, Arena * arena, Extent * edata, size_t oldusize);

/// Returns true on error.
/// jemalloc: arena_decay_ms_set
bool arenaDecayMsSet(ThreadState * tsdn, Arena * arena, ExtentState state, ssize_t decay_ms);

/// jemalloc: arena_decay_ms_get
ssize_t arenaDecayMsGet(Arena * arena, ExtentState state);

/// Called from background threads.
/// jemalloc: arena_do_deferred_work
void arenaDoDeferredWork(ThreadState * tsdn, Arena * arena);

/// Frees all allocations of a (manual) arena. The caller guarantees that no concurrent operations are happening in
/// this arena.
/// jemalloc: arena_reset
void arenaReset(ThreadState & tsd, Arena * arena);

/// jemalloc: arena_destroy
void arenaDestroy(ThreadState & tsd, Arena * arena);

/// Fills `arr->ptr[0 .. result)` with at least `nfill_min` (unless OOM) and at most `nfill_max` regions; merges the
/// tcache request counter `merge_stats` into the bin stats.
/// jemalloc: arena_ptr_array_fill_small
cache_bin_sz_t arenaPtrArrayFillSmall(
    ThreadState * tsdn,
    Arena * arena,
    szind_t binind,
    CacheBinPtrArray * arr,
    cache_bin_sz_t nfill_min,
    cache_bin_sz_t nfill_max,
    CacheBinStats merge_stats);

/// Allocates `nfill` regions from fresh slabs (`experimental.batch_alloc`). Returns the number allocated.
/// jemalloc: arena_fill_small_fresh
size_t arenaFillSmallFresh(ThreadState * tsdn, Arena * arena, szind_t binind, void ** ptrs, size_t nfill, bool zero);

/// The tcache bypass path: `arena` may be null (then chosen from the thread, possibly redirected to the huge arena).
/// jemalloc: arena_malloc_hard
void * arenaMallocHard(ThreadState * tsdn, Arena * arena, size_t size, szind_t ind, bool zero, bool slab);

/// jemalloc: arena_palloc
void * arenaPalloc(ThreadState * tsdn, Arena * arena, size_t usize, size_t alignment, bool zero, bool slab, ThreadCache * tcache);

/// Turns a sampled small allocation (served from a large extent of `bumped_usize`) into one that reports `usize`.
/// jemalloc: arena_prof_promote
void arenaProfPromote(ThreadState * tsdn, void * ptr, size_t usize, size_t bumped_usize);

/// jemalloc: arena_dalloc_promoted
void arenaDallocPromoted(ThreadState * tsdn, void * ptr, ThreadCache * tcache, bool slow_path);

/// jemalloc: arena_slab_dalloc
void arenaSlabDalloc(ThreadState * tsdn, Arena * arena, Extent * slab);

/// jemalloc: arena_dalloc_small
void arenaDallocSmall(ThreadState * tsdn, void * ptr);

/// In practice, pointers are flushed back to their original allocation arenas, so multiple arenas may be involved
/// here. `stats_arena` indicates where the cache stats (`merge_stats`, a snapshot taken by the caller) are merged.
/// Processes the pointers in batches of at most `CACHE_BIN_NFLUSH_BATCH_MAX`; reorders `arr->ptr`.
/// jemalloc: arena_ptr_array_flush
void arenaPtrArrayFlush(
    ThreadState & tsd,
    szind_t binind,
    CacheBinPtrArray * arr,
    unsigned nflush,
    bool small,
    Arena * stats_arena,
    CacheBinStats merge_stats);

/// Returns true if the allocation could not be resized in place. `*newsize` is the resulting usable size.
/// jemalloc: arena_ralloc_no_move
bool arenaRallocNoMove(ThreadState * tsdn, void * ptr, size_t oldsize, size_t size, size_t extra, bool zero, size_t * newsize);

/// The `hook_args` of jemalloc are dropped (`experimental.hooks.install` is not supported).
/// jemalloc: arena_ralloc
void * arenaRalloc(
    ThreadState * tsdn,
    Arena * arena,
    void * ptr,
    size_t oldsize,
    size_t size,
    size_t alignment,
    bool zero,
    bool slab,
    ThreadCache * tcache);

/// jemalloc: arena_dss_prec_get
DssPrec arenaDssPrecGet(Arena * arena);

/// Returns true on error.
/// jemalloc: arena_dss_prec_set
bool arenaDssPrecSet(Arena * arena, DssPrec dss_prec);

/// Copies the name (with the terminating zero) into `name` (at least `ARENA_NAME_LEN` bytes).
/// jemalloc: arena_name_get
void arenaNameGet(Arena * arena, char * name);

/// jemalloc: arena_name_set
void arenaNameSet(Arena * arena, const char * name);

/// jemalloc: arena_dirty_decay_ms_default_get, arena_dirty_decay_ms_default_set (returns true on error)
ssize_t arenaDirtyDecayMsDefaultGet();
bool arenaDirtyDecayMsDefaultSet(ssize_t decay_ms);

/// jemalloc: arena_muzzy_decay_ms_default_get, arena_muzzy_decay_ms_default_set (returns true on error)
ssize_t arenaMuzzyDecayMsDefaultGet();
bool arenaMuzzyDecayMsDefaultSet(ssize_t decay_ms);

/// Returns true on error.
/// jemalloc: arena_retain_grow_limit_get_set
bool arenaRetainGrowLimitGetSet(ThreadState & tsd, Arena * arena, size_t * old_limit, size_t * new_limit);

/// jemalloc: arena_nthreads_get
JE_ALWAYS_INLINE unsigned arenaNthreadsGet(Arena * arena, bool internal)
{
    return arena->nthreads[internal].load(std::memory_order_relaxed);
}

/// jemalloc: arena_nthreads_inc
JE_ALWAYS_INLINE void arenaNthreadsInc(Arena * arena, bool internal)
{
    arena->nthreads[internal].fetch_add(1, std::memory_order_relaxed);
}

/// jemalloc: arena_nthreads_dec
JE_ALWAYS_INLINE void arenaNthreadsDec(Arena * arena, bool internal)
{
    arena->nthreads[internal].fetch_sub(1, std::memory_order_relaxed);
}

/// The fork phases (see Fork.cpp for the order across all modules).
/// jemalloc: arena_prefork0 .. arena_prefork8, arena_postfork_parent, arena_postfork_child
void arenaPrefork0(ThreadState * tsdn, Arena * arena);
void arenaPrefork1(ThreadState * tsdn, Arena * arena);
void arenaPrefork2(ThreadState * tsdn, Arena * arena);
void arenaPrefork3(ThreadState * tsdn, Arena * arena);
void arenaPrefork4(ThreadState * tsdn, Arena * arena);
void arenaPrefork5(ThreadState * tsdn, Arena * arena);
void arenaPrefork6(ThreadState * tsdn, Arena * arena);
void arenaPrefork7(ThreadState * tsdn, Arena * arena);
void arenaPrefork8(ThreadState * tsdn, Arena * arena);
void arenaPostforkParent(ThreadState * tsdn, Arena * arena);
/// `tsdn` must not be null (it is the forking thread's tsd).
void arenaPostforkChild(ThreadState * tsdn, Arena * arena);

inline Arena * Arena::create(ThreadState * tsdn, unsigned ind, const ArenaConfig * config)
{
    return arenaNew(tsdn, ind, config);
}

/// --- large.c (ArenaLarge.cpp) --------------------------------------------------------------------------------------

/// jemalloc: large_malloc
void * largeMalloc(ThreadState * tsdn, Arena * arena, size_t usize, bool zero);

/// jemalloc: large_palloc
void * largePalloc(ThreadState * tsdn, Arena * arena, size_t usize, size_t alignment, bool zero);

/// Returns true if the allocation could not be resized in place to a usable size in [usize_min, usize_max].
/// jemalloc: large_ralloc_no_move
bool largeRallocNoMove(ThreadState * tsdn, Extent * edata, size_t usize_min, size_t usize_max, bool zero);

/// The `hook_args` of jemalloc are dropped.
/// jemalloc: large_ralloc
void * largeRalloc(ThreadState * tsdn, Arena * arena, void * ptr, size_t usize, size_t alignment, bool zero, ThreadCache * tcache);

/// Requires holding `large_mtx` of the extent's arena if it is a manual arena.
/// jemalloc: large_dalloc_prep_locked
void largeDallocPrepLocked(ThreadState * tsdn, Extent * edata);

/// jemalloc: large_dalloc_finish
void largeDallocFinish(ThreadState * tsdn, Extent * edata);

/// jemalloc: large_dalloc
void largeDalloc(ThreadState * tsdn, Extent * edata);

/// jemalloc: large_salloc
JE_ALWAYS_INLINE size_t largeSalloc(ThreadState * /*tsdn*/, const Extent * edata)
{
    return edata->usize();
}

/// jemalloc: large_prof_info_get
void largeProfInfoGet(ThreadState & tsd, Extent * edata, ProfInfo * prof_info, bool reset_recent);

/// jemalloc: large_prof_tctx_reset
void largeProfTctxReset(Extent * edata);

/// Also clears the fork's `e_prof_frag_tracked` flag (it may hold garbage from a previous slab use of the extent)
/// before the tctx is published.
/// jemalloc: large_prof_info_set
void largeProfInfoSet(Extent * edata, ProfThreadContext * tctx, size_t size);

/// --- Hooks provided by other modules (BackgroundThread.cpp, ProfData.cpp, ProfRecent.cpp, Prof.cpp) ---------------

/// jemalloc: background_thread_info_t (BackgroundThread)
struct BackgroundThreadInfo;

/// jemalloc: arena_background_thread_info_get
BackgroundThreadInfo * arenaBackgroundThreadInfoGet(Arena * arena);
/// `info->mtx`
Mutex & backgroundThreadInfoMutex(BackgroundThreadInfo * info);
/// jemalloc: background_thread_is_started
bool backgroundThreadIsStarted(BackgroundThreadInfo * info);
/// jemalloc: background_thread_indefinite_sleep
bool backgroundThreadIndefiniteSleep(BackgroundThreadInfo * info);
/// jemalloc: background_thread_wakeup_time_get
uint64_t backgroundThreadWakeupTimeGet(BackgroundThreadInfo * info);
/// `info->npages_to_purge_new`
size_t & backgroundThreadNpagesToPurgeNew(BackgroundThreadInfo * info);
/// jemalloc: background_thread_wakeup_early
void backgroundThreadWakeupEarly(BackgroundThreadInfo * info, NsTime * remaining_sleep);

/// jemalloc: prof_frag_untrack (ProfData.cpp)
void profFragUntrack(ThreadState & tsd, Extent * edata, ProfThreadContext * tctx);
/// jemalloc: prof_recent_alloc_reset (ProfRecent.cpp)
void profRecentAllocReset(ThreadState & tsd, Extent * edata);
/// jemalloc: prof_free_sampled_object (Prof.cpp; used by `arenaReset` through `prof_free`)
void profFreeSampledObject(ThreadState & tsd, const void * ptr, size_t usize, ProfInfo * prof_info);

}
