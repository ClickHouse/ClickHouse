#pragma once

/// The arena: the bins of small size classes, large allocations, and the page allocator shard (`page_allocator_shard`).
/// jemalloc: `arena.c`, `arena_structs.h`, `arena_stats.h`, `arena_types.h`, `arena_externs.h`, `arena_inlines_a.h`,
/// the tcache-independent part of `arena_inlines_b.h`, and `large.c` / `large_externs.h` (ArenaLarge.cpp).
///
/// Header layering (to break the cycle with the thread cache):
/// - Arena.h (this file): the data layout and all declarations; inline helpers that need neither the arena table nor
///   the tcache.
/// - Arenas.h: the global arena table (`arenas[]`, `num_arenas_auto`, `manual_arena_base`), `arenaGet`, `arenaIsAuto`,
///   `arenaGetFromExtent`, `arenaChoose*`, percpu arenas, bootstrap allocation helpers.
/// - ThreadCache.h (includes both of the above): the tcache.
/// - ArenaInlines.h (includes ThreadCache.h): the front-end dispatch helpers that call into the tcache
///   (`arenaMalloc`, `arenaDeallocate`, `arenaSizedDeallocate`, ...), and the ones built on the emap (`arenaAllocationSize`, ...).
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
#include <allocator/Nanoseconds.h>
#include <allocator/PRNG.h>
#include <allocator/PageAllocator.h>
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
class ProfilingThreadContext;

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
inline constexpr uint64_t ARENA_DEFERRED_PURGE_NUM_PAGES_THRESHOLD = 1024;

/// jemalloc: arena_config_t
struct ArenaConfig
{
    /// Extent hooks to be used for the arena (always the default table: custom hooks are not supported).
    const extent_hooks_t * extent_hooks_ptr;
    /// Use extent hooks for metadata (base) allocations when true.
    bool metadata_use_hooks;
};

/// jemalloc: arena_config_default
extern const ArenaConfig arena_config_default;

/// `arena_bin_offsets[bin_idx]` is the offset (from the arena) of the first bin shard for size class `bin_idx`.
/// jemalloc: arena_bin_offsets
extern constinit uint32_t arena_bin_offsets[SIZE_CLASS_NUM_BINS];

/// The total number of bin shards of an arena (sum of `bin_infos[i].n_shards`). jemalloc: nbins_total (static)
extern constinit unsigned arena_num_bins_total;

/// The effective oversize threshold (`opt.oversize_threshold` validated by `arenaInitHuge`).
/// jemalloc: oversize_threshold
extern constinit size_t oversize_threshold;

/// a0 is used to handle huge requests before malloc init completes. After that, `huge_arena_idx` is updated to point
/// to the actual huge arena, which is the last one of the auto arenas.
/// jemalloc: huge_arena_ind
extern constinit unsigned huge_arena_idx;

/// --- Profiling info of an allocation (prof_structs.h; used by the arena/large prof hooks) --------------------------

/// jemalloc: prof_info_t
struct ProfilingInfo
{
    /// Time when the allocation was made.
    Nanoseconds alloc_time;
    /// Points to the `ProfilingThreadContext` corresponding to the allocation.
    ProfilingThreadContext * alloc_thread_context;
    /// Allocation request size.
    size_t alloc_size;
};

/// jemalloc: PROF_TCTX_SENTINEL
inline ProfilingThreadContext * const PROFILING_THREAD_CONTEXT_SENTINEL = reinterpret_cast<ProfilingThreadContext *>(uintptr_t(1));

/// jemalloc: PROF_SAMPLE_ALIGNMENT
inline constexpr size_t PROFILING_SAMPLE_ALIGNMENT = PAGE;

/// jemalloc: prof_tctx_is_valid
ALLOCATOR_ALWAYS_INLINE bool profilingThreadContextIsValid(const ProfilingThreadContext * thread_context)
{
    return thread_context != nullptr && thread_context != PROFILING_THREAD_CONTEXT_SENTINEL;
}

/// --- arena_stats.h -------------------------------------------------------------------------------------------------

/// jemalloc: arena_stats_large_t
struct ArenaStatsLarge
{
    /// Total number of large allocation/deallocation requests served directly by the arena.
    LockedU64 num_allocations;
    LockedU64 num_deallocations;
    /// Total large active bytes (allocated - deallocated) served directly by the arena.
    LockedU64 active_bytes;
    /// Number of allocation requests that correspond to this size class. This includes requests served by tcache,
    /// though tcache only periodically merges into this counter.
    LockedU64 num_requests; /// Partially derived.
    /// Number of tcache fills / flushes for large (similarly, periodically merged). Note that there is no large
    /// tcache batch-fill currently (i.e. only fill 1 at a time); however flush may be batched.
    LockedU64 num_fills; /// Partially derived.
    LockedU64 num_flushes; /// Partially derived.
    /// Current number of allocations of this size class.
    size_t current_large_extents = 0; /// Derived.
};

static_assert(sizeof(ArenaStatsLarge) == 56);

/// Arena stats. Fields marked "derived" are not directly maintained within the arena code; their values are derived
/// during stats merge requests. There is no stats mutex (`JEMALLOC_ATOMIC_U64`).
/// jemalloc: arena_stats_t
struct ArenaStats
{
    /// `resident` includes the base stats -- that's why it lives here and not in `PageAllocatorShardStats`.
    size_t base = 0; /// Derived.
    size_t metadata_extent = 0; /// Derived.
    size_t metadata_radix_tree = 0; /// Derived.
    size_t resident = 0; /// Derived.
    size_t metadata_transparent_huge_pages = 0; /// Derived.
    size_t mapped = 0; /// Derived.

    std::atomic<size_t> internal{0};

    size_t allocated_large = 0; /// Derived.
    uint64_t num_allocations_large = 0; /// Derived.
    uint64_t num_deallocations_large = 0; /// Derived.
    uint64_t num_fills_large = 0; /// Derived.
    uint64_t num_flushes_large = 0; /// Derived.
    uint64_t num_requests_large = 0; /// Derived.

    /// The stats logically owned by the pa_shard in the same arena. This lives here only because it's convenient for
    /// the purposes of the ctl module -- it only knows about the single arena stats.
    PageAllocatorShardStats page_allocator_shard_stats;

    /// Number of bytes cached in tcache associated with this arena.
    size_t thread_cache_bytes = 0; /// Derived.
    size_t thread_cache_stashed_bytes = 0; /// Derived.

    MutexProfilingData mutex_profiling_data[mutex_profiling_num_arena_mutexes];

    /// One element for each large size class.
    ArenaStatsLarge large_stats[SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS];

    /// Arena uptime.
    Nanoseconds uptime = Nanoseconds::zero();
};

static_assert(offsetof(ArenaStats, internal) == 48);
static_assert(offsetof(ArenaStats, page_allocator_shard_stats) == 104);
static_assert(offsetof(ArenaStats, mutex_profiling_data) == 200);
static_assert(offsetof(ArenaStats, large_stats) == 968);
static_assert(sizeof(ArenaStats) == 968 + 56 * (SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS) + 8);

/// jemalloc: arena_stats_large_flush_nrequests_add
ALLOCATOR_ALWAYS_INLINE void
arenaStatsLargeFlushNumRequestsAdd(ThreadState * /*tsdn*/, ArenaStats * arena_stats, SizeClassIdx size_class_idx, uint64_t num_requests)
{
    ArenaStatsLarge & large_stats = arena_stats->large_stats[size_class_idx - SIZE_CLASS_NUM_BINS];
    large_stats.num_requests.increment(num_requests);
    large_stats.num_flushes.increment(1);
}

/// --- arena_structs.h -----------------------------------------------------------------------------------------------

/// jemalloc: arena_t
class alignas(CACHE_LINE) Arena
{
public:
    constexpr Arena() = default;

    Arena(const Arena &) = delete;
    Arena & operator=(const Arena &) = delete;

    /// Creates the arena (and, for `idx != 0`, its base) and publishes it in `arenas[idx]`. Returns null on failure.
    /// jemalloc: arena_new
    static Arena * create(ThreadState * thread_state, unsigned idx, const ArenaConfig * config);

    /// The bins follow the structure (cacheline-aligned); use `arenaGetBin`.
    /// jemalloc: all_bins
    ALLOCATOR_ALWAYS_INLINE Bin * allBins() { return reinterpret_cast<Bin *>(reinterpret_cast<std::byte *>(this) + sizeof(Arena)); }

    /// Number of threads currently assigned to this arena. Each thread has two distinct assignments, one for
    /// application-serving allocation, and the other for internal metadata allocation. Internal metadata must not be
    /// allocated from arenas explicitly created via the `arenas.create` mallctl, because the `arena.<i>.reset`
    /// mallctl indiscriminately discards all allocations for the affected arena.
    ///   0: Application allocation.
    ///   1: Internal metadata allocation.
    std::atomic<unsigned> num_threads[2] = {};

    /// Next bin shard for binding new threads.
    std::atomic<unsigned> bin_shard_next{0};

    /// When percpu_arena is enabled, to amortize the cost of reading / updating the current CPU id, track the most
    /// recent thread accessing this arena, and only read CPU if there is a mismatch.
    ThreadState * last_thread = nullptr;

    /// Synchronization: internal.
    ArenaStats stats;

    /// Lists of tcaches and cache bin array descriptors for extant threads associated with this arena. Stats from
    /// these are merged incrementally, and at exit if `opt.stats_print` is enabled. Synchronization: `thread_cache_list_mutex`.
    IntrusiveList<ThreadCacheSlow, &ThreadCacheSlow::link> thread_cache_list;
    IntrusiveList<CacheBinArrayDescriptor, &CacheBinArrayDescriptor::link> cache_bin_array_descriptor_list;
    /// "tcache_ql", `MutexRank::THREAD_CACHE_LIST`.
    Mutex thread_cache_list_mutex;

    /// Represents a `SbrkPrecedence`, but atomically.
    std::atomic<unsigned> sbrk_precedence{0};

    /// Extant large allocations (only tracked for manual arenas). Synchronization: `large_mutex`.
    ExtentListActive large;
    /// Synchronizes all large allocation/update/deallocation. "arena_large", `MutexRank::ARENA_LARGE`.
    Mutex large_mutex;

    /// The page-level allocator shard this arena uses.
    PageAllocatorShard page_allocator_shard;

    /// A cached copy of `base->idxGet()`. This can get accessed on hot paths; looking it up in base requires an
    /// extra pointer hop / cache miss.
    unsigned idx = 0;

    /// Base allocator, from which arena metadata are allocated. Synchronization: internal.
    Base * base = nullptr;

    /// Used to determine uptime. Read-only after initialization.
    Nanoseconds create_time = Nanoseconds::zero();

    /// The name of the arena.
    char name[ARENA_NAME_LEN] = {};
};

/// Measured from the C build (`stats.metadata` depends on the size of the arena allocation).
#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(Arena) == (LOG2_PAGE == 12 ? 80768 : (LOG2_PAGE == 14 ? 77952 : 75200)), "arena_t size (aarch64 glibc)");
static_assert(offsetof(Arena, stats) == 24);
static_assert(offsetof(Arena, page_allocator_shard) == (LOG2_PAGE == 12 ? 12288 : (LOG2_PAGE == 14 ? 11840 : 11392)));
#endif

/// --- arena_inlines_a.h ---------------------------------------------------------------------------------------------

/// jemalloc: arena_ind_get
ALLOCATOR_ALWAYS_INLINE unsigned arenaIdxGet(const Arena * arena)
{
    return arena->idx;
}

/// jemalloc: arena_internal_add
ALLOCATOR_ALWAYS_INLINE void arenaInternalAdd(Arena * arena, size_t size)
{
    arena->stats.internal.fetch_add(size, std::memory_order_relaxed);
}

/// jemalloc: arena_internal_sub
ALLOCATOR_ALWAYS_INLINE void arenaInternalSub(Arena * arena, size_t size)
{
    arena->stats.internal.fetch_sub(size, std::memory_order_relaxed);
}

/// jemalloc: arena_internal_get
ALLOCATOR_ALWAYS_INLINE size_t arenaInternalGet(Arena * arena)
{
    return arena->stats.internal.load(std::memory_order_relaxed);
}

/// --- arena_inlines_b.h (the parts that need neither the arena table nor the tcache) --------------------------------

/// jemalloc: arena_get_bin
ALLOCATOR_ALWAYS_INLINE Bin * arenaGetBin(Arena * arena, SizeClassIdx bin_idx, unsigned bin_shard)
{
    Bin * shard0 = reinterpret_cast<Bin *>(reinterpret_cast<std::byte *>(arena) + arena_bin_offsets[bin_idx]);
    return shard0 + bin_shard;
}

/// jemalloc: arena_get_ehooks
ALLOCATOR_ALWAYS_INLINE ExtentHooks * arenaGetExtentHooks(Arena * arena)
{
    return arena->base->extentHooksGet();
}

/// jemalloc: arena_decay (declared here for `arenaDecayTicks`)
void arenaDecay(ThreadState * thread_state, Arena * arena, bool is_background_thread, bool all);

/// We use the `TickerGeometric` to avoid having per-arena state in the tsd. Instead of having a countdown-until-decay timer
/// running for every arena in every thread, we flip a coin once per tick, whose probability of coming up heads is
/// 1/nticks; this is effectively the operation of the `TickerGeometric`. Each arena has the same chance of a coinflip
/// coming up heads (1/ARENA_DECAY_NUM_TICKS_PER_UPDATE), so we can use a single ticker for all of them.
/// jemalloc: arena_decay_ticks
ALLOCATOR_ALWAYS_INLINE void arenaDecayTicks(ThreadState * thread_state_ptr, Arena * arena, unsigned num_ticks)
{
    if (ALLOCATOR_UNLIKELY(thread_state_ptr == nullptr))
        return;
    ThreadState & thread_state = *thread_state_ptr;
    if (ALLOCATOR_UNLIKELY(
            thread_state.arena_decay_ticker.ticks(thread_state.prngState(), int32_t(num_ticks), thread_state.reentrancyLevel() > 0)))
        arenaDecay(thread_state_ptr, arena, false, false);
}

/// jemalloc: arena_decay_tick
ALLOCATOR_ALWAYS_INLINE void arenaDecayTick(ThreadState * thread_state, Arena * arena)
{
    arenaDecayTicks(thread_state, arena, 1);
}

/// Eagerly detect double free and sized dealloc bugs for large sizes (only with `config_opt_safety_checks`, which
/// is off). Returns true if the deallocation must be skipped.
/// jemalloc: large_dalloc_safety_checks
ALLOCATOR_ALWAYS_INLINE bool largeDeallocateSafetyChecks(Extent * extent, const void * ptr, size_t input_size)
{
    if constexpr (!config::option_safety_checks)
    {
        return false;
    }
    else
    {
        if (ALLOCATOR_UNLIKELY(extent == nullptr || extent->state() != extent_state_active))
        {
            safetyCheckFail(
                "Invalid deallocation detected: pages being freed (%p) not currently active, possibly caused by double free bugs.", ptr);
            return true;
        }
        if (ALLOCATOR_UNLIKELY(input_size != extent->usableSize() || input_size > SIZE_CLASS_LARGE_MAX_CLASS))
        {
            safetyCheckFailSizedDealloc(/* current_dealloc */ true, ptr, /* true_size */ extent->usableSize(), input_size);
            return true;
        }
        return false;
    }
}

/// Randomizes the start of a large allocation within its first page (cache-oblivious large allocations). Uses the
/// thread's PRNG state (shared with the decay ticker: the stream is part of the observable behavior); without tsd,
/// a PRNG seeded with the address of a stack variable.
/// jemalloc: arena_cache_oblivious_randomize
ALLOCATOR_ALWAYS_INLINE void arenaCacheObliviousRandomize(ThreadState * thread_state, Arena * /*arena*/, Extent * extent, size_t alignment)
{
    ALLOCATOR_ASSERT(extent->base() == extent->addr());

    if (alignment < PAGE)
    {
        unsigned log2_range = LOG2_PAGE - log2Floor(cacheLineCeiling(alignment));
        size_t r;
        if (thread_state != nullptr)
        {
            r = size_t(prngLog2RangeU64(thread_state->prngState(), log2_range));
        }
        else
        {
            uint64_t stack_value = uint64_t(reinterpret_cast<uintptr_t>(&r));
            r = size_t(prngLog2RangeU64(stack_value, log2_range));
        }
        uintptr_t random_offset = uintptr_t(r) << (LOG2_PAGE - log2_range);
        extent->setAddr(static_cast<std::byte *>(extent->addr()) + random_offset);
        ALLOCATOR_ASSERT(alignmentAddrToBase(extent->addr(), alignment) == extent->addr());
    }
}

/// --- arena.c -------------------------------------------------------------------------------------------------------

/// jemalloc: arena_new
Arena * arenaNew(ThreadState * thread_state, unsigned idx, const ArenaConfig * config);

/// Computes the decay defaults, the bin division magics and the bin offsets. `huge_page_allocator` is ignored (HPA is dropped).
/// Returns true on error.
/// jemalloc: arena_boot
bool arenaBoot(const SizeClassData * size_class_data, Base * base, bool huge_page_allocator);

/// Sets up the oversize (huge) arena: reserves its index if `opt.oversize_threshold` is a valid large size, and
/// patches the threshold of arena 0 (created before the options were parsed). Returns whether it is enabled.
/// jemalloc: arena_init_huge
bool arenaInitHuge(ThreadState * thread_state, Arena * a0);

/// Returns the huge arena (creating it on demand).
/// jemalloc: arena_choose_huge
Arena * arenaChooseHuge(ThreadState & thread_state);

/// jemalloc: arena_basic_stats_merge
void arenaBasicStatsMerge(
    ThreadState * thread_state,
    Arena * arena,
    unsigned * num_threads,
    const char ** sbrk,
    ssize_t * dirty_decay_ms,
    ssize_t * muzzy_decay_ms,
    size_t * num_active,
    size_t * num_dirty,
    size_t * num_muzzy);

/// `bin_stats` has `SIZE_CLASS_NUM_BINS` elements, `large_stats` has `SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS`, `extent_stats` has
/// `SIZE_CLASS_NUM_PAGE_SIZES`. The HPA stats
/// output of jemalloc is dropped (they would only be merged if HPA was ever used).
/// jemalloc: arena_stats_merge
void arenaStatsMerge(
    ThreadState * thread_state,
    Arena * arena,
    unsigned * num_threads,
    const char ** sbrk,
    ssize_t * dirty_decay_ms,
    ssize_t * muzzy_decay_ms,
    size_t * num_active,
    size_t * num_dirty,
    size_t * num_muzzy,
    ArenaStats * arena_stats,
    BinStatsData * bin_stats,
    ArenaStatsLarge * large_stats,
    PageAllocatorExtentStats * extent_stats);

/// React to deferred work generated by a PAI function.
/// jemalloc: arena_handle_deferred_work
void arenaHandleDeferredWork(ThreadState * thread_state, Arena * arena);

/// jemalloc: arena_extent_alloc_large
Extent * arenaExtentAllocLarge(ThreadState * thread_state, Arena * arena, size_t usable_size, size_t alignment, bool zero);

/// jemalloc: arena_extent_dalloc_large_prep
void arenaExtentDeallocateLargePrepare(ThreadState * thread_state, Arena * arena, Extent * extent);

/// jemalloc: arena_extent_ralloc_large_shrink
void arenaExtentReallocateLargeShrink(ThreadState * thread_state, Arena * arena, Extent * extent, size_t old_usable_size);

/// jemalloc: arena_extent_ralloc_large_expand
void arenaExtentReallocateLargeExpand(ThreadState * thread_state, Arena * arena, Extent * extent, size_t old_usable_size);

/// Returns true on error.
/// jemalloc: arena_decay_ms_set
bool arenaDecayMsSet(ThreadState * thread_state, Arena * arena, ExtentState state, ssize_t decay_ms);

/// jemalloc: arena_decay_ms_get
ssize_t arenaDecayMsGet(Arena * arena, ExtentState state);

/// Called from background threads.
/// jemalloc: arena_do_deferred_work
void arenaDoDeferredWork(ThreadState * thread_state, Arena * arena);

/// Frees all allocations of a (manual) arena. The caller guarantees that no concurrent operations are happening in
/// this arena.
/// jemalloc: arena_reset
void arenaReset(ThreadState & thread_state, Arena * arena);

/// jemalloc: arena_destroy
void arenaDestroy(ThreadState & thread_state, Arena * arena);

/// Fills `array->ptr[0 .. result)` with at least `num_fill_min` (unless OOM) and at most `num_fill_max` regions; merges the
/// tcache request counter `merge_stats` into the bin stats.
/// jemalloc: arena_ptr_array_fill_small
CacheBinSize arenaPtrArrayFillSmall(
    ThreadState * thread_state,
    Arena * arena,
    SizeClassIdx bin_idx,
    CacheBinPtrArray * array,
    CacheBinSize num_fill_min,
    CacheBinSize num_fill_max,
    CacheBinStats merge_stats);

/// Allocates `num_fill` regions from fresh slabs (`experimental.batch_alloc`). Returns the number allocated.
/// jemalloc: arena_fill_small_fresh
size_t arenaFillSmallFresh(ThreadState * thread_state, Arena * arena, SizeClassIdx bin_idx, void ** ptrs, size_t num_fill, bool zero);

/// The tcache bypass path: `arena` may be null (then chosen from the thread, possibly redirected to the huge arena).
/// jemalloc: arena_malloc_hard
void * arenaMallocHard(ThreadState * thread_state, Arena * arena, size_t size, SizeClassIdx idx, bool zero, bool slab);

/// jemalloc: arena_palloc
void * arenaAllocateAligned(
    ThreadState * thread_state, Arena * arena, size_t usable_size, size_t alignment, bool zero, bool slab, ThreadCache * thread_cache);

/// Turns a sampled small allocation (served from a large extent of `bumped_usable_size`) into one that reports `usable_size`.
/// jemalloc: arena_prof_promote
void arenaProfilingPromote(ThreadState * thread_state, void * ptr, size_t usable_size, size_t bumped_usable_size);

/// jemalloc: arena_dalloc_promoted
void arenaDeallocatePromoted(ThreadState * thread_state, void * ptr, ThreadCache * thread_cache, bool slow_path);

/// jemalloc: arena_slab_dalloc
void arenaSlabDeallocate(ThreadState * thread_state, Arena * arena, Extent * slab);

/// jemalloc: arena_dalloc_small
void arenaDeallocateSmall(ThreadState * thread_state, void * ptr);

/// In practice, pointers are flushed back to their original allocation arenas, so multiple arenas may be involved
/// here. `stats_arena` indicates where the cache stats (`merge_stats`, a snapshot taken by the caller) are merged.
/// Processes the pointers in batches of at most `CACHE_BIN_NUM_FLUSH_BATCH_MAX`; reorders `array->ptr`.
/// jemalloc: arena_ptr_array_flush
void arenaPtrArrayFlush(
    ThreadState & thread_state,
    SizeClassIdx bin_idx,
    CacheBinPtrArray * array,
    unsigned num_flush,
    bool small,
    Arena * stats_arena,
    CacheBinStats merge_stats);

/// Returns true if the allocation could not be resized in place. `*new_size` is the resulting usable size.
/// jemalloc: arena_ralloc_no_move
bool arenaReallocateNoMove(
    ThreadState * thread_state, void * ptr, size_t old_size, size_t size, size_t extra, bool zero, size_t * new_size);

/// The `hook_args` of jemalloc are dropped (`experimental.hooks.install` is not supported).
/// jemalloc: arena_ralloc
void * arenaReallocate(
    ThreadState * thread_state,
    Arena * arena,
    void * ptr,
    size_t old_size,
    size_t size,
    size_t alignment,
    bool zero,
    bool slab,
    ThreadCache * thread_cache);

/// jemalloc: arena_dss_prec_get
SbrkPrecedence arenaSbrkPrecedenceGet(Arena * arena);

/// Returns true on error.
/// jemalloc: arena_dss_prec_set
bool arenaSbrkPrecedenceSet(Arena * arena, SbrkPrecedence sbrk_precedence);

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
bool arenaRetainGrowLimitGetSet(ThreadState & thread_state, Arena * arena, size_t * old_limit, size_t * new_limit);

/// jemalloc: arena_nthreads_get
ALLOCATOR_ALWAYS_INLINE unsigned arenaNumThreadsGet(Arena * arena, bool internal)
{
    return arena->num_threads[internal].load(std::memory_order_relaxed);
}

/// jemalloc: arena_nthreads_inc
ALLOCATOR_ALWAYS_INLINE void arenaNumThreadsIncrement(Arena * arena, bool internal)
{
    arena->num_threads[internal].fetch_add(1, std::memory_order_relaxed);
}

/// jemalloc: arena_nthreads_dec
ALLOCATOR_ALWAYS_INLINE void arenaNumThreadsDecrement(Arena * arena, bool internal)
{
    arena->num_threads[internal].fetch_sub(1, std::memory_order_relaxed);
}

/// The fork phases (see Fork.cpp for the order across all modules).
/// jemalloc: arena_prefork0 .. arena_prefork8, arena_postfork_parent, arena_postfork_child
void arenaPrefork0(ThreadState * thread_state, Arena * arena);
void arenaPrefork1(ThreadState * thread_state, Arena * arena);
void arenaPrefork2(ThreadState * thread_state, Arena * arena);
void arenaPrefork3(ThreadState * thread_state, Arena * arena);
void arenaPrefork4(ThreadState * thread_state, Arena * arena);
void arenaPrefork5(ThreadState * thread_state, Arena * arena);
void arenaPrefork6(ThreadState * thread_state, Arena * arena);
void arenaPrefork7(ThreadState * thread_state, Arena * arena);
void arenaPrefork8(ThreadState * thread_state, Arena * arena);
void arenaPostforkParent(ThreadState * thread_state, Arena * arena);
/// `thread_state` must not be null (it is the forking thread's tsd).
void arenaPostforkChild(ThreadState * thread_state, Arena * arena);

inline Arena * Arena::create(ThreadState * thread_state, unsigned idx, const ArenaConfig * config)
{
    return arenaNew(thread_state, idx, config);
}

/// --- large.c (ArenaLarge.cpp) --------------------------------------------------------------------------------------

/// jemalloc: large_malloc
void * largeMalloc(ThreadState * thread_state, Arena * arena, size_t usable_size, bool zero);

/// jemalloc: large_palloc
void * largeAllocateAligned(ThreadState * thread_state, Arena * arena, size_t usable_size, size_t alignment, bool zero);

/// Returns true if the allocation could not be resized in place to a usable size in [usize_min, usize_max].
/// jemalloc: large_ralloc_no_move
bool largeReallocateNoMove(ThreadState * thread_state, Extent * extent, size_t usable_size_min, size_t usable_size_max, bool zero);

/// The `hook_args` of jemalloc are dropped.
/// jemalloc: large_ralloc
void * largeReallocate(
    ThreadState * thread_state, Arena * arena, void * ptr, size_t usable_size, size_t alignment, bool zero, ThreadCache * thread_cache);

/// Requires holding `large_mutex` of the extent's arena if it is a manual arena.
/// jemalloc: large_dalloc_prep_locked
void largeDeallocatePrepareLocked(ThreadState * thread_state, Extent * extent);

/// jemalloc: large_dalloc_finish
void largeDeallocateFinish(ThreadState * thread_state, Extent * extent);

/// jemalloc: large_dalloc
void largeDeallocate(ThreadState * thread_state, Extent * extent);

/// jemalloc: large_salloc
ALLOCATOR_ALWAYS_INLINE size_t largeAllocationSize(ThreadState * /*tsdn*/, const Extent * extent)
{
    return extent->usableSize();
}

/// jemalloc: large_prof_info_get
void largeProfilingInfoGet(ThreadState & thread_state, Extent * extent, ProfilingInfo * profiling_info, bool reset_recent);

/// jemalloc: large_prof_tctx_reset
void largeProfilingThreadContextReset(Extent * extent);

/// Also clears the fork's `e_profiling_fragmentation_tracked` flag (it may hold garbage from a previous slab use of the extent)
/// before the tctx is published.
/// jemalloc: large_prof_info_set
void largeProfilingInfoSet(Extent * extent, ProfilingThreadContext * thread_context, size_t size);

/// --- Hooks provided by other modules (BackgroundThread.cpp, ProfilingData.cpp, ProfilingRecent.cpp, Profiling.cpp) ---------------

/// jemalloc: background_thread_info_t (BackgroundThread)
struct BackgroundThreadInfo;

/// jemalloc: arena_background_thread_info_get
BackgroundThreadInfo * arenaBackgroundThreadInfoGet(Arena * arena);
/// `info->mutex`
Mutex & backgroundThreadInfoMutex(BackgroundThreadInfo * info);
/// jemalloc: background_thread_is_started
bool backgroundThreadIsStarted(BackgroundThreadInfo * info);
/// jemalloc: background_thread_indefinite_sleep
bool backgroundThreadIndefiniteSleep(BackgroundThreadInfo * info);
/// jemalloc: background_thread_wakeup_time_get
uint64_t backgroundThreadWakeupTimeGet(BackgroundThreadInfo * info);
/// `info->num_pages_to_purge_new`
size_t & backgroundThreadNumPagesToPurgeNew(BackgroundThreadInfo * info);
/// jemalloc: background_thread_wakeup_early
void backgroundThreadWakeupEarly(BackgroundThreadInfo * info, Nanoseconds * remaining_sleep);

/// jemalloc: prof_frag_untrack (ProfData.cpp)
void profilingFragmentationUntrack(ThreadState & thread_state, Extent * extent, ProfilingThreadContext * thread_context);
/// jemalloc: prof_recent_alloc_reset (ProfRecent.cpp)
void profilingRecentAllocReset(ThreadState & thread_state, Extent * extent);
/// jemalloc: prof_free_sampled_object (Prof.cpp; used by `arenaReset` through `prof_free`)
void profilingFreeSampledObject(ThreadState & thread_state, const void * ptr, size_t usable_size, ProfilingInfo * profiling_info);

}
