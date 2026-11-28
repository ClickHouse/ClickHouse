#pragma once

/// Internal interface of the `mallctl` implementation (jemalloc: the static parts of `ctl.c`, `ctl.h`): the access
/// check helpers (`READONLY`, `READ`, `WRITE`, ...), the leaf generators (`CTL_RO_*GEN`), the ctl state
/// (`mallctl_mutex`, `mallctl_stats`, `mallctl_arenas`) and the declarations of all leaves and index functions of the tree.
///
/// Only for the Ctl*.cpp files and Stats.cpp.
///
/// Every leaf of the tree is declared here under a name derived from jemalloc's (`arena_i_decay_ctl` ->
/// `mallctl::arenaIDecay`), and defined in the file of its subtree:
///
///     Mallctl.cpp             version, epoch, index functions of `arena`, `stats.arenas`, `experimental.arenas`
///     MallctlTree.cpp         the tree itself; `config.*`, `opt.*`, constant `arenas.*` leaves, HPA/SEC zero leaves and
///                         the mutex profiling leaves (generated from templates)
///     MallctlThread.cpp       `background_thread`, `max_background_threads`, `thread.*`, `tcache.*`
///     MallctlArenas.cpp       `arena.<i>.*`, `arenas.*`
///     MallctlProfiling.cpp         `prof.*`, `experimental.hooks.prof_*`, `experimental.prof_recent.*`
///     MallctlStats.cpp        `stats.*`, `stats.arenas.<i>.*`, `approximate_stats.*`
///     MallctlExperimental.cpp the rest of `experimental.*`
///
/// Leaves of dropped features are defined with `ALLOCATOR_MALLCTL_DROPPED(name)` (they return `ENOENT`).

#include <allocator/Arena.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Bin.h>
#include <allocator/Common.h>
#include <allocator/IntrusiveList.h>
#include <allocator/Mallctl.h>
#include <allocator/Mutex.h>
#include <allocator/Nanoseconds.h>
#include <allocator/PageAllocator.h>

#include <cerrno>
#include <climits>
#include <cstring>
#include <type_traits>

namespace jemalloc
{

/// --- Access checks (ctl.c: READONLY, WRITEONLY, ...) ---------------------------------------------------------------
///
/// Each returns 0 if the check passes, or the error code that the leaf must return immediately:
///
///     if (int ret = ctl::readOnly(newp, newlen))
///         return ret;
///
/// The order of the checks in each leaf is observable (which error wins) and must be the same as in jemalloc.

namespace mallctl
{

/// jemalloc: READONLY
ALLOCATOR_ALWAYS_INLINE int readOnly(const void * new_value, size_t new_length)
{
    return (new_value != nullptr || new_length != 0) ? EPERM : 0;
}

/// jemalloc: WRITEONLY
ALLOCATOR_ALWAYS_INLINE int writeOnly(const void * old_value, const size_t * old_length_ptr)
{
    return (old_value != nullptr || old_length_ptr != nullptr) ? EPERM : 0;
}

/// Can read or write, but not both. jemalloc: READ_XOR_WRITE
ALLOCATOR_ALWAYS_INLINE int readXorWrite(const void * old_value, const size_t * old_length_ptr, const void * new_value, size_t new_length)
{
    return ((old_value != nullptr && old_length_ptr != nullptr) && (new_value != nullptr || new_length != 0)) ? EPERM : 0;
}

/// Can neither read nor write. jemalloc: NEITHER_READ_NOR_WRITE
ALLOCATOR_ALWAYS_INLINE int
neitherReadNorWrite(const void * old_value, const size_t * old_length_ptr, const void * new_value, size_t new_length)
{
    return (old_value != nullptr || old_length_ptr != nullptr || new_value != nullptr || new_length != 0) ? EPERM : 0;
}

/// Verify that the space provided is enough; otherwise sets `*old_length_ptr` to 0 (if non-null).
/// jemalloc: VERIFY_READ
template <typename T>
ALLOCATOR_ALWAYS_INLINE int verifyRead(const void * old_value, size_t * old_length_ptr)
{
    if (old_value == nullptr || old_length_ptr == nullptr || *old_length_ptr != sizeof(T))
    {
        if (old_length_ptr != nullptr)
            *old_length_ptr = 0;
        return EINVAL;
    }
    return 0;
}

/// Reads only if both `old_value` and `old_length_ptr` are non-null. On a size mismatch copies `min(sizeof(T), *old_length_ptr)` bytes
/// of the value anyway, sets `*old_length_ptr` to that and returns `EINVAL`.
/// jemalloc: READ
template <typename T>
ALLOCATOR_ALWAYS_INLINE int read(void * old_value, size_t * old_length_ptr, const T & value)
{
    if (old_value != nullptr && old_length_ptr != nullptr)
    {
        if (*old_length_ptr != sizeof(T))
        {
            size_t copy_length = (sizeof(T) <= *old_length_ptr) ? sizeof(T) : *old_length_ptr;
            std::memcpy(old_value, static_cast<const void *>(&value), copy_length);
            *old_length_ptr = copy_length;
            return EINVAL;
        }
        std::memcpy(old_value, static_cast<const void *>(&value), sizeof(T));
    }
    return 0;
}

/// Writes only if `new_value` is non-null (`new_value == nullptr` with `new_length != 0` passes). jemalloc: WRITE
template <typename T>
ALLOCATOR_ALWAYS_INLINE int write(const void * new_value, size_t new_length, T & value)
{
    if (new_value != nullptr)
    {
        if (new_length != sizeof(T))
            return EINVAL;
        std::memcpy(static_cast<void *>(&value), new_value, sizeof(T));
    }
    return 0;
}

/// jemalloc: ASSURED_WRITE
template <typename T>
ALLOCATOR_ALWAYS_INLINE int assuredWrite(const void * new_value, size_t new_length, T & value)
{
    if (new_value == nullptr || new_length != sizeof(T))
        return EINVAL;
    std::memcpy(static_cast<void *>(&value), new_value, sizeof(T));
    return 0;
}

/// jemalloc: MIB_UNSIGNED
ALLOCATOR_ALWAYS_INLINE int numericPathComponentUnsigned(const size_t * numeric_path, size_t i, unsigned & value)
{
    if (numeric_path[i] > UINT_MAX)
        return EFAULT;
    value = static_cast<unsigned>(numeric_path[i]);
    return 0;
}

/// The return value of leaves of dropped features (HPA, SEC, `profiling_log`, `hook.c`, test hooks).
constexpr int dropped()
{
    return ENOENT;
}

}

/// --- ctl state (ctl.h, ctl.c) --------------------------------------------------------------------------------------

/// The size of `hpa_shard_stats_t` (`psset_stats_t` + `hpa_shard_nonderived_stats_t` + `sec_stats_t`; independent of
/// the page size). HPA and SEC are dropped: the statistics are always zero, but the space is kept so that the slot
/// allocation (`stats.metadata`) has the same size as jemalloc's.
inline constexpr size_t HUGE_PAGE_SHARD_STATS_PLACEHOLDER_SIZE = 3328;

/// jemalloc: hpa_shard_stats_t (always zero)
struct HugePageShardStatsPlaceholder
{
    alignas(8) unsigned char data[HUGE_PAGE_SHARD_STATS_PLACEHOLDER_SIZE];
};

/// The merged statistics of an arena (or of all / destroyed arenas).
/// jemalloc: ctl_arena_stats_t
struct MallctlArenaStats
{
    ArenaStats arena_stats;

    /// Aggregate stats for small size classes, based on bin stats.
    size_t allocated_small;
    uint64_t num_allocations_small;
    uint64_t num_deallocations_small;
    uint64_t num_requests_small;
    uint64_t num_fills_small;
    uint64_t num_flushes_small;

    BinStatsData bin_stats[SIZE_CLASS_NUM_BINS];
    ArenaStatsLarge large_stats[SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS];
    PageAllocatorExtentStats extent_stats[SIZE_CLASS_NUM_PAGE_SIZES];
    HugePageShardStatsPlaceholder huge_page_allocator_stats;
};

static_assert(offsetof(MallctlArenaStats, allocated_small) == sizeof(ArenaStats));
static_assert(offsetof(MallctlArenaStats, bin_stats) == sizeof(ArenaStats) + 48);
static_assert(
    sizeof(MallctlArenaStats)
    == sizeof(ArenaStats) + 48 + sizeof(BinStatsData) * SIZE_CLASS_NUM_BINS
        + sizeof(ArenaStatsLarge) * (SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS)
        + sizeof(PageAllocatorExtentStats) * SIZE_CLASS_NUM_PAGE_SIZES + HUGE_PAGE_SHARD_STATS_PLACEHOLDER_SIZE);
/// Measured from the C build (aarch64 glibc, LOG2_PAGE=16).
static_assert(LOG2_PAGE != 16 || sizeof(MallctlArenaStats) == 40784, "Must have the size of ctl_arena_stats_t");

/// jemalloc: ctl_arena_t
struct MallctlArena
{
    unsigned arena_idx;
    bool initialized;
    RingLink<MallctlArena> destroyed_link;

    /// Basic stats, supported even if !config_stats.
    unsigned num_threads;
    const char * sbrk;
    ssize_t dirty_decay_ms;
    ssize_t muzzy_decay_ms;
    size_t active_pages;
    size_t dirty_pages;
    size_t muzzy_pages;

    MallctlArenaStats * arena_stats;
};

static_assert(sizeof(MallctlArena) == 88, "Must have the size of ctl_arena_t");

/// jemalloc: ctl_arenas_t
struct MallctlArenas
{
    uint64_t epoch;
    unsigned num_arenas;
    IntrusiveList<MallctlArena, &MallctlArena::destroyed_link> destroyed;

    /// Element 0 corresponds to merged stats for extant arenas (accessed via MALLCTL_ARENAS_ALL), element 1
    /// corresponds to merged stats for destroyed arenas (accessed via MALLCTL_ARENAS_DESTROYED), and the remaining
    /// MALLOCX_ARENA_LIMIT elements correspond to arenas.
    MallctlArena * arenas[2 + MALLOCX_ARENA_LIMIT];
};

static_assert(sizeof(MallctlArenas) == 32800, "Must have the size of ctl_arenas_t");

/// `BackgroundThreadStats` (jemalloc: background_thread_stats_t) is in BackgroundThread.h.

/// jemalloc: ctl_stats_t
struct MallctlStats
{
    size_t allocated;
    size_t active;
    size_t metadata;
    size_t metadata_extent;
    size_t metadata_radix_tree;
    size_t metadata_transparent_huge_pages;
    size_t resident;
    size_t mapped;
    size_t retained;

    BackgroundThreadStats background_thread;
    MutexProfilingData mutex_profiling_data[mutex_profiling_num_global_mutexes];
};

static_assert(sizeof(MallctlStats) == 736, "Must have the size of ctl_stats_t");

/// `mallctl_mutex` protects `mallctl_stats->*` and `mallctl_arenas->*`. Name "ctl", rank `MutexRank::MALLCTL`.
/// jemalloc: ctl_mtx
extern constinit Mutex mallctl_mutex;
/// Allocated from `b0` by `mallctlInit` (never freed). jemalloc: ctl_stats, ctl_arenas
extern constinit MallctlStats * mallctl_stats;
extern constinit MallctlArenas * mallctl_arenas;

/// Maps an arena index to its slot in `mallctl_arenas->arenas`: `MALLCTL_ARENAS_ALL` -> 0, `MALLCTL_ARENAS_DESTROYED`
/// -> 1, `compat && i == num_arenas` -> 0 (deprecated alias of `MALLCTL_ARENAS_ALL`), `validate && i >= num_arenas` ->
/// `UINT_MAX`, otherwise `i + 2`.
/// jemalloc: arenas_i2a_impl
unsigned arenasI2aImpl(size_t i, bool compat, bool validate);

/// jemalloc: arenas_i2a
inline unsigned arenasI2a(size_t i)
{
    return arenasI2aImpl(i, true, false);
}

/// The slot of arena `i`; with `init`, allocates it (with its stats) from `b0` if needed (null on OOM).
/// jemalloc: arenas_i_impl
MallctlArena * arenasIImpl(ThreadState * thread_state, size_t i, bool compat, bool init);

/// The existing slot of arena `i` (with the compat mapping). jemalloc: arenas_i
MallctlArena * arenasI(size_t i);

/// jemalloc: ctl_arena_clear
void mallctlArenaClear(MallctlArena * mallctl_arena);

/// Returns true if `stats.arenas.<i>` / `experimental.arenas.<i>` does not exist. Requires `mallctl_mutex`.
/// jemalloc: ctl_arenas_i_verify
bool mallctlArenasIVerify(size_t i);

/// Refreshes the snapshot of the statistics (`epoch`). Requires `mallctl_mutex`.
/// jemalloc: ctl_refresh
void mallctlRefresh(ThreadState * thread_state);

/// Clears the slot of arena `i`, merges the stats of `arena` into it and then into `mallctl_summed_destroyed_arena` (the sum of all or
/// of the destroyed arenas). Requires `mallctl_mutex`.
/// jemalloc: ctl_arena_refresh
void mallctlArenaRefresh(
    ThreadState * thread_state, Arena * arena, MallctlArena * mallctl_summed_destroyed_arena, unsigned i, bool destroyed);

/// Creates an arena for `arenas.create`, recycling the index of the most recently destroyed arena if any. Returns
/// `UINT_MAX` on error. Requires `mallctl_mutex`.
/// jemalloc: ctl_arena_init
unsigned mallctlArenaInit(ThreadState & thread_state, const ArenaConfig * config);

/// --- Leaf generators (ctl.c: CTL_RO_*GEN) --------------------------------------------------------------------------

namespace mallctl
{

/// Calls a value getter, which takes either nothing or the numeric path.
template <auto get>
ALLOCATOR_ALWAYS_INLINE decltype(auto) getValue(const size_t * numeric_path)
{
    if constexpr (std::is_invocable_v<decltype(get), const size_t *>)
        return get(numeric_path);
    else
        return get();
}

/// A read-only value, no lock. jemalloc: CTL_RO_NL_GEN, CTL_RO_CONFIG_GEN
template <typename T, auto get>
int readOnlyNoLock(
    ThreadState &, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = readOnly(new_value, new_length))
        return result;
    T old_setting = getValue<get>(numeric_path);
    return read(old_value, old_length_ptr, old_setting);
}

/// A read-only value that exists only if `condition()`, no lock. jemalloc: CTL_RO_NL_CGEN
template <typename T, auto condition, auto get>
int readOnlyNoLockIf(
    ThreadState &, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (!condition())
        return ENOENT;
    if (int result = readOnly(new_value, new_length))
        return result;
    T old_setting = getValue<get>(numeric_path);
    return read(old_value, old_length_ptr, old_setting);
}

/// A read-only value under `mallctl_mutex`. jemalloc: CTL_RO_GEN
template <typename T, auto get>
int readOnlyLocked(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    MutexLock lock(&thread_state, mallctl_mutex);
    if (int result = readOnly(new_value, new_length))
        return result;
    T old_setting = getValue<get>(numeric_path);
    return read(old_value, old_length_ptr, old_setting);
}

/// A read-only value that exists only if `condition()` (checked before locking), under `mallctl_mutex`.
/// jemalloc: CTL_RO_CGEN
template <typename T, auto condition, auto get>
int readOnlyLockedIf(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (!condition())
        return ENOENT;
    MutexLock lock(&thread_state, mallctl_mutex);
    if (int result = readOnly(new_value, new_length))
        return result;
    T old_setting = getValue<get>(numeric_path);
    return read(old_value, old_length_ptr, old_setting);
}

/// The counters of a mutex profiling node, in the order of the children (`MUTEX_PROF_DATA_NODE`).
enum class MutexProfilingCounter : unsigned
{
    NumOps,
    NumWait,
    NumSpinAcquired,
    NumOwnerSwitch,
    TotalWaitTime,
    MaxWaitTime,
    MaxNumThreads,
};

/// A mutex profiling leaf: `CTL_RO_CGEN(config_stats, ...)` of one field of the `MutexProfilingData` returned by
/// `accessor(numeric_path)` (called under `mallctl_mutex`). `max_num_threads` is a `uint32_t`, the rest are `uint64_t`.
/// jemalloc: RO_MUTEX_CTL_GEN
template <auto accessor, MutexProfilingCounter counter>
int mutexProfiling(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if constexpr (!config::stats)
        return ENOENT;
    MutexLock lock(&thread_state, mallctl_mutex);
    if (int result = readOnly(new_value, new_length))
        return result;
    const MutexProfilingData * data = accessor(numeric_path);
    if constexpr (counter == MutexProfilingCounter::NumOps)
        return read<uint64_t>(old_value, old_length_ptr, data->num_lock_ops);
    else if constexpr (counter == MutexProfilingCounter::NumWait)
        return read<uint64_t>(old_value, old_length_ptr, data->num_wait_times);
    else if constexpr (counter == MutexProfilingCounter::NumSpinAcquired)
        return read<uint64_t>(old_value, old_length_ptr, data->num_spin_acquired);
    else if constexpr (counter == MutexProfilingCounter::NumOwnerSwitch)
        return read<uint64_t>(old_value, old_length_ptr, data->num_owner_switches);
    else if constexpr (counter == MutexProfilingCounter::TotalWaitTime)
        return read<uint64_t>(old_value, old_length_ptr, data->total_wait_time.ns());
    else if constexpr (counter == MutexProfilingCounter::MaxWaitTime)
        return read<uint64_t>(old_value, old_length_ptr, data->max_wait_time.ns());
    else
        return read<uint32_t>(old_value, old_length_ptr, data->max_num_threads);
}

/// --- Mutex profiling data accessors (called under `mallctl_mutex`) ---

/// `mallctl_stats->mutex_profiling_data[idx]`.
template <unsigned idx>
const MutexProfilingData * globalMutexProfilingData(const size_t *)
{
    return &mallctl_stats->mutex_profiling_data[idx];
}

/// `arenas_i(numeric_path[2])->arena_stats->arena_stats.mutex_profiling_data[idx]` (MallctlStats.cpp).
const MutexProfilingData * arenaMutexProfilingData(const size_t * numeric_path, unsigned idx);

template <unsigned idx>
const MutexProfilingData * arenaMutexProfilingDataOf(const size_t * numeric_path)
{
    return arenaMutexProfilingData(numeric_path, idx);
}

/// `arenas_i(numeric_path[2])->arena_stats->bin_stats[numeric_path[4]].mutex_data` (MallctlStats.cpp).
const MutexProfilingData * binMutexProfilingData(const size_t * numeric_path);

/// --- Leaves ---------------------------------------------------------------------------------------------------------

/// Root (Mallctl.cpp, MallctlThread.cpp).
MallctlLeaf version, epoch, backgroundThread, maxBackgroundThreads;

/// `thread.*` (MallctlThread.cpp).
MallctlLeaf threadArena, threadAllocated, threadAllocatedPtr, threadDeallocated, threadDeallocatedPtr, threadCacheEnabled, threadCacheMax,
    threadThreadCacheFlush, threadCacheNumCachedMaxReadSizeClass, threadCacheNumCachedMaxWrite, threadPeakRead, threadPeakReset,
    threadProfilingName, threadProfilingActive, threadIdle;

/// `tcache.*` (MallctlThread.cpp).
MallctlLeaf threadCacheCreate, threadCacheFlush, threadCacheDestroy;

/// `arena.<i>.*` (MallctlArenas.cpp); the index function is in Mallctl.cpp.
MallctlLeaf arenaIInitialized, arenaIDecay, arenaIPurge, arenaIReset, arenaIDestroy, arenaISbrk, arenaIOversizeThreshold,
    arenaIDirtyDecayMs, arenaIMuzzyDecayMs, arenaIExtentHooks, arenaIRetainGrowLimit, arenaIName;
MallctlIndex arenaIIndex;

/// `arenas.*` (MallctlArenas.cpp; the constant ones are generated in MallctlTree.cpp).
MallctlLeaf arenasNumArenas, arenasDirtyDecayMs, arenasMuzzyDecayMs, arenasThreadCacheMax, arenasNumThreadCacheBins, arenasCreate,
    arenasLookup;
MallctlIndex arenasBinIIndex, arenasLargeExtentIIndex;

/// `prof.*` (MallctlProfiling.cpp).
MallctlLeaf profilingThreadActiveInit, profilingActive, profilingDump, profilingGrowthDump, profilingPrefix, profilingReset,
    profilingInterval, profilingLog2Sample, profilingLogStart, profilingLogStop, profilingStatsBinsILive, profilingStatsBinsIAccumulated,
    profilingStatsLargeExtentsILive, profilingStatsLargeExtentsIAccumulated;
MallctlIndex profilingStatsBinsIIndex, profilingStatsLargeExtentsIIndex;

/// `stats.*` (MallctlStats.cpp; the mutex leaves are generated in MallctlTree.cpp).
MallctlLeaf statsAllocated, statsActive, statsMetadata, statsMetadataExtent, statsMetadataRadixTree, statsMetadataTransparentHugePages,
    statsResident, statsMapped, statsRetained, statsBackgroundThreadNumThreads, statsBackgroundThreadNumRuns,
    statsBackgroundThreadRunInterval, statsMutexesReset, statsZeroReallocs;

/// `stats.arenas.<i>.*` (MallctlStats.cpp); the index function is in Mallctl.cpp.
MallctlLeaf statsArenasINumThreads, statsArenasIUptime, statsArenasISbrk, statsArenasIDirtyDecayMs, statsArenasIMuzzyDecayMs,
    statsArenasIActivePages, statsArenasIDirtyPages, statsArenasIMuzzyPages, statsArenasIMapped, statsArenasIRetained,
    statsArenasIExtentAvailable, statsArenasIDirtyNumPurge, statsArenasIDirtyNumMadvises, statsArenasIDirtyPurged,
    statsArenasIMuzzyNumPurge, statsArenasIMuzzyNumMadvises, statsArenasIMuzzyPurged, statsArenasIBase, statsArenasIInternal,
    statsArenasIMetadataExtent, statsArenasIMetadataRadixTree, statsArenasIMetadataTransparentHugePages, statsArenasIThreadCacheBytes,
    statsArenasIThreadCacheStashedBytes, statsArenasIResident, statsArenasIAbandonedVM;
MallctlIndex statsArenasIIndex;

/// `stats.arenas.<i>.small.*`, `stats.arenas.<i>.large.*` (MallctlStats.cpp).
MallctlLeaf statsArenasISmallAllocated, statsArenasISmallNumAllocations, statsArenasISmallNumDeallocations, statsArenasISmallNumRequests,
    statsArenasISmallNumFills, statsArenasISmallNumFlushes, statsArenasILargeAllocated, statsArenasILargeNumAllocations,
    statsArenasILargeNumDeallocations, statsArenasILargeNumRequests, statsArenasILargeNumFills, statsArenasILargeNumFlushes;

/// `stats.arenas.<i>.bins.<j>.*`, `lextents.<j>.*`, `extents.<j>.*` (MallctlStats.cpp; constant index functions are in
/// MallctlTree.cpp).
MallctlLeaf statsArenasIBinsJNumAllocations, statsArenasIBinsJNumDeallocations, statsArenasIBinsJNumRequests,
    statsArenasIBinsJCurrentRegions, statsArenasIBinsJNumFills, statsArenasIBinsJNumFlushes, statsArenasIBinsJNumSlabs,
    statsArenasIBinsJNumSlabChanges, statsArenasIBinsJCurrentSlabs, statsArenasIBinsJNonFullSlabs;
MallctlLeaf statsArenasILargeExtentsJNumAllocations, statsArenasILargeExtentsJNumDeallocations, statsArenasILargeExtentsJNumRequests,
    statsArenasILargeExtentsJCurrentLargeExtents;
MallctlLeaf statsArenasIExtentsJNumDirty, statsArenasIExtentsJNumMuzzy, statsArenasIExtentsJNumRetained, statsArenasIExtentsJDirtyBytes,
    statsArenasIExtentsJMuzzyBytes, statsArenasIExtentsJRetainedBytes;
MallctlIndex statsArenasIBinsJIndex, statsArenasILargeExtentsJIndex, statsArenasIExtentsJIndex, statsArenasIHugePageShardNonFullSlabsJIndex;

/// `approximate_stats.*` (MallctlStats.cpp).
MallctlLeaf approximateStatsActive;

/// `experimental.*` (MallctlProfiling.cpp, MallctlExperimental.cpp); the index function is in Mallctl.cpp.
MallctlLeaf experimentalHooksInstall, experimentalHooksRemove, experimentalHooksProfilingBacktrace, experimentalHooksProfilingDump,
    experimentalHooksProfilingSample, experimentalHooksProfilingSampleFree, experimentalHooksSafetyCheckAbort, experimentalHooksThreadEvent,
    experimentalUtilizationQuery, experimentalUtilizationBatchQuery, experimentalArenasIActivePagesPtr, experimentalArenasCreateExtended,
    experimentalProfilingRecentAllocMax, experimentalProfilingRecentAllocDump, experimentalBatchAlloc, experimentalThreadActivityCallback;
MallctlIndex experimentalArenasIIndex;

}

}

/// Defines a leaf of a dropped feature (returns `ENOENT`); the node is kept so that the numeric paths stay identical.
#define ALLOCATOR_MALLCTL_DROPPED(name) \
    int name(ThreadState &, const size_t *, size_t, void *, size_t *, void *, size_t) \
    { \
        return dropped(); \
    }
