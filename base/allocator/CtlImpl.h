#pragma once

/// Internal interface of the `mallctl` implementation (jemalloc: the static parts of `ctl.c`, `ctl.h`): the access
/// check helpers (`READONLY`, `READ`, `WRITE`, ...), the leaf generators (`CTL_RO_*GEN`), the ctl state
/// (`ctl_mtx`, `ctl_stats`, `ctl_arenas`) and the declarations of all leaves and index functions of the tree.
///
/// Only for the Ctl*.cpp files and Stats.cpp.
///
/// Every leaf of the tree is declared here under a name derived from jemalloc's (`arena_i_decay_ctl` ->
/// `ctl::arenaIDecay`), and defined in the file of its subtree:
///
///     Ctl.cpp             version, epoch, index functions of `arena`, `stats.arenas`, `experimental.arenas`
///     CtlTree.cpp         the tree itself; `config.*`, `opt.*`, constant `arenas.*` leaves, HPA/SEC zero leaves and
///                         the mutex profiling leaves (generated from templates)
///     CtlThread.cpp       `background_thread`, `max_background_threads`, `thread.*`, `tcache.*`
///     CtlArenas.cpp       `arena.<i>.*`, `arenas.*`
///     CtlProf.cpp         `prof.*`, `experimental.hooks.prof_*`, `experimental.prof_recent.*`
///     CtlStats.cpp        `stats.*`, `stats.arenas.<i>.*`, `approximate_stats.*`
///     CtlExperimental.cpp the rest of `experimental.*`
///
/// Leaves of dropped features are defined with `JE_CTL_DROPPED(name)` (they return `ENOENT`).

#include <allocator/Arena.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Bin.h>
#include <allocator/Common.h>
#include <allocator/Ctl.h>
#include <allocator/IntrusiveList.h>
#include <allocator/Mutex.h>
#include <allocator/NsTime.h>
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

namespace ctl
{

/// jemalloc: READONLY
JE_ALWAYS_INLINE int readOnly(const void * newp, size_t newlen)
{
    return (newp != nullptr || newlen != 0) ? EPERM : 0;
}

/// jemalloc: WRITEONLY
JE_ALWAYS_INLINE int writeOnly(const void * oldp, const size_t * oldlenp)
{
    return (oldp != nullptr || oldlenp != nullptr) ? EPERM : 0;
}

/// Can read or write, but not both. jemalloc: READ_XOR_WRITE
JE_ALWAYS_INLINE int readXorWrite(const void * oldp, const size_t * oldlenp, const void * newp, size_t newlen)
{
    return ((oldp != nullptr && oldlenp != nullptr) && (newp != nullptr || newlen != 0)) ? EPERM : 0;
}

/// Can neither read nor write. jemalloc: NEITHER_READ_NOR_WRITE
JE_ALWAYS_INLINE int neitherReadNorWrite(const void * oldp, const size_t * oldlenp, const void * newp, size_t newlen)
{
    return (oldp != nullptr || oldlenp != nullptr || newp != nullptr || newlen != 0) ? EPERM : 0;
}

/// Verify that the space provided is enough; otherwise sets `*oldlenp` to 0 (if non-null).
/// jemalloc: VERIFY_READ
template <typename T>
JE_ALWAYS_INLINE int verifyRead(const void * oldp, size_t * oldlenp)
{
    if (oldp == nullptr || oldlenp == nullptr || *oldlenp != sizeof(T))
    {
        if (oldlenp != nullptr)
            *oldlenp = 0;
        return EINVAL;
    }
    return 0;
}

/// Reads only if both `oldp` and `oldlenp` are non-null. On a size mismatch copies `min(sizeof(T), *oldlenp)` bytes
/// of the value anyway, sets `*oldlenp` to that and returns `EINVAL`.
/// jemalloc: READ
template <typename T>
JE_ALWAYS_INLINE int read(void * oldp, size_t * oldlenp, const T & value)
{
    if (oldp != nullptr && oldlenp != nullptr)
    {
        if (*oldlenp != sizeof(T))
        {
            size_t copylen = (sizeof(T) <= *oldlenp) ? sizeof(T) : *oldlenp;
            std::memcpy(oldp, static_cast<const void *>(&value), copylen);
            *oldlenp = copylen;
            return EINVAL;
        }
        std::memcpy(oldp, static_cast<const void *>(&value), sizeof(T));
    }
    return 0;
}

/// Writes only if `newp` is non-null (`newp == nullptr` with `newlen != 0` passes). jemalloc: WRITE
template <typename T>
JE_ALWAYS_INLINE int write(const void * newp, size_t newlen, T & value)
{
    if (newp != nullptr)
    {
        if (newlen != sizeof(T))
            return EINVAL;
        std::memcpy(static_cast<void *>(&value), newp, sizeof(T));
    }
    return 0;
}

/// jemalloc: ASSURED_WRITE
template <typename T>
JE_ALWAYS_INLINE int assuredWrite(const void * newp, size_t newlen, T & value)
{
    if (newp == nullptr || newlen != sizeof(T))
        return EINVAL;
    std::memcpy(static_cast<void *>(&value), newp, sizeof(T));
    return 0;
}

/// jemalloc: MIB_UNSIGNED
JE_ALWAYS_INLINE int mibUnsigned(const size_t * mib, size_t i, unsigned & value)
{
    if (mib[i] > UINT_MAX)
        return EFAULT;
    value = static_cast<unsigned>(mib[i]);
    return 0;
}

/// The return value of leaves of dropped features (HPA, SEC, `prof_log`, `hook.c`, test hooks).
constexpr int dropped()
{
    return ENOENT;
}

}

/// --- ctl state (ctl.h, ctl.c) --------------------------------------------------------------------------------------

/// The size of `hpa_shard_stats_t` (`psset_stats_t` + `hpa_shard_nonderived_stats_t` + `sec_stats_t`; independent of
/// the page size). HPA and SEC are dropped: the statistics are always zero, but the space is kept so that the slot
/// allocation (`stats.metadata`) has the same size as jemalloc's.
inline constexpr size_t HPA_SHARD_STATS_PLACEHOLDER_SIZE = 3328;

/// jemalloc: hpa_shard_stats_t (always zero)
struct HpaShardStatsPlaceholder
{
    alignas(8) unsigned char data[HPA_SHARD_STATS_PLACEHOLDER_SIZE];
};

/// The merged statistics of an arena (or of all / destroyed arenas).
/// jemalloc: ctl_arena_stats_t
struct CtlArenaStats
{
    ArenaStats astats;

    /// Aggregate stats for small size classes, based on bin stats.
    size_t allocated_small;
    uint64_t nmalloc_small;
    uint64_t ndalloc_small;
    uint64_t nrequests_small;
    uint64_t nfills_small;
    uint64_t nflushes_small;

    BinStatsData bstats[SC_NBINS];
    ArenaStatsLarge lstats[SC_NSIZES - SC_NBINS];
    PacExtentStats estats[SC_NPSIZES];
    HpaShardStatsPlaceholder hpastats;
};

static_assert(offsetof(CtlArenaStats, allocated_small) == sizeof(ArenaStats));
static_assert(offsetof(CtlArenaStats, bstats) == sizeof(ArenaStats) + 48);
static_assert(
    sizeof(CtlArenaStats)
    == sizeof(ArenaStats) + 48 + sizeof(BinStatsData) * SC_NBINS + sizeof(ArenaStatsLarge) * (SC_NSIZES - SC_NBINS)
        + sizeof(PacExtentStats) * SC_NPSIZES + HPA_SHARD_STATS_PLACEHOLDER_SIZE);
/// Measured from the C build (aarch64 glibc, LG_PAGE=16).
static_assert(LG_PAGE != 16 || sizeof(CtlArenaStats) == 40784, "Must have the size of ctl_arena_stats_t");

/// jemalloc: ctl_arena_t
struct CtlArena
{
    unsigned arena_ind;
    bool initialized;
    RingLink<CtlArena> destroyed_link;

    /// Basic stats, supported even if !config_stats.
    unsigned nthreads;
    const char * dss;
    ssize_t dirty_decay_ms;
    ssize_t muzzy_decay_ms;
    size_t pactive;
    size_t pdirty;
    size_t pmuzzy;

    CtlArenaStats * astats;
};

static_assert(sizeof(CtlArena) == 88, "Must have the size of ctl_arena_t");

/// jemalloc: ctl_arenas_t
struct CtlArenas
{
    uint64_t epoch;
    unsigned narenas;
    IntrusiveList<CtlArena, &CtlArena::destroyed_link> destroyed;

    /// Element 0 corresponds to merged stats for extant arenas (accessed via MALLCTL_ARENAS_ALL), element 1
    /// corresponds to merged stats for destroyed arenas (accessed via MALLCTL_ARENAS_DESTROYED), and the remaining
    /// MALLOCX_ARENA_LIMIT elements correspond to arenas.
    CtlArena * arenas[2 + MALLOCX_ARENA_LIMIT];
};

static_assert(sizeof(CtlArenas) == 32800, "Must have the size of ctl_arenas_t");

/// `BackgroundThreadStats` (jemalloc: background_thread_stats_t) is in BackgroundThread.h.

/// jemalloc: ctl_stats_t
struct CtlStats
{
    size_t allocated;
    size_t active;
    size_t metadata;
    size_t metadata_edata;
    size_t metadata_rtree;
    size_t metadata_thp;
    size_t resident;
    size_t mapped;
    size_t retained;

    BackgroundThreadStats background_thread;
    MutexProfData mutex_prof_data[mutex_prof_num_global_mutexes];
};

static_assert(sizeof(CtlStats) == 736, "Must have the size of ctl_stats_t");

/// `ctl_mtx` protects `ctl_stats->*` and `ctl_arenas->*`. Name "ctl", rank `MutexRank::CTL`.
/// jemalloc: ctl_mtx
extern constinit Mutex ctl_mtx;
/// Allocated from `b0` by `ctlInit` (never freed). jemalloc: ctl_stats, ctl_arenas
extern constinit CtlStats * ctl_stats;
extern constinit CtlArenas * ctl_arenas;

/// Maps an arena index to its slot in `ctl_arenas->arenas`: `MALLCTL_ARENAS_ALL` -> 0, `MALLCTL_ARENAS_DESTROYED`
/// -> 1, `compat && i == narenas` -> 0 (deprecated alias of `MALLCTL_ARENAS_ALL`), `validate && i >= narenas` ->
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
CtlArena * arenasIImpl(ThreadState * tsdn, size_t i, bool compat, bool init);

/// The existing slot of arena `i` (with the compat mapping). jemalloc: arenas_i
CtlArena * arenasI(size_t i);

/// jemalloc: ctl_arena_clear
void ctlArenaClear(CtlArena * ctl_arena);

/// Returns true if `stats.arenas.<i>` / `experimental.arenas.<i>` does not exist. Requires `ctl_mtx`.
/// jemalloc: ctl_arenas_i_verify
bool ctlArenasIVerify(size_t i);

/// Refreshes the snapshot of the statistics (`epoch`). Requires `ctl_mtx`.
/// jemalloc: ctl_refresh
void ctlRefresh(ThreadState * tsdn);

/// Clears the slot of arena `i`, merges the stats of `arena` into it and then into `ctl_sdarena` (the sum of all or
/// of the destroyed arenas). Requires `ctl_mtx`.
/// jemalloc: ctl_arena_refresh
void ctlArenaRefresh(ThreadState * tsdn, Arena * arena, CtlArena * ctl_sdarena, unsigned i, bool destroyed);

/// Creates an arena for `arenas.create`, recycling the index of the most recently destroyed arena if any. Returns
/// `UINT_MAX` on error. Requires `ctl_mtx`.
/// jemalloc: ctl_arena_init
unsigned ctlArenaInit(ThreadState & tsd, const ArenaConfig * config);

/// --- Leaf generators (ctl.c: CTL_RO_*GEN) --------------------------------------------------------------------------

namespace ctl
{

/// Calls a value getter, which takes either nothing or the MIB.
template <auto get>
JE_ALWAYS_INLINE decltype(auto) getValue(const size_t * mib)
{
    if constexpr (std::is_invocable_v<decltype(get), const size_t *>)
        return get(mib);
    else
        return get();
}

/// A read-only value, no lock. jemalloc: CTL_RO_NL_GEN, CTL_RO_CONFIG_GEN
template <typename T, auto get>
int readOnlyNl(ThreadState &, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = readOnly(newp, newlen))
        return ret;
    T oldval = getValue<get>(mib);
    return read(oldp, oldlenp, oldval);
}

/// A read-only value that exists only if `cond()`, no lock. jemalloc: CTL_RO_NL_CGEN
template <typename T, auto cond, auto get>
int readOnlyNlIf(ThreadState &, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (!cond())
        return ENOENT;
    if (int ret = readOnly(newp, newlen))
        return ret;
    T oldval = getValue<get>(mib);
    return read(oldp, oldlenp, oldval);
}

/// A read-only value under `ctl_mtx`. jemalloc: CTL_RO_GEN
template <typename T, auto get>
int readOnlyLocked(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    MutexLock lock(&tsd, ctl_mtx);
    if (int ret = readOnly(newp, newlen))
        return ret;
    T oldval = getValue<get>(mib);
    return read(oldp, oldlenp, oldval);
}

/// A read-only value that exists only if `cond()` (checked before locking), under `ctl_mtx`.
/// jemalloc: CTL_RO_CGEN
template <typename T, auto cond, auto get>
int readOnlyLockedIf(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (!cond())
        return ENOENT;
    MutexLock lock(&tsd, ctl_mtx);
    if (int ret = readOnly(newp, newlen))
        return ret;
    T oldval = getValue<get>(mib);
    return read(oldp, oldlenp, oldval);
}

/// The counters of a mutex profiling node, in the order of the children (`MUTEX_PROF_DATA_NODE`).
enum class MutexProfCounter : unsigned
{
    NumOps,
    NumWait,
    NumSpinAcq,
    NumOwnerSwitch,
    TotalWaitTime,
    MaxWaitTime,
    MaxNumThds,
};

/// A mutex profiling leaf: `CTL_RO_CGEN(config_stats, ...)` of one field of the `MutexProfData` returned by
/// `accessor(mib)` (called under `ctl_mtx`). `max_num_thds` is a `uint32_t`, the rest are `uint64_t`.
/// jemalloc: RO_MUTEX_CTL_GEN
template <auto accessor, MutexProfCounter counter>
int mutexProf(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if constexpr (!config::stats)
        return ENOENT;
    MutexLock lock(&tsd, ctl_mtx);
    if (int ret = readOnly(newp, newlen))
        return ret;
    const MutexProfData * data = accessor(mib);
    if constexpr (counter == MutexProfCounter::NumOps)
        return read<uint64_t>(oldp, oldlenp, data->n_lock_ops);
    else if constexpr (counter == MutexProfCounter::NumWait)
        return read<uint64_t>(oldp, oldlenp, data->n_wait_times);
    else if constexpr (counter == MutexProfCounter::NumSpinAcq)
        return read<uint64_t>(oldp, oldlenp, data->n_spin_acquired);
    else if constexpr (counter == MutexProfCounter::NumOwnerSwitch)
        return read<uint64_t>(oldp, oldlenp, data->n_owner_switches);
    else if constexpr (counter == MutexProfCounter::TotalWaitTime)
        return read<uint64_t>(oldp, oldlenp, data->tot_wait_time.ns());
    else if constexpr (counter == MutexProfCounter::MaxWaitTime)
        return read<uint64_t>(oldp, oldlenp, data->max_wait_time.ns());
    else
        return read<uint32_t>(oldp, oldlenp, data->max_n_thds);
}

/// --- Mutex profiling data accessors (called under `ctl_mtx`) ---

/// `ctl_stats->mutex_prof_data[ind]`.
template <unsigned ind>
const MutexProfData * globalMutexProfData(const size_t *)
{
    return &ctl_stats->mutex_prof_data[ind];
}

/// `arenas_i(mib[2])->astats->astats.mutex_prof_data[ind]` (CtlStats.cpp).
const MutexProfData * arenaMutexProfData(const size_t * mib, unsigned ind);

template <unsigned ind>
const MutexProfData * arenaMutexProfDataOf(const size_t * mib)
{
    return arenaMutexProfData(mib, ind);
}

/// `arenas_i(mib[2])->astats->bstats[mib[4]].mutex_data` (CtlStats.cpp).
const MutexProfData * binMutexProfData(const size_t * mib);

/// --- Leaves ---------------------------------------------------------------------------------------------------------

/// Root (Ctl.cpp, CtlThread.cpp).
CtlLeaf version, epoch, backgroundThread, maxBackgroundThreads;

/// `thread.*` (CtlThread.cpp).
CtlLeaf threadArena, threadAllocated, threadAllocatedp, threadDeallocated, threadDeallocatedp, threadTcacheEnabled,
    threadTcacheMax, threadTcacheFlush, threadTcacheNcachedMaxReadSizeclass, threadTcacheNcachedMaxWrite, threadPeakRead,
    threadPeakReset, threadProfName, threadProfActive, threadIdle;

/// `tcache.*` (CtlThread.cpp).
CtlLeaf tcacheCreate, tcacheFlush, tcacheDestroy;

/// `arena.<i>.*` (CtlArenas.cpp); the index function is in Ctl.cpp.
CtlLeaf arenaIInitialized, arenaIDecay, arenaIPurge, arenaIReset, arenaIDestroy, arenaIDss, arenaIOversizeThreshold,
    arenaIDirtyDecayMs, arenaIMuzzyDecayMs, arenaIExtentHooks, arenaIRetainGrowLimit, arenaIName;
CtlIndex arenaIIndex;

/// `arenas.*` (CtlArenas.cpp; the constant ones are generated in CtlTree.cpp).
CtlLeaf arenasNarenas, arenasDirtyDecayMs, arenasMuzzyDecayMs, arenasTcacheMax, arenasNhbins, arenasCreate, arenasLookup;
CtlIndex arenasBinIIndex, arenasLextentIIndex;

/// `prof.*` (CtlProf.cpp).
CtlLeaf profThreadActiveInit, profActive, profDump, profGdump, profPrefix, profReset, profInterval, profLgSample,
    profLogStart, profLogStop, profStatsBinsILive, profStatsBinsIAccum, profStatsLextentsILive, profStatsLextentsIAccum;
CtlIndex profStatsBinsIIndex, profStatsLextentsIIndex;

/// `stats.*` (CtlStats.cpp; the mutex leaves are generated in CtlTree.cpp).
CtlLeaf statsAllocated, statsActive, statsMetadata, statsMetadataEdata, statsMetadataRtree, statsMetadataThp,
    statsResident, statsMapped, statsRetained, statsBackgroundThreadNumThreads, statsBackgroundThreadNumRuns,
    statsBackgroundThreadRunInterval, statsMutexesReset, statsZeroReallocs;

/// `stats.arenas.<i>.*` (CtlStats.cpp); the index function is in Ctl.cpp.
CtlLeaf statsArenasINthreads, statsArenasIUptime, statsArenasIDss, statsArenasIDirtyDecayMs, statsArenasIMuzzyDecayMs,
    statsArenasIPactive, statsArenasIPdirty, statsArenasIPmuzzy, statsArenasIMapped, statsArenasIRetained,
    statsArenasIExtentAvail, statsArenasIDirtyNpurge, statsArenasIDirtyNmadvise, statsArenasIDirtyPurged,
    statsArenasIMuzzyNpurge, statsArenasIMuzzyNmadvise, statsArenasIMuzzyPurged, statsArenasIBase, statsArenasIInternal,
    statsArenasIMetadataEdata, statsArenasIMetadataRtree, statsArenasIMetadataThp, statsArenasITcacheBytes,
    statsArenasITcacheStashedBytes, statsArenasIResident, statsArenasIAbandonedVm;
CtlIndex statsArenasIIndex;

/// `stats.arenas.<i>.small.*`, `stats.arenas.<i>.large.*` (CtlStats.cpp).
CtlLeaf statsArenasISmallAllocated, statsArenasISmallNmalloc, statsArenasISmallNdalloc, statsArenasISmallNrequests,
    statsArenasISmallNfills, statsArenasISmallNflushes, statsArenasILargeAllocated, statsArenasILargeNmalloc,
    statsArenasILargeNdalloc, statsArenasILargeNrequests, statsArenasILargeNfills, statsArenasILargeNflushes;

/// `stats.arenas.<i>.bins.<j>.*`, `lextents.<j>.*`, `extents.<j>.*` (CtlStats.cpp; constant index functions are in
/// CtlTree.cpp).
CtlLeaf statsArenasIBinsJNmalloc, statsArenasIBinsJNdalloc, statsArenasIBinsJNrequests, statsArenasIBinsJCurregs,
    statsArenasIBinsJNfills, statsArenasIBinsJNflushes, statsArenasIBinsJNslabs, statsArenasIBinsJNreslabs,
    statsArenasIBinsJCurslabs, statsArenasIBinsJNonfullSlabs;
CtlLeaf statsArenasILextentsJNmalloc, statsArenasILextentsJNdalloc, statsArenasILextentsJNrequests,
    statsArenasILextentsJCurlextents;
CtlLeaf statsArenasIExtentsJNdirty, statsArenasIExtentsJNmuzzy, statsArenasIExtentsJNretained,
    statsArenasIExtentsJDirtyBytes, statsArenasIExtentsJMuzzyBytes, statsArenasIExtentsJRetainedBytes;
CtlIndex statsArenasIBinsJIndex, statsArenasILextentsJIndex, statsArenasIExtentsJIndex,
    statsArenasIHpaShardNonfullSlabsJIndex;

/// `approximate_stats.*` (CtlStats.cpp).
CtlLeaf approximateStatsActive;

/// `experimental.*` (CtlProf.cpp, CtlExperimental.cpp); the index function is in Ctl.cpp.
CtlLeaf experimentalHooksInstall, experimentalHooksRemove, experimentalHooksProfBacktrace, experimentalHooksProfDump,
    experimentalHooksProfSample, experimentalHooksProfSampleFree, experimentalHooksSafetyCheckAbort,
    experimentalHooksThreadEvent, experimentalUtilizationQuery, experimentalUtilizationBatchQuery,
    experimentalArenasIPactivep, experimentalArenasCreateExt, experimentalProfRecentAllocMax,
    experimentalProfRecentAllocDump, experimentalBatchAlloc, experimentalThreadActivityCallback;
CtlIndex experimentalArenasIIndex;

}

}

/// Defines a leaf of a dropped feature (returns `ENOENT`); the node is kept so that the MIBs stay identical.
#define JE_CTL_DROPPED(name) \
    int name(ThreadState &, const size_t *, size_t, void *, size_t *, void *, size_t) \
    { \
        return dropped(); \
    }
