/// `stats.*`, `stats.arenas.<i>.*`, `approximate_stats.*` (jemalloc: `ctl.c`).
///
/// The values are the snapshot taken by the last `epoch` refresh (`ctlRefresh`), except `stats.zero_reallocs` and
/// `approximate_stats.active`, which are live. The mutex profiling leaves are generated in CtlTree.cpp from the
/// accessors defined here; the index function of `stats.arenas` is in Ctl.cpp, the constant index functions of
/// `bins`, `lextents`, `extents` and `hpa_shard.nonfull_slabs` are in CtlTree.cpp.

#include <allocator/CtlImpl.h>

#include <allocator/Arenas.h>
#include <allocator/Options.h>
#include <allocator/Prof.h>
#include <allocator/ThreadState.h>

#include <atomic>

namespace jemalloc
{

/// The number of `realloc(ptr, 0)` calls (`stats.zero_reallocs`). Defined by the front-end (Api.cpp).
/// jemalloc: zero_realloc_count
extern std::atomic<size_t> zero_realloc_count;

}

namespace jemalloc::ctl
{

namespace
{

constexpr bool statsEnabled()
{
    return config::stats;
}

}

/// --- stats.* ---------------------------------------------------------------------------------------------------------

/// jemalloc: CTL_RO_CGEN(config_stats, stats_*, ctl_stats->*, ...)
#define JE_CTL_STATS_LEAF(name, type, expr) \
    int name(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen) \
    { \
        return readOnlyLockedIf<type, statsEnabled, [] { return expr; }>(tsd, mib, miblen, oldp, oldlenp, newp, newlen); \
    }

JE_CTL_STATS_LEAF(statsAllocated, size_t, ctl_stats->allocated)
JE_CTL_STATS_LEAF(statsActive, size_t, ctl_stats->active)
JE_CTL_STATS_LEAF(statsMetadata, size_t, ctl_stats->metadata)
JE_CTL_STATS_LEAF(statsMetadataEdata, size_t, ctl_stats->metadata_edata)
JE_CTL_STATS_LEAF(statsMetadataRtree, size_t, ctl_stats->metadata_rtree)
JE_CTL_STATS_LEAF(statsMetadataThp, size_t, ctl_stats->metadata_thp)
JE_CTL_STATS_LEAF(statsResident, size_t, ctl_stats->resident)
JE_CTL_STATS_LEAF(statsMapped, size_t, ctl_stats->mapped)
JE_CTL_STATS_LEAF(statsRetained, size_t, ctl_stats->retained)

JE_CTL_STATS_LEAF(statsBackgroundThreadNumThreads, size_t, ctl_stats->background_thread.num_threads)
JE_CTL_STATS_LEAF(statsBackgroundThreadNumRuns, uint64_t, ctl_stats->background_thread.num_runs)
JE_CTL_STATS_LEAF(statsBackgroundThreadRunInterval, uint64_t, ctl_stats->background_thread.run_interval.ns())

#undef JE_CTL_STATS_LEAF

namespace
{

/// jemalloc: MUTEX_PROF_RESET
void mutexProfReset(ThreadState * tsdn, Mutex & mtx)
{
    MutexLock lock(tsdn, mtx);
    mtx.profDataReset(tsdn);
}

}

/// Resets all mutex stats, including global, arena and bin mutexes. No access checks.
/// jemalloc: stats_mutexes_reset_ctl
int statsMutexesReset(ThreadState & tsd, const size_t *, size_t, void *, size_t *, void *, size_t)
{
    if constexpr (!config::stats)
        return ENOENT;

    ThreadState * tsdn = &tsd;

    /// Global mutexes: ctl and prof.
    mutexProfReset(tsdn, ctl_mtx);
    if constexpr (config::background_thread)
        mutexProfReset(tsdn, background_thread_lock);
    if (config::prof && opt.prof)
    {
        mutexProfReset(tsdn, bt2gctx_mtx);
        mutexProfReset(tsdn, tdatas_mtx);
        mutexProfReset(tsdn, prof_dump_mtx);
        mutexProfReset(tsdn, prof_recent_alloc_mtx);
        mutexProfReset(tsdn, prof_recent_dump_mtx);
        mutexProfReset(tsdn, prof_stats_mtx);
    }

    /// Per arena mutexes.
    unsigned n = narenasTotalGet();
    for (unsigned i = 0; i < n; ++i)
    {
        Arena * arena = arenaGet(tsdn, i, false);
        if (arena == nullptr)
            continue;
        mutexProfReset(tsdn, arena->large_mtx);
        mutexProfReset(tsdn, arena->pa_shard.edata_cache.mutex());
        mutexProfReset(tsdn, arena->pa_shard.pac.ecache_dirty.mtx);
        mutexProfReset(tsdn, arena->pa_shard.pac.ecache_muzzy.mtx);
        mutexProfReset(tsdn, arena->pa_shard.pac.ecache_retained.mtx);
        mutexProfReset(tsdn, arena->pa_shard.pac.decay_dirty.mtx);
        mutexProfReset(tsdn, arena->pa_shard.pac.decay_muzzy.mtx);
        mutexProfReset(tsdn, arena->tcache_ql_mtx);
        mutexProfReset(tsdn, arena->base->mutex());

        for (szind_t j = 0; j < SC_NBINS; ++j)
        {
            for (unsigned k = 0; k < bin_infos[j].n_shards; ++k)
                mutexProfReset(tsdn, arenaGetBin(arena, j, k)->lock);
        }
    }
    return 0;
}

/// jemalloc: CTL_RO_CGEN(config_stats, stats_zero_reallocs, atomic_load_zu(&zero_realloc_count, ATOMIC_RELAXED), size_t)
int statsZeroReallocs(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return readOnlyLockedIf<size_t, statsEnabled, [] { return zero_realloc_count.load(std::memory_order_relaxed); }>(
        tsd, mib, miblen, oldp, oldlenp, newp, newlen);
}

/// Live (not the epoch snapshot): the sum of the active pages of all arenas. It should not be compared with other
/// stats.
/// jemalloc: approximate_stats_active_ctl
int approximateStatsActive(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = readOnly(newp, newlen))
        return ret;

    ThreadState * tsdn = &tsd;
    unsigned n = narenasTotalGet();
    size_t approximate_nactive = 0;
    for (unsigned i = 0; i < n; ++i)
    {
        Arena * arena = arenaGet(tsdn, i, false);
        if (arena == nullptr)
            continue;
        /// Accumulate nactive pages from each arena's pa_shard.
        approximate_nactive += arena->pa_shard.nactiveGet();
    }

    size_t approximate_active_bytes = approximate_nactive << LG_PAGE;
    return read(oldp, oldlenp, approximate_active_bytes);
}

/// --- stats.arenas.<i>.* -------------------------------------------------------------------------------------------

/// The basic fields of the slot (`ctl_arena_t`), under `ctl_mtx`. jemalloc: CTL_RO_GEN(stats_arenas_i_*,
/// arenas_i(mib[2])->*, ...)
#define JE_CTL_ARENA_LEAF(name, type, field) \
    int name(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen) \
    { \
        return readOnlyLocked<type, [](const size_t * m) { return arenasI(m[2])->field; }>( \
            tsd, mib, miblen, oldp, oldlenp, newp, newlen); \
    }

JE_CTL_ARENA_LEAF(statsArenasIDss, const char *, dss)
JE_CTL_ARENA_LEAF(statsArenasIDirtyDecayMs, ssize_t, dirty_decay_ms)
JE_CTL_ARENA_LEAF(statsArenasIMuzzyDecayMs, ssize_t, muzzy_decay_ms)
JE_CTL_ARENA_LEAF(statsArenasINthreads, unsigned, nthreads)
JE_CTL_ARENA_LEAF(statsArenasIPactive, size_t, pactive)
JE_CTL_ARENA_LEAF(statsArenasIPdirty, size_t, pdirty)
JE_CTL_ARENA_LEAF(statsArenasIPmuzzy, size_t, pmuzzy)

#undef JE_CTL_ARENA_LEAF

/// The aggregate small stats of the slot (`ctl_arena_stats_t`). jemalloc: CTL_RO_CGEN(config_stats,
/// stats_arenas_i_small_*, arenas_i(mib[2])->astats->*_small, ...)
#define JE_CTL_ARENA_STATS_LEAF(name, type, field) \
    int name(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen) \
    { \
        return readOnlyLockedIf<type, statsEnabled, [](const size_t * m) { return arenasI(m[2])->astats->field; }>( \
            tsd, mib, miblen, oldp, oldlenp, newp, newlen); \
    }

JE_CTL_ARENA_STATS_LEAF(statsArenasISmallAllocated, size_t, allocated_small)
JE_CTL_ARENA_STATS_LEAF(statsArenasISmallNmalloc, uint64_t, nmalloc_small)
JE_CTL_ARENA_STATS_LEAF(statsArenasISmallNdalloc, uint64_t, ndalloc_small)
JE_CTL_ARENA_STATS_LEAF(statsArenasISmallNrequests, uint64_t, nrequests_small)
JE_CTL_ARENA_STATS_LEAF(statsArenasISmallNfills, uint64_t, nfills_small)
JE_CTL_ARENA_STATS_LEAF(statsArenasISmallNflushes, uint64_t, nflushes_small)

#undef JE_CTL_ARENA_STATS_LEAF

namespace
{

/// `arenas_i(mib[2])->astats` (requires `ctl_mtx`).
CtlArenaStats * slotStats(const size_t * mib)
{
    return arenasI(mib[2])->astats;
}

/// `astats->bstats[mib[4]]`. The index function accepts `j == SC_NBINS` (jemalloc compatibility), which reads the
/// memory that follows the array (the beginning of `lstats`), as jemalloc does: the address is computed from the
/// beginning of the structure.
const BinStatsData & slotBin(const size_t * mib)
{
    const CtlArenaStats * astats = slotStats(mib);
    return *reinterpret_cast<const BinStatsData *>(
        reinterpret_cast<const char *>(astats) + offsetof(CtlArenaStats, bstats) + mib[4] * sizeof(BinStatsData));
}

/// `astats->lstats[mib[4]]` (`j == SC_NSIZES - SC_NBINS` reads the beginning of `estats`, see above).
const ArenaStatsLarge & slotLarge(const size_t * mib)
{
    const CtlArenaStats * astats = slotStats(mib);
    return *reinterpret_cast<const ArenaStatsLarge *>(
        reinterpret_cast<const char *>(astats) + offsetof(CtlArenaStats, lstats) + mib[4] * sizeof(ArenaStatsLarge));
}

/// `astats->estats[mib[4]]`.
const PacExtentStats & slotExtents(const size_t * mib)
{
    return slotStats(mib)->estats[mib[4]];
}

const PacStats & slotPac(const size_t * mib)
{
    return slotStats(mib)->astats.pa_shard_stats.pac_stats;
}

}

/// jemalloc: CTL_RO_CGEN(config_stats, stats_arenas_i_*, <expr of mib>, type)
#define JE_CTL_SLOT_LEAF(name, type, expr) \
    int name(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen) \
    { \
        return readOnlyLockedIf<type, statsEnabled, [](const size_t * m) -> type { return expr; }>( \
            tsd, mib, miblen, oldp, oldlenp, newp, newlen); \
    }

/// jemalloc: CTL_RO_GEN(stats_arenas_i_uptime, nstime_ns(&arenas_i(mib[2])->astats->astats.uptime), uint64_t)
int statsArenasIUptime(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return readOnlyLocked<uint64_t, [](const size_t * m) { return slotStats(m)->astats.uptime.ns(); }>(
        tsd, mib, miblen, oldp, oldlenp, newp, newlen);
}

JE_CTL_SLOT_LEAF(statsArenasIMapped, size_t, slotStats(m)->astats.mapped)
JE_CTL_SLOT_LEAF(statsArenasIRetained, size_t, slotPac(m).retained)
JE_CTL_SLOT_LEAF(statsArenasIExtentAvail, size_t, slotStats(m)->astats.pa_shard_stats.edata_avail)
JE_CTL_SLOT_LEAF(statsArenasIDirtyNpurge, uint64_t, slotPac(m).decay_dirty.npurge.readUnsynchronized())
JE_CTL_SLOT_LEAF(statsArenasIDirtyNmadvise, uint64_t, slotPac(m).decay_dirty.nmadvise.readUnsynchronized())
JE_CTL_SLOT_LEAF(statsArenasIDirtyPurged, uint64_t, slotPac(m).decay_dirty.purged.readUnsynchronized())
JE_CTL_SLOT_LEAF(statsArenasIMuzzyNpurge, uint64_t, slotPac(m).decay_muzzy.npurge.readUnsynchronized())
JE_CTL_SLOT_LEAF(statsArenasIMuzzyNmadvise, uint64_t, slotPac(m).decay_muzzy.nmadvise.readUnsynchronized())
JE_CTL_SLOT_LEAF(statsArenasIMuzzyPurged, uint64_t, slotPac(m).decay_muzzy.purged.readUnsynchronized())
JE_CTL_SLOT_LEAF(statsArenasIBase, size_t, slotStats(m)->astats.base)
JE_CTL_SLOT_LEAF(statsArenasIInternal, size_t, slotStats(m)->astats.internal.load(std::memory_order_relaxed))
JE_CTL_SLOT_LEAF(statsArenasIMetadataEdata, size_t, slotStats(m)->astats.metadata_edata)
JE_CTL_SLOT_LEAF(statsArenasIMetadataRtree, size_t, slotStats(m)->astats.metadata_rtree)
JE_CTL_SLOT_LEAF(statsArenasIMetadataThp, size_t, slotStats(m)->astats.metadata_thp)
JE_CTL_SLOT_LEAF(statsArenasITcacheBytes, size_t, slotStats(m)->astats.tcache_bytes)
JE_CTL_SLOT_LEAF(statsArenasITcacheStashedBytes, size_t, slotStats(m)->astats.tcache_stashed_bytes)
JE_CTL_SLOT_LEAF(statsArenasIResident, size_t, slotStats(m)->astats.resident)
JE_CTL_SLOT_LEAF(statsArenasIAbandonedVm, size_t, slotPac(m).abandoned_vm.load(std::memory_order_relaxed))

JE_CTL_SLOT_LEAF(statsArenasILargeAllocated, size_t, slotStats(m)->astats.allocated_large)
JE_CTL_SLOT_LEAF(statsArenasILargeNmalloc, uint64_t, slotStats(m)->astats.nmalloc_large)
JE_CTL_SLOT_LEAF(statsArenasILargeNdalloc, uint64_t, slotStats(m)->astats.ndalloc_large)
JE_CTL_SLOT_LEAF(statsArenasILargeNrequests, uint64_t, slotStats(m)->astats.nrequests_large)
/// Note: "nmalloc_large" here instead of "nfills" in the read. This is intentional (large has no batch fill).
JE_CTL_SLOT_LEAF(statsArenasILargeNfills, uint64_t, slotStats(m)->astats.nmalloc_large)
JE_CTL_SLOT_LEAF(statsArenasILargeNflushes, uint64_t, slotStats(m)->astats.nflushes_large)

JE_CTL_SLOT_LEAF(statsArenasIBinsJNmalloc, uint64_t, slotBin(m).stats_data.nmalloc)
JE_CTL_SLOT_LEAF(statsArenasIBinsJNdalloc, uint64_t, slotBin(m).stats_data.ndalloc)
JE_CTL_SLOT_LEAF(statsArenasIBinsJNrequests, uint64_t, slotBin(m).stats_data.nrequests)
JE_CTL_SLOT_LEAF(statsArenasIBinsJCurregs, size_t, slotBin(m).stats_data.curregs)
JE_CTL_SLOT_LEAF(statsArenasIBinsJNfills, uint64_t, slotBin(m).stats_data.nfills)
JE_CTL_SLOT_LEAF(statsArenasIBinsJNflushes, uint64_t, slotBin(m).stats_data.nflushes)
JE_CTL_SLOT_LEAF(statsArenasIBinsJNslabs, uint64_t, slotBin(m).stats_data.nslabs)
JE_CTL_SLOT_LEAF(statsArenasIBinsJNreslabs, uint64_t, slotBin(m).stats_data.reslabs)
JE_CTL_SLOT_LEAF(statsArenasIBinsJCurslabs, size_t, slotBin(m).stats_data.curslabs)
JE_CTL_SLOT_LEAF(statsArenasIBinsJNonfullSlabs, size_t, slotBin(m).stats_data.nonfull_slabs)

JE_CTL_SLOT_LEAF(statsArenasILextentsJNmalloc, uint64_t, slotLarge(m).nmalloc.readUnsynchronized())
JE_CTL_SLOT_LEAF(statsArenasILextentsJNdalloc, uint64_t, slotLarge(m).ndalloc.readUnsynchronized())
JE_CTL_SLOT_LEAF(statsArenasILextentsJNrequests, uint64_t, slotLarge(m).nrequests.readUnsynchronized())
JE_CTL_SLOT_LEAF(statsArenasILextentsJCurlextents, size_t, slotLarge(m).curlextents)

JE_CTL_SLOT_LEAF(statsArenasIExtentsJNdirty, size_t, slotExtents(m).ndirty)
JE_CTL_SLOT_LEAF(statsArenasIExtentsJNmuzzy, size_t, slotExtents(m).nmuzzy)
JE_CTL_SLOT_LEAF(statsArenasIExtentsJNretained, size_t, slotExtents(m).nretained)
JE_CTL_SLOT_LEAF(statsArenasIExtentsJDirtyBytes, size_t, slotExtents(m).dirty_bytes)
JE_CTL_SLOT_LEAF(statsArenasIExtentsJMuzzyBytes, size_t, slotExtents(m).muzzy_bytes)
JE_CTL_SLOT_LEAF(statsArenasIExtentsJRetainedBytes, size_t, slotExtents(m).retained_bytes)

#undef JE_CTL_SLOT_LEAF

/// --- Mutex profiling accessors ---------------------------------------------------------------------------------------

/// `&arenas_i(mib[2])->astats->astats.mutex_prof_data[ind]`.
const MutexProfData * arenaMutexProfData(const size_t * mib, unsigned ind)
{
    return &slotStats(mib)->astats.mutex_prof_data[ind];
}

/// `&arenas_i(mib[2])->astats->bstats[mib[4]].mutex_data`.
const MutexProfData * binMutexProfData(const size_t * mib)
{
    return &slotBin(mib).mutex_data;
}

}
