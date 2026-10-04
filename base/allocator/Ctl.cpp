/// The `mallctl` machinery (jemalloc: `ctl.c`): name and MIB lookup, the entry points, the ctl state (`ctl_mtx`,
/// `ctl_stats`, `ctl_arenas`, lazy initialization and the `epoch` refresh) and the leaves and index functions that
/// depend only on the ctl state.

#include <allocator/Ctl.h>

#include <allocator/Arenas.h>
#include <allocator/Base.h>
#include <allocator/CtlImpl.h>
#include <allocator/ExtentHooks.h>
#include <allocator/Format.h>
#include <allocator/Options.h>
#include <allocator/Prof.h>
#include <allocator/ThreadState.h>

#include <atomic>
#include <cstdint>
#include <cstring>

namespace jemalloc
{

/// --- ctl state -------------------------------------------------------------------------------------------------------

constinit Mutex ctl_mtx;
constinit CtlStats * ctl_stats = nullptr;
constinit CtlArenas * ctl_arenas = nullptr;

namespace
{

/// Checked without the lock by every entry point, then again under `ctl_mtx` by `ctlInit`.
/// jemalloc: ctl_initialized
constinit std::atomic<bool> ctl_initialized{false};

/// jemalloc: JEMALLOC_VERSION (`jemalloc_macros.h`)
constexpr const char * JEMALLOC_VERSION = "5.3-RC";

}

/// jemalloc: arenas_i2a_impl
unsigned arenasI2aImpl(size_t i, bool compat, bool validate)
{
    switch (i)
    {
        case MALLCTL_ARENAS_ALL:
            return 0;
        case MALLCTL_ARENAS_DESTROYED:
            return 1;
        default:
            if (compat && i == ctl_arenas->narenas)
            {
                /// Provide deprecated backward compatibility for accessing the merged stats at index narenas rather
                /// than via MALLCTL_ARENAS_ALL. This is scheduled for removal in 6.0.0.
                return 0;
            }
            if (validate && i >= ctl_arenas->narenas)
                return UINT_MAX;
            /// This function should never be called for an index more than one past the range of indices that have
            /// initialized ctl data.
            JE_ASSERT(i < ctl_arenas->narenas || (!validate && i == ctl_arenas->narenas));
            return static_cast<unsigned>(i) + 2;
    }
}

/// jemalloc: arenas_i_impl
CtlArena * arenasIImpl(ThreadState * tsdn, size_t i, bool compat, bool init)
{
    JE_ASSERT(!compat || !init);

    CtlArena * ret = ctl_arenas->arenas[arenasI2aImpl(i, compat, false)];
    if (init && ret == nullptr)
    {
        /// The slot and its stats are allocated together (jemalloc: `struct container_s`).
        struct Container
        {
            CtlArena ctl_arena;
            CtlArenaStats astats;
        };
        auto * container = static_cast<Container *>(b0get()->alloc(tsdn, sizeof(Container), QUANTUM));
        if (container == nullptr)
            return nullptr;
        ret = &container->ctl_arena;
        ret->astats = &container->astats;
        ret->arena_ind = static_cast<unsigned>(i);
        ctl_arenas->arenas[arenasI2aImpl(i, compat, false)] = ret;
    }

    JE_ASSERT(ret == nullptr || arenasI2a(ret->arena_ind) == arenasI2a(i));
    return ret;
}

/// jemalloc: arenas_i
CtlArena * arenasI(size_t i)
{
    CtlArena * ret = arenasIImpl(nullptr, i, true, false);
    JE_ASSERT(ret != nullptr);
    return ret;
}

/// jemalloc: ctl_arena_clear
void ctlArenaClear(CtlArena * ctl_arena)
{
    ctl_arena->nthreads = 0;
    ctl_arena->dss = dss_prec_names[unsigned(DssPrec::Limit)];
    ctl_arena->dirty_decay_ms = -1;
    ctl_arena->muzzy_decay_ms = -1;
    ctl_arena->pactive = 0;
    ctl_arena->pdirty = 0;
    ctl_arena->pmuzzy = 0;
    if constexpr (config::stats)
        std::memset(static_cast<void *>(ctl_arena->astats), 0, sizeof(*ctl_arena->astats));
}

/// jemalloc: ctl_arenas_i_verify
bool ctlArenasIVerify(size_t i)
{
    unsigned a = arenasI2aImpl(i, true, true);
    return a == UINT_MAX || !ctl_arenas->arenas[a]->initialized;
}

namespace
{

/// jemalloc: ctl_background_thread_stats_read
void ctlBackgroundThreadStatsRead(ThreadState * tsdn)
{
    BackgroundThreadStats * stats = &ctl_stats->background_thread;
    if (!config::background_thread || backgroundThreadStatsRead(tsdn, stats))
    {
        std::memset(static_cast<void *>(stats), 0, sizeof(BackgroundThreadStats));
        stats->run_interval.initZero();
    }
    ctl_stats->mutex_prof_data[global_prof_mutex_max_per_bg_thd].copyFrom(stats->max_counter_per_bg_thd);
}

}

namespace
{

/// Sets `*dst += *src` non-atomically. This is safe, since everything is synchronized by the ctl mutex.
/// jemalloc: ctl_accum_locked_u64
void ctlAccumLockedU64(LockedU64 & dst, const LockedU64 & src)
{
    dst.incUnsynchronized(src.readUnsynchronized());
}

/// jemalloc: ctl_accum_atomic_zu
void ctlAccumAtomicZu(std::atomic<size_t> & dst, const std::atomic<size_t> & src)
{
    size_t cur_dst = dst.load(std::memory_order_relaxed);
    size_t cur_src = src.load(std::memory_order_relaxed);
    dst.store(cur_dst + cur_src, std::memory_order_relaxed);
}

/// jemalloc: ctl_arena_stats_amerge
void ctlArenaStatsAmerge(ThreadState * tsdn, CtlArena * ctl_arena, Arena * arena)
{
    if constexpr (config::stats)
    {
        CtlArenaStats * astats = ctl_arena->astats;
        arenaStatsMerge(
            tsdn,
            arena,
            &ctl_arena->nthreads,
            &ctl_arena->dss,
            &ctl_arena->dirty_decay_ms,
            &ctl_arena->muzzy_decay_ms,
            &ctl_arena->pactive,
            &ctl_arena->pdirty,
            &ctl_arena->pmuzzy,
            &astats->astats,
            astats->bstats,
            astats->lstats,
            astats->estats);

        for (unsigned i = 0; i < SC_NBINS; ++i)
        {
            const BinStats & bstats = astats->bstats[i].stats_data;
            astats->allocated_small += bstats.curregs * sz::indexToSize(i);
            astats->nmalloc_small += bstats.nmalloc;
            astats->ndalloc_small += bstats.ndalloc;
            astats->nrequests_small += bstats.nrequests;
            astats->nfills_small += bstats.nfills;
            astats->nflushes_small += bstats.nflushes;
        }
    }
    else
    {
        arenaBasicStatsMerge(
            tsdn,
            arena,
            &ctl_arena->nthreads,
            &ctl_arena->dss,
            &ctl_arena->dirty_decay_ms,
            &ctl_arena->muzzy_decay_ms,
            &ctl_arena->pactive,
            &ctl_arena->pdirty,
            &ctl_arena->pmuzzy);
    }
}

/// jemalloc: ctl_arena_stats_sdmerge
void ctlArenaStatsSdmerge(CtlArena * ctl_sdarena, CtlArena * ctl_arena, bool destroyed)
{
    if (!destroyed)
    {
        ctl_sdarena->nthreads += ctl_arena->nthreads;
        ctl_sdarena->pactive += ctl_arena->pactive;
        ctl_sdarena->pdirty += ctl_arena->pdirty;
        ctl_sdarena->pmuzzy += ctl_arena->pmuzzy;
    }
    else
    {
        JE_ASSERT(ctl_arena->nthreads == 0);
        JE_ASSERT(ctl_arena->pactive == 0);
        JE_ASSERT(ctl_arena->pdirty == 0);
        JE_ASSERT(ctl_arena->pmuzzy == 0);
    }

    if constexpr (config::stats)
    {
        CtlArenaStats * sdstats = ctl_sdarena->astats;
        CtlArenaStats * astats = ctl_arena->astats;
        PacStats & sdpac = sdstats->astats.pa_shard_stats.pac_stats;
        PacStats & apac = astats->astats.pa_shard_stats.pac_stats;

        if (!destroyed)
        {
            sdstats->astats.mapped += astats->astats.mapped;
            sdpac.retained += apac.retained;
            sdstats->astats.pa_shard_stats.edata_avail += astats->astats.pa_shard_stats.edata_avail;
        }

        ctlAccumLockedU64(sdpac.decay_dirty.npurge, apac.decay_dirty.npurge);
        ctlAccumLockedU64(sdpac.decay_dirty.nmadvise, apac.decay_dirty.nmadvise);
        ctlAccumLockedU64(sdpac.decay_dirty.purged, apac.decay_dirty.purged);

        ctlAccumLockedU64(sdpac.decay_muzzy.npurge, apac.decay_muzzy.npurge);
        ctlAccumLockedU64(sdpac.decay_muzzy.nmadvise, apac.decay_muzzy.nmadvise);
        ctlAccumLockedU64(sdpac.decay_muzzy.purged, apac.decay_muzzy.purged);

        for (unsigned i = 0; i < mutex_prof_num_arena_mutexes; ++i)
            sdstats->astats.mutex_prof_data[i].merge(astats->astats.mutex_prof_data[i]);

        if (!destroyed)
        {
            sdstats->astats.base += astats->astats.base;
            sdstats->astats.metadata_edata += astats->astats.metadata_edata;
            sdstats->astats.metadata_rtree += astats->astats.metadata_rtree;
            sdstats->astats.resident += astats->astats.resident;
            sdstats->astats.metadata_thp += astats->astats.metadata_thp;
            ctlAccumAtomicZu(sdstats->astats.internal, astats->astats.internal);
        }
        else
        {
            JE_ASSERT(astats->astats.internal.load(std::memory_order_relaxed) == 0);
        }

        if (!destroyed)
            sdstats->allocated_small += astats->allocated_small;
        else
            JE_ASSERT(astats->allocated_small == 0);
        sdstats->nmalloc_small += astats->nmalloc_small;
        sdstats->ndalloc_small += astats->ndalloc_small;
        sdstats->nrequests_small += astats->nrequests_small;
        sdstats->nfills_small += astats->nfills_small;
        sdstats->nflushes_small += astats->nflushes_small;

        if (!destroyed)
            sdstats->astats.allocated_large += astats->astats.allocated_large;
        else
            JE_ASSERT(astats->astats.allocated_large == 0);
        sdstats->astats.nmalloc_large += astats->astats.nmalloc_large;
        sdstats->astats.ndalloc_large += astats->astats.ndalloc_large;
        sdstats->astats.nrequests_large += astats->astats.nrequests_large;
        sdstats->astats.nflushes_large += astats->astats.nflushes_large;
        ctlAccumAtomicZu(sdpac.abandoned_vm, apac.abandoned_vm);

        sdstats->astats.tcache_bytes += astats->astats.tcache_bytes;
        sdstats->astats.tcache_stashed_bytes += astats->astats.tcache_stashed_bytes;

        if (ctl_arena->arena_ind == 0)
            sdstats->astats.uptime = astats->astats.uptime;

        /// Merge bin stats.
        for (unsigned i = 0; i < SC_NBINS; ++i)
        {
            const BinStats & bstats = astats->bstats[i].stats_data;
            BinStats & merged = sdstats->bstats[i].stats_data;
            merged.nmalloc += bstats.nmalloc;
            merged.ndalloc += bstats.ndalloc;
            merged.nrequests += bstats.nrequests;
            if (!destroyed)
                merged.curregs += bstats.curregs;
            else
                JE_ASSERT(bstats.curregs == 0);
            merged.nfills += bstats.nfills;
            merged.nflushes += bstats.nflushes;
            merged.nslabs += bstats.nslabs;
            merged.reslabs += bstats.reslabs;
            if (!destroyed)
            {
                merged.curslabs += bstats.curslabs;
                merged.nonfull_slabs += bstats.nonfull_slabs;
            }
            else
            {
                JE_ASSERT(bstats.curslabs == 0);
                JE_ASSERT(bstats.nonfull_slabs == 0);
            }
            sdstats->bstats[i].mutex_data.merge(astats->bstats[i].mutex_data);
        }

        /// Merge stats for large allocations.
        for (unsigned i = 0; i < SC_NSIZES - SC_NBINS; ++i)
        {
            ctlAccumLockedU64(sdstats->lstats[i].nmalloc, astats->lstats[i].nmalloc);
            ctlAccumLockedU64(sdstats->lstats[i].ndalloc, astats->lstats[i].ndalloc);
            ctlAccumLockedU64(sdstats->lstats[i].nrequests, astats->lstats[i].nrequests);
            if (!destroyed)
                sdstats->lstats[i].curlextents += astats->lstats[i].curlextents;
            else
                JE_ASSERT(astats->lstats[i].curlextents == 0);
        }

        /// Merge extents stats.
        for (unsigned i = 0; i < SC_NPSIZES; ++i)
        {
            sdstats->estats[i].ndirty += astats->estats[i].ndirty;
            sdstats->estats[i].nmuzzy += astats->estats[i].nmuzzy;
            sdstats->estats[i].nretained += astats->estats[i].nretained;
            sdstats->estats[i].dirty_bytes += astats->estats[i].dirty_bytes;
            sdstats->estats[i].muzzy_bytes += astats->estats[i].muzzy_bytes;
            sdstats->estats[i].retained_bytes += astats->estats[i].retained_bytes;
        }

        /// The HPA stats (`hpa_shard_stats_accum`) are always zero.
    }
}

}

/// jemalloc: ctl_arena_refresh
void ctlArenaRefresh(ThreadState * tsdn, Arena * arena, CtlArena * ctl_sdarena, unsigned i, bool destroyed)
{
    CtlArena * ctl_arena = arenasI(i);

    ctlArenaClear(ctl_arena);
    ctlArenaStatsAmerge(tsdn, ctl_arena, arena);
    /// Merge into sum stats as well.
    ctlArenaStatsSdmerge(ctl_sdarena, ctl_arena, destroyed);
}

/// jemalloc: ctl_arena_init
unsigned ctlArenaInit(ThreadState & tsd, const ArenaConfig * config)
{
    unsigned arena_ind;
    CtlArena * ctl_arena = ctl_arenas->destroyed.last();
    if (ctl_arena != nullptr)
    {
        ctl_arenas->destroyed.remove(ctl_arena);
        arena_ind = ctl_arena->arena_ind;
    }
    else
    {
        arena_ind = ctl_arenas->narenas;
    }

    /// Trigger stats allocation.
    if (arenasIImpl(&tsd, arena_ind, false, true) == nullptr)
        return UINT_MAX;

    /// Initialize new arena.
    if (arenaInit(&tsd, arena_ind, config) == nullptr)
        return UINT_MAX;

    if (arena_ind == ctl_arenas->narenas)
        ++ctl_arenas->narenas;

    return arena_ind;
}

/// jemalloc: ctl_refresh
void ctlRefresh(ThreadState * tsdn)
{
    ctl_mtx.assertOwner(tsdn);
    /// `ctl_arenas->narenas` does not change underneath us since we hold `ctl_mtx`.
    const unsigned narenas = ctl_arenas->narenas;
    CtlArena * ctl_sarena = arenasI(MALLCTL_ARENAS_ALL);

    Arena * tarenas[MALLOCX_ARENA_LIMIT];

    /// Clear sum stats, since they will be merged into by `ctl_arena_refresh`.
    ctlArenaClear(ctl_sarena);

    for (unsigned i = 0; i < narenas; ++i)
        tarenas[i] = arenaGet(tsdn, i, false);

    for (unsigned i = 0; i < narenas; ++i)
    {
        CtlArena * ctl_arena = arenasI(i);
        bool initialized = (tarenas[i] != nullptr);
        ctl_arena->initialized = initialized;
        if (initialized)
            ctlArenaRefresh(tsdn, tarenas[i], ctl_sarena, i, false);
    }

    if constexpr (config::stats)
    {
        CtlArenaStats * sstats = ctl_sarena->astats;
        ctl_stats->allocated = sstats->allocated_small + sstats->astats.allocated_large;
        ctl_stats->active = (ctl_sarena->pactive << LG_PAGE);
        ctl_stats->metadata = sstats->astats.base + sstats->astats.internal.load(std::memory_order_relaxed);
        ctl_stats->metadata_edata = sstats->astats.metadata_edata;
        ctl_stats->metadata_rtree = sstats->astats.metadata_rtree;
        ctl_stats->resident = sstats->astats.resident;
        ctl_stats->metadata_thp = sstats->astats.metadata_thp;
        ctl_stats->mapped = sstats->astats.mapped;
        ctl_stats->retained = sstats->astats.pa_shard_stats.pac_stats.retained;

        ctlBackgroundThreadStatsRead(tsdn);

        /// jemalloc: READ_GLOBAL_MUTEX_PROF_DATA
        auto read_global_mutex_prof_data = [&](MutexProfGlobalInd ind, Mutex & mtx)
        {
            MutexLock lock(tsdn, mtx);
            mtx.profRead(tsdn, ctl_stats->mutex_prof_data[ind]);
        };
        if (config::prof && opt.prof)
        {
            read_global_mutex_prof_data(global_prof_mutex_prof, bt2gctx_mtx);
            read_global_mutex_prof_data(global_prof_mutex_prof_thds_data, tdatas_mtx);
            read_global_mutex_prof_data(global_prof_mutex_prof_dump, prof_dump_mtx);
            read_global_mutex_prof_data(global_prof_mutex_prof_recent_alloc, prof_recent_alloc_mtx);
            read_global_mutex_prof_data(global_prof_mutex_prof_recent_dump, prof_recent_dump_mtx);
            read_global_mutex_prof_data(global_prof_mutex_prof_stats, prof_stats_mtx);
        }
        if constexpr (config::background_thread)
        {
            MutexLock lock(tsdn, background_thread_lock);
            background_thread_lock.profRead(tsdn, ctl_stats->mutex_prof_data[global_prof_mutex_background_thread]);
        }
        else
        {
            ctl_stats->mutex_prof_data[global_prof_mutex_background_thread].reset();
        }

        /// We own the ctl mutex already.
        ctl_mtx.profRead(tsdn, ctl_stats->mutex_prof_data[global_prof_mutex_ctl]);
    }
    ++ctl_arenas->epoch;
}

namespace
{

/// Returns true on error (OOM). jemalloc: ctl_init
bool ctlInit(ThreadState & tsd)
{
    ThreadState * tsdn = &tsd;
    MutexLock lock(tsdn, ctl_mtx);
    if (ctl_initialized.load(std::memory_order_relaxed))
        return false;

    /// Allocate demand-zeroed space for pointers to the full range of supported arena indices.
    if (ctl_arenas == nullptr)
    {
        ctl_arenas = static_cast<CtlArenas *>(b0get()->alloc(tsdn, sizeof(CtlArenas), QUANTUM));
        if (ctl_arenas == nullptr)
            return true;
    }

    if (config::stats && ctl_stats == nullptr)
    {
        ctl_stats = static_cast<CtlStats *>(b0get()->alloc(tsdn, sizeof(CtlStats), QUANTUM));
        if (ctl_stats == nullptr)
            return true;
    }

    /// Allocate space for the current full range of arenas here rather than doing it lazily elsewhere, in order to
    /// limit when OOM-caused errors can occur.
    CtlArena * ctl_sarena = arenasIImpl(tsdn, MALLCTL_ARENAS_ALL, false, true);
    if (ctl_sarena == nullptr)
        return true;
    ctl_sarena->initialized = true;

    CtlArena * ctl_darena = arenasIImpl(tsdn, MALLCTL_ARENAS_DESTROYED, false, true);
    if (ctl_darena == nullptr)
        return true;
    ctlArenaClear(ctl_darena);
    /// Don't toggle `ctl_darena` to initialized until an arena is actually destroyed, so that
    /// `arena.<i>.initialized` can be used to query whether the stats are relevant.

    ctl_arenas->narenas = narenasTotalGet();
    for (unsigned i = 0; i < ctl_arenas->narenas; ++i)
    {
        if (arenasIImpl(tsdn, i, false, true) == nullptr)
            return true;
    }

    ctl_arenas->destroyed.init();
    ctlRefresh(tsdn);

    ctl_initialized.store(true, std::memory_order_release);
    return false;
}

/// Returns true on error.
JE_ALWAYS_INLINE bool ctlEnsureInitialized(ThreadState & tsd)
{
    return !ctl_initialized.load(std::memory_order_acquire) && ctlInit(tsd);
}

/// Equivalent to `strchrnul`.
const char * findDot(const char * elm)
{
    const char * dot = std::strchr(elm, '.');
    return dot != nullptr ? dot : elm + std::strlen(elm);
}

/// jemalloc: ctl_lookup
int ctlLookup(
    ThreadState * tsdn, const CtlNode * starting_node, const char * name, const CtlNode ** ending_nodep, size_t * mibp, size_t * depthp)
{
    const char * elm = name;
    const char * dot = findDot(elm);
    size_t elen = static_cast<size_t>(dot - elm);
    if (elen == 0)
        return ENOENT;

    const CtlNode * node = starting_node;
    for (size_t i = 0; i < *depthp; ++i)
    {
        JE_ASSERT(node != nullptr);
        JE_ASSERT(node->nchildren > 0);
        if (!node->isIndexed())
        {
            /// Children are named.
            const CtlNode * pnode = node;
            for (size_t j = 0; j < node->nchildren; ++j)
            {
                const CtlNode * child = &pnode->children[j];
                if (std::strlen(child->name) == elen && std::strncmp(elm, child->name, elen) == 0)
                {
                    node = child;
                    mibp[i] = j;
                    break;
                }
            }
            if (node == pnode)
                return ENOENT;
        }
        else
        {
            /// Children are indexed.
            uintmax_t index = strToUMax(elm, static_cast<const char **>(nullptr), 10);
            if (index == UINTMAX_MAX || index > SIZE_MAX)
                return ENOENT;

            if (!node->index(tsdn, mibp, *depthp, static_cast<size_t>(index)))
                return ENOENT;
            node = node->children;
            mibp[i] = static_cast<size_t>(index);
        }

        /// Reached the end?
        if (node->isLeaf() || *dot == '\0')
        {
            /// Terminal node.
            if (*dot != '\0')
            {
                /// The name contains more elements than are in this path through the tree.
                return ENOENT;
            }
            /// Complete lookup successful.
            *depthp = i + 1;
            break;
        }

        /// Update elm. An empty element is not rejected here: it matches no named child and has no digits.
        elm = &dot[1];
        dot = findDot(elm);
        elen = static_cast<size_t>(dot - elm);
    }
    if (ending_nodep != nullptr)
        *ending_nodep = node;
    return 0;
}

/// jemalloc: ctl_lookupbymib
int ctlLookupByMib(ThreadState * tsdn, const CtlNode ** ending_nodep, const size_t * mib, size_t miblen)
{
    const CtlNode * node = ctl_super_root_node;
    for (size_t i = 0; i < miblen; ++i)
    {
        JE_ASSERT(node != nullptr);
        /// jemalloc asserts `node->nchildren > 0` and walks past a terminal node in release builds (undefined
        /// behavior); a MIB that is longer than the path is rejected instead.
        if (node->isLeaf())
            return ENOENT;
        if (!node->isIndexed())
        {
            /// Children are named.
            if (node->nchildren <= mib[i])
                return ENOENT;
            node = &node->children[mib[i]];
        }
        else
        {
            /// Indexed element.
            if (!node->index(tsdn, mib, miblen, mib[i]))
                return ENOENT;
            node = node->children;
        }
    }
    JE_ASSERT(ending_nodep != nullptr);
    *ending_nodep = node;
    return 0;
}

}

/// --- Entry points ----------------------------------------------------------------------------------------------------

/// jemalloc: ctl_byname
int ctlByName(ThreadState & tsd, const char * name, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (ctlEnsureInitialized(tsd))
        return EAGAIN;

    size_t depth = CTL_MAX_DEPTH;
    size_t mib[CTL_MAX_DEPTH];
    const CtlNode * node = nullptr;
    int ret = ctlLookup(&tsd, ctl_super_root_node, name, &node, mib, &depth);
    if (ret != 0)
        return ret;

    if (node != nullptr && node->isLeaf())
        return node->leaf(tsd, mib, depth, oldp, oldlenp, newp, newlen);
    /// The name refers to a partial path through the ctl tree.
    return ENOENT;
}

/// jemalloc: ctl_nametomib
int ctlNameToMib(ThreadState & tsd, const char * name, size_t * mibp, size_t * miblenp)
{
    if (ctlEnsureInitialized(tsd))
        return EAGAIN;
    return ctlLookup(&tsd, ctl_super_root_node, name, nullptr, mibp, miblenp);
}

/// jemalloc: ctl_bymib
int ctlByMib(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (ctlEnsureInitialized(tsd))
        return EAGAIN;

    const CtlNode * node = nullptr;
    int ret = ctlLookupByMib(&tsd, &node, mib, miblen);
    if (ret != 0)
        return ret;

    /// Call the ctl function.
    if (node != nullptr && node->isLeaf())
        return node->leaf(tsd, mib, miblen, oldp, oldlenp, newp, newlen);
    /// Partial MIB.
    return ENOENT;
}

/// jemalloc: ctl_mibnametomib
int ctlMibNameToMib(ThreadState & tsd, size_t * mib, size_t miblen, const char * name, size_t * miblenp)
{
    if (ctlEnsureInitialized(tsd))
        return EAGAIN;

    const CtlNode * node = nullptr;
    int ret = ctlLookupByMib(&tsd, &node, mib, miblen);
    if (ret != 0)
        return ret;
    if (node == nullptr || node->isLeaf())
        return ENOENT;

    JE_ASSERT(miblenp != nullptr);
    JE_ASSERT(*miblenp >= miblen);
    *miblenp -= miblen;
    ret = ctlLookup(&tsd, node, name, nullptr, mib + miblen, miblenp);
    *miblenp += miblen;
    return ret;
}

/// jemalloc: ctl_bymibname
int ctlByMibName(
    ThreadState & tsd,
    size_t * mib,
    size_t miblen,
    const char * name,
    size_t * miblenp,
    void * oldp,
    size_t * oldlenp,
    void * newp,
    size_t newlen)
{
    if (ctlEnsureInitialized(tsd))
        return EAGAIN;

    const CtlNode * node = nullptr;
    int ret = ctlLookupByMib(&tsd, &node, mib, miblen);
    if (ret != 0)
        return ret;
    if (node == nullptr || node->isLeaf())
        return ENOENT;

    JE_ASSERT(miblenp != nullptr);
    JE_ASSERT(*miblenp >= miblen);
    *miblenp -= miblen;
    /// The same node supplies the starting node and stores the ending node.
    ret = ctlLookup(&tsd, node, name, &node, mib + miblen, miblenp);
    *miblenp += miblen;
    if (ret != 0)
        return ret;

    if (node != nullptr && node->isLeaf())
        return node->leaf(tsd, mib, *miblenp, oldp, oldlenp, newp, newlen);
    /// The name refers to a partial path through the ctl tree.
    return ENOENT;
}

/// jemalloc: ctl_boot
bool ctlBoot()
{
    if (ctl_mtx.init("ctl", MutexRank::CTL, MutexLockOrder::RankExclusive))
        return true;
    ctl_initialized.store(false, std::memory_order_relaxed);
    return false;
}

/// jemalloc: ctl_prefork
void ctlPrefork(ThreadState * tsdn)
{
    ctl_mtx.prefork(tsdn);
}

/// jemalloc: ctl_postfork_parent
void ctlPostforkParent(ThreadState * tsdn)
{
    ctl_mtx.postforkParent(tsdn);
}

/// jemalloc: ctl_postfork_child
void ctlPostforkChild(ThreadState * tsdn)
{
    ctl_mtx.postforkChild(tsdn);
}

/// jemalloc: ctl_mtx_assert_held
void ctlMtxAssertHeld(ThreadState * tsdn)
{
    ctl_mtx.assertOwner(tsdn);
}

/// --- Leaves and index functions that depend only on the ctl state ---------------------------------------------------

namespace ctl
{

/// jemalloc: version_ctl (CTL_RO_NL_GEN)
int version(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return readOnlyNl<const char *, [] { return JEMALLOC_VERSION; }>(tsd, mib, miblen, oldp, oldlenp, newp, newlen);
}

/// Any write refreshes the statistics snapshot (the written value is ignored); reads return the current epoch.
/// jemalloc: epoch_ctl
int epoch(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    MutexLock lock(&tsd, ctl_mtx);
    uint64_t newval;
    if (int ret = write(newp, newlen, newval))
        return ret;
    if (newp != nullptr)
        ctlRefresh(&tsd);
    return read(oldp, oldlenp, ctl_arenas->epoch);
}

/// Accepts `MALLCTL_ARENAS_ALL`, `MALLCTL_ARENAS_DESTROYED` and every `i <= narenas` (`i == narenas` is the
/// deprecated alias of `MALLCTL_ARENAS_ALL`).
/// jemalloc: arena_i_index
bool arenaIIndex(ThreadState * tsdn, const size_t *, size_t, size_t i)
{
    MutexLock lock(tsdn, ctl_mtx);
    switch (i)
    {
        case MALLCTL_ARENAS_ALL:
        case MALLCTL_ARENAS_DESTROYED:
            return true;
        default:
            return i <= ctl_arenas->narenas;
    }
}

/// jemalloc: stats_arenas_i_index
bool statsArenasIIndex(ThreadState * tsdn, const size_t *, size_t, size_t i)
{
    MutexLock lock(tsdn, ctl_mtx);
    return !ctlArenasIVerify(i);
}

/// jemalloc: experimental_arenas_i_index
bool experimentalArenasIIndex(ThreadState * tsdn, const size_t *, size_t, size_t i)
{
    MutexLock lock(tsdn, ctl_mtx);
    return !ctlArenasIVerify(i);
}

}

}
