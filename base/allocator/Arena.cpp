#include <allocator/Arena.h>

#include <allocator/ArenaInlines.h>
#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/ExtentMap.h>
#include <allocator/Format.h>
#include <allocator/Options.h>
#include <allocator/Sanitizer.h>
#include <allocator/ThreadCache.h>

#include <cstdlib>
#include <cstring>
#include <new>

namespace jemalloc
{

/// --- Data ----------------------------------------------------------------------------------------------------------

/// The runtime defaults ("arenas.dirty_decay_ms", "arenas.muzzy_decay_ms").
/// jemalloc: dirty_decay_ms_default, muzzy_decay_ms_default (static)
static constinit std::atomic<ssize_t> dirty_decay_ms_default{0};
static constinit std::atomic<ssize_t> muzzy_decay_ms_default{0};

constinit DivInfo arena_binind_div_info[SC_NBINS] = {};

constinit size_t oversize_threshold = OVERSIZE_THRESHOLD_DEFAULT;

constinit uint32_t arena_bin_offsets[SC_NBINS] = {};
constinit unsigned arena_nbins_total = 0;

constinit unsigned huge_arena_ind = 0;

const ArenaConfig arena_config_default = {
    /* .extent_hooks = */ &ehooks_default_extent_hooks,
    /* .metadata_use_hooks = */ true,
};

/// --- Stats ---------------------------------------------------------------------------------------------------------

/// jemalloc: arena_basic_stats_merge
void arenaBasicStatsMerge(
    ThreadState * /*tsdn*/,
    Arena * arena,
    unsigned * nthreads,
    const char ** dss,
    ssize_t * dirty_decay_ms,
    ssize_t * muzzy_decay_ms,
    size_t * nactive,
    size_t * ndirty,
    size_t * nmuzzy)
{
    *nthreads += arenaNthreadsGet(arena, false);
    *dss = dss_prec_names[unsigned(arenaDssPrecGet(arena))];
    *dirty_decay_ms = arenaDecayMsGet(arena, extent_state_dirty);
    *muzzy_decay_ms = arenaDecayMsGet(arena, extent_state_muzzy);
    arena->pa_shard.basicStatsMerge(nactive, ndirty, nmuzzy);
}

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
    PacExtentStats * estats)
{
    static_assert(config::stats);

    arenaBasicStatsMerge(tsdn, arena, nthreads, dss, dirty_decay_ms, muzzy_decay_ms, nactive, ndirty, nmuzzy);

    size_t base_allocated;
    size_t base_edata_allocated;
    size_t base_rtree_allocated;
    size_t base_resident;
    size_t base_mapped;
    size_t metadata_thp;
    arena->base->statsGet(
        tsdn, &base_allocated, &base_edata_allocated, &base_rtree_allocated, &base_resident, &base_mapped, &metadata_thp);
    size_t pac_mapped_sz = arena->pa_shard.pac.mapped();
    astats->mapped += base_mapped + pac_mapped_sz;
    astats->resident += base_resident;

    /// LOCKEDINT_MTX_LOCK: no stats mutex.

    astats->base += base_allocated;
    astats->metadata_edata += base_edata_allocated;
    astats->metadata_rtree += base_rtree_allocated;
    /// atomic_load_add_store_zu
    astats->internal.store(astats->internal.load(std::memory_order_relaxed) + arenaInternalGet(arena), std::memory_order_relaxed);
    astats->metadata_thp += metadata_thp;

    for (szind_t i = 0; i < SC_NSIZES - SC_NBINS; ++i)
    {
        /// ndalloc should be read before nmalloc, since otherwise it is possible for ndalloc to be incremented, and the
        /// following can become true: ndalloc > nmalloc.
        uint64_t ndalloc = arena->stats.lstats[i].ndalloc.read();
        lstats[i].ndalloc.incUnsynchronized(ndalloc);
        astats->ndalloc_large += ndalloc;

        uint64_t nmalloc = arena->stats.lstats[i].nmalloc.read();
        lstats[i].nmalloc.incUnsynchronized(nmalloc);
        astats->nmalloc_large += nmalloc;

        uint64_t nrequests = arena->stats.lstats[i].nrequests.read();
        lstats[i].nrequests.incUnsynchronized(nmalloc + nrequests);
        astats->nrequests_large += nmalloc + nrequests;

        /// nfill == nmalloc for large currently.
        lstats[i].nfills.incUnsynchronized(nmalloc);
        astats->nfills_large += nmalloc;

        uint64_t nflush = arena->stats.lstats[i].nflushes.read();
        lstats[i].nflushes.incUnsynchronized(nflush);
        astats->nflushes_large += nflush;

        JE_ASSERT(nmalloc >= ndalloc);
        JE_ASSERT(nmalloc - ndalloc <= SIZE_MAX);
        size_t curlextents = size_t(nmalloc - ndalloc);
        lstats[i].curlextents += curlextents;

        uint64_t active_bytes = arena->stats.lstats[i].active_bytes.read();
        lstats[i].active_bytes.incUnsynchronized(active_bytes);
        astats->allocated_large += active_bytes;
    }

    arena->pa_shard.statsMerge(tsdn, &astats->pa_shard_stats, estats, &astats->resident);

    /// LOCKEDINT_MTX_UNLOCK: no stats mutex.

    /// Currently cached bytes and sanitizer-stashed bytes in tcache.
    astats->tcache_bytes = 0;
    astats->tcache_stashed_bytes = 0;
    arena->tcache_ql_mtx.lock(tsdn);
    arena->cache_bin_array_descriptor_ql.forEach(
        [&](CacheBinArrayDescriptor * descriptor)
        {
            for (szind_t i = 0; i < TCACHE_NBINS_MAX; ++i)
            {
                CacheBin * cache_bin = &descriptor->bins[i];
                if (cache_bin->disabled())
                    continue;

                cache_bin_sz_t ncached;
                cache_bin_sz_t nstashed;
                cache_bin->nitemsGetRemote(ncached, nstashed);
                astats->tcache_bytes += ncached * sz::indexToSize(i);
                astats->tcache_stashed_bytes += nstashed * sz::indexToSize(i);
            }
        });
    arena->tcache_ql_mtx.profRead(tsdn, astats->mutex_prof_data[arena_prof_mutex_tcache_list]);
    arena->tcache_ql_mtx.unlock(tsdn);

    /// Gather per arena mutex profiling data.
    arena->large_mtx.lock(tsdn);
    arena->large_mtx.profRead(tsdn, astats->mutex_prof_data[arena_prof_mutex_large]);
    arena->large_mtx.unlock(tsdn);
    Mutex & base_mtx = arena->base->mutex();
    base_mtx.lock(tsdn);
    base_mtx.profRead(tsdn, astats->mutex_prof_data[arena_prof_mutex_base]);
    base_mtx.unlock(tsdn);
    arena->pa_shard.mtxStatsRead(tsdn, astats->mutex_prof_data);

    astats->uptime.copy(arena->create_time);
    astats->uptime.update();
    astats->uptime.subtract(arena->create_time);

    for (szind_t i = 0; i < SC_NBINS; ++i)
    {
        for (unsigned j = 0; j < bin_infos[i].n_shards; ++j)
            arenaGetBin(arena, i, j)->statsMerge(tsdn, bstats[i]);
    }
}

/// --- Decay wiring --------------------------------------------------------------------------------------------------

static void arenaMaybeDoDeferredWork(ThreadState * tsdn, Arena * arena, Decay * decay, size_t npages_new);
static bool arenaDecayDirty(ThreadState * tsdn, Arena * arena, bool is_background_thread, bool all);

/// jemalloc: arena_background_thread_inactivity_check
static void arenaBackgroundThreadInactivityCheck(ThreadState * tsdn, Arena * arena, bool is_background_thread)
{
    if (!backgroundThreadEnabled() || is_background_thread)
        return;
    BackgroundThreadInfo * info = arenaBackgroundThreadInfoGet(arena);
    if (backgroundThreadIndefiniteSleep(info))
        arenaMaybeDoDeferredWork(tsdn, arena, &arena->pa_shard.pac.decay_dirty, 0);
}

/// jemalloc: arena_handle_deferred_work
void arenaHandleDeferredWork(ThreadState * tsdn, Arena * arena)
{
    if (arena->pa_shard.pac.decay_dirty.immediately())
        arenaDecayDirty(tsdn, arena, false, true);
    arenaBackgroundThreadInactivityCheck(tsdn, arena, false);
}

/// In situations where we're not forcing a decay (i.e. because the user specifically requested it), should we purge
/// ourselves, or wait for the background thread to get to it.
/// jemalloc: arena_decide_unforced_purge_eagerness
static PacPurgeEagerness arenaDecideUnforcedPurgeEagerness(bool is_background_thread)
{
    if (is_background_thread)
        return PAC_PURGE_ALWAYS;
    else if (!is_background_thread && backgroundThreadEnabled())
        return PAC_PURGE_NEVER;
    else
        return PAC_PURGE_ON_EPOCH_ADVANCE;
}

/// jemalloc: arena_decay_ms_set
bool arenaDecayMsSet(ThreadState * tsdn, Arena * arena, ExtentState state, ssize_t decay_ms)
{
    PacPurgeEagerness eagerness = arenaDecideUnforcedPurgeEagerness(/* is_background_thread */ false);
    return arena->pa_shard.decayMsSet(tsdn, state, decay_ms, eagerness);
}

/// jemalloc: arena_decay_ms_get
ssize_t arenaDecayMsGet(Arena * arena, ExtentState state)
{
    return arena->pa_shard.decayMsGet(state);
}

/// Returns true if another thread is decaying (the decay mutex is busy).
/// jemalloc: arena_decay_impl
static bool arenaDecayImpl(
    ThreadState * tsdn, Arena * arena, Decay * decay, DecayStats * decay_stats, ExtentCache * ecache, bool is_background_thread, bool all)
{
    if (all)
    {
        decay->mtx.lock(tsdn);
        arena->pa_shard.pac.decayAll(tsdn, decay, decay_stats, ecache, /* fully_decay */ all);
        decay->mtx.unlock(tsdn);
        return false;
    }

    if (!decay->mtx.tryLock(tsdn))
    {
        /// No need to wait if another thread is in progress.
        return true;
    }
    PacPurgeEagerness eagerness = arenaDecideUnforcedPurgeEagerness(is_background_thread);
    bool epoch_advanced = arena->pa_shard.pac.maybeDecayPurge(tsdn, decay, decay_stats, ecache, eagerness);
    size_t npages_new = 0;
    if (epoch_advanced)
    {
        /// Backlog is updated on epoch advance.
        npages_new = decay->epochNpagesDelta();
    }
    decay->mtx.unlock(tsdn);

    if (config::background_thread && backgroundThreadEnabled() && epoch_advanced && !is_background_thread)
        arenaMaybeDoDeferredWork(tsdn, arena, decay, npages_new);

    return false;
}

/// jemalloc: arena_decay_dirty
static bool arenaDecayDirty(ThreadState * tsdn, Arena * arena, bool is_background_thread, bool all)
{
    PageAllocator & pac = arena->pa_shard.pac;
    return arenaDecayImpl(tsdn, arena, &pac.decay_dirty, &pac.stats->decay_dirty, &pac.ecache_dirty, is_background_thread, all);
}

/// jemalloc: arena_decay_muzzy
static bool arenaDecayMuzzy(ThreadState * tsdn, Arena * arena, bool is_background_thread, bool all)
{
    if (arena->pa_shard.dontDecayMuzzy())
        return false;
    PageAllocator & pac = arena->pa_shard.pac;
    return arenaDecayImpl(tsdn, arena, &pac.decay_muzzy, &pac.stats->decay_muzzy, &pac.ecache_muzzy, is_background_thread, all);
}

/// jemalloc: arena_decay
void arenaDecay(ThreadState * tsdn, Arena * arena, bool is_background_thread, bool all)
{
    if (all)
    {
        /// We should take a purge of "all" to mean "save as much memory as possible", including flushing any caches
        /// (for situations like thread death, or manual purge calls).
        arena->pa_shard.flush(tsdn);
    }
    if (arenaDecayDirty(tsdn, arena, is_background_thread, all))
        return;
    arenaDecayMuzzy(tsdn, arena, is_background_thread, all);
}

/// jemalloc: arena_should_decay_early
static bool arenaShouldDecayEarly(
    ThreadState * tsdn, Arena * /*arena*/, Decay * decay, BackgroundThreadInfo * info, NsTime * remaining_sleep, size_t npages_new)
{
    backgroundThreadInfoMutex(info).assertOwner(tsdn);

    if (!decay->mtx.tryLock(tsdn))
        return false;

    if (!decay->gradually())
    {
        decay->mtx.unlock(tsdn);
        return false;
    }

    remaining_sleep->init(backgroundThreadWakeupTimeGet(info));
    if (remaining_sleep->compare(decay->epoch) <= 0)
    {
        decay->mtx.unlock(tsdn);
        return false;
    }
    remaining_sleep->subtract(decay->epoch);
    if (npages_new > 0)
    {
        uint64_t npurge_new = decay->npagesPurgeIn(*remaining_sleep, npages_new);
        backgroundThreadNpagesToPurgeNew(info) += npurge_new;
    }
    decay->mtx.unlock(tsdn);
    return backgroundThreadNpagesToPurgeNew(info) > ARENA_DEFERRED_PURGE_NPAGES_THRESHOLD;
}

/// Check if deferred work needs to be done sooner than planned. For decay we might want to wake up earlier because of
/// an influx of dirty pages. Rather than waiting for previously estimated time, we proactively purge those pages. If
/// background thread sleeps indefinitely, always wake up because some deferred work has been generated.
/// jemalloc: arena_maybe_do_deferred_work
static void arenaMaybeDoDeferredWork(ThreadState * tsdn, Arena * arena, Decay * decay, size_t npages_new)
{
    BackgroundThreadInfo * info = arenaBackgroundThreadInfoGet(arena);
    Mutex & info_mtx = backgroundThreadInfoMutex(info);
    if (!info_mtx.tryLock(tsdn))
    {
        /// Background thread may hold the mutex for a long period of time. We'd like to avoid the variance on
        /// application threads. So keep this non-blocking, and leave the work to a future epoch.
        return;
    }
    if (backgroundThreadIsStarted(info))
    {
        NsTime remaining_sleep = NsTime::zero();
        if (backgroundThreadIndefiniteSleep(info))
        {
            backgroundThreadWakeupEarly(info, nullptr);
        }
        else if (arenaShouldDecayEarly(tsdn, arena, decay, info, &remaining_sleep, npages_new))
        {
            backgroundThreadNpagesToPurgeNew(info) = 0;
            backgroundThreadWakeupEarly(info, &remaining_sleep);
        }
    }
    info_mtx.unlock(tsdn);
}

/// jemalloc: arena_do_deferred_work
void arenaDoDeferredWork(ThreadState * tsdn, Arena * arena)
{
    arenaDecay(tsdn, arena, true, false);
    arena->pa_shard.doDeferredWork(tsdn);
}

/// --- Large extent helpers ------------------------------------------------------------------------------------------

/// jemalloc: arena_large_malloc_stats_update
static void arenaLargeMallocStatsUpdate(ThreadState * tsdn, Arena * arena, size_t usize)
{
    static_assert(config::stats);

    szind_t index = sz::sizeToIndex(usize);
    /// This only occurs when we have a sampled small allocation.
    if (usize < SC_LARGE_MINCLASS)
    {
        JE_ASSERT(index < SC_NBINS);
        JE_ASSERT(usize >= PAGE && usize % PAGE == 0);
        Bin * bin = arenaGetBin(arena, index, /* binshard */ 0);
        bin->lock.lock(tsdn);
        ++bin->stats.nmalloc;
        bin->lock.unlock(tsdn);
    }
    else
    {
        JE_ASSERT(index >= SC_NBINS);
        szind_t hindex = index - SC_NBINS;
        arena->stats.lstats[hindex].nmalloc.inc(1);
        arena->stats.lstats[hindex].active_bytes.inc(usize);
    }
}

/// jemalloc: arena_large_dalloc_stats_update
static void arenaLargeDallocStatsUpdate(ThreadState * tsdn, Arena * arena, size_t usize)
{
    static_assert(config::stats);

    szind_t index = sz::sizeToIndex(usize);
    /// This only occurs when we have a sampled small allocation.
    if (usize < SC_LARGE_MINCLASS)
    {
        JE_ASSERT(index < SC_NBINS);
        JE_ASSERT(usize >= PAGE && usize % PAGE == 0);
        Bin * bin = arenaGetBin(arena, index, /* binshard */ 0);
        bin->lock.lock(tsdn);
        ++bin->stats.ndalloc;
        bin->lock.unlock(tsdn);
    }
    else
    {
        JE_ASSERT(index >= SC_NBINS);
        szind_t hindex = index - SC_NBINS;
        arena->stats.lstats[hindex].ndalloc.inc(1);
        arena->stats.lstats[hindex].active_bytes.dec(usize);
    }
}

/// jemalloc: arena_large_ralloc_stats_update
static void arenaLargeRallocStatsUpdate(ThreadState * tsdn, Arena * arena, size_t oldusize, size_t usize)
{
    arenaLargeMallocStatsUpdate(tsdn, arena, usize);
    arenaLargeDallocStatsUpdate(tsdn, arena, oldusize);
}

/// jemalloc: arena_extent_alloc_large
Extent * arenaExtentAllocLarge(ThreadState * tsdn, Arena * arena, size_t usize, size_t alignment, bool zero)
{
    bool deferred_work_generated = false;
    szind_t szind = sz::sizeToIndex(usize);
    size_t esize = usize + sz_large_pad;

    bool guarded = sanLargeExtentDecideGuard(tsdn, arenaGetEhooks(arena), esize, alignment);

    /// - if usize >= opt.calloc_madvise_threshold,
    ///     - pa_alloc(..., zero_override = zero, ...)
    /// - otherwise,
    ///     - pa_alloc(..., zero_override = false, ...)
    ///     - use memset() to zero out memory if zero == true.
    bool zero_override = zero && (usize >= opt.calloc_madvise_threshold);
    Extent * edata = arena->pa_shard.alloc(
        tsdn, esize, alignment, /* slab */ false, szind, zero_override, guarded, &deferred_work_generated);

    if (edata == nullptr)
        return nullptr;

    if constexpr (config::stats)
        arenaLargeMallocStatsUpdate(tsdn, arena, usize);
    if (sz_large_pad != 0)
        arenaCacheObliviousRandomize(tsdn, arena, edata, alignment);
    /// This branch should be put after the randomization so that the addr returned by `addr()` has already be
    /// randomized, if cache_oblivious is enabled.
    if (zero && !zero_override && !edata->zeroed())
    {
        void * addr = edata->addr();
        size_t edata_usize = edata->usize();
        memset(addr, 0, edata_usize);
    }

    return edata;
}

/// jemalloc: arena_extent_dalloc_large_prep
void arenaExtentDallocLargePrep(ThreadState * tsdn, Arena * arena, Extent * edata)
{
    if constexpr (config::stats)
        arenaLargeDallocStatsUpdate(tsdn, arena, edata->usize());
}

/// jemalloc: arena_extent_ralloc_large_shrink
void arenaExtentRallocLargeShrink(ThreadState * tsdn, Arena * arena, Extent * edata, size_t oldusize)
{
    size_t usize = edata->usize();

    if constexpr (config::stats)
        arenaLargeRallocStatsUpdate(tsdn, arena, oldusize, usize);
}

/// jemalloc: arena_extent_ralloc_large_expand
void arenaExtentRallocLargeExpand(ThreadState * tsdn, Arena * arena, Extent * edata, size_t oldusize)
{
    size_t usize = edata->usize();

    if constexpr (config::stats)
        arenaLargeRallocStatsUpdate(tsdn, arena, oldusize, usize);
}

/// --- Slabs ---------------------------------------------------------------------------------------------------------

/// jemalloc: arena_slab_dalloc
void arenaSlabDalloc(ThreadState * tsdn, Arena * arena, Extent * slab)
{
    bool deferred_work_generated = false;
    arena->pa_shard.dalloc(tsdn, slab, &deferred_work_generated);
    if (deferred_work_generated)
        arenaHandleDeferredWork(tsdn, arena);
}

/// jemalloc: arena_slab_alloc
static Extent * arenaSlabAlloc(ThreadState * tsdn, Arena * arena, szind_t binind, unsigned binshard, const BinInfo & bin_info)
{
    bool deferred_work_generated = false;

    bool guarded = sanSlabExtentDecideGuard(tsdn, arenaGetEhooks(arena));
    Extent * slab = arena->pa_shard.alloc(
        tsdn,
        bin_info.slab_size,
        /* alignment */ PAGE,
        /* slab */ true,
        /* szind */ binind,
        /* zero */ false,
        guarded,
        &deferred_work_generated);

    if (deferred_work_generated)
        arenaHandleDeferredWork(tsdn, arena);

    if (slab == nullptr)
        return nullptr;
    JE_ASSERT(slab->slab());

    /// Initialize slab internals.
    SlabData * slab_data = slab->slabData();
    slab->setNfreeBinshard(bin_info.nregs, binshard);
    bitmapInit(slab_data->bitmap, bin_info.bitmap_info, false);

    return slab;
}

/// jemalloc: arena_bin_reset
static void arenaBinReset(ThreadState & tsd, Arena * arena, Bin * bin)
{
    ThreadState * tsdn = &tsd;
    Extent * slab;

    bin->lock.lock(tsdn);

    if (bin->slabcur != nullptr)
    {
        slab = bin->slabcur;
        bin->slabcur = nullptr;
        bin->lock.unlock(tsdn);
        arenaSlabDalloc(tsdn, arena, slab);
        bin->lock.lock(tsdn);
    }
    while ((slab = bin->slabs_nonfull.removeFirst()) != nullptr)
    {
        bin->lock.unlock(tsdn);
        arenaSlabDalloc(tsdn, arena, slab);
        bin->lock.lock(tsdn);
    }
    for (slab = bin->slabs_full.first(); slab != nullptr; slab = bin->slabs_full.first())
    {
        bin->slabsFullRemove(false, slab);
        bin->lock.unlock(tsdn);
        arenaSlabDalloc(tsdn, arena, slab);
        bin->lock.lock(tsdn);
    }
    if constexpr (config::stats)
    {
        bin->stats.curregs = 0;
        bin->stats.curslabs = 0;
    }
    bin->lock.unlock(tsdn);
}

/// --- Profiling -----------------------------------------------------------------------------------------------------

/// jemalloc: arena_prof_promote
void arenaProfPromote(ThreadState * tsdn, void * ptr, [[maybe_unused]] size_t usize, [[maybe_unused]] size_t bumped_usize)
{
    static_assert(config::prof);
    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(arenaSalloc(tsdn, ptr) == bumped_usize);
    JE_ASSERT(sz::canUseSlab(usize));

    /// `config_opt_safety_checks` (redzones) is off.

    Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);

    szind_t szind = sz::sizeToIndex(usize);
    edata->setSzind(szind);
    arena_emap_global.remap(tsdn, edata, szind, /* slab */ false);

    JE_ASSERT(arenaSalloc(tsdn, ptr) == usize);
}

/// jemalloc: arena_prof_demote
static size_t arenaProfDemote(ThreadState * tsdn, Extent * edata, const void * ptr)
{
    static_assert(config::prof);
    JE_ASSERT(ptr != nullptr);
    size_t usize = arenaSalloc(tsdn, ptr);
    size_t bumped_usize = sz::sa2u(usize, PROF_SAMPLE_ALIGNMENT);
    JE_ASSERT(bumped_usize <= SC_LARGE_MINCLASS && pageCeiling(bumped_usize) == bumped_usize);
    JE_ASSERT(edata->size() - bumped_usize <= sz_large_pad);
    szind_t szind = sz::sizeToIndex(bumped_usize);

    edata->setSzind(szind);
    arena_emap_global.remap(tsdn, edata, szind, /* slab */ false);

    JE_ASSERT(arenaSalloc(tsdn, ptr) == bumped_usize);

    return bumped_usize;
}

/// jemalloc: arena_dalloc_promoted_impl
static void arenaDallocPromotedImpl(ThreadState * tsdn, void * ptr, ThreadCache * tcache, bool slow_path, Extent * edata)
{
    static_assert(config::prof);
    JE_ASSERT(opt.prof);

    [[maybe_unused]] size_t usize = edata->usize();
    size_t bumped_usize = arenaProfDemote(tsdn, edata, ptr);
    /// `config_opt_safety_checks` (redzone verification) is off.
    szind_t bumped_ind = sz::sizeToIndex(bumped_usize);
    if (bumped_usize >= SC_LARGE_MINCLASS && tcache != nullptr && bumped_ind < TCACHE_NBINS_MAX
        && !tcacheBinDisabled(bumped_ind, &tcache->bins[bumped_ind], tcache->tcache_slow))
    {
        tcacheDallocLarge(*tsdn, tcache, ptr, bumped_ind, slow_path);
    }
    else
    {
        largeDalloc(tsdn, edata);
    }
}

/// jemalloc: arena_dalloc_promoted
void arenaDallocPromoted(ThreadState * tsdn, void * ptr, ThreadCache * tcache, bool slow_path)
{
    Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);
    arenaDallocPromotedImpl(tsdn, ptr, tcache, slow_path, edata);
}

/// --- Reset / destroy -----------------------------------------------------------------------------------------------

/// jemalloc: arena_reset
void arenaReset(ThreadState & tsd, Arena * arena)
{
    /// Locking in this function is unintuitive. The caller guarantees that no concurrent operations are happening in
    /// this arena, but there are still reasons that some locking is necessary:
    /// - Some of the functions in the transitive closure of calls assume appropriate locks are held, and in some cases
    ///   these locks are temporarily dropped to avoid lock order reversal or deadlock due to reentry.
    /// - mallctl("epoch", ...) may concurrently refresh stats. While strictly speaking this is a "concurrent
    ///   operation", disallowing stats refreshes would impose an inconvenient burden.
    ThreadState * tsdn = &tsd;

    /// Large allocations.
    arena->large_mtx.lock(tsdn);

    for (Extent * edata = arena->large.first(); edata != nullptr; edata = arena->large.first())
    {
        void * ptr = edata->base();
        size_t usize = 0;

        arena->large_mtx.unlock(tsdn);
        AllocContext alloc_ctx;
        arena_emap_global.allocCtxLookup(tsdn, ptr, &alloc_ctx);
        JE_ASSERT(alloc_ctx.szind != SC_NSIZES);

        if (config::stats || (config::prof && opt.prof))
        {
            usize = alloc_ctx.usizeGet();
            JE_ASSERT(usize == arenaSalloc(tsdn, ptr));
        }
        /// Remove large allocation from prof sample set.
        if (config::prof && opt.prof)
        {
            /// jemalloc: prof_free
            ProfInfo prof_info;
            arenaProfInfoGet(tsd, ptr, &alloc_ctx, &prof_info, /* reset_recent */ true);
            if (JE_UNLIKELY(profTctxIsValid(prof_info.alloc_tctx)))
                profFreeSampledObject(tsd, ptr, usize, &prof_info);
        }
        if (config::prof && opt.prof && alloc_ctx.szind < SC_NBINS)
            arenaDallocPromotedImpl(tsdn, ptr, /* tcache */ nullptr, /* slow_path */ true, edata);
        else
            largeDalloc(tsdn, edata);
        arena->large_mtx.lock(tsdn);
    }
    arena->large_mtx.unlock(tsdn);

    /// Bins.
    for (unsigned i = 0; i < SC_NBINS; ++i)
    {
        for (unsigned j = 0; j < bin_infos[i].n_shards; ++j)
            arenaBinReset(tsd, arena, arenaGetBin(arena, i, j));
    }
    arena->pa_shard.reset(tsdn);
}

/// jemalloc: arena_prepare_base_deletion_sync_finish
static void arenaPrepareBaseDeletionSyncFinish(ThreadState & tsd, Mutex ** mutexes, unsigned n_mtx)
{
    for (unsigned i = 0; i < n_mtx; ++i)
    {
        mutexes[i]->lock(&tsd);
        mutexes[i]->unlock(&tsd);
    }
}

/// jemalloc: ARENA_DESTROY_MAX_DELAYED_MTX
static constexpr unsigned ARENA_DESTROY_MAX_DELAYED_MTX = 32;

/// jemalloc: arena_prepare_base_deletion_sync
static void arenaPrepareBaseDeletionSync(ThreadState & tsd, Mutex * mtx, Mutex ** delayed_mtx, unsigned * n_delayed)
{
    if (mtx->tryLock(&tsd))
    {
        /// No contention.
        mtx->unlock(&tsd);
        return;
    }
    unsigned n = *n_delayed;
    JE_ASSERT(n < ARENA_DESTROY_MAX_DELAYED_MTX);
    /// Add another to the batch.
    delayed_mtx[n++] = mtx;

    if (n == ARENA_DESTROY_MAX_DELAYED_MTX)
    {
        arenaPrepareBaseDeletionSyncFinish(tsd, delayed_mtx, n);
        n = 0;
    }
    *n_delayed = n;
}

/// In order to coalesce, `tryAcquireEdataNeighbor` will attempt to check neighbor extent's state to determine
/// eligibility. This means under certain conditions, the metadata from an arena can be accessed without holding any
/// locks from that arena. In order to guarantee safe memory access, the metadata and the underlying base allocator
/// needs to be kept alive, until all pending accesses are done.
///
/// 1) with `opt.retain`, the arena boundary implies the is_head state (tracked in the rtree leaf), and the coalesce
/// flow will stop at the head state branch. Therefore no cross arena metadata access possible.
///
/// 2) without `opt.retain`, the arena id needs to be read from the extent, meaning read only cross-arena metadata
/// access is possible. The coalesce attempt will stop at the arena_id mismatch, and is always under one of the ecache
/// locks. To allow safe passthrough of such metadata accesses, the loop below will iterate through all manual arenas'
/// ecache locks. As all the metadata from this base allocator have been unlinked from the rtree, after going through
/// all the relevant ecache locks, it's safe to say that a) pending accesses are all finished, and b) no new access will
/// be generated.
/// jemalloc: arena_prepare_base_deletion
static void arenaPrepareBaseDeletion(ThreadState & tsd, Base * base_to_destroy)
{
    if (opt.retain)
        return;
    unsigned destroy_ind = base_to_destroy->indGet();
    JE_ASSERT(destroy_ind >= manual_arena_base);

    ThreadState * tsdn = &tsd;
    Mutex * delayed_mtx[ARENA_DESTROY_MAX_DELAYED_MTX];
    unsigned n_delayed = 0;
    unsigned total = narenasTotalGet();
    for (unsigned i = 0; i < total; ++i)
    {
        if (i == destroy_ind)
            continue;
        Arena * arena = arenaGet(tsdn, i, false);
        if (arena == nullptr)
            continue;
        PageAllocator & pac = arena->pa_shard.pac;
        arenaPrepareBaseDeletionSync(tsd, &pac.ecache_dirty.mtx, delayed_mtx, &n_delayed);
        arenaPrepareBaseDeletionSync(tsd, &pac.ecache_muzzy.mtx, delayed_mtx, &n_delayed);
        arenaPrepareBaseDeletionSync(tsd, &pac.ecache_retained.mtx, delayed_mtx, &n_delayed);
    }
    arenaPrepareBaseDeletionSyncFinish(tsd, delayed_mtx, n_delayed);
}

/// jemalloc: arena_destroy
void arenaDestroy(ThreadState & tsd, Arena * arena)
{
    JE_ASSERT(arena->base->indGet() >= narenas_auto);
    JE_ASSERT(arenaNthreadsGet(arena, false) == 0);
    JE_ASSERT(arenaNthreadsGet(arena, true) == 0);

    /// No allocations have occurred since `arenaReset` was called. Furthermore, the caller (`arena.<i>.destroy`)
    /// purged all cached extents, so only retained extents may remain and it's safe to destroy them.
    arena->pa_shard.destroy(&tsd);

    /// Remove the arena pointer from the arenas array. We rely on the fact that there is no way for the application
    /// to get a dirty read from the arenas array unless there is an inherent race in the application involving access
    /// of an arena being concurrently destroyed. The application must synchronize knowledge of the arena's validity,
    /// so as long as we use an atomic write to update the arenas array, the application will get a clean read any
    /// time after it synchronizes knowledge that the arena is no longer valid.
    arenaSet(arena->base->indGet(), nullptr);

    /// Destroy the base allocator, which manages all metadata ever mapped by this arena. The prepare function will
    /// make sure no pending access to the metadata in this base anymore.
    Base * base = arena->base;
    arenaPrepareBaseDeletion(tsd, base);
    base->destroy(&tsd);
}

/// --- Small allocation ----------------------------------------------------------------------------------------------

/// jemalloc: arena_ptr_array_fill_small
cache_bin_sz_t arenaPtrArrayFillSmall(
    ThreadState * tsdn,
    Arena * arena,
    szind_t binind,
    CacheBinPtrArray * arr,
    const cache_bin_sz_t nfill_min,
    const cache_bin_sz_t nfill_max,
    CacheBinStats merge_stats)
{
    JE_ASSERT(nfill_min > 0 && nfill_min <= nfill_max);

    const BinInfo & bin_info = bin_infos[binind];
    /// Bin-local resources are used first: 1) bin->slabcur, and 2) nonfull slabs. After both are exhausted, new slabs
    /// will be allocated through `arenaSlabAlloc`.
    ///
    /// Bin lock is only taken / released right before / after the while(...) refill loop, with new slab allocation
    /// (which has its own locking) kept outside of the loop. This setup facilitates flat combining, at the cost of the
    /// nested loop (through the refill label).
    ///
    /// To optimize for cases with contention and limited resources (e.g. hugepage-backed or non-overcommit arenas),
    /// each fill-iteration gets one chance of slab_alloc, and a retry of bin local resources after the slab
    /// allocation (regardless if slab_alloc failed, because the bin lock is dropped during the slab allocation).
    ///
    /// In other words, new slab allocation is allowed, as long as there was progress since the previous slab_alloc.
    /// This is tracked with made_progress below, initialized to true to jump start the first iteration.
    ///
    /// In other words (again), the loop will only terminate early (i.e. stop with filled < nfill) after going through
    /// the three steps: a) bin local exhausted, b) unlock and slab_alloc returns null, c) re-lock and bin local fails
    /// again.
    bool made_progress = true;
    Extent * fresh_slab = nullptr;
    bool alloc_and_retry = false;
    bool is_auto = arenaIsAuto(arena);
    cache_bin_sz_t filled = 0;
    unsigned binshard;
    Bin * bin = binChoose(tsdn, arena, binind, &binshard);

    while (true) /// label_refill
    {
        bin->lock.lock(tsdn);

        while (filled < nfill_min)
        {
            /// Try batch-fill from slabcur first.
            Extent * slabcur = bin->slabcur;
            if (slabcur != nullptr && slabcur->nfree() > 0)
            {
                /// Use up the free slots if the total filled <= nfill_max. Otherwise, fallback to nfill_min for a more
                /// conservative memory usage.
                unsigned cnt = slabcur->nfree();
                if (cnt + filled > nfill_max)
                    cnt = nfill_min - filled;

                Bin::slabRegAllocBatch(slabcur, bin_info, cnt, &arr->ptr[filled]);
                made_progress = true;
                filled = cache_bin_sz_t(filled + cnt);
                continue;
            }
            /// Next try refilling slabcur from nonfull slabs.
            if (!bin->refillSlabcurNoFreshSlab(tsdn, is_auto))
            {
                JE_ASSERT(bin->slabcur != nullptr);
                continue;
            }

            /// Then see if a new slab was reserved already.
            if (fresh_slab != nullptr)
            {
                bin->refillSlabcurWithFreshSlab(tsdn, binind, fresh_slab);
                JE_ASSERT(bin->slabcur != nullptr);
                fresh_slab = nullptr;
                continue;
            }

            /// Try slab_alloc if made progress (or never did slab_alloc).
            if (made_progress)
            {
                JE_ASSERT(bin->slabcur == nullptr);
                JE_ASSERT(fresh_slab == nullptr);
                alloc_and_retry = true;
                /// Alloc a new slab then come back.
                break;
            }

            /// OOM.
            JE_ASSERT(fresh_slab == nullptr);
            JE_ASSERT(!alloc_and_retry);
            break;
        }

        if (config::stats && !alloc_and_retry)
        {
            bin->stats.nmalloc += filled;
            bin->stats.nrequests += merge_stats.nrequests;
            bin->stats.curregs += filled;
            ++bin->stats.nfills;
        }

        bin->lock.unlock(tsdn);

        if (alloc_and_retry)
        {
            JE_ASSERT(fresh_slab == nullptr);
            JE_ASSERT(filled < nfill_min);
            JE_ASSERT(made_progress);

            fresh_slab = arenaSlabAlloc(tsdn, arena, binind, binshard, bin_info);
            /// fresh_slab null case handled in the loop.

            alloc_and_retry = false;
            made_progress = false;
            continue;
        }
        break;
    }
    JE_ASSERT((filled >= nfill_min && filled <= nfill_max) || (fresh_slab == nullptr && !made_progress));

    /// Release if allocated but not used.
    if (fresh_slab != nullptr)
    {
        JE_ASSERT(fresh_slab->nfree() == bin_info.nregs);
        arenaSlabDalloc(tsdn, arena, fresh_slab);
        fresh_slab = nullptr;
    }

    arenaDecayTick(tsdn, arena);
    return filled;
}

/// jemalloc: arena_fill_small_fresh
size_t arenaFillSmallFresh(ThreadState * tsdn, Arena * arena, szind_t binind, void ** ptrs, size_t nfill, bool zero)
{
    JE_ASSERT(binind < SC_NBINS);
    const BinInfo & bin_info = bin_infos[binind];
    const size_t nregs = bin_info.nregs;
    JE_ASSERT(nregs > 0);
    const size_t usize = bin_info.reg_size;

    const bool manual_arena = !arenaIsAuto(arena);
    unsigned binshard;
    Bin * bin = binChoose(tsdn, arena, binind, &binshard);

    size_t nslab = 0;
    size_t filled = 0;
    Extent * slab = nullptr;
    ExtentListActive fulls;
    fulls.init();

    while (filled < nfill && (slab = arenaSlabAlloc(tsdn, arena, binind, binshard, bin_info)) != nullptr)
    {
        JE_ASSERT(size_t(slab->nfree()) == nregs);
        ++nslab;
        size_t batch = nfill - filled;
        if (batch > nregs)
            batch = nregs;
        JE_ASSERT(batch > 0);
        Bin::slabRegAllocBatch(slab, bin_info, unsigned(batch), &ptrs[filled]);
        JE_ASSERT(slab->addr() == ptrs[filled]);
        if (zero)
            memset(ptrs[filled], 0, batch * usize);
        filled += batch;
        if (batch == nregs)
        {
            if (manual_arena)
                fulls.append(slab);
            slab = nullptr;
        }
    }

    bin->lock.lock(tsdn);
    /// Only the last slab can be non-empty, and the last slab is non-empty iff slab != null.
    if (slab != nullptr)
        bin->lowerSlab(tsdn, !manual_arena, slab);
    if (manual_arena)
        bin->slabs_full.concat(fulls);
    JE_ASSERT(fulls.empty());
    if constexpr (config::stats)
    {
        bin->stats.nslabs += nslab;
        bin->stats.curslabs += nslab;
        bin->stats.nmalloc += filled;
        bin->stats.nrequests += filled;
        bin->stats.curregs += filled;
    }
    bin->lock.unlock(tsdn);

    arenaDecayTick(tsdn, arena);
    return filled;
}

/// jemalloc: arena_malloc_small
static void * arenaMallocSmall(ThreadState * tsdn, Arena * arena, szind_t binind, bool zero)
{
    JE_ASSERT(binind < SC_NBINS);
    const BinInfo & bin_info = bin_infos[binind];
    size_t usize = sz::indexToSize(binind);
    bool is_auto = arenaIsAuto(arena);
    unsigned binshard;
    Bin * bin = binChoose(tsdn, arena, binind, &binshard);

    bin->lock.lock(tsdn);
    Extent * fresh_slab = nullptr;
    void * ret = bin->mallocNoFreshSlab(tsdn, is_auto, binind);
    if (ret == nullptr)
    {
        bin->lock.unlock(tsdn);
        fresh_slab = arenaSlabAlloc(tsdn, arena, binind, binshard, bin_info);
        bin->lock.lock(tsdn);
        /// Retry since the lock was dropped.
        ret = bin->mallocNoFreshSlab(tsdn, is_auto, binind);
        if (ret == nullptr)
        {
            if (fresh_slab == nullptr)
            {
                /// OOM.
                bin->lock.unlock(tsdn);
                return nullptr;
            }
            ret = bin->mallocWithFreshSlab(tsdn, binind, fresh_slab);
            fresh_slab = nullptr;
        }
    }
    if constexpr (config::stats)
    {
        ++bin->stats.nmalloc;
        ++bin->stats.nrequests;
        ++bin->stats.curregs;
    }
    bin->lock.unlock(tsdn);

    if (fresh_slab != nullptr)
        arenaSlabDalloc(tsdn, arena, fresh_slab);
    if (zero)
        memset(ret, 0, usize);
    arenaDecayTick(tsdn, arena);

    return ret;
}

/// jemalloc: arena_malloc_hard
void * arenaMallocHard(ThreadState * tsdn, Arena * arena, size_t size, szind_t ind, bool zero, bool slab)
{
    JE_ASSERT(tsdn != nullptr || arena != nullptr);

    if (JE_LIKELY(tsdn != nullptr))
        arena = arenaChooseMaybeHuge(*tsdn, arena, size);
    if (JE_UNLIKELY(arena == nullptr))
        return nullptr;

    if (JE_LIKELY(slab))
    {
        JE_ASSERT(sz::canUseSlab(size));
        return arenaMallocSmall(tsdn, arena, ind, zero);
    }
    else
    {
        return largeMalloc(tsdn, arena, sz::s2u(size), zero);
    }
}

/// jemalloc: arena_palloc
void * arenaPalloc(ThreadState * tsdn, Arena * arena, size_t usize, size_t alignment, bool zero, bool slab, ThreadCache * tcache)
{
    if (slab)
    {
        JE_ASSERT(sz::canUseSlab(usize));
        /// Small; alignment doesn't require special slab placement.

        /// usize should be a result of `sz::sa2u`.
        JE_ASSERT((usize & (alignment - 1)) == 0);

        /// Small usize can't come from an alignment larger than a page.
        JE_ASSERT(alignment <= PAGE);

        return arenaMalloc(tsdn, arena, usize, sz::sizeToIndex(usize), zero, slab, tcache, true);
    }
    else
    {
        if (JE_LIKELY(alignment <= CACHELINE))
            return largeMalloc(tsdn, arena, usize, zero);
        else
            return largePalloc(tsdn, arena, usize, alignment, zero);
    }
}

/// --- Small deallocation --------------------------------------------------------------------------------------------

/// jemalloc: arena_dalloc_bin
static void arenaDallocBin(ThreadState * tsdn, Arena * arena, Extent * edata, void * ptr)
{
    szind_t binind = edata->szind();
    unsigned binshard = edata->binshard();
    Bin * bin = arenaGetBin(arena, binind, binshard);

    bin->lock.lock(tsdn);
    BinDallocLockedInfo info;
    Bin::dallocLockedBegin(info, binind);
    bool ret = bin->dallocLockedStep(tsdn, arenaIsAuto(arena), info, binind, edata, ptr);
    bin->dallocLockedFinish(tsdn, info);
    bin->lock.unlock(tsdn);

    if (ret)
        arenaSlabDalloc(tsdn, arena, edata);
}

/// jemalloc: arena_dalloc_small
void arenaDallocSmall(ThreadState * tsdn, void * ptr)
{
    Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);
    Arena * arena = arenaGetFromEdata(edata);

    arenaDallocBin(tsdn, arena, edata, ptr);
    arenaDecayTick(tsdn, arena);
}

/// jemalloc: arena_ptr_array_flush_ptr_getter
static const void * arenaPtrArrayFlushPtrGetter(void * arr_ctx, size_t ind)
{
    CacheBinPtrArray * arr = static_cast<CacheBinPtrArray *>(arr_ctx);
    return arr->ptr[ind];
}

/// jemalloc: arena_ptr_array_flush_metadata_visitor
static void arenaPtrArrayFlushMetadataVisitor(void * szind_sum_ctx, FullAllocContext * alloc_ctx)
{
    size_t * szind_sum = static_cast<size_t *>(szind_sum_ctx);
    *szind_sum -= alloc_ctx->szind;
    /// util_prefetch_write_range(alloc_ctx->edata, sizeof(edata_t))
    for (size_t i = 0; i < sizeof(Extent); i += CACHELINE)
    {
        std::byte * p = reinterpret_cast<std::byte *>(alloc_ctx->edata) + i;
        if constexpr (config::debug)
            *reinterpret_cast<volatile char *>(p);
        __builtin_prefetch(p, 1, 3);
    }
}

/// jemalloc: arena_ptr_array_flush_size_check_fail
[[maybe_unused]] JE_NOINLINE static void
arenaPtrArrayFlushSizeCheckFail(CacheBinPtrArray * arr, szind_t szind, size_t nptrs, ExtentMapBatchLookupResult * edatas)
{
    [[maybe_unused]] bool found_mismatch = false;
    for (size_t i = 0; i < nptrs; ++i)
    {
        szind_t true_szind = edatas[i].edata->szind();
        if (true_szind != szind)
        {
            found_mismatch = true;
            safetyCheckFailSizedDealloc(
                /* current_dealloc */ false,
                /* ptr */ arenaPtrArrayFlushPtrGetter(arr, i),
                /* true_size */ sz::indexToSize(true_szind),
                /* input_size */ sz::indexToSize(szind));
        }
    }
    JE_ASSERT(found_mismatch);
}

/// jemalloc: arena_ptr_array_flush_impl_small
JE_ALWAYS_INLINE static void arenaPtrArrayFlushImplSmall(
    ThreadState * tsdn,
    szind_t binind,
    CacheBinPtrArray * arr,
    ExtentMapBatchLookupResult * item_edata,
    cache_bin_sz_t nflush,
    Arena * stats_arena,
    CacheBinStats ** merge_stats)
{
    /// The slabs where we freed the last remaining object in the slab (and so need to free the slab itself).
    unsigned dalloc_count = 0;
    /// VARIABLE_ARRAY(edata_t *, dalloc_slabs, nflush + 1); nflush <= CACHE_BIN_NFLUSH_BATCH_MAX.
    Extent * dalloc_slabs[CACHE_BIN_NFLUSH_BATCH_MAX + 1];
    JE_ASSERT(nflush <= CACHE_BIN_NFLUSH_BATCH_MAX);

    /// We're about to grab a bunch of locks. If one of them happens to be the one guarding the arena-level stats
    /// counters we flush our thread-local ones to, we do so under one critical section.
    ///
    /// We maintain the invariant that all edatas yet to be flushed are contained in the half-open range
    /// [flush_start, flush_end). We'll repeatedly partition the array so that the unflushed items are at the end.
    unsigned flush_start = 0;

    while (flush_start < nflush)
    {
        /// After our partitioning step, all objects to flush will be in the half-open range
        /// [prev_flush_start, flush_start), and flush_start will be updated to correspond to the next loop iteration.
        unsigned prev_flush_start = flush_start;

        Extent * cur_edata = item_edata[flush_start].edata;
        unsigned cur_arena_ind = cur_edata->arenaInd();
        Arena * cur_arena = arenaGet(tsdn, cur_arena_ind, false);

        unsigned cur_binshard = cur_edata->binshard();
        Bin * cur_bin = arenaGetBin(cur_arena, binind, cur_binshard);
        JE_ASSERT(cur_binshard < bin_infos[binind].n_shards);
        /// Start off the partition; item_edata[i] always matches itself of course.
        ++flush_start;
        for (unsigned i = flush_start; i < nflush; ++i)
        {
            [[maybe_unused]] void * ptr = arr->ptr[i];
            Extent * edata = item_edata[i].edata;
            JE_ASSERT(ptr != nullptr && edata != nullptr);
            JE_ASSERT(reinterpret_cast<uintptr_t>(ptr) >= reinterpret_cast<uintptr_t>(edata->addr()));
            JE_ASSERT(reinterpret_cast<uintptr_t>(ptr) < reinterpret_cast<uintptr_t>(edata->past()));
            if (edata->arenaInd() == cur_arena_ind && edata->binshard() == cur_binshard)
            {
                /// Swap the edatas.
                ExtentMapBatchLookupResult temp_edata = item_edata[flush_start];
                item_edata[flush_start] = item_edata[i];
                item_edata[i] = temp_edata;
                /// Swap the pointers.
                void * temp_ptr = arr->ptr[flush_start];
                arr->ptr[flush_start] = arr->ptr[i];
                arr->ptr[i] = temp_ptr;
                ++flush_start;
            }
        }
        /// Make sure we implemented partitioning correctly.
        if constexpr (config::debug)
        {
            for (unsigned i = prev_flush_start; i < flush_start; ++i)
            {
                Extent * edata = item_edata[i].edata;
                JE_ASSERT(edata->arenaInd() == cur_arena_ind);
                JE_ASSERT(edata->binshard() == cur_binshard);
            }
            for (unsigned i = flush_start; i < nflush; ++i)
            {
                Extent * edata = item_edata[i].edata;
                JE_ASSERT(edata->arenaInd() != cur_arena_ind || edata->binshard() != cur_binshard);
            }
        }

        /// Actually do the flushing.
        cur_bin->lock.lock(tsdn);

        /// Flush stats first, if that was the right lock. Note that we don't actually have to flush stats into the
        /// current thread's binshard. Flushing into any binshard in the same arena is enough; we don't expose stats
        /// on per-binshard basis (just per-bin).
        if (config::stats && stats_arena == cur_arena && *merge_stats != nullptr)
        {
            ++cur_bin->stats.nflushes;
            cur_bin->stats.nrequests += (*merge_stats)->nrequests;
            *merge_stats = nullptr;
        }

        /// Next flush objects.
        BinDallocLockedInfo dalloc_bin_info = {};
        Bin::dallocLockedBegin(dalloc_bin_info, binind);
        for (unsigned i = prev_flush_start; i < flush_start; ++i)
        {
            void * ptr = arr->ptr[i];
            Extent * edata = item_edata[i].edata;
            if (cur_bin->dallocLockedStep(tsdn, arenaIsAuto(cur_arena), dalloc_bin_info, binind, edata, ptr))
            {
                dalloc_slabs[dalloc_count] = edata;
                ++dalloc_count;
            }
        }

        cur_bin->dallocLockedFinish(tsdn, dalloc_bin_info);
        cur_bin->lock.unlock(tsdn);

        arenaDecayTicks(tsdn, cur_arena, flush_start - prev_flush_start);
    }

    /// Handle all deferred slab dalloc.
    for (unsigned i = 0; i < dalloc_count; ++i)
    {
        Extent * slab = dalloc_slabs[i];
        arenaSlabDalloc(tsdn, arenaGetFromEdata(slab), slab);
    }

    if (config::stats && *merge_stats != nullptr)
    {
        /// The flush loop didn't happen to flush to this thread's arena, so the stats didn't get merged. Manually do
        /// so now.
        Bin * bin = binChoose(tsdn, stats_arena, binind, nullptr);
        bin->lock.lock(tsdn);
        ++bin->stats.nflushes;
        bin->stats.nrequests += (*merge_stats)->nrequests;
        *merge_stats = nullptr;
        bin->lock.unlock(tsdn);
    }
}

/// jemalloc: arena_ptr_array_flush_impl_large
JE_ALWAYS_INLINE static void arenaPtrArrayFlushImplLarge(
    ThreadState * tsdn,
    szind_t binind,
    CacheBinPtrArray * arr,
    ExtentMapBatchLookupResult * item_edata,
    cache_bin_sz_t nflush,
    Arena * stats_arena,
    CacheBinStats ** merge_stats)
{
    /// We're about to grab a bunch of locks. If one of them happens to be the one guarding the arena-level stats
    /// counters we flush our thread-local ones to, we do so under one critical section.
    while (nflush > 0)
    {
        /// Lock the arena, or bin, associated with the first object.
        Extent * edata = item_edata[0].edata;
        unsigned cur_arena_ind = edata->arenaInd();
        Arena * cur_arena = arenaGet(tsdn, cur_arena_ind, false);

        if (!arenaIsAuto(cur_arena))
            cur_arena->large_mtx.lock(tsdn);

        /// If we acquired the right lock and have some stats to flush, flush them.
        if (config::stats && stats_arena == cur_arena && *merge_stats != nullptr)
        {
            arenaStatsLargeFlushNrequestsAdd(tsdn, &stats_arena->stats, binind, (*merge_stats)->nrequests);
            *merge_stats = nullptr;
        }

        /// Large allocations need special prep done. Afterwards, we can drop the large lock.
        for (unsigned i = 0; i < nflush; ++i)
        {
            [[maybe_unused]] void * ptr = arr->ptr[i];
            edata = item_edata[i].edata;
            JE_ASSERT(ptr != nullptr && edata != nullptr);

            if (edata->arenaInd() == cur_arena_ind)
                largeDallocPrepLocked(tsdn, edata);
        }
        if (!arenaIsAuto(cur_arena))
            cur_arena->large_mtx.unlock(tsdn);

        /// Deallocate whatever we can.
        unsigned ndeferred = 0;
        for (unsigned i = 0; i < nflush; ++i)
        {
            void * ptr = arr->ptr[i];
            edata = item_edata[i].edata;
            JE_ASSERT(ptr != nullptr && edata != nullptr);
            if (edata->arenaInd() != cur_arena_ind)
            {
                /// The object was allocated either via a different arena, or a different bin in this arena. Either
                /// way, stash the object so that it can be handled in a future pass.
                arr->ptr[ndeferred] = ptr;
                item_edata[ndeferred].edata = edata;
                ++ndeferred;
                continue;
            }
            if (largeDallocSafetyChecks(edata, ptr, sz::indexToSize(binind)))
            {
                /// See the comment in isfree.
                continue;
            }
            largeDallocFinish(tsdn, edata);
        }
        arenaDecayTicks(tsdn, cur_arena, nflush - ndeferred);
        nflush = cache_bin_sz_t(ndeferred);
    }

    if (config::stats && *merge_stats != nullptr)
    {
        arenaStatsLargeFlushNrequestsAdd(tsdn, &stats_arena->stats, binind, (*merge_stats)->nrequests);
        *merge_stats = nullptr;
    }
}

/// jemalloc: arena_ptr_array_flush_impl
JE_ALWAYS_INLINE static void arenaPtrArrayFlushImpl(
    ThreadState & tsd, szind_t binind, CacheBinPtrArray * arr, unsigned nflush, bool small, Arena * stats_arena, CacheBinStats ** merge_stats)
{
    ThreadState * tsdn = &tsd;
    /// VARIABLE_ARRAY(emap_batch_lookup_result_t, item_edata, nflush + 1): the last element is never touched.
    ExtentMapBatchLookupResult item_edata[CACHE_BIN_NFLUSH_BATCH_MAX + 1];
    JE_ASSERT(nflush <= CACHE_BIN_NFLUSH_BATCH_MAX);
    /// This gets compiled away when `config_opt_safety_checks` is false. Checks for sized deallocation bugs, failing
    /// early rather than corrupting metadata.
    size_t szind_sum = size_t(binind) * nflush;
    arena_emap_global.edataLookupBatch(
        tsd, nflush, &arenaPtrArrayFlushPtrGetter, arr, &arenaPtrArrayFlushMetadataVisitor, &szind_sum, item_edata);
    if (config::opt_safety_checks && JE_UNLIKELY(szind_sum != 0))
        arenaPtrArrayFlushSizeCheckFail(arr, binind, nflush, item_edata);

    /// The small/large flush logic is very similar; you might conclude that it's a good opportunity to share code.
    /// We've tried this, and by and large found this to obscure more than it helps; there are so many fiddly bits
    /// around things like stats handling, precisely when and which mutexes are acquired, etc., that almost all code
    /// ends up being gated behind 'if (small) { ... } else { ... }'. Even though the '...' is morally equivalent, the
    /// code itself needs slight tweaks.
    if (small)
        arenaPtrArrayFlushImplSmall(tsdn, binind, arr, item_edata, cache_bin_sz_t(nflush), stats_arena, merge_stats);
    else
        arenaPtrArrayFlushImplLarge(tsdn, binind, arr, item_edata, cache_bin_sz_t(nflush), stats_arena, merge_stats);
}

/// jemalloc: arena_ptr_array_flush
void arenaPtrArrayFlush(
    ThreadState & tsd, szind_t binind, CacheBinPtrArray * arr, unsigned nflush, bool small, Arena * stats_arena, CacheBinStats merge_stats)
{
    JE_ASSERT(arr != nullptr && arr->ptr != nullptr);
    /// The input cache bin stats represent a snapshot taken when the pointer array is set up, and will be merged into
    /// the next-level bin stats. The original bin stats will be reset by the caller itself. This separation ensures
    /// that each layer operates independently and does not modify another layer's data directly.
    CacheBinStats * stats = &merge_stats;
    unsigned nflush_batch;
    unsigned nflushed = 0;
    CacheBinPtrArray ptrs_batch;
    do
    {
        nflush_batch = nflush - nflushed;
        if (nflush_batch > CACHE_BIN_NFLUSH_BATCH_MAX)
            nflush_batch = CACHE_BIN_NFLUSH_BATCH_MAX;
        JE_ASSERT(nflush_batch <= CACHE_BIN_NFLUSH_BATCH_MAX);
        ptrs_batch.n = cache_bin_sz_t(nflush_batch);
        ptrs_batch.ptr = arr->ptr + nflushed;
        arenaPtrArrayFlushImpl(tsd, binind, &ptrs_batch, nflush_batch, small, stats_arena, &stats);
        nflushed += nflush_batch;
    } while (nflushed < nflush);
    JE_ASSERT(nflush == nflushed);
    JE_ASSERT((arr->ptr + nflush) == (ptrs_batch.ptr + nflush_batch));
    if constexpr (config::stats)
        JE_ASSERT(stats == nullptr);
}

/// --- Reallocation --------------------------------------------------------------------------------------------------

/// jemalloc: arena_ralloc_no_move
bool arenaRallocNoMove(ThreadState * tsdn, void * ptr, size_t oldsize, size_t size, size_t extra, bool zero, size_t * newsize)
{
    bool ret;
    /// Calls with non-zero extra had to clamp extra.
    JE_ASSERT(extra == 0 || size + extra <= SC_LARGE_MAXCLASS);

    Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);
    if (JE_UNLIKELY(size > SC_LARGE_MAXCLASS))
    {
        ret = true;
    }
    else
    {
        size_t usize_min = sz::s2u(size);
        size_t usize_max = sz::s2u(size + extra);
        if (JE_LIKELY(oldsize <= SC_SMALL_MAXCLASS && usize_min <= SC_SMALL_MAXCLASS))
        {
            /// Avoid moving the allocation if the size class can be left the same.
            JE_ASSERT(bin_infos[sz::sizeToIndex(oldsize)].reg_size == oldsize);
            if ((usize_max > SC_SMALL_MAXCLASS || sz::sizeToIndex(usize_max) != sz::sizeToIndex(oldsize))
                && (size > oldsize || usize_max < oldsize))
            {
                ret = true;
            }
            else
            {
                Arena * arena = arenaGetFromEdata(edata);
                arenaDecayTick(tsdn, arena);
                ret = false;
            }
        }
        else if (oldsize >= SC_LARGE_MINCLASS && usize_max >= SC_LARGE_MINCLASS)
        {
            ret = largeRallocNoMove(tsdn, edata, usize_min, usize_max, zero);
        }
        else
        {
            ret = true;
        }
    }
    /// done:
    JE_ASSERT(edata == arena_emap_global.edataLookup(tsdn, ptr));
    *newsize = edata->usize();

    return ret;
}

/// jemalloc: arena_ralloc_move_helper
static void * arenaRallocMoveHelper(ThreadState * tsdn, Arena * arena, size_t usize, size_t alignment, bool zero, bool slab, ThreadCache * tcache)
{
    if (alignment == 0)
        return arenaMalloc(tsdn, arena, usize, sz::sizeToIndex(usize), zero, slab, tcache, true);
    usize = sz::sa2u(usize, alignment);
    if (JE_UNLIKELY(usize == 0 || usize > SC_LARGE_MAXCLASS))
        return nullptr;
    /// ipalloct_explicit_slab -> ipallocztm_explicit_slab(..., is_internal = false, arena) -> arena_palloc.
    void * ret = arenaPalloc(tsdn, arena, usize, alignment, zero, slab, tcache);
    JE_ASSERT(alignmentAddrToBase(ret, alignment) == ret);
    return ret;
}

/// jemalloc: arena_ralloc
void * arenaRalloc(
    ThreadState * tsdn, Arena * arena, void * ptr, size_t oldsize, size_t size, size_t alignment, bool zero, bool slab, ThreadCache * tcache)
{
    size_t usize = alignment == 0 ? sz::s2u(size) : sz::sa2u(size, alignment);
    if (JE_UNLIKELY(usize == 0 || size > SC_LARGE_MAXCLASS))
        return nullptr;

    if (JE_LIKELY(slab))
    {
        JE_ASSERT(sz::canUseSlab(usize));
        /// Try to avoid moving the allocation.
        size_t newsize;
        if (!arenaRallocNoMove(tsdn, ptr, oldsize, usize, 0, zero, &newsize))
        {
            /// hook_invoke_expand: hooks are dropped.
            return ptr;
        }
    }

    if (oldsize >= SC_LARGE_MINCLASS && usize >= SC_LARGE_MINCLASS)
        return largeRalloc(tsdn, arena, ptr, usize, alignment, zero, tcache);

    /// size and oldsize are different enough that we need to move the object. In that case, fall back to allocating
    /// new space and copying.
    void * ret = arenaRallocMoveHelper(tsdn, arena, usize, alignment, zero, slab, tcache);
    if (ret == nullptr)
        return nullptr;

    /// hook_invoke_alloc, hook_invoke_dalloc: hooks are dropped.

    /// Junk/zero-filling were already done by ipalloc()/arena_malloc().
    size_t copysize = (usize < oldsize) ? usize : oldsize;
    memcpy(ret, ptr, copysize);
    /// isdalloct(tsdn, ptr, oldsize, tcache, NULL, true)
    arenaSdalloc(tsdn, ptr, oldsize, tcache, nullptr, true);
    return ret;
}

/// --- Misc ----------------------------------------------------------------------------------------------------------

/// jemalloc: arena_dss_prec_get
DssPrec arenaDssPrecGet(Arena * arena)
{
    return DssPrec(arena->dss_prec.load(std::memory_order_acquire));
}

/// jemalloc: arena_dss_prec_set
bool arenaDssPrecSet(Arena * arena, DssPrec dss_prec)
{
    if constexpr (!config::have_dss)
        return dss_prec != DssPrec::Disabled;
    arena->dss_prec.store(unsigned(dss_prec), std::memory_order_release);
    return false;
}

/// jemalloc: arena_name_get
void arenaNameGet(Arena * arena, char * name)
{
    const char * end = static_cast<const char *>(memchr(arena->name, '\0', ARENA_NAME_LEN));
    JE_ASSERT(end != nullptr);
    size_t len = size_t(end - arena->name) + 1;
    JE_ASSERT(len > 0 && len <= ARENA_NAME_LEN);

    strncpy(name, arena->name, len);
}

/// jemalloc: arena_name_set
void arenaNameSet(Arena * arena, const char * name)
{
    strncpy(arena->name, name, ARENA_NAME_LEN);
    arena->name[ARENA_NAME_LEN - 1] = '\0';
}

/// jemalloc: arena_dirty_decay_ms_default_get
ssize_t arenaDirtyDecayMsDefaultGet()
{
    return dirty_decay_ms_default.load(std::memory_order_relaxed);
}

/// jemalloc: arena_dirty_decay_ms_default_set
bool arenaDirtyDecayMsDefaultSet(ssize_t decay_ms)
{
    if (!Decay::msValid(decay_ms))
        return true;
    dirty_decay_ms_default.store(decay_ms, std::memory_order_relaxed);
    return false;
}

/// jemalloc: arena_muzzy_decay_ms_default_get
ssize_t arenaMuzzyDecayMsDefaultGet()
{
    return muzzy_decay_ms_default.load(std::memory_order_relaxed);
}

/// jemalloc: arena_muzzy_decay_ms_default_set
bool arenaMuzzyDecayMsDefaultSet(ssize_t decay_ms)
{
    if (!Decay::msValid(decay_ms))
        return true;
    muzzy_decay_ms_default.store(decay_ms, std::memory_order_relaxed);
    return false;
}

/// jemalloc: arena_retain_grow_limit_get_set
bool arenaRetainGrowLimitGetSet(ThreadState & tsd, Arena * arena, size_t * old_limit, size_t * new_limit)
{
    JE_ASSERT(opt.retain);
    return arena->pa_shard.pac.retainGrowLimitGetSet(&tsd, old_limit, new_limit);
}

/// --- Creation ------------------------------------------------------------------------------------------------------

/// jemalloc: arena_new
Arena * arenaNew(ThreadState * tsdn, unsigned ind, const ArenaConfig * config)
{
    Base * base;
    if (ind == 0)
    {
        base = b0get();
    }
    else
    {
        base = Base::create(tsdn, ind, config->extent_hooks, config->metadata_use_hooks);
        if (base == nullptr)
            return nullptr;
    }

    Arena * arena = nullptr;
    NsTime cur_time = NsTime::zero();

    size_t arena_size = alignmentCeiling(sizeof(Arena), CACHELINE) + sizeof(Bin) * arena_nbins_total;
    void * mem = base->alloc(tsdn, arena_size, CACHELINE);
    if (mem == nullptr)
        goto label_error;

    /// The memory is zeroed; the constructors only produce the zero state (plus the static mutex initializers).
    arena = new (mem) Arena;
    JE_ASSERT(reinterpret_cast<uintptr_t>(arena->allBins() + arena_nbins_total) <= reinterpret_cast<uintptr_t>(arena) + arena_size);
    arena->nthreads[0].store(0, std::memory_order_relaxed);
    arena->nthreads[1].store(0, std::memory_order_relaxed);
    arena->last_thd = nullptr;

    if constexpr (config::stats)
    {
        /// arena_stats_init: there is no stats mutex, and the memory is zeroed.
        arena->tcache_ql.init();
        arena->cache_bin_array_descriptor_ql.init();
        if (arena->tcache_ql_mtx.init("tcache_ql", MutexRank::TCACHE_QL, MutexLockOrder::RankExclusive))
            goto label_error;
    }

    arena->dss_prec.store(unsigned(extentDssPrecGet()), std::memory_order_relaxed);

    arena->large.init();
    if (arena->large_mtx.init("arena_large", MutexRank::ARENA_LARGE, MutexLockOrder::RankExclusive))
        goto label_error;

    cur_time.initUpdate();
    if (arena->pa_shard.init(
            tsdn,
            &arena_emap_global,
            base,
            ind,
            &arena->stats.pa_shard_stats,
            /* stats_mtx */ nullptr,
            cur_time,
            oversize_threshold,
            arenaDirtyDecayMsDefaultGet(),
            arenaMuzzyDecayMsDefaultGet()))
        goto label_error;

    /// Initialize bins.
    arena->binshard_next.store(0, std::memory_order_release);
    for (unsigned i = 0; i < arena_nbins_total; ++i)
    {
        Bin * bin = new (arena->allBins() + i) Bin;
        if (bin->init())
            goto label_error;
    }

    arena->base = base;
    /// jemalloc stores `ind` right after publishing the arena; it is stored first here (the value is the same as
    /// `base->indGet()`, so the readers cannot observe a difference other than a race on a not yet written field).
    arena->ind = ind;
    /// Set arena before creating background threads.
    arenaSet(ind, arena);

    /// Init the name.
    format(arena->name, sizeof(arena->name), "%s_%u", arenaIsAuto(arena) ? "auto" : "manual", arena->ind);
    arena->name[ARENA_NAME_LEN - 1] = '\0';

    arena->create_time.initUpdate();

    /// HPA is dropped (`opt.hpa` is always false).

    /// We don't support reentrancy for arena 0 bootstrapping.
    if (ind != 0)
    {
        /// If we're here, then arena 0 already exists, so bootstrapping is done enough that we should have tsd.
        JE_ASSERT(tsdn != nullptr);
        preReentrancy(*tsdn, arena);
        /// test_hooks_arena_new_hook: test hooks are dropped.
        postReentrancy(*tsdn);
    }

    return arena;

label_error:
    if (ind != 0)
        base->destroy(tsdn);
    return nullptr;
}

/// jemalloc: arena_create_huge_arena
static Arena * arenaCreateHugeArena(ThreadState & tsd, unsigned ind)
{
    JE_ASSERT(ind != 0);

    Arena * huge_arena = arenaGet(&tsd, ind, true);
    if (huge_arena == nullptr)
        return nullptr;

    const char * huge_arena_name = "auto_oversize";
    strncpy(huge_arena->name, huge_arena_name, ARENA_NAME_LEN);
    huge_arena->name[ARENA_NAME_LEN - 1] = '\0';

    /// Purge eagerly for huge allocations, because: 1) number of huge allocations is usually small, which means ticker
    /// based decay is not reliable; and 2) less immediate reuse is expected for huge allocations.
    ///
    /// However, with background threads enabled, keep normal purging since the purging delay is bounded.
    if (!backgroundThreadEnabled() && arenaDirtyDecayMsDefaultGet() > 0)
        arenaDecayMsSet(&tsd, huge_arena, extent_state_dirty, 0);
    if (!backgroundThreadEnabled() && arenaMuzzyDecayMsDefaultGet() > 0)
        arenaDecayMsSet(&tsd, huge_arena, extent_state_muzzy, 0);

    return huge_arena;
}

/// jemalloc: arena_choose_huge
Arena * arenaChooseHuge(ThreadState & tsd)
{
    /// huge_arena_ind can be 0 during init (will use a0).

    Arena * huge_arena = arenaGet(&tsd, huge_arena_ind, false);
    if (huge_arena == nullptr)
    {
        /// Create the huge arena on demand.
        huge_arena = arenaCreateHugeArena(tsd, huge_arena_ind);
    }

    return huge_arena;
}

/// jemalloc: arena_init_huge
bool arenaInitHuge(ThreadState * tsdn, Arena * a0_)
{
    bool huge_enabled;
    JE_ASSERT(huge_arena_ind == 0);

    /// The threshold should be large size class.
    if (opt.oversize_threshold > SC_LARGE_MAXCLASS || opt.oversize_threshold < SC_LARGE_MINCLASS)
    {
        opt.oversize_threshold = 0;
        oversize_threshold = SC_LARGE_MAXCLASS + PAGE;
        huge_enabled = false;
    }
    else
    {
        /// Reserve the index for the huge arena.
        huge_arena_ind = narenasTotalGet();
        JE_ASSERT(huge_arena_ind != 0);
        oversize_threshold = opt.oversize_threshold;
        /// a0 init happened before the options were parsed.
        a0_->pa_shard.pac.oversize_threshold.store(oversize_threshold, std::memory_order_relaxed);
        /// Initialize the `huge_arena_pac_thp` fields under b0's mutex (so that b0's THP auto-switch won't happen
        /// concurrently). `opt.huge_arena_pac_thp` is not ported (off by default): only the locking is kept, because
        /// it is observable through the mutex stats of the base.
        Mutex & b0_mtx = a0_->base->mutex();
        b0_mtx.lock(tsdn);
        b0_mtx.unlock(tsdn);
        huge_enabled = true;
    }

    return huge_enabled;
}

/// jemalloc: arena_boot
bool arenaBoot(const SizeClassData * sc_data, Base * /*base*/, bool /*hpa*/)
{
    arenaDirtyDecayMsDefaultSet(opt.dirty_decay_ms);
    arenaMuzzyDecayMsDefaultSet(opt.muzzy_decay_ms);
    for (unsigned i = 0; i < SC_NBINS; ++i)
    {
        const SizeClass & sc = sc_data->sc[i];
        arena_binind_div_info[i].init((size_t(1) << sc.lg_base) + (size_t(sc.ndelta) << sc.lg_delta));
    }

    uint32_t cur_offset = uint32_t(sizeof(Arena));
    arena_nbins_total = 0;
    for (szind_t i = 0; i < SC_NBINS; ++i)
    {
        arena_bin_offsets[i] = cur_offset;
        arena_nbins_total += bin_infos[i].n_shards;
        cur_offset += uint32_t(bin_infos[i].n_shards * sizeof(Bin));
    }
    /// pa_central_init: HPA only.
    return false;
}

/// --- Fork ----------------------------------------------------------------------------------------------------------

/// jemalloc: arena_prefork0
void arenaPrefork0(ThreadState * tsdn, Arena * arena)
{
    arena->pa_shard.prefork0(tsdn);
}

/// jemalloc: arena_prefork1
void arenaPrefork1(ThreadState * tsdn, Arena * arena)
{
    if constexpr (config::stats)
        arena->tcache_ql_mtx.prefork(tsdn);
}

/// jemalloc: arena_prefork2
void arenaPrefork2(ThreadState * tsdn, Arena * arena)
{
    arena->pa_shard.prefork2(tsdn);
}

/// jemalloc: arena_prefork3
void arenaPrefork3(ThreadState * tsdn, Arena * arena)
{
    arena->pa_shard.prefork3(tsdn);
}

/// jemalloc: arena_prefork4
void arenaPrefork4(ThreadState * tsdn, Arena * arena)
{
    arena->pa_shard.prefork4(tsdn);
}

/// jemalloc: arena_prefork5
void arenaPrefork5(ThreadState * tsdn, Arena * arena)
{
    arena->pa_shard.prefork5(tsdn);
}

/// jemalloc: arena_prefork6
void arenaPrefork6(ThreadState * tsdn, Arena * arena)
{
    arena->base->prefork(tsdn);
}

/// jemalloc: arena_prefork7
void arenaPrefork7(ThreadState * tsdn, Arena * arena)
{
    arena->large_mtx.prefork(tsdn);
}

/// jemalloc: arena_prefork8
void arenaPrefork8(ThreadState * tsdn, Arena * arena)
{
    for (unsigned i = 0; i < arena_nbins_total; ++i)
        arena->allBins()[i].prefork(tsdn);
}

/// jemalloc: arena_postfork_parent
void arenaPostforkParent(ThreadState * tsdn, Arena * arena)
{
    for (unsigned i = 0; i < arena_nbins_total; ++i)
        arena->allBins()[i].postforkParent(tsdn);

    arena->large_mtx.postforkParent(tsdn);
    arena->base->postforkParent(tsdn);
    arena->pa_shard.postforkParent(tsdn);
    if constexpr (config::stats)
        arena->tcache_ql_mtx.postforkParent(tsdn);
}

/// jemalloc: arena_postfork_child
void arenaPostforkChild(ThreadState * tsdn, Arena * arena)
{
    ThreadState & tsd = *tsdn;
    arena->nthreads[0].store(0, std::memory_order_relaxed);
    arena->nthreads[1].store(0, std::memory_order_relaxed);
    if (tsd.arena == arena)
        arenaNthreadsInc(arena, false);
    if (tsd.iarena == arena)
        arenaNthreadsInc(arena, true);
    if constexpr (config::stats)
    {
        arena->tcache_ql.init();
        arena->cache_bin_array_descriptor_ql.init();
        ThreadCacheSlow * tcache_slow = tcacheSlowGet(tsd);
        if (tcache_slow != nullptr && tcache_slow->arena == arena)
        {
            ThreadCache * tcache = tcache_slow->tcache;
            arena->tcache_ql.elementInit(tcache_slow);
            arena->tcache_ql.tailInsert(tcache_slow);
            tcache_slow->cache_bin_array_descriptor.init(tcache->bins);
            arena->cache_bin_array_descriptor_ql.tailInsert(&tcache_slow->cache_bin_array_descriptor);
        }
    }

    for (unsigned i = 0; i < arena_nbins_total; ++i)
        arena->allBins()[i].postforkChild(tsdn);

    arena->large_mtx.postforkChild(tsdn);
    arena->base->postforkChild(tsdn);
    arena->pa_shard.postforkChild(tsdn);
    if constexpr (config::stats)
        arena->tcache_ql_mtx.postforkChild(tsdn);
}

}
