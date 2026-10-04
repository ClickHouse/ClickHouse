#include <allocator/PageAllocator.h>

#include <allocator/Arena.h>
#include <allocator/BackgroundThread.h>
#include <allocator/ExtentOps.h>
#include <allocator/Options.h>

#include <cstring>

namespace jemalloc
{

/// --- PageAllocator (pac.c) -----------------------------------------------------------------------------------------

void PageAllocator::decayDataGet(ExtentState state, Decay ** r_decay, DecayStats ** r_decay_stats, ExtentCache ** r_ecache)
{
    switch (state)
    {
        case extent_state_dirty:
            *r_decay = &decay_dirty;
            *r_decay_stats = &stats->decay_dirty;
            *r_ecache = &ecache_dirty;
            return;
        case extent_state_muzzy:
            *r_decay = &decay_muzzy;
            *r_decay_stats = &stats->decay_muzzy;
            *r_ecache = &ecache_muzzy;
            return;
        case extent_state_active:
        case extent_state_retained:
        case extent_state_transition:
        case extent_state_merging:
        default:
            JE_NOT_REACHED();
    }
}

bool PageAllocator::init(
    ThreadState * tsdn,
    Base * base_,
    ExtentMap * emap_,
    ExtentPool * edata_cache_,
    const NsTime & cur_time,
    size_t pac_oversize_threshold,
    ssize_t dirty_decay_ms,
    ssize_t muzzy_decay_ms,
    PacStats * pac_stats,
    Mutex * stats_mtx_)
{
    unsigned ind = base_->indGet();
    /// Delay coalescing for dirty extents despite the disruptive effect on memory layout for best-fit extent
    /// allocation, since cached extents are likely to be reused soon after deallocation, and the cost of
    /// merging/splitting extents is non-trivial.
    if (ecache_dirty.init(tsdn, extent_state_dirty, ind, /* delay_coalesce */ true))
        return true;
    /// Coalesce muzzy extents immediately, because operations on them are in the critical path much less often than
    /// for dirty extents.
    if (ecache_muzzy.init(tsdn, extent_state_muzzy, ind, /* delay_coalesce */ false))
        return true;
    /// Coalesce retained extents immediately, in part because they will never be evicted (and therefore there's no
    /// opportunity for delayed coalescing), but also because operations on retained extents are not in the critical
    /// path.
    if (ecache_retained.init(tsdn, extent_state_retained, ind, /* delay_coalesce */ false))
        return true;
    exp_grow.init();
    if (grow_mtx.init("extent_grow", MutexRank::EXTENT_GROW, MutexLockOrder::RankExclusive))
        return true;
    oversize_threshold.store(pac_oversize_threshold, std::memory_order_relaxed);
    if (decay_dirty.init(cur_time, dirty_decay_ms))
        return true;
    if (decay_muzzy.init(cur_time, muzzy_decay_ms))
        return true;
    if (sba.init())
        return true;

    base = base_;
    emap = emap_;
    edata_cache = edata_cache_;
    stats = pac_stats;
    stats_mtx = stats_mtx_;
    extent_sn_next.store(0, std::memory_order_relaxed);

    return false;
}

size_t pacAllocRetainedBatchedSize(size_t size)
{
    if (size > SC_LARGE_MAXCLASS)
    {
        /// A valid input with usize SC_LARGE_MAXCLASS could still reach here because of `sz_large_pad`. Such a request
        /// is valid but we should not further increase it. Thus, directly return size for such cases.
        return size;
    }
    size_t batched_size = sz::s2uComputeUsingDelta(size);
    size_t next_hugepage_size = hugepageCeiling(size);
    return batched_size > next_hugepage_size ? next_hugepage_size : batched_size;
}

Extent * PageAllocator::allocReal(ThreadState * tsdn, ExtentHooks * ehooks, size_t size, size_t alignment, bool zero, bool guarded)
{
    JE_ASSERT(!guarded || alignment <= PAGE);
    size_t newly_mapped_size = 0;

    Extent * edata = ecacheAlloc(tsdn, this, ehooks, &ecache_dirty, nullptr, size, alignment, zero, guarded);

    if (edata == nullptr && mayHaveMuzzy())
        edata = ecacheAlloc(tsdn, this, ehooks, &ecache_muzzy, nullptr, size, alignment, zero, guarded);

    /// We batch-allocate a larger extent with large size classes disabled, because the reuse of extents in the dirty
    /// pool is worse without size classes for large allocations. For instance, with size classes, 1.1MB, 1.15MB, and
    /// 1.2MB allocations are all ceiled to 1.25MB and can reuse the same buffer if they are allocated and deallocated
    /// sequentially; without them, their sequential allocations and deallocations result in three different extents.
    /// Thus, we cache extra mergeable extents in the dirty pool to improve the reuse. This is skipped if both
    /// `maps_coalesce` and `retain` are disabled because VM is not cheap enough in such cases to be used aggressively
    /// and extents cannot be merged at will.
    if (sz::largeSizeClassesDisabled() && edata == nullptr && (config::maps_coalesce || opt.retain))
    {
        size_t batched_size = pacAllocRetainedBatchedSize(size);
        /// Note that `ecacheAllocGrow` will try to retrieve virtual memory from both the retained pool and directly
        /// from the OS through `extentAllocWrapper` if the retained pool has no qualified extents. This is also why the
        /// overcaching still works even with `retain` off.
        edata = ecacheAllocGrow(tsdn, this, ehooks, &ecache_retained, nullptr, batched_size, alignment, zero, guarded);

        if (edata != nullptr && batched_size > size)
        {
            Extent * trail = extentSplitWrapper(tsdn, this, ehooks, edata, size, batched_size - size, /* holding_core_locks */ false);
            if (trail == nullptr)
            {
                ecacheDalloc(tsdn, this, ehooks, &ecache_retained, edata);
                edata = nullptr;
            }
            else
            {
                ecacheDalloc(tsdn, this, ehooks, &ecache_dirty, trail);
            }
        }

        if (edata != nullptr)
            newly_mapped_size = batched_size;
    }

    if (edata == nullptr)
    {
        edata = ecacheAllocGrow(tsdn, this, ehooks, &ecache_retained, nullptr, size, alignment, zero, guarded);
        /// jemalloc compatibility: counted even if the allocation failed.
        newly_mapped_size = size;
    }

    /// jemalloc compatibility: the batched size is counted as newly mapped even if it came out of the retained cache,
    /// and `size` is counted even when the final allocation failed.
    if (config::stats && newly_mapped_size != 0)
        stats->pac_mapped.fetch_add(newly_mapped_size, std::memory_order_relaxed);

    return edata;
}

Extent * PageAllocator::allocNewGuarded(ThreadState * tsdn, ExtentHooks * ehooks, size_t size, [[maybe_unused]] size_t alignment, bool zero, bool frequent_reuse)
{
    JE_ASSERT(alignment <= PAGE);

    Extent * edata;
    if (sanBumpEnabled() && frequent_reuse)
    {
        edata = sba.alloc(tsdn, this, ehooks, size, zero);
    }
    else
    {
        size_t size_with_guards = sanTwoSideGuardedSize(size);
        /// Alloc a non-guarded extent first.
        edata = allocReal(tsdn, ehooks, size_with_guards, /* alignment */ PAGE, zero, /* guarded */ false);
        if (edata != nullptr)
        {
            /// Add guards around it.
            JE_ASSERT(edata->size() == size_with_guards);
            sanGuardPagesTwoSided(tsdn, ehooks, edata, emap, true);
        }
    }
    JE_ASSERT(edata == nullptr || (edata->guarded() && edata->size() == size));

    return edata;
}

Extent * PageAllocator::alloc(
    ThreadState * tsdn, size_t size, size_t alignment, bool zero, bool guarded, bool frequent_reuse, bool * /*deferred_work_generated*/)
{
    ExtentHooks * ehooks = ehooksGet();

    Extent * edata = nullptr;
    /// The condition is an optimization - not frequently reused guarded allocations are never put in the cache.
    /// `allocReal` also doesn't grow retained for guarded allocations. So `allocReal` for such allocations would
    /// always return null.
    if (!guarded || frequent_reuse)
        edata = allocReal(tsdn, ehooks, size, alignment, zero, guarded);
    if (edata == nullptr && guarded)
    {
        /// No cached guarded extents; creating a new one.
        edata = allocNewGuarded(tsdn, ehooks, size, alignment, zero, frequent_reuse);
    }

    return edata;
}

bool PageAllocator::expand(ThreadState * tsdn, Extent * edata, size_t old_size, size_t new_size, bool zero, bool * /*deferred_work_generated*/)
{
    ExtentHooks * ehooks = ehooksGet();

    size_t mapped_add = 0;
    size_t expand_amount = new_size - old_size;

    if (ehooks->mergeWillFail())
        return true;
    Extent * trail = ecacheAlloc(tsdn, this, ehooks, &ecache_dirty, edata, expand_amount, PAGE, zero, /* guarded */ false);
    if (trail == nullptr)
        trail = ecacheAlloc(tsdn, this, ehooks, &ecache_muzzy, edata, expand_amount, PAGE, zero, /* guarded */ false);
    if (trail == nullptr)
    {
        trail = ecacheAllocGrow(tsdn, this, ehooks, &ecache_retained, edata, expand_amount, PAGE, zero, /* guarded */ false);
        mapped_add = expand_amount;
    }
    if (trail == nullptr)
        return true;
    if (extentMergeWrapper(tsdn, this, ehooks, edata, trail))
    {
        extentDallocWrapper(tsdn, this, ehooks, trail);
        return true;
    }
    if (config::stats && mapped_add > 0)
        stats->pac_mapped.fetch_add(mapped_add, std::memory_order_relaxed);
    return false;
}

bool PageAllocator::shrink(ThreadState * tsdn, Extent * edata, size_t old_size, size_t new_size, bool * deferred_work_generated)
{
    ExtentHooks * ehooks = ehooksGet();

    size_t shrink_amount = old_size - new_size;

    if (ehooks->splitWillFail())
        return true;

    Extent * trail = extentSplitWrapper(tsdn, this, ehooks, edata, new_size, shrink_amount, /* holding_core_locks */ false);
    if (trail == nullptr)
        return true;
    ecacheDalloc(tsdn, this, ehooks, &ecache_dirty, trail);
    *deferred_work_generated = true;
    return false;
}

void PageAllocator::dalloc(ThreadState * tsdn, Extent * edata, bool * deferred_work_generated)
{
    ExtentHooks * ehooks = ehooksGet();

    if (edata->guarded())
    {
        /// Because cached guarded extents do exact fit only, large guarded extents are restored on dalloc eagerly
        /// (otherwise they will not be reused efficiently). Slab sizes have a limited number of size classes, and tend
        /// to cycle faster.
        ///
        /// In the case where coalesce is restrained (VirtualFree on Windows), guarded extents are also not cached --
        /// otherwise during arena destroy / reset, the retained extents would not be whole regions (i.e. they are
        /// split between regular and guarded).
        if (!edata->slab() || !config::maps_coalesce)
        {
            JE_ASSERT(edata->size() >= SC_LARGE_MINCLASS || !config::maps_coalesce);
            sanUnguardPagesTwoSided(tsdn, ehooks, edata, emap);
        }
    }

    ecacheDalloc(tsdn, this, ehooks, &ecache_dirty, edata);
    /// Purging of deallocated pages is deferred.
    *deferred_work_generated = true;
}

namespace
{

/// jemalloc: pac_ns_until_purge
JE_ALWAYS_INLINE uint64_t pacNsUntilPurge(ThreadState * tsdn, Decay * decay, size_t npages)
{
    if (!decay->mtx.tryLock(tsdn))
    {
        /// Use minimal interval if decay is contended.
        return BACKGROUND_THREAD_DEFERRED_MIN;
    }
    uint64_t result = decay->nsUntilPurge(npages, ARENA_DEFERRED_PURGE_NPAGES_THRESHOLD);

    decay->mtx.unlock(tsdn);
    return result;
}

}

uint64_t PageAllocator::timeUntilDeferredWork(ThreadState * tsdn)
{
    uint64_t time = pacNsUntilPurge(tsdn, &decay_dirty, ecache_dirty.npagesGet());
    if (time == BACKGROUND_THREAD_DEFERRED_MIN)
        return time;

    uint64_t muzzy = pacNsUntilPurge(tsdn, &decay_muzzy, ecache_muzzy.npagesGet());
    if (muzzy < time)
        time = muzzy;
    return time;
}

bool PageAllocator::retainGrowLimitGetSet(ThreadState * tsdn, size_t * old_limit, size_t * new_limit)
{
    pszind_t new_ind = 0;
    if (new_limit != nullptr)
    {
        size_t limit = *new_limit;
        /// Grow no more than the new limit.
        if ((new_ind = sz::psz2ind(limit + 1) - 1) >= SC_NPSIZES)
            return true;
    }

    grow_mtx.lock(tsdn);
    if (old_limit != nullptr)
        *old_limit = sz::pind2sz(exp_grow.limit);
    if (new_limit != nullptr)
        exp_grow.limit = new_ind;
    grow_mtx.unlock(tsdn);

    return false;
}

size_t PageAllocator::stashDecayed(
    ThreadState * tsdn, ExtentCache * ecache, size_t npages_limit, size_t npages_decay_max, ExtentListInactive * result)
{
    ExtentHooks * ehooks = ehooksGet();

    /// Stash extents according to `npages_limit`.
    size_t nstashed = 0;
    while (nstashed < npages_decay_max)
    {
        Extent * edata = ecacheEvict(tsdn, this, ehooks, ecache, npages_limit);
        if (edata == nullptr)
            break;
        result->append(edata);
        nstashed += edata->size() >> LG_PAGE;
    }
    return nstashed;
}

size_t PageAllocator::decayStashed(
    ThreadState * tsdn, Decay * /*decay*/, DecayStats * decay_stats, ExtentCache * ecache, bool fully_decay, ExtentListInactive * decay_extents)
{
    bool err;

    size_t nmadvise = 0;
    size_t nunmapped = 0;
    size_t npurged = 0;

    ExtentHooks * ehooks = ehooksGet();

    bool try_muzzy = !fully_decay && decayMsGet(extent_state_muzzy) != 0;

    bool purge_to_retained = !try_muzzy || ecache->state == extent_state_muzzy;
    /// Attempt process_madvise only if 1) enabled, 2) purging to retained, and 3) not using custom hooks.
    /// `opt.process_madvise_max_batch` is always 0 (`JEMALLOC_HAVE_PROCESS_MADVISE` is not configured), so the batch
    /// purge (`decay_with_process_madvise`) is not ported and nothing is ever "already purged".
    bool try_process_madvise = (opt.process_madvise_max_batch > 0) && purge_to_retained && ehooks->dallocWillFail();
    JE_ASSERT(!try_process_madvise);
    (void)try_process_madvise;
    bool already_purged = false;

    for (Extent * edata = decay_extents->first(); edata != nullptr; edata = decay_extents->first())
    {
        decay_extents->remove(edata);

        size_t size = edata->size();
        size_t npages = size >> LG_PAGE;

        ++nmadvise;
        npurged += npages;

        switch (ecache->state)
        {
            case extent_state_dirty:
                if (try_muzzy)
                {
                    err = extentPurgeLazyWrapper(tsdn, ehooks, edata, /* offset */ 0, size);
                    if (!err)
                    {
                        ecacheDalloc(tsdn, this, ehooks, &ecache_muzzy, edata);
                        break;
                    }
                }
                [[fallthrough]];
            case extent_state_muzzy:
                if (already_purged)
                    extentDallocWrapperPurged(tsdn, this, ehooks, edata);
                else
                    extentDallocWrapper(tsdn, this, ehooks, edata);
                nunmapped += npages;
                break;
            case extent_state_active:
            case extent_state_retained:
            case extent_state_transition:
            case extent_state_merging:
            default:
                JE_NOT_REACHED();
        }
    }

    if constexpr (config::stats)
    {
        decay_stats->npurge.inc(1);
        decay_stats->nmadvise.inc(nmadvise);
        decay_stats->purged.inc(npurged);
        stats->pac_mapped.fetch_sub(nunmapped << LG_PAGE, std::memory_order_relaxed);
    }

    return npurged;
}

void PageAllocator::decayToLimit(
    ThreadState * tsdn,
    Decay * decay,
    DecayStats * decay_stats,
    ExtentCache * ecache,
    bool fully_decay,
    size_t npages_limit,
    size_t npages_decay_max)
{
    if (decay->purging || npages_decay_max == 0)
        return;
    decay->purging = true;
    decay->mtx.unlock(tsdn);

    ExtentListInactive decay_extents;
    decay_extents.init();
    size_t npurge = stashDecayed(tsdn, ecache, npages_limit, npages_decay_max, &decay_extents);
    if (npurge != 0)
    {
        [[maybe_unused]] size_t npurged = decayStashed(tsdn, decay, decay_stats, ecache, fully_decay, &decay_extents);
        JE_ASSERT(npurged == npurge);
    }

    decay->mtx.lock(tsdn);
    decay->purging = false;
}

void PageAllocator::decayAll(ThreadState * tsdn, Decay * decay, DecayStats * decay_stats, ExtentCache * ecache, bool fully_decay)
{
    decay->mtx.assertOwner(tsdn);
    decayToLimit(tsdn, decay, decay_stats, ecache, fully_decay, /* npages_limit */ 0, ecache->npagesGet());
}

void PageAllocator::decayTryPurge(
    ThreadState * tsdn, Decay * decay, DecayStats * decay_stats, ExtentCache * ecache, size_t current_npages, size_t npages_limit)
{
    if (current_npages > npages_limit)
        decayToLimit(tsdn, decay, decay_stats, ecache, /* fully_decay */ false, npages_limit, current_npages - npages_limit);
}

bool PageAllocator::maybeDecayPurge(ThreadState * tsdn, Decay * decay, DecayStats * decay_stats, ExtentCache * ecache, PacPurgeEagerness eagerness)
{
    decay->mtx.assertOwner(tsdn);

    /// Purge all or nothing if the option is disabled.
    ssize_t decay_ms = decay->msRead();
    if (decay_ms <= 0)
    {
        if (decay_ms == 0)
            decayToLimit(tsdn, decay, decay_stats, ecache, /* fully_decay */ false, /* npages_limit */ 0, ecache->npagesGet());
        return false;
    }

    /// If the deadline has been reached, advance to the current epoch and purge to the new limit if necessary. Note
    /// that dirty pages created during the current epoch are not subject to purge until a future epoch, so as a result
    /// purging only happens during epoch advances, or being triggered by background threads (scheduled event).
    NsTime time;
    time.initUpdate();
    size_t npages_current = ecache->npagesGet();
    bool epoch_advanced = decay->maybeAdvanceEpoch(time, npages_current);
    if (eagerness == PAC_PURGE_ALWAYS || (epoch_advanced && eagerness == PAC_PURGE_ON_EPOCH_ADVANCE))
    {
        size_t npages_limit = decay->npagesLimitGet();
        decayTryPurge(tsdn, decay, decay_stats, ecache, npages_current, npages_limit);
    }

    return epoch_advanced;
}

bool PageAllocator::decayMsSet(ThreadState * tsdn, ExtentState state, ssize_t decay_ms, PacPurgeEagerness eagerness)
{
    Decay * decay;
    DecayStats * decay_stats;
    ExtentCache * ecache;
    decayDataGet(state, &decay, &decay_stats, &ecache);

    if (!Decay::msValid(decay_ms))
        return true;

    decay->mtx.lock(tsdn);
    /// Restart decay backlog from scratch, which may cause many dirty pages to be immediately purged. It would
    /// conceptually be possible to map the old backlog onto the new backlog, but there is no justification for such
    /// complexity since decay_ms changes are intended to be infrequent, either between the {-1, 0, >0} states, or a
    /// one-time arbitrary change during initial arena configuration.
    NsTime cur_time;
    cur_time.initUpdate();
    decay->reinit(cur_time, decay_ms);
    maybeDecayPurge(tsdn, decay, decay_stats, ecache, eagerness);
    decay->mtx.unlock(tsdn);

    return false;
}

ssize_t PageAllocator::decayMsGet(ExtentState state)
{
    Decay * decay;
    DecayStats * decay_stats;
    ExtentCache * ecache;
    decayDataGet(state, &decay, &decay_stats, &ecache);
    return decay->msRead();
}

void PageAllocator::reset(ThreadState * /*tsdn*/)
{
    /// No-op for now; purging is still done at the arena-level. It should get moved in here, though.
}

void PageAllocator::destroy(ThreadState * tsdn)
{
    JE_ASSERT(ecache_dirty.npagesGet() == 0);
    JE_ASSERT(ecache_muzzy.npagesGet() == 0);
    /// Iterate over the retained extents and destroy them. This gives the extent allocator underlying the extent hooks
    /// an opportunity to unmap all retained memory without having to keep its own metadata structures.
    ExtentHooks * ehooks = ehooksGet();
    Extent * edata;
    while ((edata = ecacheEvict(tsdn, this, ehooks, &ecache_retained, 0)) != nullptr)
        extentDestroyWrapper(tsdn, this, ehooks, edata);
}

/// --- PaShard (pa.c) --------------------------------------------------------------------------------------------------

bool PaShard::init(
    ThreadState * tsdn,
    ExtentMap * emap_,
    Base * base_,
    unsigned ind_,
    PaShardStats * stats_,
    Mutex * stats_mtx_,
    const NsTime & cur_time,
    size_t pac_oversize_threshold,
    ssize_t dirty_decay_ms,
    ssize_t muzzy_decay_ms)
{
    /// This will change eventually, but for now it should hold.
    JE_ASSERT(base_->indGet() == ind_);
    if (edata_cache.init(base_))
        return true;

    if (pac.init(
            tsdn, base_, emap_, &edata_cache, cur_time, pac_oversize_threshold, dirty_decay_ms, muzzy_decay_ms, &stats_->pac_stats, stats_mtx_))
        return true;

    ind = ind_;

    ever_used_hpa = false;
    use_hpa.store(false, std::memory_order_relaxed);

    nactive.store(0, std::memory_order_relaxed);

    stats_mtx = stats_mtx_;
    stats = stats_;
    /// jemalloc memsets the stats to zero (after `pac_init`, which does not write to them).
    stats->edata_avail = 0;
    for (DecayStats * decay_stats : {&stats->pac_stats.decay_dirty, &stats->pac_stats.decay_muzzy})
    {
        decay_stats->npurge.initUnsynchronized(0);
        decay_stats->nmadvise.initUnsynchronized(0);
        decay_stats->purged.initUnsynchronized(0);
    }
    stats->pac_stats.retained = 0;
    stats->pac_stats.pac_mapped.store(0, std::memory_order_relaxed);
    stats->pac_stats.abandoned_vm.store(0, std::memory_order_relaxed);

    central = nullptr;
    emap = emap_;
    base = base_;

    return false;
}

bool PaShard::enableHpa(ThreadState * /*tsdn*/)
{
    return true;
}

void PaShard::disableHpa(ThreadState * /*tsdn*/)
{
    use_hpa.store(false, std::memory_order_relaxed);
}

void PaShard::reset(ThreadState * tsdn)
{
    nactive.store(0, std::memory_order_relaxed);
    flush(tsdn);
}

void PaShard::flush(ThreadState * /*tsdn*/)
{
    JE_ASSERT(!ever_used_hpa);
}

void PaShard::destroy(ThreadState * tsdn)
{
    pac.destroy(tsdn);
    JE_ASSERT(!ever_used_hpa);
}

Extent * PaShard::alloc(
    ThreadState * tsdn, size_t size, size_t alignment, bool slab, szind_t szind, bool zero, bool guarded, bool * deferred_work_generated)
{
    JE_ASSERT(!guarded || alignment <= PAGE);

    /// The HPA is never used (`use_hpa` is always false); allocate from the PAC.
    Extent * edata = pac.alloc(tsdn, size, alignment, zero, guarded, slab, deferred_work_generated);
    if (edata != nullptr)
    {
        JE_ASSERT(edata->size() == size);
        nactiveAdd(size >> LG_PAGE);
        emap->remap(tsdn, edata, szind, slab);
        edata->setSzind(szind);
        edata->setSlab(slab);
        if (slab && (size > 2 * PAGE))
            emap->registerInterior(tsdn, edata, szind);
        JE_ASSERT(edata->arenaInd() == ind);
    }
    return edata;
}

bool PaShard::expand(
    ThreadState * tsdn, Extent * edata, size_t old_size, size_t new_size, szind_t szind, bool zero, bool * deferred_work_generated)
{
    JE_ASSERT(new_size > old_size);
    JE_ASSERT(edata->size() == old_size);
    JE_ASSERT((new_size & PAGE_MASK) == 0);
    if (edata->guarded())
        return true;
    size_t expand_amount = new_size - old_size;

    JE_ASSERT(edata->pai() == EXTENT_PAI_PAC);
    bool error = pac.expand(tsdn, edata, old_size, new_size, zero, deferred_work_generated);
    if (error)
        return true;

    nactiveAdd(expand_amount >> LG_PAGE);
    edata->setSzind(szind);
    emap->remap(tsdn, edata, szind, /* slab */ false);
    return false;
}

bool PaShard::shrink(ThreadState * tsdn, Extent * edata, size_t old_size, size_t new_size, szind_t szind, bool * deferred_work_generated)
{
    JE_ASSERT(new_size < old_size);
    JE_ASSERT(edata->size() == old_size);
    JE_ASSERT((new_size & PAGE_MASK) == 0);
    if (edata->guarded())
        return true;
    size_t shrink_amount = old_size - new_size;

    JE_ASSERT(edata->pai() == EXTENT_PAI_PAC);
    bool error = pac.shrink(tsdn, edata, old_size, new_size, deferred_work_generated);
    if (error)
        return true;
    nactiveSub(shrink_amount >> LG_PAGE);

    edata->setSzind(szind);
    emap->remap(tsdn, edata, szind, /* slab */ false);
    return false;
}

void PaShard::dalloc(ThreadState * tsdn, Extent * edata, bool * deferred_work_generated)
{
    emap->remap(tsdn, edata, SC_NSIZES, /* slab */ false);
    if (edata->slab())
    {
        emap->deregisterInterior(tsdn, edata);
        /// The slab state of the extent isn't cleared. It may be used by the page allocator, e.g. to make caching
        /// decisions.
    }
    edata->setAddr(edata->base());
    edata->setSzind(SC_NSIZES);
    nactiveSub(edata->size() >> LG_PAGE);
    JE_ASSERT(edata->pai() == EXTENT_PAI_PAC);
    pac.dalloc(tsdn, edata, deferred_work_generated);
}

bool PaShard::decayMsSet(ThreadState * tsdn, ExtentState state, ssize_t decay_ms, PacPurgeEagerness eagerness)
{
    return pac.decayMsSet(tsdn, state, decay_ms, eagerness);
}

ssize_t PaShard::decayMsGet(ExtentState state)
{
    return pac.decayMsGet(state);
}

void PaShard::setDeferralAllowed(ThreadState * /*tsdn*/, bool /*deferral_allowed*/)
{
    /// HPA only.
}

void PaShard::doDeferredWork(ThreadState * /*tsdn*/)
{
    /// HPA only.
}

uint64_t PaShard::timeUntilDeferredWork(ThreadState * tsdn)
{
    /// The HPA part is never used.
    return pac.timeUntilDeferredWork(tsdn);
}

/// --- PaShard (pa_extra.c) ------------------------------------------------------------------------------------------

void PaShard::prefork0(ThreadState * tsdn)
{
    pac.decay_dirty.mtx.prefork(tsdn);
    pac.decay_muzzy.mtx.prefork(tsdn);
}

void PaShard::prefork2(ThreadState * /*tsdn*/)
{
    /// HPA only.
}

void PaShard::prefork3(ThreadState * tsdn)
{
    pac.grow_mtx.prefork(tsdn);
}

void PaShard::prefork4(ThreadState * tsdn)
{
    pac.ecache_dirty.prefork(tsdn);
    pac.ecache_muzzy.prefork(tsdn);
    pac.ecache_retained.prefork(tsdn);
}

void PaShard::prefork5(ThreadState * tsdn)
{
    edata_cache.prefork(tsdn);
}

void PaShard::postforkParent(ThreadState * tsdn)
{
    edata_cache.postforkParent(tsdn);
    pac.ecache_dirty.postforkParent(tsdn);
    pac.ecache_muzzy.postforkParent(tsdn);
    pac.ecache_retained.postforkParent(tsdn);
    pac.grow_mtx.postforkParent(tsdn);
    pac.decay_dirty.mtx.postforkParent(tsdn);
    pac.decay_muzzy.mtx.postforkParent(tsdn);
}

void PaShard::postforkChild(ThreadState * tsdn)
{
    edata_cache.postforkChild(tsdn);
    pac.ecache_dirty.postforkChild(tsdn);
    pac.ecache_muzzy.postforkChild(tsdn);
    pac.ecache_retained.postforkChild(tsdn);
    pac.grow_mtx.postforkChild(tsdn);
    pac.decay_dirty.mtx.postforkChild(tsdn);
    pac.decay_muzzy.mtx.postforkChild(tsdn);
}

void PaShard::basicStatsMerge(size_t * nactive_, size_t * ndirty, size_t * nmuzzy) const
{
    *nactive_ += nactiveGet();
    *ndirty += ndirtyGet();
    *nmuzzy += nmuzzyGet();
}

void PaShard::statsMerge(ThreadState * /*tsdn*/, PaShardStats * pa_shard_stats_out, PacExtentStats * estats_out, size_t * resident)
{
    static_assert(config::stats);

    pa_shard_stats_out->pac_stats.retained += pac.ecache_retained.npagesGet() << LG_PAGE;
    pa_shard_stats_out->edata_avail += edata_cache.count();

    size_t resident_pgs = 0;
    resident_pgs += nactiveGet();
    resident_pgs += ndirtyGet();
    *resident += (resident_pgs << LG_PAGE);

    /// Dirty decay stats.
    pa_shard_stats_out->pac_stats.decay_dirty.npurge.incUnsynchronized(pac.stats->decay_dirty.npurge.read());
    pa_shard_stats_out->pac_stats.decay_dirty.nmadvise.incUnsynchronized(pac.stats->decay_dirty.nmadvise.read());
    pa_shard_stats_out->pac_stats.decay_dirty.purged.incUnsynchronized(pac.stats->decay_dirty.purged.read());

    /// Muzzy decay stats.
    pa_shard_stats_out->pac_stats.decay_muzzy.npurge.incUnsynchronized(pac.stats->decay_muzzy.npurge.read());
    pa_shard_stats_out->pac_stats.decay_muzzy.nmadvise.incUnsynchronized(pac.stats->decay_muzzy.nmadvise.read());
    pa_shard_stats_out->pac_stats.decay_muzzy.purged.incUnsynchronized(pac.stats->decay_muzzy.purged.read());

    /// jemalloc: atomic_load_add_store_zu
    size_t abandoned_vm = pac.stats->abandoned_vm.load(std::memory_order_relaxed);
    pa_shard_stats_out->pac_stats.abandoned_vm.store(
        pa_shard_stats_out->pac_stats.abandoned_vm.load(std::memory_order_relaxed) + abandoned_vm, std::memory_order_relaxed);

    for (pszind_t i = 0; i < SC_NPSIZES; ++i)
    {
        estats_out[i].ndirty = pac.ecache_dirty.nextentsGet(i);
        estats_out[i].nmuzzy = pac.ecache_muzzy.nextentsGet(i);
        estats_out[i].nretained = pac.ecache_retained.nextentsGet(i);
        estats_out[i].dirty_bytes = pac.ecache_dirty.nbytesGet(i);
        estats_out[i].muzzy_bytes = pac.ecache_muzzy.nbytesGet(i);
        estats_out[i].retained_bytes = pac.ecache_retained.nbytesGet(i);
    }
}

namespace
{

/// jemalloc: pa_shard_mtx_stats_read_single
void paShardMtxStatsReadSingle(ThreadState * tsdn, MutexProfData * mutex_prof_data, Mutex & mtx, unsigned ind)
{
    mtx.lock(tsdn);
    mtx.profRead(tsdn, mutex_prof_data[ind]);
    mtx.unlock(tsdn);
}

}

void PaShard::mtxStatsRead(ThreadState * tsdn, MutexProfData (&mutex_prof_data)[mutex_prof_num_arena_mutexes])
{
    paShardMtxStatsReadSingle(tsdn, mutex_prof_data, edata_cache.mutex(), arena_prof_mutex_extent_avail);
    paShardMtxStatsReadSingle(tsdn, mutex_prof_data, pac.ecache_dirty.mtx, arena_prof_mutex_extents_dirty);
    paShardMtxStatsReadSingle(tsdn, mutex_prof_data, pac.ecache_muzzy.mtx, arena_prof_mutex_extents_muzzy);
    paShardMtxStatsReadSingle(tsdn, mutex_prof_data, pac.ecache_retained.mtx, arena_prof_mutex_extents_retained);
    paShardMtxStatsReadSingle(tsdn, mutex_prof_data, pac.decay_dirty.mtx, arena_prof_mutex_decay_dirty);
    paShardMtxStatsReadSingle(tsdn, mutex_prof_data, pac.decay_muzzy.mtx, arena_prof_mutex_decay_muzzy);
    /// The HPA/SEC entries are left untouched.
}

}
