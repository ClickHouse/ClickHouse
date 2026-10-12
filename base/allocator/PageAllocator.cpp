#include <allocator/PageAllocator.h>

#include <allocator/Arena.h>
#include <allocator/BackgroundThread.h>
#include <allocator/ExtentOps.h>
#include <allocator/Options.h>

#include <cstring>

namespace jemalloc
{

/// --- PageAllocator (pac.c) -----------------------------------------------------------------------------------------

void PageAllocator::decayDataGet(ExtentState state, Decay ** result_decay, DecayStats ** result_decay_stats, ExtentCache ** r_extent_cache)
{
    switch (state)
    {
        case extent_state_dirty:
            *result_decay = &decay_dirty;
            *result_decay_stats = &stats->decay_dirty;
            *r_extent_cache = &extent_cache_dirty;
            return;
        case extent_state_muzzy:
            *result_decay = &decay_muzzy;
            *result_decay_stats = &stats->decay_muzzy;
            *r_extent_cache = &extent_cache_muzzy;
            return;
        case extent_state_active:
        case extent_state_retained:
        case extent_state_transition:
        case extent_state_merging:
        default: ALLOCATOR_NOT_REACHED();
    }
}

bool PageAllocator::init(
    ThreadState * thread_state,
    Base * base_,
    ExtentMap * extent_map_,
    ExtentPool * extent_pool_,
    const Nanoseconds & current_time,
    size_t page_allocator_oversize_threshold,
    ssize_t dirty_decay_ms,
    ssize_t muzzy_decay_ms,
    PageAllocatorStats * page_allocator_stats,
    Mutex * stats_mutex_)
{
    unsigned idx = base_->idxGet();
    /// Delay coalescing for dirty extents despite the disruptive effect on memory layout for best-fit extent
    /// allocation, since cached extents are likely to be reused soon after deallocation, and the cost of
    /// merging/splitting extents is non-trivial.
    if (extent_cache_dirty.init(thread_state, extent_state_dirty, idx, /* delay_coalesce */ true))
        return true;
    /// Coalesce muzzy extents immediately, because operations on them are in the critical path much less often than
    /// for dirty extents.
    if (extent_cache_muzzy.init(thread_state, extent_state_muzzy, idx, /* delay_coalesce */ false))
        return true;
    /// Coalesce retained extents immediately, in part because they will never be evicted (and therefore there's no
    /// opportunity for delayed coalescing), but also because operations on retained extents are not in the critical
    /// path.
    if (extent_cache_retained.init(thread_state, extent_state_retained, idx, /* delay_coalesce */ false))
        return true;
    exponential_grow.init();
    if (grow_mutex.init("extent_grow", MutexRank::EXTENT_GROW, MutexLockOrder::RankExclusive))
        return true;
    oversize_threshold.store(page_allocator_oversize_threshold, std::memory_order_relaxed);
    if (decay_dirty.init(current_time, dirty_decay_ms))
        return true;
    if (decay_muzzy.init(current_time, muzzy_decay_ms))
        return true;
    if (sanitizer_bump_alloc.init())
        return true;

    base = base_;
    extent_map = extent_map_;
    extent_pool = extent_pool_;
    stats = page_allocator_stats;
    stats_mutex = stats_mutex_;
    extent_serial_number_next.store(0, std::memory_order_relaxed);

    return false;
}

size_t pageAllocatorAllocRetainedBatchedSize(size_t size)
{
    if (size > SIZE_CLASS_LARGE_MAX_CLASS)
    {
        /// A valid input with usize SIZE_CLASS_LARGE_MAX_CLASS could still reach here because of `large_pad`. Such a request
        /// is valid but we should not further increase it. Thus, directly return size for such cases.
        return size;
    }
    size_t batched_size = size_classes::sizeToUsableSizeComputeUsingDelta(size);
    size_t next_huge_page_size = hugePageCeiling(size);
    return batched_size > next_huge_page_size ? next_huge_page_size : batched_size;
}

Extent *
PageAllocator::allocReal(ThreadState * thread_state, ExtentHooks * extent_hooks, size_t size, size_t alignment, bool zero, bool guarded)
{
    ALLOCATOR_ASSERT(!guarded || alignment <= PAGE);
    size_t newly_mapped_size = 0;

    Extent * extent = extentCacheAlloc(thread_state, this, extent_hooks, &extent_cache_dirty, nullptr, size, alignment, zero, guarded);

    if (extent == nullptr && mayHaveMuzzy())
        extent = extentCacheAlloc(thread_state, this, extent_hooks, &extent_cache_muzzy, nullptr, size, alignment, zero, guarded);

    /// We batch-allocate a larger extent with large size classes disabled, because the reuse of extents in the dirty
    /// pool is worse without size classes for large allocations. For instance, with size classes, 1.1MB, 1.15MB, and
    /// 1.2MB allocations are all ceiled to 1.25MB and can reuse the same buffer if they are allocated and deallocated
    /// sequentially; without them, their sequential allocations and deallocations result in three different extents.
    /// Thus, we cache extra mergeable extents in the dirty pool to improve the reuse. This is skipped if both
    /// `maps_coalesce` and `retain` are disabled because VM is not cheap enough in such cases to be used aggressively
    /// and extents cannot be merged at will.
    if (size_classes::largeSizeClassesDisabled() && extent == nullptr && (config::maps_coalesce || options.retain))
    {
        size_t batched_size = pageAllocatorAllocRetainedBatchedSize(size);
        /// Note that `extentCacheAllocGrow` will try to retrieve virtual memory from both the retained pool and directly
        /// from the OS through `extentAllocWrapper` if the retained pool has no qualified extents. This is also why the
        /// overcaching still works even with `retain` off.
        extent = extentCacheAllocGrow(
            thread_state, this, extent_hooks, &extent_cache_retained, nullptr, batched_size, alignment, zero, guarded);

        if (extent != nullptr && batched_size > size)
        {
            Extent * trail
                = extentSplitWrapper(thread_state, this, extent_hooks, extent, size, batched_size - size, /* holding_core_locks */ false);
            if (trail == nullptr)
            {
                extentCacheDeallocate(thread_state, this, extent_hooks, &extent_cache_retained, extent);
                extent = nullptr;
            }
            else
            {
                extentCacheDeallocate(thread_state, this, extent_hooks, &extent_cache_dirty, trail);
            }
        }

        if (extent != nullptr)
            newly_mapped_size = batched_size;
    }

    if (extent == nullptr)
    {
        extent = extentCacheAllocGrow(thread_state, this, extent_hooks, &extent_cache_retained, nullptr, size, alignment, zero, guarded);
        /// jemalloc compatibility: counted even if the allocation failed.
        newly_mapped_size = size;
    }

    /// jemalloc compatibility: the batched size is counted as newly mapped even if it came out of the retained cache,
    /// and `size` is counted even when the final allocation failed.
    if (config::stats && newly_mapped_size != 0)
        stats->page_allocator_mapped.fetch_add(newly_mapped_size, std::memory_order_relaxed);

    return extent;
}

Extent * PageAllocator::allocNewGuarded(
    ThreadState * thread_state, ExtentHooks * extent_hooks, size_t size, [[maybe_unused]] size_t alignment, bool zero, bool frequent_reuse)
{
    ALLOCATOR_ASSERT(alignment <= PAGE);

    Extent * extent;
    if (sanitizerBumpEnabled() && frequent_reuse)
    {
        extent = sanitizer_bump_alloc.alloc(thread_state, this, extent_hooks, size, zero);
    }
    else
    {
        size_t size_with_guards = sanitizerTwoSideGuardedSize(size);
        /// Alloc a non-guarded extent first.
        extent = allocReal(thread_state, extent_hooks, size_with_guards, /* alignment */ PAGE, zero, /* guarded */ false);
        if (extent != nullptr)
        {
            /// Add guards around it.
            ALLOCATOR_ASSERT(extent->size() == size_with_guards);
            sanitizerGuardPagesTwoSided(thread_state, extent_hooks, extent, extent_map, true);
        }
    }
    ALLOCATOR_ASSERT(extent == nullptr || (extent->guarded() && extent->size() == size));

    return extent;
}

Extent * PageAllocator::alloc(
    ThreadState * thread_state,
    size_t size,
    size_t alignment,
    bool zero,
    bool guarded,
    bool frequent_reuse,
    bool * /*deferred_work_generated*/)
{
    ExtentHooks * extent_hooks = extentHooksGet();

    Extent * extent = nullptr;
    /// The condition is an optimization - not frequently reused guarded allocations are never put in the cache.
    /// `allocReal` also doesn't grow retained for guarded allocations. So `allocReal` for such allocations would
    /// always return null.
    if (!guarded || frequent_reuse)
        extent = allocReal(thread_state, extent_hooks, size, alignment, zero, guarded);
    if (extent == nullptr && guarded)
    {
        /// No cached guarded extents; creating a new one.
        extent = allocNewGuarded(thread_state, extent_hooks, size, alignment, zero, frequent_reuse);
    }

    return extent;
}

bool PageAllocator::expand(
    ThreadState * thread_state, Extent * extent, size_t old_size, size_t new_size, bool zero, bool * /*deferred_work_generated*/)
{
    ExtentHooks * extent_hooks = extentHooksGet();

    size_t mapped_add = 0;
    size_t expand_amount = new_size - old_size;

    if (extent_hooks->mergeWillFail())
        return true;
    Extent * trail
        = extentCacheAlloc(thread_state, this, extent_hooks, &extent_cache_dirty, extent, expand_amount, PAGE, zero, /* guarded */ false);
    if (trail == nullptr)
        trail = extentCacheAlloc(
            thread_state, this, extent_hooks, &extent_cache_muzzy, extent, expand_amount, PAGE, zero, /* guarded */ false);
    if (trail == nullptr)
    {
        trail = extentCacheAllocGrow(
            thread_state, this, extent_hooks, &extent_cache_retained, extent, expand_amount, PAGE, zero, /* guarded */ false);
        mapped_add = expand_amount;
    }
    if (trail == nullptr)
        return true;
    if (extentMergeWrapper(thread_state, this, extent_hooks, extent, trail))
    {
        extentDeallocateWrapper(thread_state, this, extent_hooks, trail);
        return true;
    }
    if (config::stats && mapped_add > 0)
        stats->page_allocator_mapped.fetch_add(mapped_add, std::memory_order_relaxed);
    return false;
}

bool PageAllocator::shrink(ThreadState * thread_state, Extent * extent, size_t old_size, size_t new_size, bool * deferred_work_generated)
{
    ExtentHooks * extent_hooks = extentHooksGet();

    size_t shrink_amount = old_size - new_size;

    if (extent_hooks->splitWillFail())
        return true;

    Extent * trail = extentSplitWrapper(thread_state, this, extent_hooks, extent, new_size, shrink_amount, /* holding_core_locks */ false);
    if (trail == nullptr)
        return true;
    extentCacheDeallocate(thread_state, this, extent_hooks, &extent_cache_dirty, trail);
    *deferred_work_generated = true;
    return false;
}

void PageAllocator::deallocate(ThreadState * thread_state, Extent * extent, bool * deferred_work_generated)
{
    ExtentHooks * extent_hooks = extentHooksGet();

    if (extent->guarded())
    {
        /// Because cached guarded extents do exact fit only, large guarded extents are restored on dalloc eagerly
        /// (otherwise they will not be reused efficiently). Slab sizes have a limited number of size classes, and tend
        /// to cycle faster.
        ///
        /// In the case where coalesce is restrained (VirtualFree on Windows), guarded extents are also not cached --
        /// otherwise during arena destroy / reset, the retained extents would not be whole regions (i.e. they are
        /// split between regular and guarded).
        if (!extent->slab() || !config::maps_coalesce)
        {
            ALLOCATOR_ASSERT(extent->size() >= SIZE_CLASS_LARGE_MIN_CLASS || !config::maps_coalesce);
            sanitizerUnguardPagesTwoSided(thread_state, extent_hooks, extent, extent_map);
        }
    }

    extentCacheDeallocate(thread_state, this, extent_hooks, &extent_cache_dirty, extent);
    /// Purging of deallocated pages is deferred.
    *deferred_work_generated = true;
}

namespace
{

/// jemalloc: pac_ns_until_purge
ALLOCATOR_ALWAYS_INLINE uint64_t pageAllocatorNsUntilPurge(ThreadState * thread_state, Decay * decay, size_t num_pages)
{
    if (!decay->mutex.tryLock(thread_state))
    {
        /// Use minimal interval if decay is contended.
        return BACKGROUND_THREAD_DEFERRED_MIN;
    }
    uint64_t result = decay->nsUntilPurge(num_pages, ARENA_DEFERRED_PURGE_NUM_PAGES_THRESHOLD);

    decay->mutex.unlock(thread_state);
    return result;
}

}

uint64_t PageAllocator::timeUntilDeferredWork(ThreadState * thread_state)
{
    uint64_t time = pageAllocatorNsUntilPurge(thread_state, &decay_dirty, extent_cache_dirty.numPagesGet());
    if (time == BACKGROUND_THREAD_DEFERRED_MIN)
        return time;

    uint64_t muzzy = pageAllocatorNsUntilPurge(thread_state, &decay_muzzy, extent_cache_muzzy.numPagesGet());
    if (muzzy < time)
        time = muzzy;
    return time;
}

bool PageAllocator::retainGrowLimitGetSet(ThreadState * thread_state, size_t * old_limit, size_t * new_limit)
{
    PageSizeClassIdx new_idx = 0;
    if (new_limit != nullptr)
    {
        size_t limit = *new_limit;
        /// Grow no more than the new limit.
        if ((new_idx = size_classes::pageSizeToPageSizeClassIdx(limit + 1) - 1) >= SIZE_CLASS_NUM_PAGE_SIZES)
            return true;
    }

    grow_mutex.lock(thread_state);
    if (old_limit != nullptr)
        *old_limit = size_classes::pageSizeClassIdxToSize(exponential_grow.limit);
    if (new_limit != nullptr)
        exponential_grow.limit = new_idx;
    grow_mutex.unlock(thread_state);

    return false;
}

size_t PageAllocator::stashDecayed(
    ThreadState * thread_state, ExtentCache * extent_cache, size_t num_pages_limit, size_t num_pages_decay_max, ExtentListInactive * result)
{
    ExtentHooks * extent_hooks = extentHooksGet();

    /// Stash extents according to `num_pages_limit`.
    size_t num_stashed = 0;
    while (num_stashed < num_pages_decay_max)
    {
        Extent * extent = extentCacheEvict(thread_state, this, extent_hooks, extent_cache, num_pages_limit);
        if (extent == nullptr)
            break;
        result->append(extent);
        num_stashed += extent->size() >> LOG2_PAGE;
    }
    return num_stashed;
}

size_t PageAllocator::decayStashed(
    ThreadState * thread_state,
    Decay * /*decay*/,
    DecayStats * decay_stats,
    ExtentCache * extent_cache,
    bool fully_decay,
    ExtentListInactive * decay_extents)
{
    bool error;

    size_t num_madvises = 0;
    size_t num_unmapped = 0;
    size_t num_purged = 0;

    ExtentHooks * extent_hooks = extentHooksGet();

    bool try_muzzy = !fully_decay && decayMsGet(extent_state_muzzy) != 0;

    bool purge_to_retained = !try_muzzy || extent_cache->state == extent_state_muzzy;
    /// Attempt process_madvise only if 1) enabled, 2) purging to retained, and 3) not using custom hooks.
    /// `opt.process_madvise_max_batch` is always 0 (`JEMALLOC_HAVE_PROCESS_MADVISE` is not configured), so the batch
    /// purge (`decay_with_process_madvise`) is not ported and nothing is ever "already purged".
    bool try_process_madvise = (options.process_madvise_max_batch > 0) && purge_to_retained && extent_hooks->deallocateWillFail();
    ALLOCATOR_ASSERT(!try_process_madvise);
    (void)try_process_madvise;
    bool already_purged = false;

    for (Extent * extent = decay_extents->first(); extent != nullptr; extent = decay_extents->first())
    {
        decay_extents->remove(extent);

        size_t size = extent->size();
        size_t num_pages = size >> LOG2_PAGE;

        ++num_madvises;
        num_purged += num_pages;

        switch (extent_cache->state)
        {
            case extent_state_dirty:
                if (try_muzzy)
                {
                    error = extentPurgeLazyWrapper(thread_state, extent_hooks, extent, /* offset */ 0, size);
                    if (!error)
                    {
                        extentCacheDeallocate(thread_state, this, extent_hooks, &extent_cache_muzzy, extent);
                        break;
                    }
                }
                [[fallthrough]];
            case extent_state_muzzy:
                if (already_purged)
                    extentDeallocateWrapperPurged(thread_state, this, extent_hooks, extent);
                else
                    extentDeallocateWrapper(thread_state, this, extent_hooks, extent);
                num_unmapped += num_pages;
                break;
            case extent_state_active:
            case extent_state_retained:
            case extent_state_transition:
            case extent_state_merging:
            default: ALLOCATOR_NOT_REACHED();
        }
    }

    if constexpr (config::stats)
    {
        decay_stats->num_purge.increment(1);
        decay_stats->num_madvises.increment(num_madvises);
        decay_stats->purged.increment(num_purged);
        stats->page_allocator_mapped.fetch_sub(num_unmapped << LOG2_PAGE, std::memory_order_relaxed);
    }

    return num_purged;
}

void PageAllocator::decayToLimit(
    ThreadState * thread_state,
    Decay * decay,
    DecayStats * decay_stats,
    ExtentCache * extent_cache,
    bool fully_decay,
    size_t num_pages_limit,
    size_t num_pages_decay_max)
{
    if (decay->purging || num_pages_decay_max == 0)
        return;
    decay->purging = true;
    decay->mutex.unlock(thread_state);

    ExtentListInactive decay_extents;
    decay_extents.init();
    size_t num_purge = stashDecayed(thread_state, extent_cache, num_pages_limit, num_pages_decay_max, &decay_extents);
    if (num_purge != 0)
    {
        [[maybe_unused]] size_t num_purged = decayStashed(thread_state, decay, decay_stats, extent_cache, fully_decay, &decay_extents);
        ALLOCATOR_ASSERT(num_purged == num_purge);
    }

    decay->mutex.lock(thread_state);
    decay->purging = false;
}

void PageAllocator::decayAll(
    ThreadState * thread_state, Decay * decay, DecayStats * decay_stats, ExtentCache * extent_cache, bool fully_decay)
{
    decay->mutex.assertOwner(thread_state);
    decayToLimit(thread_state, decay, decay_stats, extent_cache, fully_decay, /* npages_limit */ 0, extent_cache->numPagesGet());
}

void PageAllocator::decayTryPurge(
    ThreadState * thread_state,
    Decay * decay,
    DecayStats * decay_stats,
    ExtentCache * extent_cache,
    size_t current_num_pages,
    size_t num_pages_limit)
{
    if (current_num_pages > num_pages_limit)
        decayToLimit(
            thread_state, decay, decay_stats, extent_cache, /* fully_decay */ false, num_pages_limit, current_num_pages - num_pages_limit);
}

bool PageAllocator::maybeDecayPurge(
    ThreadState * thread_state, Decay * decay, DecayStats * decay_stats, ExtentCache * extent_cache, PageAllocatorPurgeEagerness eagerness)
{
    decay->mutex.assertOwner(thread_state);

    /// Purge all or nothing if the option is disabled.
    ssize_t decay_ms = decay->msRead();
    if (decay_ms <= 0)
    {
        if (decay_ms == 0)
            decayToLimit(
                thread_state, decay, decay_stats, extent_cache, /* fully_decay */ false, /* npages_limit */ 0, extent_cache->numPagesGet());
        return false;
    }

    /// If the deadline has been reached, advance to the current epoch and purge to the new limit if necessary. Note
    /// that dirty pages created during the current epoch are not subject to purge until a future epoch, so as a result
    /// purging only happens during epoch advances, or being triggered by background threads (scheduled event).
    Nanoseconds time;
    time.initUpdate();
    size_t num_pages_current = extent_cache->numPagesGet();
    bool epoch_advanced = decay->maybeAdvanceEpoch(time, num_pages_current);
    if (eagerness == PAGE_ALLOCATOR_PURGE_ALWAYS || (epoch_advanced && eagerness == PAGE_ALLOCATOR_PURGE_ON_EPOCH_ADVANCE))
    {
        size_t num_pages_limit = decay->numPagesLimitGet();
        decayTryPurge(thread_state, decay, decay_stats, extent_cache, num_pages_current, num_pages_limit);
    }

    return epoch_advanced;
}

bool PageAllocator::decayMsSet(ThreadState * thread_state, ExtentState state, ssize_t decay_ms, PageAllocatorPurgeEagerness eagerness)
{
    Decay * decay;
    DecayStats * decay_stats;
    ExtentCache * extent_cache;
    decayDataGet(state, &decay, &decay_stats, &extent_cache);

    if (!Decay::msValid(decay_ms))
        return true;

    decay->mutex.lock(thread_state);
    /// Restart decay backlog from scratch, which may cause many dirty pages to be immediately purged. It would
    /// conceptually be possible to map the old backlog onto the new backlog, but there is no justification for such
    /// complexity since decay_ms changes are intended to be infrequent, either between the {-1, 0, >0} states, or a
    /// one-time arbitrary change during initial arena configuration.
    Nanoseconds current_time;
    current_time.initUpdate();
    decay->reinit(current_time, decay_ms);
    maybeDecayPurge(thread_state, decay, decay_stats, extent_cache, eagerness);
    decay->mutex.unlock(thread_state);

    return false;
}

ssize_t PageAllocator::decayMsGet(ExtentState state)
{
    Decay * decay;
    DecayStats * decay_stats;
    ExtentCache * extent_cache;
    decayDataGet(state, &decay, &decay_stats, &extent_cache);
    return decay->msRead();
}

void PageAllocator::reset(ThreadState * /*tsdn*/)
{
    /// No-op for now; purging is still done at the arena-level. It should get moved in here, though.
}

void PageAllocator::destroy(ThreadState * thread_state)
{
    ALLOCATOR_ASSERT(extent_cache_dirty.numPagesGet() == 0);
    ALLOCATOR_ASSERT(extent_cache_muzzy.numPagesGet() == 0);
    /// Iterate over the retained extents and destroy them. This gives the extent allocator underlying the extent hooks
    /// an opportunity to unmap all retained memory without having to keep its own metadata structures.
    ExtentHooks * extent_hooks = extentHooksGet();
    Extent * extent;
    while ((extent = extentCacheEvict(thread_state, this, extent_hooks, &extent_cache_retained, 0)) != nullptr)
        extentDestroyWrapper(thread_state, this, extent_hooks, extent);
}

/// --- PageAllocatorShard (pa.c) --------------------------------------------------------------------------------------------------

bool PageAllocatorShard::init(
    ThreadState * thread_state,
    ExtentMap * extent_map_,
    Base * base_,
    unsigned idx_,
    PageAllocatorShardStats * stats_,
    Mutex * stats_mutex_,
    const Nanoseconds & current_time,
    size_t page_allocator_oversize_threshold,
    ssize_t dirty_decay_ms,
    ssize_t muzzy_decay_ms)
{
    /// This will change eventually, but for now it should hold.
    ALLOCATOR_ASSERT(base_->idxGet() == idx_);
    if (extent_pool.init(base_))
        return true;

    if (page_allocator.init(
            thread_state,
            base_,
            extent_map_,
            &extent_pool,
            current_time,
            page_allocator_oversize_threshold,
            dirty_decay_ms,
            muzzy_decay_ms,
            &stats_->page_allocator_stats,
            stats_mutex_))
        return true;

    idx = idx_;

    ever_used_huge_page_allocator = false;
    use_huge_page_allocator.store(false, std::memory_order_relaxed);

    num_active.store(0, std::memory_order_relaxed);

    stats_mutex = stats_mutex_;
    stats = stats_;
    /// jemalloc memsets the stats to zero (after `pac_init`, which does not write to them).
    stats->extent_available = 0;
    for (DecayStats * decay_stats : {&stats->page_allocator_stats.decay_dirty, &stats->page_allocator_stats.decay_muzzy})
    {
        decay_stats->num_purge.initUnsynchronized(0);
        decay_stats->num_madvises.initUnsynchronized(0);
        decay_stats->purged.initUnsynchronized(0);
    }
    stats->page_allocator_stats.retained = 0;
    stats->page_allocator_stats.page_allocator_mapped.store(0, std::memory_order_relaxed);
    stats->page_allocator_stats.abandoned_vm.store(0, std::memory_order_relaxed);

    central = nullptr;
    extent_map = extent_map_;
    base = base_;

    return false;
}

bool PageAllocatorShard::enableHugePageAllocator(ThreadState * /*tsdn*/)
{
    return true;
}

void PageAllocatorShard::disableHugePageAllocator(ThreadState * /*tsdn*/)
{
    use_huge_page_allocator.store(false, std::memory_order_relaxed);
}

void PageAllocatorShard::reset(ThreadState * thread_state)
{
    num_active.store(0, std::memory_order_relaxed);
    flush(thread_state);
}

void PageAllocatorShard::flush(ThreadState * /*tsdn*/)
{
    ALLOCATOR_ASSERT(!ever_used_huge_page_allocator);
}

void PageAllocatorShard::destroy(ThreadState * thread_state)
{
    page_allocator.destroy(thread_state);
    ALLOCATOR_ASSERT(!ever_used_huge_page_allocator);
}

Extent * PageAllocatorShard::alloc(
    ThreadState * thread_state,
    size_t size,
    size_t alignment,
    bool slab,
    SizeClassIdx size_class_idx,
    bool zero,
    bool guarded,
    bool * deferred_work_generated)
{
    ALLOCATOR_ASSERT(!guarded || alignment <= PAGE);

    /// The HPA is never used (`use_huge_page_allocator` is always false); allocate from the PAC.
    Extent * extent = page_allocator.alloc(thread_state, size, alignment, zero, guarded, slab, deferred_work_generated);
    if (extent != nullptr)
    {
        ALLOCATOR_ASSERT(extent->size() == size);
        numActiveAdd(size >> LOG2_PAGE);
        extent_map->remap(thread_state, extent, size_class_idx, slab);
        extent->setSizeClassIdx(size_class_idx);
        extent->setSlab(slab);
        if (slab && (size > 2 * PAGE))
            extent_map->registerInterior(thread_state, extent, size_class_idx);
        ALLOCATOR_ASSERT(extent->arenaIdx() == idx);
    }
    return extent;
}

bool PageAllocatorShard::expand(
    ThreadState * thread_state,
    Extent * extent,
    size_t old_size,
    size_t new_size,
    SizeClassIdx size_class_idx,
    bool zero,
    bool * deferred_work_generated)
{
    ALLOCATOR_ASSERT(new_size > old_size);
    ALLOCATOR_ASSERT(extent->size() == old_size);
    ALLOCATOR_ASSERT((new_size & PAGE_MASK) == 0);
    if (extent->guarded())
        return true;
    size_t expand_amount = new_size - old_size;

    ALLOCATOR_ASSERT(extent->allocatorKind() == EXTENT_ALLOCATOR_PAGE_ALLOCATOR);
    bool error = page_allocator.expand(thread_state, extent, old_size, new_size, zero, deferred_work_generated);
    if (error)
        return true;

    numActiveAdd(expand_amount >> LOG2_PAGE);
    extent->setSizeClassIdx(size_class_idx);
    extent_map->remap(thread_state, extent, size_class_idx, /* slab */ false);
    return false;
}

bool PageAllocatorShard::shrink(
    ThreadState * thread_state,
    Extent * extent,
    size_t old_size,
    size_t new_size,
    SizeClassIdx size_class_idx,
    bool * deferred_work_generated)
{
    ALLOCATOR_ASSERT(new_size < old_size);
    ALLOCATOR_ASSERT(extent->size() == old_size);
    ALLOCATOR_ASSERT((new_size & PAGE_MASK) == 0);
    if (extent->guarded())
        return true;
    size_t shrink_amount = old_size - new_size;

    ALLOCATOR_ASSERT(extent->allocatorKind() == EXTENT_ALLOCATOR_PAGE_ALLOCATOR);
    bool error = page_allocator.shrink(thread_state, extent, old_size, new_size, deferred_work_generated);
    if (error)
        return true;
    numActiveSub(shrink_amount >> LOG2_PAGE);

    extent->setSizeClassIdx(size_class_idx);
    extent_map->remap(thread_state, extent, size_class_idx, /* slab */ false);
    return false;
}

void PageAllocatorShard::deallocate(ThreadState * thread_state, Extent * extent, bool * deferred_work_generated)
{
    extent_map->remap(thread_state, extent, SIZE_CLASS_NUM_SIZES, /* slab */ false);
    if (extent->slab())
    {
        extent_map->deregisterInterior(thread_state, extent);
        /// The slab state of the extent isn't cleared. It may be used by the page allocator, e.g. to make caching
        /// decisions.
    }
    extent->setAddr(extent->base());
    extent->setSizeClassIdx(SIZE_CLASS_NUM_SIZES);
    numActiveSub(extent->size() >> LOG2_PAGE);
    ALLOCATOR_ASSERT(extent->allocatorKind() == EXTENT_ALLOCATOR_PAGE_ALLOCATOR);
    page_allocator.deallocate(thread_state, extent, deferred_work_generated);
}

bool PageAllocatorShard::decayMsSet(ThreadState * thread_state, ExtentState state, ssize_t decay_ms, PageAllocatorPurgeEagerness eagerness)
{
    return page_allocator.decayMsSet(thread_state, state, decay_ms, eagerness);
}

ssize_t PageAllocatorShard::decayMsGet(ExtentState state)
{
    return page_allocator.decayMsGet(state);
}

void PageAllocatorShard::setDeferralAllowed(ThreadState * /*tsdn*/, bool /*deferral_allowed*/)
{
    /// HPA only.
}

void PageAllocatorShard::doDeferredWork(ThreadState * /*tsdn*/)
{
    /// HPA only.
}

uint64_t PageAllocatorShard::timeUntilDeferredWork(ThreadState * thread_state)
{
    /// The HPA part is never used.
    return page_allocator.timeUntilDeferredWork(thread_state);
}

/// --- PageAllocatorShard (pa_extra.c) ------------------------------------------------------------------------------------------

void PageAllocatorShard::prefork0(ThreadState * thread_state)
{
    page_allocator.decay_dirty.mutex.prefork(thread_state);
    page_allocator.decay_muzzy.mutex.prefork(thread_state);
}

void PageAllocatorShard::prefork2(ThreadState * /*tsdn*/)
{
    /// HPA only.
}

void PageAllocatorShard::prefork3(ThreadState * thread_state)
{
    page_allocator.grow_mutex.prefork(thread_state);
}

void PageAllocatorShard::prefork4(ThreadState * thread_state)
{
    page_allocator.extent_cache_dirty.prefork(thread_state);
    page_allocator.extent_cache_muzzy.prefork(thread_state);
    page_allocator.extent_cache_retained.prefork(thread_state);
}

void PageAllocatorShard::prefork5(ThreadState * thread_state)
{
    extent_pool.prefork(thread_state);
}

void PageAllocatorShard::postforkParent(ThreadState * thread_state)
{
    extent_pool.postforkParent(thread_state);
    page_allocator.extent_cache_dirty.postforkParent(thread_state);
    page_allocator.extent_cache_muzzy.postforkParent(thread_state);
    page_allocator.extent_cache_retained.postforkParent(thread_state);
    page_allocator.grow_mutex.postforkParent(thread_state);
    page_allocator.decay_dirty.mutex.postforkParent(thread_state);
    page_allocator.decay_muzzy.mutex.postforkParent(thread_state);
}

void PageAllocatorShard::postforkChild(ThreadState * thread_state)
{
    extent_pool.postforkChild(thread_state);
    page_allocator.extent_cache_dirty.postforkChild(thread_state);
    page_allocator.extent_cache_muzzy.postforkChild(thread_state);
    page_allocator.extent_cache_retained.postforkChild(thread_state);
    page_allocator.grow_mutex.postforkChild(thread_state);
    page_allocator.decay_dirty.mutex.postforkChild(thread_state);
    page_allocator.decay_muzzy.mutex.postforkChild(thread_state);
}

void PageAllocatorShard::basicStatsMerge(size_t * num_active_, size_t * num_dirty, size_t * num_muzzy) const
{
    *num_active_ += numActiveGet();
    *num_dirty += numDirtyGet();
    *num_muzzy += numMuzzyGet();
}

void PageAllocatorShard::statsMerge(
    ThreadState * /*tsdn*/,
    PageAllocatorShardStats * page_allocator_shard_stats_out,
    PageAllocatorExtentStats * extent_stats_out,
    size_t * resident)
{
    static_assert(config::stats);

    page_allocator_shard_stats_out->page_allocator_stats.retained += page_allocator.extent_cache_retained.numPagesGet() << LOG2_PAGE;
    page_allocator_shard_stats_out->extent_available += extent_pool.count();

    size_t resident_pages = 0;
    resident_pages += numActiveGet();
    resident_pages += numDirtyGet();
    *resident += (resident_pages << LOG2_PAGE);

    /// Dirty decay stats.
    page_allocator_shard_stats_out->page_allocator_stats.decay_dirty.num_purge.incrementUnsynchronized(
        page_allocator.stats->decay_dirty.num_purge.read());
    page_allocator_shard_stats_out->page_allocator_stats.decay_dirty.num_madvises.incrementUnsynchronized(
        page_allocator.stats->decay_dirty.num_madvises.read());
    page_allocator_shard_stats_out->page_allocator_stats.decay_dirty.purged.incrementUnsynchronized(
        page_allocator.stats->decay_dirty.purged.read());

    /// Muzzy decay stats.
    page_allocator_shard_stats_out->page_allocator_stats.decay_muzzy.num_purge.incrementUnsynchronized(
        page_allocator.stats->decay_muzzy.num_purge.read());
    page_allocator_shard_stats_out->page_allocator_stats.decay_muzzy.num_madvises.incrementUnsynchronized(
        page_allocator.stats->decay_muzzy.num_madvises.read());
    page_allocator_shard_stats_out->page_allocator_stats.decay_muzzy.purged.incrementUnsynchronized(
        page_allocator.stats->decay_muzzy.purged.read());

    /// jemalloc: atomic_load_add_store_zu
    size_t abandoned_vm = page_allocator.stats->abandoned_vm.load(std::memory_order_relaxed);
    page_allocator_shard_stats_out->page_allocator_stats.abandoned_vm.store(
        page_allocator_shard_stats_out->page_allocator_stats.abandoned_vm.load(std::memory_order_relaxed) + abandoned_vm,
        std::memory_order_relaxed);

    for (PageSizeClassIdx i = 0; i < SIZE_CLASS_NUM_PAGE_SIZES; ++i)
    {
        extent_stats_out[i].num_dirty = page_allocator.extent_cache_dirty.numExtentsGet(i);
        extent_stats_out[i].num_muzzy = page_allocator.extent_cache_muzzy.numExtentsGet(i);
        extent_stats_out[i].num_retained = page_allocator.extent_cache_retained.numExtentsGet(i);
        extent_stats_out[i].dirty_bytes = page_allocator.extent_cache_dirty.numBytesGet(i);
        extent_stats_out[i].muzzy_bytes = page_allocator.extent_cache_muzzy.numBytesGet(i);
        extent_stats_out[i].retained_bytes = page_allocator.extent_cache_retained.numBytesGet(i);
    }
}

namespace
{

/// jemalloc: pa_shard_mtx_stats_read_single
void pageAllocatorShardMutexStatsReadSingle(
    ThreadState * thread_state, MutexProfilingData * mutex_profiling_data, Mutex & mutex, unsigned idx)
{
    mutex.lock(thread_state);
    mutex.profilingRead(thread_state, mutex_profiling_data[idx]);
    mutex.unlock(thread_state);
}

}

void PageAllocatorShard::mutexStatsRead(
    ThreadState * thread_state, MutexProfilingData (&mutex_profiling_data)[mutex_profiling_num_arena_mutexes])
{
    pageAllocatorShardMutexStatsReadSingle(
        thread_state, mutex_profiling_data, extent_pool.getMutex(), arena_profiling_mutex_extent_available);
    pageAllocatorShardMutexStatsReadSingle(
        thread_state, mutex_profiling_data, page_allocator.extent_cache_dirty.mutex, arena_profiling_mutex_extents_dirty);
    pageAllocatorShardMutexStatsReadSingle(
        thread_state, mutex_profiling_data, page_allocator.extent_cache_muzzy.mutex, arena_profiling_mutex_extents_muzzy);
    pageAllocatorShardMutexStatsReadSingle(
        thread_state, mutex_profiling_data, page_allocator.extent_cache_retained.mutex, arena_profiling_mutex_extents_retained);
    pageAllocatorShardMutexStatsReadSingle(
        thread_state, mutex_profiling_data, page_allocator.decay_dirty.mutex, arena_profiling_mutex_decay_dirty);
    pageAllocatorShardMutexStatsReadSingle(
        thread_state, mutex_profiling_data, page_allocator.decay_muzzy.mutex, arena_profiling_mutex_decay_muzzy);
    /// The HPA/SEC entries are left untouched.
}

}
