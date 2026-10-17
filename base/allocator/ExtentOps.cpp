#include <allocator/ExtentOps.h>

#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/ExtentCache.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/ExtentPool.h>
#include <allocator/Options.h>
#include <allocator/PageAllocator.h>
#include <allocator/Profiling.h>
#include <allocator/Sanitizer.h>

#include <atomic>

namespace jemalloc
{

namespace
{

/// Used exclusively for gdump triggering.
/// jemalloc: curpages, highpages
constinit std::atomic<size_t> current_pages{0};
constinit std::atomic<size_t> high_pages{0};

/// The result of `extentSplitInterior`.
/// jemalloc: extent_split_interior_result_t
enum class SplitInteriorResult
{
    /// Split successfully. lead, edata, and trail are modified to extents describing the ranges before, in, and after
    /// the given allocation.
    Ok,
    /// The extent can't satisfy the given allocation request. None of the input pointers are touched.
    CantAlloc,
    /// In a potentially invalid state. Must leak (if `*to_leak` is non-null), and salvage what's still salvageable
    /// (if `*to_salvage` is non-null). None of lead, edata, or trail are valid.
    Error,
};

Extent * extentRecycle(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    ExtentCache * extent_cache,
    Extent * expand_extent,
    size_t size,
    size_t alignment,
    bool zero,
    bool * commit,
    bool growing_retained,
    bool guarded);
Extent * extentTryCoalesce(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    ExtentCache * extent_cache,
    Extent * extent,
    bool * coalesced);
Extent * extentAllocRetained(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    Extent * expand_extent,
    size_t size,
    size_t alignment,
    bool zero,
    bool * commit,
    bool guarded);
bool extentCommitImpl(
    ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, size_t offset, size_t length, bool growing_retained);
bool extentDecommitWrapper(ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, size_t offset, size_t length);
bool extentPurgeLazyImpl(
    ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, size_t offset, size_t length, bool growing_retained);
bool extentPurgeForcedImpl(
    ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, size_t offset, size_t length, bool growing_retained);
Extent * extentSplitImpl(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    Extent * extent,
    size_t size_a,
    size_t size_b,
    bool holding_core_locks);
bool extentMergeImpl(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    Extent * a,
    Extent * b,
    bool holding_core_locks);

/// jemalloc: extent_may_force_decay
ALLOCATOR_ALWAYS_INLINE bool extentMayForceDecay(PageAllocator * page_allocator)
{
    return !(page_allocator->decayMsGet(extent_state_dirty) == -1 || page_allocator->decayMsGet(extent_state_muzzy) == -1);
}

/// --- Registration --------------------------------------------------------------------------------------------------

/// jemalloc: extent_gdump_sub
void extentGrowthDumpSub(ThreadState * /*tsdn*/, const Extent * extent)
{
    static_assert(config::profiling);

    if (options.profiling && extent->state() == extent_state_active)
    {
        size_t num_subtract = extent->size() >> LOG2_PAGE;
        ALLOCATOR_ASSERT(current_pages.load(std::memory_order_relaxed) >= num_subtract);
        current_pages.fetch_sub(num_subtract, std::memory_order_relaxed);
    }
}

/// jemalloc: extent_register_impl
bool extentRegisterImpl(ThreadState * thread_state, PageAllocator * page_allocator, Extent * extent, bool growth_dump_add)
{
    ALLOCATOR_ASSERT(extent->state() == extent_state_active);
    /// No locking needed, as the extent must be in active state, which prevents other threads from accessing it.
    if (page_allocator->extent_map->registerBoundary(thread_state, extent, SIZE_CLASS_NUM_SIZES, /* slab */ false))
        return true;

    if (config::profiling && growth_dump_add)
        extentGrowthDumpAdd(thread_state, extent);

    return false;
}

/// jemalloc: extent_register
bool extentRegister(ThreadState * thread_state, PageAllocator * page_allocator, Extent * extent)
{
    return extentRegisterImpl(thread_state, page_allocator, extent, true);
}

/// jemalloc: extent_register_no_gdump_add
bool extentRegisterNoGrowthDumpAdd(ThreadState * thread_state, PageAllocator * page_allocator, Extent * extent)
{
    return extentRegisterImpl(thread_state, page_allocator, extent, false);
}

/// jemalloc: extent_reregister
void extentReregister(ThreadState * thread_state, PageAllocator * page_allocator, Extent * extent)
{
    [[maybe_unused]] bool error = extentRegister(thread_state, page_allocator, extent);
    ALLOCATOR_ASSERT(!error);
}

/// Removes all pointers to the given extent from the global rtree.
/// jemalloc: extent_deregister_impl
void extentDeregisterImpl(ThreadState * thread_state, PageAllocator * page_allocator, Extent * extent, bool growth_dump)
{
    page_allocator->extent_map->deregisterBoundary(thread_state, extent);

    if (config::profiling && growth_dump)
        extentGrowthDumpSub(thread_state, extent);
}

/// jemalloc: extent_deregister
void extentDeregister(ThreadState * thread_state, PageAllocator * page_allocator, Extent * extent)
{
    extentDeregisterImpl(thread_state, page_allocator, extent, true);
}

/// jemalloc: extent_deregister_no_gdump_sub
void extentDeregisterNoGrowthDumpSub(ThreadState * thread_state, PageAllocator * page_allocator, Extent * extent)
{
    extentDeregisterImpl(thread_state, page_allocator, extent, false);
}

/// --- State transitions under the cache lock --------------------------------------------------------------------------

/// jemalloc: extent_deactivate_locked_impl
void extentDeactivateLockedImpl(ThreadState * thread_state, PageAllocator * page_allocator, ExtentCache * extent_cache, Extent * extent)
{
    extent_cache->mutex.assertOwner(thread_state);
    ALLOCATOR_ASSERT(extent->arenaIdx() == extent_cache->idxGet());

    page_allocator->extent_map->updateExtentState(thread_state, extent, extent_cache->state);
    ExtentSet * extent_set = extent->guarded() ? &extent_cache->guarded_extent_set : &extent_cache->extent_set;
    extent_set->insert(extent);
}

/// jemalloc: extent_deactivate_locked
void extentDeactivateLocked(ThreadState * thread_state, PageAllocator * page_allocator, ExtentCache * extent_cache, Extent * extent)
{
    ALLOCATOR_ASSERT(extent->state() == extent_state_active);
    extentDeactivateLockedImpl(thread_state, page_allocator, extent_cache, extent);
}

/// jemalloc: extent_deactivate_check_state_locked
void extentDeactivateCheckStateLocked(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentCache * extent_cache,
    Extent * extent,
    [[maybe_unused]] ExtentState expected_state)
{
    ALLOCATOR_ASSERT(extent->state() == expected_state);
    extentDeactivateLockedImpl(thread_state, page_allocator, extent_cache, extent);
}

/// jemalloc: extent_activate_locked
void extentActivateLocked(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    [[maybe_unused]] ExtentCache * extent_cache,
    ExtentSet * extent_set,
    Extent * extent)
{
    ALLOCATOR_ASSERT(extent->arenaIdx() == extent_cache->idxGet());
    ALLOCATOR_ASSERT(extent->state() == extent_cache->state || extent->state() == extent_state_merging);

    extent_set->remove(extent);
    page_allocator->extent_map->updateExtentState(thread_state, extent, extent_state_active);
}

/// jemalloc: extent_try_delayed_coalesce
bool extentTryDelayedCoalesce(
    ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, ExtentCache * extent_cache, Extent * extent)
{
    page_allocator->extent_map->updateExtentState(thread_state, extent, extent_state_active);

    bool coalesced;
    extent = extentTryCoalesce(thread_state, page_allocator, extent_hooks, extent_cache, extent, &coalesced);
    page_allocator->extent_map->updateExtentState(thread_state, extent, extent_cache->state);

    if (!coalesced)
        return true;
    /// NOTE: the merged extent goes to the LRU tail (not "at its neighbor's position" as jemalloc's comment says).
    extent_cache->extent_set.insert(extent);
    return false;
}

/// This can only happen when we fail to allocate a new extent struct (which indicates OOM), e.g. when trying to split
/// an existing extent.
/// jemalloc: extents_abandon_vm
void extentsAbandonVM(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    ExtentCache * extent_cache,
    Extent * extent,
    bool growing_retained)
{
    size_t size = extent->size();
    if constexpr (config::stats)
        page_allocator->stats->abandoned_vm.fetch_add(size, std::memory_order_relaxed);
    /// Leak the extent after making sure its pages have already been purged, so that this is only a virtual memory
    /// leak.
    if (extent_cache->state == extent_state_dirty)
    {
        if (extentPurgeLazyImpl(thread_state, extent_hooks, extent, 0, size, growing_retained))
            extentPurgeForcedImpl(thread_state, extent_hooks, extent, 0, extent->size(), growing_retained);
    }
    page_allocator->extent_pool->put(thread_state, extent);
}

/// --- Recycling -----------------------------------------------------------------------------------------------------

/// Tries to find and remove an extent from `extent_cache` that can be used for the given allocation request.
/// jemalloc: extent_recycle_extract
Extent * extentRecycleExtract(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * /*ehooks*/,
    ExtentCache * extent_cache,
    Extent * expand_extent,
    size_t size,
    size_t alignment,
    bool guarded)
{
    extent_cache->mutex.assertOwner(thread_state);
    ALLOCATOR_ASSERT(alignment > 0);
    if constexpr (config::debug)
    {
        if (expand_extent != nullptr)
        {
            /// Non-null `expand_extent` indicates in-place expanding realloc. `new_addr` must either refer to a
            /// non-existing extent, or to the base of an extant extent, since only active slabs support interior
            /// lookups (which of course cannot be recycled).
            [[maybe_unused]] void * new_addr = expand_extent->past();
            ALLOCATOR_ASSERT(pageAddrToBase(new_addr) == new_addr);
            ALLOCATOR_ASSERT(alignment <= PAGE);
        }
    }

    Extent * extent;
    ExtentSet * extent_set = guarded ? &extent_cache->guarded_extent_set : &extent_cache->extent_set;
    if (expand_extent != nullptr)
    {
        extent = page_allocator->extent_map->tryAcquireExtentNeighborExpand(
            thread_state, expand_extent, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_cache->state);
        if (extent != nullptr)
        {
            extentAssertCanExpand(expand_extent, extent);
            if (extent->size() < size)
            {
                page_allocator->extent_map->releaseExtent(thread_state, extent, extent_cache->state);
                extent = nullptr;
            }
        }
    }
    else
    {
        /// A large extent might be broken up from its original size to some small size to satisfy a small request.
        /// When that small request is freed, though, it won't merge back with the larger extent if delayed coalescing
        /// is on. The large extent can then no longer satisfy a request for its original size. To limit this effect,
        /// when delayed coalescing is enabled, we put a cap on how big an extent we can split for a request.
        unsigned log2_max_fit = extent_cache->delay_coalesce ? unsigned(options.log2_extent_max_active_fit) : SIZE_CLASS_PTR_BITS;

        /// If split and merge are not allowed (Windows w/o retain), try exact fit only. For simplicity purposes,
        /// splitting guarded extents is not supported. Hence, we do only exact fit for guarded allocations.
        bool exact_only = (!config::maps_coalesce && !options.retain) || guarded;
        extent = extent_set->fit(size, alignment, exact_only, log2_max_fit);
    }
    if (extent == nullptr)
        return nullptr;
    ALLOCATOR_ASSERT(!guarded || extent->guarded());
    extentActivateLocked(thread_state, page_allocator, extent_cache, extent_set, extent);

    return extent;
}

/// Given an allocation request and an extent guaranteed to be able to satisfy it, this splits off lead and trail
/// extents, leaving `*extent` pointing to an extent satisfying the allocation. This function doesn't put lead or trail
/// into any cache; it's the caller's job to ensure that they can be reused.
/// jemalloc: extent_split_interior
SplitInteriorResult extentSplitInterior(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    /// The result of splitting, in case of success.
    Extent ** extent,
    Extent ** lead,
    Extent ** trail,
    /// The mess to clean up, in case of error.
    Extent ** to_leak,
    Extent ** to_salvage,
    [[maybe_unused]] Extent * expand_extent,
    size_t size,
    size_t alignment)
{
    size_t lead_size = alignmentCeiling(reinterpret_cast<uintptr_t>((*extent)->base()), pageCeiling(alignment))
        - reinterpret_cast<uintptr_t>((*extent)->base());
    ALLOCATOR_ASSERT(expand_extent == nullptr || lead_size == 0);
    if ((*extent)->size() < lead_size + size)
        return SplitInteriorResult::CantAlloc;
    size_t trail_size = (*extent)->size() - lead_size - size;

    *lead = nullptr;
    *trail = nullptr;
    *to_leak = nullptr;
    *to_salvage = nullptr;

    /// Split the lead.
    if (lead_size != 0)
    {
        ALLOCATOR_ASSERT(!(*extent)->guarded());
        *lead = *extent;
        *extent = extentSplitImpl(
            thread_state, page_allocator, extent_hooks, *lead, lead_size, size + trail_size, /* holding_core_locks */ true);
        if (*extent == nullptr)
        {
            *to_leak = *lead;
            *lead = nullptr;
            return SplitInteriorResult::Error;
        }
    }

    /// Split the trail.
    if (trail_size != 0)
    {
        ALLOCATOR_ASSERT(!(*extent)->guarded());
        *trail = extentSplitImpl(thread_state, page_allocator, extent_hooks, *extent, size, trail_size, /* holding_core_locks */ true);
        if (*trail == nullptr)
        {
            *to_leak = *extent;
            *to_salvage = *lead;
            *lead = nullptr;
            *extent = nullptr;
            return SplitInteriorResult::Error;
        }
    }

    return SplitInteriorResult::Ok;
}

/// This fulfills the indicated allocation request out of the given extent (which the caller should have ensured was
/// big enough). If there's any unused space before or after the resulting allocation, that space is given its own
/// extent and put back into `extent_cache`.
/// jemalloc: extent_recycle_split
Extent * extentRecycleSplit(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    ExtentCache * extent_cache,
    Extent * expand_extent,
    size_t size,
    size_t alignment,
    Extent * extent,
    bool growing_retained)
{
    ALLOCATOR_ASSERT(!extent->guarded() || size == extent->size());
    extent_cache->mutex.assertOwner(thread_state);

    Extent * lead;
    Extent * trail;
    Extent * to_leak = nullptr;
    Extent * to_salvage = nullptr;

    SplitInteriorResult result = extentSplitInterior(
        thread_state, page_allocator, extent_hooks, &extent, &lead, &trail, &to_leak, &to_salvage, expand_extent, size, alignment);

    if (!config::maps_coalesce && result != SplitInteriorResult::Ok && !options.retain)
    {
        /// Split isn't supported (implies Windows w/o retain). Avoid leaking the extent.
        ALLOCATOR_ASSERT(to_leak != nullptr && lead == nullptr && trail == nullptr);
        extentDeactivateLocked(thread_state, page_allocator, extent_cache, to_leak);
        return nullptr;
    }

    if (result == SplitInteriorResult::Ok)
    {
        if (lead != nullptr)
            extentDeactivateLocked(thread_state, page_allocator, extent_cache, lead);
        if (trail != nullptr)
            extentDeactivateLocked(thread_state, page_allocator, extent_cache, trail);
        return extent;
    }
    else
    {
        /// We should have picked an extent that was large enough to fulfill our allocation request.
        ALLOCATOR_ASSERT(result == SplitInteriorResult::Error);
        if (to_salvage != nullptr)
            extentDeregister(thread_state, page_allocator, to_salvage);
        if (to_leak != nullptr)
        {
            extentDeregisterNoGrowthDumpSub(thread_state, page_allocator, to_leak);
            /// May go down the purge path (which assumes no cache locks). Only happens with OOM caused split failures.
            extent_cache->mutex.unlock(thread_state);
            extentsAbandonVM(thread_state, page_allocator, extent_hooks, extent_cache, to_leak, growing_retained);
            extent_cache->mutex.lock(thread_state);
        }
        return nullptr;
    }
}

/// Tries to satisfy the given allocation request by reusing one of the extents in the given cache.
/// jemalloc: extent_recycle
Extent * extentRecycle(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    ExtentCache * extent_cache,
    Extent * expand_extent,
    size_t size,
    size_t alignment,
    bool zero,
    bool * commit,
    bool growing_retained,
    bool guarded)
{
    ALLOCATOR_ASSERT(!guarded || expand_extent == nullptr);
    ALLOCATOR_ASSERT(!guarded || alignment <= PAGE);

    extent_cache->mutex.lock(thread_state);

    Extent * extent
        = extentRecycleExtract(thread_state, page_allocator, extent_hooks, extent_cache, expand_extent, size, alignment, guarded);
    if (extent == nullptr)
    {
        extent_cache->mutex.unlock(thread_state);
        return nullptr;
    }

    extent = extentRecycleSplit(
        thread_state, page_allocator, extent_hooks, extent_cache, expand_extent, size, alignment, extent, growing_retained);
    extent_cache->mutex.unlock(thread_state);
    if (extent == nullptr)
        return nullptr;

    ALLOCATOR_ASSERT(extent->state() == extent_state_active);
    if (extentCommitZero(thread_state, extent_hooks, extent, *commit, zero, growing_retained))
    {
        extentRecord(thread_state, page_allocator, extent_hooks, extent_cache, extent);
        return nullptr;
    }
    if (extent->committed())
    {
        /// This reverses the purpose of this variable - previously it was treated as an input parameter, now it turns
        /// into an output parameter, reporting if the extent has actually been committed.
        *commit = true;
    }
    return extent;
}

/// If virtual memory is retained, create increasingly larger extents from which to split requested extents in order
/// to limit the total number of disjoint virtual memory ranges retained by each shard. `page_allocator->grow_mutex` is held on
/// entry and always released.
/// jemalloc: extent_grow_retained
Extent * extentGrowRetained(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    size_t size,
    size_t alignment,
    bool zero,
    bool * commit)
{
    page_allocator->grow_mutex.assertOwner(thread_state);

    size_t alloc_size_min = size + pageCeiling(alignment) - PAGE;
    size_t alloc_size;
    PageSizeClassIdx exponential_grow_skip;
    Extent * extent;
    void * ptr;
    bool zeroed;
    bool committed;
    Extent * lead;
    Extent * trail;
    Extent * to_leak = nullptr;
    Extent * to_salvage = nullptr;
    SplitInteriorResult result;

    /// Beware size_t wrap-around.
    if (alloc_size_min < size)
        goto label_error;
    /// Find the next extent size in the series that would be large enough to satisfy this request.
    if (page_allocator->exponential_grow.sizePrepare(alloc_size_min, &alloc_size, &exponential_grow_skip))
        goto label_error;

    extent = page_allocator->extent_pool->get(thread_state);
    if (extent == nullptr)
        goto label_error;
    zeroed = false;
    committed = false;

    ptr = extent_hooks->alloc(thread_state, nullptr, alloc_size, PAGE, &zeroed, &committed);

    if (ptr == nullptr)
    {
        page_allocator->extent_pool->put(thread_state, extent);
        goto label_error;
    }

    extent->init(
        page_allocator->extent_cache_retained.idxGet(),
        ptr,
        alloc_size,
        false,
        SIZE_CLASS_NUM_SIZES,
        extentSerialNumberNext(page_allocator),
        extent_state_active,
        zeroed,
        committed,
        EXTENT_ALLOCATOR_PAGE_ALLOCATOR,
        EXTENT_IS_HEAD);

    if (extentRegisterNoGrowthDumpAdd(thread_state, page_allocator, extent))
    {
        page_allocator->extent_pool->put(thread_state, extent);
        goto label_error;
    }

    if (extent->committed())
        *commit = true;

    result = extentSplitInterior(
        thread_state, page_allocator, extent_hooks, &extent, &lead, &trail, &to_leak, &to_salvage, nullptr, size, alignment);

    if (result == SplitInteriorResult::Ok)
    {
        if (lead != nullptr)
            extentRecord(thread_state, page_allocator, extent_hooks, &page_allocator->extent_cache_retained, lead);
        if (trail != nullptr)
            extentRecord(thread_state, page_allocator, extent_hooks, &page_allocator->extent_cache_retained, trail);
    }
    else
    {
        /// We should have allocated a sufficiently large extent; the cant_alloc case should not occur.
        ALLOCATOR_ASSERT(result == SplitInteriorResult::Error);
        if (to_salvage != nullptr)
        {
            if constexpr (config::profiling)
                extentGrowthDumpAdd(thread_state, to_salvage);
            extentRecord(thread_state, page_allocator, extent_hooks, &page_allocator->extent_cache_retained, to_salvage);
        }
        if (to_leak != nullptr)
        {
            extentDeregisterNoGrowthDumpSub(thread_state, page_allocator, to_leak);
            extentsAbandonVM(thread_state, page_allocator, extent_hooks, &page_allocator->extent_cache_retained, to_leak, true);
        }
        goto label_error;
    }

    if (*commit && !extent->committed())
    {
        if (extentCommitImpl(thread_state, extent_hooks, extent, 0, extent->size(), true))
        {
            extentRecord(thread_state, page_allocator, extent_hooks, &page_allocator->extent_cache_retained, extent);
            goto label_error;
        }
        /// A successful commit should return zeroed memory.
        if constexpr (config::debug)
        {
            const size_t * p = static_cast<const size_t *>(extent->addr());
            /// Check the first page only.
            for (size_t i = 0; i < PAGE / sizeof(size_t); ++i)
                ALLOCATOR_ASSERT(p[i] == 0);
        }
    }

    /// Increment the grow index if doing so wouldn't exceed the allowed range. All opportunities for failure are past.
    page_allocator->exponential_grow.sizeCommit(exponential_grow_skip);
    page_allocator->grow_mutex.unlock(thread_state);

    /// The THP handling of the huge arena (`huge_arena_pac_thp.thp_madvise`, `extent_handle_huge_arena_thp`) is not
    /// ported: it requires `opt.huge_arena_pac_thp` and `metadata_transparent_huge_pages`, both off by default (dead for ClickHouse).

    if constexpr (config::profiling)
    {
        /// Adjust gdump stats now that the extent is final size.
        extentGrowthDumpAdd(thread_state, extent);
    }
    if (zero && !extent->zeroed())
        extent_hooks->zero(thread_state, extent->base(), extent->size());
    return extent;

label_error:
    page_allocator->grow_mutex.unlock(thread_state);
    return nullptr;
}

/// jemalloc: extent_alloc_retained
Extent * extentAllocRetained(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    Extent * expand_extent,
    size_t size,
    size_t alignment,
    bool zero,
    bool * commit,
    bool guarded)
{
    ALLOCATOR_ASSERT(size != 0);
    ALLOCATOR_ASSERT(alignment != 0);

    page_allocator->grow_mutex.lock(thread_state);

    Extent * extent = extentRecycle(
        thread_state,
        page_allocator,
        extent_hooks,
        &page_allocator->extent_cache_retained,
        expand_extent,
        size,
        alignment,
        zero,
        commit,
        /* growing_retained */ true,
        guarded);
    if (extent != nullptr)
    {
        page_allocator->grow_mutex.unlock(thread_state);
        if constexpr (config::profiling)
            extentGrowthDumpAdd(thread_state, extent);
    }
    else if (options.retain && expand_extent == nullptr && !guarded)
    {
        /// `extentGrowRetained` always releases `page_allocator->grow_mutex`.
        extent = extentGrowRetained(thread_state, page_allocator, extent_hooks, size, alignment, zero, commit);
    }
    else
    {
        page_allocator->grow_mutex.unlock(thread_state);
    }
    page_allocator->grow_mutex.assertNotOwner(thread_state);

    return extent;
}

/// --- Coalescing ----------------------------------------------------------------------------------------------------

/// jemalloc: extent_coalesce
bool extentCoalesce(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    ExtentCache * extent_cache,
    Extent * inner,
    Extent * outer,
    bool forward)
{
    extentAssertCanCoalesce(inner, outer);
    extent_cache->extent_set.remove(outer);

    bool error = extentMergeImpl(
        thread_state, page_allocator, extent_hooks, forward ? inner : outer, forward ? outer : inner, /* holding_core_locks */ true);
    if (error)
        extentDeactivateCheckStateLocked(thread_state, page_allocator, extent_cache, outer, extent_state_merging);

    return error;
}

/// jemalloc: extent_try_coalesce_impl
Extent * extentTryCoalesceImpl(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    ExtentCache * extent_cache,
    Extent * extent,
    size_t max_size,
    bool * coalesced)
{
    ALLOCATOR_ASSERT(!extent->guarded());
    ALLOCATOR_ASSERT(coalesced != nullptr);
    *coalesced = false;
    /// We avoid checking / locking inactive neighbors for large size classes, since they are eagerly coalesced on
    /// deallocation which can cause lock contention.
    ///
    /// Continue attempting to coalesce until failure, to protect against races with other threads that are thwarted
    /// by this one.
    bool again;
    do
    {
        again = false;

        /// Try to coalesce forward.
        Extent * next = page_allocator->extent_map->tryAcquireExtentNeighbor(
            thread_state, extent, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_cache->state, /* forward */ true);
        size_t max_next_neighbor = max_size > extent->size() ? max_size - extent->size() : 0;
        /// jemalloc compatibility: (fork patch 3c14707b) a neighbor that was acquired (its state is now `merging` in
        /// the extent and in the rtree) but is rejected by the size limit is NOT released: it stays in its set in the
        /// `merging` state, invisible to further coalescing and to expand-acquire, until it is extracted by `fit`
        /// (`extentActivateLocked` accepts `merging`) or evicted. Only reachable from the large-dirty path of
        /// `extentRecord`; all other callers pass `SIZE_CLASS_LARGE_MAX_CLASS`.
        if (next != nullptr && next->size() <= max_next_neighbor)
        {
            if (!extentCoalesce(thread_state, page_allocator, extent_hooks, extent_cache, extent, next, true))
            {
                if (extent_cache->delay_coalesce)
                {
                    /// Do minimal coalescing.
                    *coalesced = true;
                    return extent;
                }
                again = true;
            }
        }

        /// Try to coalesce backward.
        Extent * prev = page_allocator->extent_map->tryAcquireExtentNeighbor(
            thread_state, extent, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_cache->state, /* forward */ false);
        size_t max_prev_neighbor = max_size > extent->size() ? max_size - extent->size() : 0;
        /// jemalloc compatibility: the same 3c14707b quirk for the backward neighbor.
        if (prev != nullptr && prev->size() <= max_prev_neighbor)
        {
            if (!extentCoalesce(thread_state, page_allocator, extent_hooks, extent_cache, extent, prev, false))
            {
                extent = prev;
                if (extent_cache->delay_coalesce)
                {
                    /// Do minimal coalescing.
                    *coalesced = true;
                    return extent;
                }
                again = true;
            }
        }
    } while (again);

    if (extent_cache->delay_coalesce)
        *coalesced = false;
    return extent;
}

/// jemalloc: extent_try_coalesce
Extent * extentTryCoalesce(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    ExtentCache * extent_cache,
    Extent * extent,
    bool * coalesced)
{
    return extentTryCoalesceImpl(thread_state, page_allocator, extent_hooks, extent_cache, extent, SIZE_CLASS_LARGE_MAX_CLASS, coalesced);
}

/// jemalloc: extent_try_coalesce_large
Extent * extentTryCoalesceLarge(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    ExtentCache * extent_cache,
    Extent * extent,
    size_t max_size,
    bool * coalesced)
{
    return extentTryCoalesceImpl(thread_state, page_allocator, extent_hooks, extent_cache, extent, max_size, coalesced);
}

/// Purge a single extent to retained / unmapped directly.
/// jemalloc: extent_maximally_purge
void extentMaximallyPurge(ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, Extent * extent)
{
    size_t extent_size = extent->size();
    extentDeallocateWrapper(thread_state, page_allocator, extent_hooks, extent);
    if constexpr (config::stats)
    {
        /// Update stats accordingly (`stats_mutex` is null: the counters are atomic).
        page_allocator->stats->decay_dirty.num_madvises.increment(1);
        page_allocator->stats->decay_dirty.purged.increment(extent_size >> LOG2_PAGE);
        page_allocator->stats->page_allocator_mapped.fetch_sub(extent_size, std::memory_order_relaxed);
    }
}

/// --- OS-level operations -------------------------------------------------------------------------------------------

/// jemalloc: extent_dalloc_wrapper_try
bool extentDeallocateWrapperTry(ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, Extent * extent)
{
    ALLOCATOR_ASSERT(extent->base() != nullptr);
    ALLOCATOR_ASSERT(extent->size() != 0);

    extent->setAddr(extent->base());

    /// Try to deallocate.
    bool error = extent_hooks->deallocate(thread_state, extent->base(), extent->size(), extent->committed());

    if (!error)
        page_allocator->extent_pool->put(thread_state, extent);

    return error;
}

/// jemalloc: extent_dalloc_wrapper_finish
void extentDeallocateWrapperFinish(ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, Extent * extent)
{
    if constexpr (config::profiling)
        extentGrowthDumpSub(thread_state, extent);
    extentRecord(thread_state, page_allocator, extent_hooks, &page_allocator->extent_cache_retained, extent);
}

/// jemalloc: extent_commit_impl
bool extentCommitImpl(
    ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, size_t offset, size_t length, bool /*growing_retained*/)
{
    bool error = extent_hooks->commit(thread_state, extent->base(), extent->size(), offset, length);
    extent->setCommitted(extent->committed() || !error);
    return error;
}

/// jemalloc: extent_decommit_wrapper
bool extentDecommitWrapper(ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, size_t offset, size_t length)
{
    bool error = extent_hooks->decommit(thread_state, extent->base(), extent->size(), offset, length);
    extent->setCommitted(extent->committed() && error);
    return error;
}

/// jemalloc: extent_purge_lazy_impl
bool extentPurgeLazyImpl(
    ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, size_t offset, size_t length, bool /*growing_retained*/)
{
    return extent_hooks->purgeLazy(thread_state, extent->base(), extent->size(), offset, length);
}

/// jemalloc: extent_purge_forced_impl
bool extentPurgeForcedImpl(
    ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, size_t offset, size_t length, bool /*growing_retained*/)
{
    return extent_hooks->purgeForced(thread_state, extent->base(), extent->size(), offset, length);
}

/// Accepts the extent to split, and the characteristics of each side of the split. The 'a' parameters go with the
/// lead of the resulting pair of extents (the lower addressed portion of the split), and the 'b' parameters go with
/// the trail (the higher addressed portion). This makes `extent` the lead, and returns the trail (except in case of
/// error).
/// jemalloc: extent_split_impl
Extent * extentSplitImpl(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    Extent * extent,
    size_t size_a,
    size_t size_b,
    bool /*holding_core_locks*/)
{
    ALLOCATOR_ASSERT(extent->size() == size_a + size_b);

    if (extent_hooks->splitWillFail())
        return nullptr;

    Extent * trail = page_allocator->extent_pool->get(thread_state);
    if (trail == nullptr)
        return nullptr;

    trail->init(
        extent->arenaIdx(),
        static_cast<std::byte *>(extent->base()) + size_a,
        size_b,
        /* slab */ false,
        SIZE_CLASS_NUM_SIZES,
        extent->serialNumber(),
        extent->state(),
        extent->zeroed(),
        extent->committed(),
        EXTENT_ALLOCATOR_PAGE_ALLOCATOR,
        EXTENT_NOT_HEAD);
    ExtentMapPrepare prepare;
    bool error = page_allocator->extent_map->splitPrepare(thread_state, &prepare, extent, size_a, trail, size_b);
    if (error)
    {
        page_allocator->extent_pool->put(thread_state, trail);
        return nullptr;
    }

    /// No need to acquire trail or edata, because: 1) trail was new (just allocated); and 2) edata is either an active
    /// allocation (the shrink path), or in an acquired state (extracted from the cache on the recycle-split path).
    ALLOCATOR_ASSERT(page_allocator->extent_map->extentIsAcquired(thread_state, extent) || !config::debug);
    ALLOCATOR_ASSERT(page_allocator->extent_map->extentIsAcquired(thread_state, trail) || !config::debug);

    error = extent_hooks->split(thread_state, extent->base(), size_a + size_b, size_a, size_b, extent->committed());

    if (error)
    {
        page_allocator->extent_pool->put(thread_state, trail);
        return nullptr;
    }

    extent->setSize(size_a);
    page_allocator->extent_map->splitCommit(thread_state, &prepare, extent, size_a, trail, size_b);

    return trail;
}

/// jemalloc: extent_merge_impl
bool extentMergeImpl(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    Extent * a,
    Extent * b,
    bool /*holding_core_locks*/)
{
    ALLOCATOR_ASSERT(a->base() < b->base());
    ALLOCATOR_ASSERT(a->arenaIdx() == b->arenaIdx());
    ALLOCATOR_ASSERT(a->arenaIdx() == extent_hooks->idxGet());
    page_allocator->extent_map->assertMapped(thread_state, a);
    page_allocator->extent_map->assertMapped(thread_state, b);
    /// The `config_debug` check of `ehooks_default_merge_impl` (head states via the emap): the higher extent must not
    /// be a head extent (`ExtentHooks::merge` does not port it).
    ALLOCATOR_ASSERT(extentNeighborHeadStateMergeable(a->isHead(), b->isHead(), /* forward */ true));

    bool error = extent_hooks->merge(thread_state, a->base(), a->size(), b->base(), b->size(), a->committed());

    if (error)
        return true;

    /// The rtree writes must happen while all the relevant elements are owned, so the following code uses decomposed
    /// helper functions rather than register/deregister to do things in the right order.
    ExtentMapPrepare prepare;
    page_allocator->extent_map->mergePrepare(thread_state, &prepare, a, b);

    ALLOCATOR_ASSERT(a->state() == extent_state_active || a->state() == extent_state_merging);
    a->setState(extent_state_active);
    a->setSize(a->size() + b->size());
    a->setSerialNumber((a->serialNumber() < b->serialNumber()) ? a->serialNumber() : b->serialNumber());
    a->setZeroed(a->zeroed() && b->zeroed());

    page_allocator->extent_map->mergeCommit(thread_state, &prepare, a, b);

    page_allocator->extent_pool->put(thread_state, b);

    return false;
}

}

/// --- Public functions ----------------------------------------------------------------------------------------------

size_t extentSerialNumberNext(PageAllocator * page_allocator)
{
    return page_allocator->extent_serial_number_next.fetch_add(1, std::memory_order_relaxed);
}

Extent * extentCacheAlloc(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    ExtentCache * extent_cache,
    Extent * expand_extent,
    size_t size,
    size_t alignment,
    bool zero,
    bool guarded)
{
    ALLOCATOR_ASSERT(size != 0);
    ALLOCATOR_ASSERT(alignment != 0);

    bool commit = true;
    Extent * extent = extentRecycle(
        thread_state, page_allocator, extent_hooks, extent_cache, expand_extent, size, alignment, zero, &commit, false, guarded);
    ALLOCATOR_ASSERT(extent == nullptr || extent->allocatorKind() == EXTENT_ALLOCATOR_PAGE_ALLOCATOR);
    ALLOCATOR_ASSERT(extent == nullptr || extent->guarded() == guarded);
    return extent;
}

Extent * extentCacheAllocGrow(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    ExtentCache * /*ecache*/,
    Extent * expand_extent,
    size_t size,
    size_t alignment,
    bool zero,
    bool guarded)
{
    ALLOCATOR_ASSERT(size != 0);
    ALLOCATOR_ASSERT(alignment != 0);

    bool commit = true;
    Extent * extent
        = extentAllocRetained(thread_state, page_allocator, extent_hooks, expand_extent, size, alignment, zero, &commit, guarded);
    if (extent == nullptr)
    {
        if (options.retain && expand_extent != nullptr)
        {
            /// When retain is enabled and trying to expand, we do not attempt `extentAllocWrapper` which does mmap
            /// that is very unlikely to succeed (unless it happens to be at the end).
            return nullptr;
        }
        if (guarded)
        {
            /// Means no cached guarded extents available (and no grow_retained was attempted). The `pac_alloc` flow
            /// will alloc regular extents to make new guarded ones.
            return nullptr;
        }
        void * new_addr = (expand_extent == nullptr) ? nullptr : expand_extent->past();
        extent = extentAllocWrapper(
            thread_state, page_allocator, extent_hooks, new_addr, size, alignment, zero, &commit, /* growing_retained */ false);
    }

    ALLOCATOR_ASSERT(extent == nullptr || extent->allocatorKind() == EXTENT_ALLOCATOR_PAGE_ALLOCATOR);
    return extent;
}

void extentCacheDeallocate(
    ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, ExtentCache * extent_cache, Extent * extent)
{
    ALLOCATOR_ASSERT(extent->base() != nullptr);
    ALLOCATOR_ASSERT(extent->size() != 0);
    ALLOCATOR_ASSERT(extent->allocatorKind() == EXTENT_ALLOCATOR_PAGE_ALLOCATOR);

    extent->setAddr(extent->base());
    extent->setZeroed(false);

    extentRecord(thread_state, page_allocator, extent_hooks, extent_cache, extent);
}

Extent * extentCacheEvict(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    ExtentCache * extent_cache,
    size_t num_pages_min)
{
    extent_cache->mutex.lock(thread_state);

    /// Get the LRU coalesced extent, if any. If coalescing was delayed, the loop will iterate until the LRU extent is
    /// fully coalesced.
    Extent * extent;
    while (true)
    {
        /// Get the LRU extent, if any.
        ExtentSet * extent_set = &extent_cache->extent_set;
        extent = extent_set->lruFirst();
        if (extent == nullptr)
        {
            /// Next check if there are guarded extents. They are more expensive to purge (since they are not
            /// mergeable), thus in favor of caching them longer.
            extent_set = &extent_cache->guarded_extent_set;
            extent = extent_set->lruFirst();
            if (extent == nullptr)
                goto label_return;
        }
        /// Check the eviction limit.
        size_t extents_num_pages = extent_cache->numPagesGet();
        if (extents_num_pages <= num_pages_min)
        {
            extent = nullptr;
            goto label_return;
        }
        extent_set->remove(extent);
        if (!extent_cache->delay_coalesce || extent->guarded())
            break;
        /// Try to coalesce.
        if (extentTryDelayedCoalesce(thread_state, page_allocator, extent_hooks, extent_cache, extent))
            break;
        /// The LRU extent was just coalesced and the result placed in the LRU (at its tail). Start over.
    }

    /// Either mark the extent active or deregister it to protect against concurrent operations.
    switch (extent_cache->state)
    {
        case extent_state_dirty:
        case extent_state_muzzy: page_allocator->extent_map->updateExtentState(thread_state, extent, extent_state_active); break;
        case extent_state_retained: extentDeregister(thread_state, page_allocator, extent); break;
        case extent_state_active:
        case extent_state_transition:
        case extent_state_merging:
        default: ALLOCATOR_NOT_REACHED();
    }

label_return:
    extent_cache->mutex.unlock(thread_state);
    return extent;
}

void extentGrowthDumpAdd(ThreadState * thread_state, const Extent * extent)
{
    static_assert(config::profiling);

    if (options.profiling && extent->state() == extent_state_active)
    {
        size_t num_add = extent->size() >> LOG2_PAGE;
        size_t current = current_pages.fetch_add(num_add, std::memory_order_relaxed) + num_add;
        size_t high = high_pages.load(std::memory_order_relaxed);
        while (current > high && !high_pages.compare_exchange_weak(high, current, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            /// Don't refresh cur, because it may have decreased since this thread lost the highpages update race.
            /// Note that high is updated in case of CAS failure.
        }
        if (current > high && profilingGrowthDumpGetUnlocked())
            profilingGrowthDump(thread_state);
    }
}

void extentRecord(
    ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, ExtentCache * extent_cache, Extent * extent)
{
    ALLOCATOR_ASSERT((extent_cache->state != extent_state_dirty && extent_cache->state != extent_state_muzzy) || !extent->zeroed());

    extent_cache->mutex.lock(thread_state);

    page_allocator->extent_map->assertMapped(thread_state, extent);

    if (extent->guarded())
        goto label_skip_coalesce;
    if (!extent_cache->delay_coalesce)
    {
        bool coalesced_unused;
        extent = extentTryCoalesce(thread_state, page_allocator, extent_hooks, extent_cache, extent, &coalesced_unused);
    }
    else if (extent->size() >= SIZE_CLASS_LARGE_MIN_CLASS)
    {
        ALLOCATOR_ASSERT(extent_cache == &page_allocator->extent_cache_dirty);
        /// Always coalesce large extents eagerly.
        ///
        /// (fork patch 3c14707b) The maximum size of large extents after coalescing in the dirty cache: if the combined
        /// size of two extents would exceed it, the coalescing is skipped. This improves dirty cache reuse efficiency
        /// by maintaining appropriately sized extents that match common allocation requests, similar to how
        /// `log2_max_fit` is used during extent reuse. During decay/purge, no coalescing restrictions are applied to the
        /// dirty cache, so the final coalescing from dirty to muzzy/retained is not compromised.
        unsigned log2_max_coalesce = unsigned(options.log2_extent_max_active_fit);
        size_t extent_size = extent->size();
        size_t max_size = (SIZE_CLASS_LARGE_MAX_CLASS >> log2_max_coalesce) > extent_size ? (extent_size << log2_max_coalesce)
                                                                                          : SIZE_CLASS_LARGE_MAX_CLASS;
        bool coalesced;
        do
        {
            ALLOCATOR_ASSERT(extent->state() == extent_state_active);
            extent = extentTryCoalesceLarge(thread_state, page_allocator, extent_hooks, extent_cache, extent, max_size, &coalesced);
        } while (coalesced);
        if (extent->size() >= page_allocator->oversize_threshold.load(std::memory_order_relaxed) && !backgroundThreadEnabled()
            && extentMayForceDecay(page_allocator))
        {
            /// Shortcut to purge the oversize extent eagerly.
            extent_cache->mutex.unlock(thread_state);
            extentMaximallyPurge(thread_state, page_allocator, extent_hooks, extent);
            return;
        }
    }
label_skip_coalesce:
    extentDeactivateLocked(thread_state, page_allocator, extent_cache, extent);

    extent_cache->mutex.unlock(thread_state);
}

namespace
{

/// The failure path of the DSS allocation (`sbrk` is never used). jemalloc's `extent_alloc_dss` takes a gap `Extent`
/// from the arena's cache (`extent_avail`) before it finds out that it cannot satisfy the request, and puts it back;
/// this is observable through the mutex and base stats. With the default `sbrk:secondary`, the DSS is only tried when
/// mmap fails, which in practice means a fixed `new_addr` (in-place expansion with `retain:false`). The reference
/// cannot satisfy it either: `new_addr` is never at the edge of the DSS because all extents are mapped with mmap.
///
/// This belongs to the default alloc hook (`ehooks_default_alloc_impl` -> `extent_alloc_core`), but `ExtentHooks` is
/// below the arena layer (the base uses it), so it is done here, in the only caller that passes the arena's hooks
/// with a fixed address. The arena's `extent_pool` is `page_allocator->extent_pool`.
/// jemalloc: extent_alloc_dss (the `label_oom` path)
void extentAllocSbrkFailed(ThreadState * thread_state, PageAllocator * page_allocator)
{
    Extent * gap = page_allocator->extent_pool->get(thread_state);
    if (gap == nullptr)
        return;
    page_allocator->extent_pool->put(thread_state, gap);
}

/// jemalloc: ehooks_alloc (the DSS part of `extent_alloc_core` around the mmap attempt)
void * extentHooksAllocWithSbrk(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    void * new_addr,
    size_t size,
    size_t alignment,
    bool * zero,
    bool * commit)
{
    if constexpr (!config::have_sbrk)
        return extent_hooks->alloc(thread_state, new_addr, size, alignment, zero, commit);

    Arena * arena = arenaGet(thread_state, extent_hooks->idxGet(), false);
    /// A null arena indicates `arena_create`.
    SbrkPrecedence sbrk
        = arena == nullptr ? SbrkPrecedence::Disabled : SbrkPrecedence(arena->sbrk_precedence.load(std::memory_order_relaxed));
    if (sbrk == SbrkPrecedence::Primary)
        extentAllocSbrkFailed(thread_state, page_allocator);
    void * result = extent_hooks->alloc(thread_state, new_addr, size, alignment, zero, commit);
    if (result == nullptr && sbrk == SbrkPrecedence::Secondary)
        extentAllocSbrkFailed(thread_state, page_allocator);
    return result;
}

}

Extent * extentAllocWrapper(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    void * new_addr,
    size_t size,
    size_t alignment,
    bool zero,
    bool * commit,
    bool growing_retained)
{
    Extent * extent = page_allocator->extent_pool->get(thread_state);
    if (extent == nullptr)
        return nullptr;
    size_t page_alignment = alignmentCeiling(alignment, PAGE);
    void * addr = extentHooksAllocWithSbrk(thread_state, page_allocator, extent_hooks, new_addr, size, page_alignment, &zero, commit);
    if (addr == nullptr)
    {
        page_allocator->extent_pool->put(thread_state, extent);
        return nullptr;
    }
    extent->init(
        page_allocator->extent_cache_dirty.idxGet(),
        addr,
        size,
        /* slab */ false,
        SIZE_CLASS_NUM_SIZES,
        extentSerialNumberNext(page_allocator),
        extent_state_active,
        zero,
        *commit,
        EXTENT_ALLOCATOR_PAGE_ALLOCATOR,
        options.retain ? EXTENT_IS_HEAD : EXTENT_NOT_HEAD);
    /// Retained memory is not counted towards gdump. Only if an extent is allocated as a separate mapping, i.e.
    /// `growing_retained` is false, then gdump should be updated.
    bool growth_dump_add = !growing_retained;
    if (extentRegisterImpl(thread_state, page_allocator, extent, growth_dump_add))
    {
        page_allocator->extent_pool->put(thread_state, extent);
        return nullptr;
    }

    return extent;
}

void extentDeallocateWrapperPurged(ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, Extent * extent)
{
    ALLOCATOR_ASSERT(extent->allocatorKind() == EXTENT_ALLOCATOR_PAGE_ALLOCATOR);

    /// Verify that will not go down the dalloc / munmap route.
    ALLOCATOR_ASSERT(extent_hooks->deallocateWillFail());

    extent->setZeroed(true);
    extentDeallocateWrapperFinish(thread_state, page_allocator, extent_hooks, extent);
}

void extentDeallocateWrapper(ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, Extent * extent)
{
    ALLOCATOR_ASSERT(extent->allocatorKind() == EXTENT_ALLOCATOR_PAGE_ALLOCATOR);

    /// Avoid calling the default extent dalloc unless we have to.
    if (!extent_hooks->deallocateWillFail())
    {
        /// Remove guard pages for dalloc / unmap.
        if (extent->guarded())
        {
            ALLOCATOR_ASSERT(extent_hooks->areDefault());
            sanitizerUnguardPagesTwoSided(thread_state, extent_hooks, extent, page_allocator->extent_map);
        }
        /// Deregister first to avoid a race with other allocating threads, and reregister if deallocation fails.
        extentDeregister(thread_state, page_allocator, extent);
        if (!extentDeallocateWrapperTry(thread_state, page_allocator, extent_hooks, extent))
            return;
        extentReregister(thread_state, page_allocator, extent);
    }

    /// Try to decommit; purge if that fails.
    bool zeroed;
    if (!extent->committed())
        zeroed = true;
    else if (!extentDecommitWrapper(thread_state, extent_hooks, extent, 0, extent->size()))
        zeroed = true;
    else if (!extent_hooks->purgeForced(thread_state, extent->base(), extent->size(), 0, extent->size()))
        zeroed = true;
    else if (
        extent->state() == extent_state_muzzy || !extent_hooks->purgeLazy(thread_state, extent->base(), extent->size(), 0, extent->size()))
        zeroed = false;
    else
        zeroed = false;
    extent->setZeroed(zeroed);

    extentDeallocateWrapperFinish(thread_state, page_allocator, extent_hooks, extent);
}

void extentDestroyWrapper(ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, Extent * extent)
{
    ALLOCATOR_ASSERT(extent->base() != nullptr);
    ALLOCATOR_ASSERT(extent->size() != 0);
    ALLOCATOR_ASSERT(extent->state() == extent_state_retained || extent->state() == extent_state_active);
    ALLOCATOR_ASSERT(page_allocator->extent_map->extentIsAcquired(thread_state, extent) || !config::debug);

    if (extent->guarded())
    {
        ALLOCATOR_ASSERT(options.retain);
        sanitizerUnguardPagesPreDestroy(thread_state, extent_hooks, extent, page_allocator->extent_map);
    }
    extent->setAddr(extent->base());

    /// Try to destroy; silently fail otherwise.
    extent_hooks->destroy(thread_state, extent->base(), extent->size(), extent->committed());

    page_allocator->extent_pool->put(thread_state, extent);
}

bool extentCommitWrapper(ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, size_t offset, size_t length)
{
    return extentCommitImpl(thread_state, extent_hooks, extent, offset, length, /* growing_retained */ false);
}

bool extentPurgeLazyWrapper(ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, size_t offset, size_t length)
{
    return extentPurgeLazyImpl(thread_state, extent_hooks, extent, offset, length, false);
}

bool extentPurgeForcedWrapper(ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, size_t offset, size_t length)
{
    return extentPurgeForcedImpl(thread_state, extent_hooks, extent, offset, length, false);
}

Extent * extentSplitWrapper(
    ThreadState * thread_state,
    PageAllocator * page_allocator,
    ExtentHooks * extent_hooks,
    Extent * extent,
    size_t size_a,
    size_t size_b,
    bool holding_core_locks)
{
    return extentSplitImpl(thread_state, page_allocator, extent_hooks, extent, size_a, size_b, holding_core_locks);
}

bool extentMergeWrapper(ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, Extent * a, Extent * b)
{
    return extentMergeImpl(thread_state, page_allocator, extent_hooks, a, b, /* holding_core_locks */ false);
}

bool extentCommitZero(
    ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, bool commit, bool zero, bool growing_retained)
{
    if (commit && !extent->committed())
    {
        if (extentCommitImpl(thread_state, extent_hooks, extent, 0, extent->size(), growing_retained))
            return true;
    }
    if (zero && !extent->zeroed())
    {
        void * addr = extent->base();
        size_t size = extent->size();
        extent_hooks->zero(thread_state, addr, size);
    }
    return false;
}

bool extentBoot()
{
    static_assert(sizeof(SlabData) >= sizeof(ExtentProfilingInfo));
    /// DSS is dropped (`extent_dss_boot` only records `sbrk(0)`).
    return false;
}

size_t extentGrowthDumpCurrentPages()
{
    return current_pages.load(std::memory_order_relaxed);
}

size_t extentGrowthDumpHighPages()
{
    return high_pages.load(std::memory_order_relaxed);
}

}
