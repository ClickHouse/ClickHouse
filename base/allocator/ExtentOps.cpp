#include <allocator/ExtentOps.h>

#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/ExtentCache.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/ExtentPool.h>
#include <allocator/Options.h>
#include <allocator/PageAllocator.h>
#include <allocator/Prof.h>
#include <allocator/Sanitizer.h>

#include <atomic>

namespace jemalloc
{

namespace
{

/// Used exclusively for gdump triggering.
/// jemalloc: curpages, highpages
constinit std::atomic<size_t> curpages{0};
constinit std::atomic<size_t> highpages{0};

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
    ThreadState * tsdn,
    PageAllocator * pac,
    ExtentHooks * ehooks,
    ExtentCache * ecache,
    Extent * expand_edata,
    size_t size,
    size_t alignment,
    bool zero,
    bool * commit,
    bool growing_retained,
    bool guarded);
Extent * extentTryCoalesce(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, ExtentCache * ecache, Extent * edata, bool * coalesced);
Extent * extentAllocRetained(
    ThreadState * tsdn,
    PageAllocator * pac,
    ExtentHooks * ehooks,
    Extent * expand_edata,
    size_t size,
    size_t alignment,
    bool zero,
    bool * commit,
    bool guarded);
bool extentCommitImpl(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length, bool growing_retained);
bool extentDecommitWrapper(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length);
bool extentPurgeLazyImpl(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length, bool growing_retained);
bool extentPurgeForcedImpl(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length, bool growing_retained);
Extent * extentSplitImpl(
    ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * edata, size_t size_a, size_t size_b, bool holding_core_locks);
bool extentMergeImpl(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * a, Extent * b, bool holding_core_locks);

/// jemalloc: extent_may_force_decay
JE_ALWAYS_INLINE bool extentMayForceDecay(PageAllocator * pac)
{
    return !(pac->decayMsGet(extent_state_dirty) == -1 || pac->decayMsGet(extent_state_muzzy) == -1);
}

/// --- Registration --------------------------------------------------------------------------------------------------

/// jemalloc: extent_gdump_sub
void extentGdumpSub(ThreadState * /*tsdn*/, const Extent * edata)
{
    static_assert(config::prof);

    if (opt.prof && edata->state() == extent_state_active)
    {
        size_t nsub = edata->size() >> LG_PAGE;
        JE_ASSERT(curpages.load(std::memory_order_relaxed) >= nsub);
        curpages.fetch_sub(nsub, std::memory_order_relaxed);
    }
}

/// jemalloc: extent_register_impl
bool extentRegisterImpl(ThreadState * tsdn, PageAllocator * pac, Extent * edata, bool gdump_add)
{
    JE_ASSERT(edata->state() == extent_state_active);
    /// No locking needed, as the extent must be in active state, which prevents other threads from accessing it.
    if (pac->emap->registerBoundary(tsdn, edata, SC_NSIZES, /* slab */ false))
        return true;

    if (config::prof && gdump_add)
        extentGdumpAdd(tsdn, edata);

    return false;
}

/// jemalloc: extent_register
bool extentRegister(ThreadState * tsdn, PageAllocator * pac, Extent * edata)
{
    return extentRegisterImpl(tsdn, pac, edata, true);
}

/// jemalloc: extent_register_no_gdump_add
bool extentRegisterNoGdumpAdd(ThreadState * tsdn, PageAllocator * pac, Extent * edata)
{
    return extentRegisterImpl(tsdn, pac, edata, false);
}

/// jemalloc: extent_reregister
void extentReregister(ThreadState * tsdn, PageAllocator * pac, Extent * edata)
{
    [[maybe_unused]] bool err = extentRegister(tsdn, pac, edata);
    JE_ASSERT(!err);
}

/// Removes all pointers to the given extent from the global rtree.
/// jemalloc: extent_deregister_impl
void extentDeregisterImpl(ThreadState * tsdn, PageAllocator * pac, Extent * edata, bool gdump)
{
    pac->emap->deregisterBoundary(tsdn, edata);

    if (config::prof && gdump)
        extentGdumpSub(tsdn, edata);
}

/// jemalloc: extent_deregister
void extentDeregister(ThreadState * tsdn, PageAllocator * pac, Extent * edata)
{
    extentDeregisterImpl(tsdn, pac, edata, true);
}

/// jemalloc: extent_deregister_no_gdump_sub
void extentDeregisterNoGdumpSub(ThreadState * tsdn, PageAllocator * pac, Extent * edata)
{
    extentDeregisterImpl(tsdn, pac, edata, false);
}

/// --- State transitions under the cache lock --------------------------------------------------------------------------

/// jemalloc: extent_deactivate_locked_impl
void extentDeactivateLockedImpl(ThreadState * tsdn, PageAllocator * pac, ExtentCache * ecache, Extent * edata)
{
    ecache->mtx.assertOwner(tsdn);
    JE_ASSERT(edata->arenaInd() == ecache->indGet());

    pac->emap->updateEdataState(tsdn, edata, ecache->state);
    ExtentSet * eset = edata->guarded() ? &ecache->guarded_eset : &ecache->eset;
    eset->insert(edata);
}

/// jemalloc: extent_deactivate_locked
void extentDeactivateLocked(ThreadState * tsdn, PageAllocator * pac, ExtentCache * ecache, Extent * edata)
{
    JE_ASSERT(edata->state() == extent_state_active);
    extentDeactivateLockedImpl(tsdn, pac, ecache, edata);
}

/// jemalloc: extent_deactivate_check_state_locked
void extentDeactivateCheckStateLocked(
    ThreadState * tsdn, PageAllocator * pac, ExtentCache * ecache, Extent * edata, [[maybe_unused]] ExtentState expected_state)
{
    JE_ASSERT(edata->state() == expected_state);
    extentDeactivateLockedImpl(tsdn, pac, ecache, edata);
}

/// jemalloc: extent_activate_locked
void extentActivateLocked(ThreadState * tsdn, PageAllocator * pac, [[maybe_unused]] ExtentCache * ecache, ExtentSet * eset, Extent * edata)
{
    JE_ASSERT(edata->arenaInd() == ecache->indGet());
    JE_ASSERT(edata->state() == ecache->state || edata->state() == extent_state_merging);

    eset->remove(edata);
    pac->emap->updateEdataState(tsdn, edata, extent_state_active);
}

/// jemalloc: extent_try_delayed_coalesce
bool extentTryDelayedCoalesce(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, ExtentCache * ecache, Extent * edata)
{
    pac->emap->updateEdataState(tsdn, edata, extent_state_active);

    bool coalesced;
    edata = extentTryCoalesce(tsdn, pac, ehooks, ecache, edata, &coalesced);
    pac->emap->updateEdataState(tsdn, edata, ecache->state);

    if (!coalesced)
        return true;
    /// NOTE: the merged extent goes to the LRU tail (not "at its neighbor's position" as jemalloc's comment says).
    ecache->eset.insert(edata);
    return false;
}

/// This can only happen when we fail to allocate a new extent struct (which indicates OOM), e.g. when trying to split
/// an existing extent.
/// jemalloc: extents_abandon_vm
void extentsAbandonVm(
    ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, ExtentCache * ecache, Extent * edata, bool growing_retained)
{
    size_t sz = edata->size();
    if constexpr (config::stats)
        pac->stats->abandoned_vm.fetch_add(sz, std::memory_order_relaxed);
    /// Leak the extent after making sure its pages have already been purged, so that this is only a virtual memory
    /// leak.
    if (ecache->state == extent_state_dirty)
    {
        if (extentPurgeLazyImpl(tsdn, ehooks, edata, 0, sz, growing_retained))
            extentPurgeForcedImpl(tsdn, ehooks, edata, 0, edata->size(), growing_retained);
    }
    pac->edata_cache->put(tsdn, edata);
}

/// --- Recycling -----------------------------------------------------------------------------------------------------

/// Tries to find and remove an extent from `ecache` that can be used for the given allocation request.
/// jemalloc: extent_recycle_extract
Extent * extentRecycleExtract(
    ThreadState * tsdn,
    PageAllocator * pac,
    ExtentHooks * /*ehooks*/,
    ExtentCache * ecache,
    Extent * expand_edata,
    size_t size,
    size_t alignment,
    bool guarded)
{
    ecache->mtx.assertOwner(tsdn);
    JE_ASSERT(alignment > 0);
    if constexpr (config::debug)
    {
        if (expand_edata != nullptr)
        {
            /// Non-null `expand_edata` indicates in-place expanding realloc. `new_addr` must either refer to a
            /// non-existing extent, or to the base of an extant extent, since only active slabs support interior
            /// lookups (which of course cannot be recycled).
            [[maybe_unused]] void * new_addr = expand_edata->past();
            JE_ASSERT(pageAddrToBase(new_addr) == new_addr);
            JE_ASSERT(alignment <= PAGE);
        }
    }

    Extent * edata;
    ExtentSet * eset = guarded ? &ecache->guarded_eset : &ecache->eset;
    if (expand_edata != nullptr)
    {
        edata = pac->emap->tryAcquireEdataNeighborExpand(tsdn, expand_edata, EXTENT_PAI_PAC, ecache->state);
        if (edata != nullptr)
        {
            extentAssertCanExpand(expand_edata, edata);
            if (edata->size() < size)
            {
                pac->emap->releaseEdata(tsdn, edata, ecache->state);
                edata = nullptr;
            }
        }
    }
    else
    {
        /// A large extent might be broken up from its original size to some small size to satisfy a small request.
        /// When that small request is freed, though, it won't merge back with the larger extent if delayed coalescing
        /// is on. The large extent can then no longer satisfy a request for its original size. To limit this effect,
        /// when delayed coalescing is enabled, we put a cap on how big an extent we can split for a request.
        unsigned lg_max_fit = ecache->delay_coalesce ? unsigned(opt.lg_extent_max_active_fit) : SC_PTR_BITS;

        /// If split and merge are not allowed (Windows w/o retain), try exact fit only. For simplicity purposes,
        /// splitting guarded extents is not supported. Hence, we do only exact fit for guarded allocations.
        bool exact_only = (!config::maps_coalesce && !opt.retain) || guarded;
        edata = eset->fit(size, alignment, exact_only, lg_max_fit);
    }
    if (edata == nullptr)
        return nullptr;
    JE_ASSERT(!guarded || edata->guarded());
    extentActivateLocked(tsdn, pac, ecache, eset, edata);

    return edata;
}

/// Given an allocation request and an extent guaranteed to be able to satisfy it, this splits off lead and trail
/// extents, leaving `*edata` pointing to an extent satisfying the allocation. This function doesn't put lead or trail
/// into any cache; it's the caller's job to ensure that they can be reused.
/// jemalloc: extent_split_interior
SplitInteriorResult extentSplitInterior(
    ThreadState * tsdn,
    PageAllocator * pac,
    ExtentHooks * ehooks,
    /// The result of splitting, in case of success.
    Extent ** edata,
    Extent ** lead,
    Extent ** trail,
    /// The mess to clean up, in case of error.
    Extent ** to_leak,
    Extent ** to_salvage,
    [[maybe_unused]] Extent * expand_edata,
    size_t size,
    size_t alignment)
{
    size_t leadsize = alignmentCeiling(reinterpret_cast<uintptr_t>((*edata)->base()), pageCeiling(alignment))
        - reinterpret_cast<uintptr_t>((*edata)->base());
    JE_ASSERT(expand_edata == nullptr || leadsize == 0);
    if ((*edata)->size() < leadsize + size)
        return SplitInteriorResult::CantAlloc;
    size_t trailsize = (*edata)->size() - leadsize - size;

    *lead = nullptr;
    *trail = nullptr;
    *to_leak = nullptr;
    *to_salvage = nullptr;

    /// Split the lead.
    if (leadsize != 0)
    {
        JE_ASSERT(!(*edata)->guarded());
        *lead = *edata;
        *edata = extentSplitImpl(tsdn, pac, ehooks, *lead, leadsize, size + trailsize, /* holding_core_locks */ true);
        if (*edata == nullptr)
        {
            *to_leak = *lead;
            *lead = nullptr;
            return SplitInteriorResult::Error;
        }
    }

    /// Split the trail.
    if (trailsize != 0)
    {
        JE_ASSERT(!(*edata)->guarded());
        *trail = extentSplitImpl(tsdn, pac, ehooks, *edata, size, trailsize, /* holding_core_locks */ true);
        if (*trail == nullptr)
        {
            *to_leak = *edata;
            *to_salvage = *lead;
            *lead = nullptr;
            *edata = nullptr;
            return SplitInteriorResult::Error;
        }
    }

    return SplitInteriorResult::Ok;
}

/// This fulfills the indicated allocation request out of the given extent (which the caller should have ensured was
/// big enough). If there's any unused space before or after the resulting allocation, that space is given its own
/// extent and put back into `ecache`.
/// jemalloc: extent_recycle_split
Extent * extentRecycleSplit(
    ThreadState * tsdn,
    PageAllocator * pac,
    ExtentHooks * ehooks,
    ExtentCache * ecache,
    Extent * expand_edata,
    size_t size,
    size_t alignment,
    Extent * edata,
    bool growing_retained)
{
    JE_ASSERT(!edata->guarded() || size == edata->size());
    ecache->mtx.assertOwner(tsdn);

    Extent * lead;
    Extent * trail;
    Extent * to_leak = nullptr;
    Extent * to_salvage = nullptr;

    SplitInteriorResult result
        = extentSplitInterior(tsdn, pac, ehooks, &edata, &lead, &trail, &to_leak, &to_salvage, expand_edata, size, alignment);

    if (!config::maps_coalesce && result != SplitInteriorResult::Ok && !opt.retain)
    {
        /// Split isn't supported (implies Windows w/o retain). Avoid leaking the extent.
        JE_ASSERT(to_leak != nullptr && lead == nullptr && trail == nullptr);
        extentDeactivateLocked(tsdn, pac, ecache, to_leak);
        return nullptr;
    }

    if (result == SplitInteriorResult::Ok)
    {
        if (lead != nullptr)
            extentDeactivateLocked(tsdn, pac, ecache, lead);
        if (trail != nullptr)
            extentDeactivateLocked(tsdn, pac, ecache, trail);
        return edata;
    }
    else
    {
        /// We should have picked an extent that was large enough to fulfill our allocation request.
        JE_ASSERT(result == SplitInteriorResult::Error);
        if (to_salvage != nullptr)
            extentDeregister(tsdn, pac, to_salvage);
        if (to_leak != nullptr)
        {
            extentDeregisterNoGdumpSub(tsdn, pac, to_leak);
            /// May go down the purge path (which assumes no cache locks). Only happens with OOM caused split failures.
            ecache->mtx.unlock(tsdn);
            extentsAbandonVm(tsdn, pac, ehooks, ecache, to_leak, growing_retained);
            ecache->mtx.lock(tsdn);
        }
        return nullptr;
    }
}

/// Tries to satisfy the given allocation request by reusing one of the extents in the given cache.
/// jemalloc: extent_recycle
Extent * extentRecycle(
    ThreadState * tsdn,
    PageAllocator * pac,
    ExtentHooks * ehooks,
    ExtentCache * ecache,
    Extent * expand_edata,
    size_t size,
    size_t alignment,
    bool zero,
    bool * commit,
    bool growing_retained,
    bool guarded)
{
    JE_ASSERT(!guarded || expand_edata == nullptr);
    JE_ASSERT(!guarded || alignment <= PAGE);

    ecache->mtx.lock(tsdn);

    Extent * edata = extentRecycleExtract(tsdn, pac, ehooks, ecache, expand_edata, size, alignment, guarded);
    if (edata == nullptr)
    {
        ecache->mtx.unlock(tsdn);
        return nullptr;
    }

    edata = extentRecycleSplit(tsdn, pac, ehooks, ecache, expand_edata, size, alignment, edata, growing_retained);
    ecache->mtx.unlock(tsdn);
    if (edata == nullptr)
        return nullptr;

    JE_ASSERT(edata->state() == extent_state_active);
    if (extentCommitZero(tsdn, ehooks, edata, *commit, zero, growing_retained))
    {
        extentRecord(tsdn, pac, ehooks, ecache, edata);
        return nullptr;
    }
    if (edata->committed())
    {
        /// This reverses the purpose of this variable - previously it was treated as an input parameter, now it turns
        /// into an output parameter, reporting if the extent has actually been committed.
        *commit = true;
    }
    return edata;
}

/// If virtual memory is retained, create increasingly larger extents from which to split requested extents in order
/// to limit the total number of disjoint virtual memory ranges retained by each shard. `pac->grow_mtx` is held on
/// entry and always released.
/// jemalloc: extent_grow_retained
Extent * extentGrowRetained(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, size_t size, size_t alignment, bool zero, bool * commit)
{
    pac->grow_mtx.assertOwner(tsdn);

    size_t alloc_size_min = size + pageCeiling(alignment) - PAGE;
    size_t alloc_size;
    pszind_t exp_grow_skip;
    Extent * edata;
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
        goto label_err;
    /// Find the next extent size in the series that would be large enough to satisfy this request.
    if (pac->exp_grow.sizePrepare(alloc_size_min, &alloc_size, &exp_grow_skip))
        goto label_err;

    edata = pac->edata_cache->get(tsdn);
    if (edata == nullptr)
        goto label_err;
    zeroed = false;
    committed = false;

    ptr = ehooks->alloc(tsdn, nullptr, alloc_size, PAGE, &zeroed, &committed);

    if (ptr == nullptr)
    {
        pac->edata_cache->put(tsdn, edata);
        goto label_err;
    }

    edata->init(
        pac->ecache_retained.indGet(),
        ptr,
        alloc_size,
        false,
        SC_NSIZES,
        extentSnNext(pac),
        extent_state_active,
        zeroed,
        committed,
        EXTENT_PAI_PAC,
        EXTENT_IS_HEAD);

    if (extentRegisterNoGdumpAdd(tsdn, pac, edata))
    {
        pac->edata_cache->put(tsdn, edata);
        goto label_err;
    }

    if (edata->committed())
        *commit = true;

    result = extentSplitInterior(tsdn, pac, ehooks, &edata, &lead, &trail, &to_leak, &to_salvage, nullptr, size, alignment);

    if (result == SplitInteriorResult::Ok)
    {
        if (lead != nullptr)
            extentRecord(tsdn, pac, ehooks, &pac->ecache_retained, lead);
        if (trail != nullptr)
            extentRecord(tsdn, pac, ehooks, &pac->ecache_retained, trail);
    }
    else
    {
        /// We should have allocated a sufficiently large extent; the cant_alloc case should not occur.
        JE_ASSERT(result == SplitInteriorResult::Error);
        if (to_salvage != nullptr)
        {
            if constexpr (config::prof)
                extentGdumpAdd(tsdn, to_salvage);
            extentRecord(tsdn, pac, ehooks, &pac->ecache_retained, to_salvage);
        }
        if (to_leak != nullptr)
        {
            extentDeregisterNoGdumpSub(tsdn, pac, to_leak);
            extentsAbandonVm(tsdn, pac, ehooks, &pac->ecache_retained, to_leak, true);
        }
        goto label_err;
    }

    if (*commit && !edata->committed())
    {
        if (extentCommitImpl(tsdn, ehooks, edata, 0, edata->size(), true))
        {
            extentRecord(tsdn, pac, ehooks, &pac->ecache_retained, edata);
            goto label_err;
        }
        /// A successful commit should return zeroed memory.
        if constexpr (config::debug)
        {
            const size_t * p = static_cast<const size_t *>(edata->addr());
            /// Check the first page only.
            for (size_t i = 0; i < PAGE / sizeof(size_t); ++i)
                JE_ASSERT(p[i] == 0);
        }
    }

    /// Increment the grow index if doing so wouldn't exceed the allowed range. All opportunities for failure are past.
    pac->exp_grow.sizeCommit(exp_grow_skip);
    pac->grow_mtx.unlock(tsdn);

    /// The THP handling of the huge arena (`huge_arena_pac_thp.thp_madvise`, `extent_handle_huge_arena_thp`) is not
    /// ported: it requires `opt.huge_arena_pac_thp` and `metadata_thp`, both off by default (dead for ClickHouse).

    if constexpr (config::prof)
    {
        /// Adjust gdump stats now that the extent is final size.
        extentGdumpAdd(tsdn, edata);
    }
    if (zero && !edata->zeroed())
        ehooks->zero(tsdn, edata->base(), edata->size());
    return edata;

label_err:
    pac->grow_mtx.unlock(tsdn);
    return nullptr;
}

/// jemalloc: extent_alloc_retained
Extent * extentAllocRetained(
    ThreadState * tsdn,
    PageAllocator * pac,
    ExtentHooks * ehooks,
    Extent * expand_edata,
    size_t size,
    size_t alignment,
    bool zero,
    bool * commit,
    bool guarded)
{
    JE_ASSERT(size != 0);
    JE_ASSERT(alignment != 0);

    pac->grow_mtx.lock(tsdn);

    Extent * edata = extentRecycle(
        tsdn, pac, ehooks, &pac->ecache_retained, expand_edata, size, alignment, zero, commit, /* growing_retained */ true, guarded);
    if (edata != nullptr)
    {
        pac->grow_mtx.unlock(tsdn);
        if constexpr (config::prof)
            extentGdumpAdd(tsdn, edata);
    }
    else if (opt.retain && expand_edata == nullptr && !guarded)
    {
        /// `extentGrowRetained` always releases `pac->grow_mtx`.
        edata = extentGrowRetained(tsdn, pac, ehooks, size, alignment, zero, commit);
    }
    else
    {
        pac->grow_mtx.unlock(tsdn);
    }
    pac->grow_mtx.assertNotOwner(tsdn);

    return edata;
}

/// --- Coalescing ----------------------------------------------------------------------------------------------------

/// jemalloc: extent_coalesce
bool extentCoalesce(
    ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, ExtentCache * ecache, Extent * inner, Extent * outer, bool forward)
{
    extentAssertCanCoalesce(inner, outer);
    ecache->eset.remove(outer);

    bool err = extentMergeImpl(tsdn, pac, ehooks, forward ? inner : outer, forward ? outer : inner, /* holding_core_locks */ true);
    if (err)
        extentDeactivateCheckStateLocked(tsdn, pac, ecache, outer, extent_state_merging);

    return err;
}

/// jemalloc: extent_try_coalesce_impl
Extent * extentTryCoalesceImpl(
    ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, ExtentCache * ecache, Extent * edata, size_t max_size, bool * coalesced)
{
    JE_ASSERT(!edata->guarded());
    JE_ASSERT(coalesced != nullptr);
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
        Extent * next = pac->emap->tryAcquireEdataNeighbor(tsdn, edata, EXTENT_PAI_PAC, ecache->state, /* forward */ true);
        size_t max_next_neighbor = max_size > edata->size() ? max_size - edata->size() : 0;
        /// jemalloc compatibility: (fork patch 3c14707b) a neighbor that was acquired (its state is now `merging` in
        /// the extent and in the rtree) but is rejected by the size limit is NOT released: it stays in its set in the
        /// `merging` state, invisible to further coalescing and to expand-acquire, until it is extracted by `fit`
        /// (`extentActivateLocked` accepts `merging`) or evicted. Only reachable from the large-dirty path of
        /// `extentRecord`; all other callers pass `SC_LARGE_MAXCLASS`.
        if (next != nullptr && next->size() <= max_next_neighbor)
        {
            if (!extentCoalesce(tsdn, pac, ehooks, ecache, edata, next, true))
            {
                if (ecache->delay_coalesce)
                {
                    /// Do minimal coalescing.
                    *coalesced = true;
                    return edata;
                }
                again = true;
            }
        }

        /// Try to coalesce backward.
        Extent * prev = pac->emap->tryAcquireEdataNeighbor(tsdn, edata, EXTENT_PAI_PAC, ecache->state, /* forward */ false);
        size_t max_prev_neighbor = max_size > edata->size() ? max_size - edata->size() : 0;
        /// jemalloc compatibility: the same 3c14707b quirk for the backward neighbor.
        if (prev != nullptr && prev->size() <= max_prev_neighbor)
        {
            if (!extentCoalesce(tsdn, pac, ehooks, ecache, edata, prev, false))
            {
                edata = prev;
                if (ecache->delay_coalesce)
                {
                    /// Do minimal coalescing.
                    *coalesced = true;
                    return edata;
                }
                again = true;
            }
        }
    } while (again);

    if (ecache->delay_coalesce)
        *coalesced = false;
    return edata;
}

/// jemalloc: extent_try_coalesce
Extent * extentTryCoalesce(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, ExtentCache * ecache, Extent * edata, bool * coalesced)
{
    return extentTryCoalesceImpl(tsdn, pac, ehooks, ecache, edata, SC_LARGE_MAXCLASS, coalesced);
}

/// jemalloc: extent_try_coalesce_large
Extent * extentTryCoalesceLarge(
    ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, ExtentCache * ecache, Extent * edata, size_t max_size, bool * coalesced)
{
    return extentTryCoalesceImpl(tsdn, pac, ehooks, ecache, edata, max_size, coalesced);
}

/// Purge a single extent to retained / unmapped directly.
/// jemalloc: extent_maximally_purge
void extentMaximallyPurge(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * edata)
{
    size_t extent_size = edata->size();
    extentDallocWrapper(tsdn, pac, ehooks, edata);
    if constexpr (config::stats)
    {
        /// Update stats accordingly (`stats_mtx` is null: the counters are atomic).
        pac->stats->decay_dirty.nmadvise.inc(1);
        pac->stats->decay_dirty.purged.inc(extent_size >> LG_PAGE);
        pac->stats->pac_mapped.fetch_sub(extent_size, std::memory_order_relaxed);
    }
}

/// --- OS-level operations -------------------------------------------------------------------------------------------

/// jemalloc: extent_dalloc_wrapper_try
bool extentDallocWrapperTry(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * edata)
{
    JE_ASSERT(edata->base() != nullptr);
    JE_ASSERT(edata->size() != 0);

    edata->setAddr(edata->base());

    /// Try to deallocate.
    bool err = ehooks->dalloc(tsdn, edata->base(), edata->size(), edata->committed());

    if (!err)
        pac->edata_cache->put(tsdn, edata);

    return err;
}

/// jemalloc: extent_dalloc_wrapper_finish
void extentDallocWrapperFinish(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * edata)
{
    if constexpr (config::prof)
        extentGdumpSub(tsdn, edata);
    extentRecord(tsdn, pac, ehooks, &pac->ecache_retained, edata);
}

/// jemalloc: extent_commit_impl
bool extentCommitImpl(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length, bool /*growing_retained*/)
{
    bool err = ehooks->commit(tsdn, edata->base(), edata->size(), offset, length);
    edata->setCommitted(edata->committed() || !err);
    return err;
}

/// jemalloc: extent_decommit_wrapper
bool extentDecommitWrapper(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length)
{
    bool err = ehooks->decommit(tsdn, edata->base(), edata->size(), offset, length);
    edata->setCommitted(edata->committed() && err);
    return err;
}

/// jemalloc: extent_purge_lazy_impl
bool extentPurgeLazyImpl(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length, bool /*growing_retained*/)
{
    return ehooks->purgeLazy(tsdn, edata->base(), edata->size(), offset, length);
}

/// jemalloc: extent_purge_forced_impl
bool extentPurgeForcedImpl(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length, bool /*growing_retained*/)
{
    return ehooks->purgeForced(tsdn, edata->base(), edata->size(), offset, length);
}

/// Accepts the extent to split, and the characteristics of each side of the split. The 'a' parameters go with the
/// lead of the resulting pair of extents (the lower addressed portion of the split), and the 'b' parameters go with
/// the trail (the higher addressed portion). This makes `edata` the lead, and returns the trail (except in case of
/// error).
/// jemalloc: extent_split_impl
Extent * extentSplitImpl(
    ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * edata, size_t size_a, size_t size_b, bool /*holding_core_locks*/)
{
    JE_ASSERT(edata->size() == size_a + size_b);

    if (ehooks->splitWillFail())
        return nullptr;

    Extent * trail = pac->edata_cache->get(tsdn);
    if (trail == nullptr)
        return nullptr;

    trail->init(
        edata->arenaInd(),
        static_cast<std::byte *>(edata->base()) + size_a,
        size_b,
        /* slab */ false,
        SC_NSIZES,
        edata->sn(),
        edata->state(),
        edata->zeroed(),
        edata->committed(),
        EXTENT_PAI_PAC,
        EXTENT_NOT_HEAD);
    ExtentMapPrepare prepare;
    bool err = pac->emap->splitPrepare(tsdn, &prepare, edata, size_a, trail, size_b);
    if (err)
    {
        pac->edata_cache->put(tsdn, trail);
        return nullptr;
    }

    /// No need to acquire trail or edata, because: 1) trail was new (just allocated); and 2) edata is either an active
    /// allocation (the shrink path), or in an acquired state (extracted from the cache on the recycle-split path).
    JE_ASSERT(pac->emap->edataIsAcquired(tsdn, edata) || !config::debug);
    JE_ASSERT(pac->emap->edataIsAcquired(tsdn, trail) || !config::debug);

    err = ehooks->split(tsdn, edata->base(), size_a + size_b, size_a, size_b, edata->committed());

    if (err)
    {
        pac->edata_cache->put(tsdn, trail);
        return nullptr;
    }

    edata->setSize(size_a);
    pac->emap->splitCommit(tsdn, &prepare, edata, size_a, trail, size_b);

    return trail;
}

/// jemalloc: extent_merge_impl
bool extentMergeImpl(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * a, Extent * b, bool /*holding_core_locks*/)
{
    JE_ASSERT(a->base() < b->base());
    JE_ASSERT(a->arenaInd() == b->arenaInd());
    JE_ASSERT(a->arenaInd() == ehooks->indGet());
    pac->emap->assertMapped(tsdn, a);
    pac->emap->assertMapped(tsdn, b);
    /// The `config_debug` check of `ehooks_default_merge_impl` (head states via the emap): the higher extent must not
    /// be a head extent (`ExtentHooks::merge` does not port it).
    JE_ASSERT(extentNeighborHeadStateMergeable(a->isHead(), b->isHead(), /* forward */ true));

    bool err = ehooks->merge(tsdn, a->base(), a->size(), b->base(), b->size(), a->committed());

    if (err)
        return true;

    /// The rtree writes must happen while all the relevant elements are owned, so the following code uses decomposed
    /// helper functions rather than register/deregister to do things in the right order.
    ExtentMapPrepare prepare;
    pac->emap->mergePrepare(tsdn, &prepare, a, b);

    JE_ASSERT(a->state() == extent_state_active || a->state() == extent_state_merging);
    a->setState(extent_state_active);
    a->setSize(a->size() + b->size());
    a->setSn((a->sn() < b->sn()) ? a->sn() : b->sn());
    a->setZeroed(a->zeroed() && b->zeroed());

    pac->emap->mergeCommit(tsdn, &prepare, a, b);

    pac->edata_cache->put(tsdn, b);

    return false;
}

}

/// --- Public functions ----------------------------------------------------------------------------------------------

size_t extentSnNext(PageAllocator * pac)
{
    return pac->extent_sn_next.fetch_add(1, std::memory_order_relaxed);
}

Extent * ecacheAlloc(
    ThreadState * tsdn,
    PageAllocator * pac,
    ExtentHooks * ehooks,
    ExtentCache * ecache,
    Extent * expand_edata,
    size_t size,
    size_t alignment,
    bool zero,
    bool guarded)
{
    JE_ASSERT(size != 0);
    JE_ASSERT(alignment != 0);

    bool commit = true;
    Extent * edata = extentRecycle(tsdn, pac, ehooks, ecache, expand_edata, size, alignment, zero, &commit, false, guarded);
    JE_ASSERT(edata == nullptr || edata->pai() == EXTENT_PAI_PAC);
    JE_ASSERT(edata == nullptr || edata->guarded() == guarded);
    return edata;
}

Extent * ecacheAllocGrow(
    ThreadState * tsdn,
    PageAllocator * pac,
    ExtentHooks * ehooks,
    ExtentCache * /*ecache*/,
    Extent * expand_edata,
    size_t size,
    size_t alignment,
    bool zero,
    bool guarded)
{
    JE_ASSERT(size != 0);
    JE_ASSERT(alignment != 0);

    bool commit = true;
    Extent * edata = extentAllocRetained(tsdn, pac, ehooks, expand_edata, size, alignment, zero, &commit, guarded);
    if (edata == nullptr)
    {
        if (opt.retain && expand_edata != nullptr)
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
        void * new_addr = (expand_edata == nullptr) ? nullptr : expand_edata->past();
        edata = extentAllocWrapper(tsdn, pac, ehooks, new_addr, size, alignment, zero, &commit, /* growing_retained */ false);
    }

    JE_ASSERT(edata == nullptr || edata->pai() == EXTENT_PAI_PAC);
    return edata;
}

void ecacheDalloc(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, ExtentCache * ecache, Extent * edata)
{
    JE_ASSERT(edata->base() != nullptr);
    JE_ASSERT(edata->size() != 0);
    JE_ASSERT(edata->pai() == EXTENT_PAI_PAC);

    edata->setAddr(edata->base());
    edata->setZeroed(false);

    extentRecord(tsdn, pac, ehooks, ecache, edata);
}

Extent * ecacheEvict(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, ExtentCache * ecache, size_t npages_min)
{
    ecache->mtx.lock(tsdn);

    /// Get the LRU coalesced extent, if any. If coalescing was delayed, the loop will iterate until the LRU extent is
    /// fully coalesced.
    Extent * edata;
    while (true)
    {
        /// Get the LRU extent, if any.
        ExtentSet * eset = &ecache->eset;
        edata = eset->lruFirst();
        if (edata == nullptr)
        {
            /// Next check if there are guarded extents. They are more expensive to purge (since they are not
            /// mergeable), thus in favor of caching them longer.
            eset = &ecache->guarded_eset;
            edata = eset->lruFirst();
            if (edata == nullptr)
                goto label_return;
        }
        /// Check the eviction limit.
        size_t extents_npages = ecache->npagesGet();
        if (extents_npages <= npages_min)
        {
            edata = nullptr;
            goto label_return;
        }
        eset->remove(edata);
        if (!ecache->delay_coalesce || edata->guarded())
            break;
        /// Try to coalesce.
        if (extentTryDelayedCoalesce(tsdn, pac, ehooks, ecache, edata))
            break;
        /// The LRU extent was just coalesced and the result placed in the LRU (at its tail). Start over.
    }

    /// Either mark the extent active or deregister it to protect against concurrent operations.
    switch (ecache->state)
    {
        case extent_state_dirty:
        case extent_state_muzzy:
            pac->emap->updateEdataState(tsdn, edata, extent_state_active);
            break;
        case extent_state_retained:
            extentDeregister(tsdn, pac, edata);
            break;
        case extent_state_active:
        case extent_state_transition:
        case extent_state_merging:
        default:
            JE_NOT_REACHED();
    }

label_return:
    ecache->mtx.unlock(tsdn);
    return edata;
}

void extentGdumpAdd(ThreadState * tsdn, const Extent * edata)
{
    static_assert(config::prof);

    if (opt.prof && edata->state() == extent_state_active)
    {
        size_t nadd = edata->size() >> LG_PAGE;
        size_t cur = curpages.fetch_add(nadd, std::memory_order_relaxed) + nadd;
        size_t high = highpages.load(std::memory_order_relaxed);
        while (cur > high && !highpages.compare_exchange_weak(high, cur, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            /// Don't refresh cur, because it may have decreased since this thread lost the highpages update race.
            /// Note that high is updated in case of CAS failure.
        }
        if (cur > high && profGdumpGetUnlocked())
            profGdump(tsdn);
    }
}

void extentRecord(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, ExtentCache * ecache, Extent * edata)
{
    JE_ASSERT((ecache->state != extent_state_dirty && ecache->state != extent_state_muzzy) || !edata->zeroed());

    ecache->mtx.lock(tsdn);

    pac->emap->assertMapped(tsdn, edata);

    if (edata->guarded())
        goto label_skip_coalesce;
    if (!ecache->delay_coalesce)
    {
        bool coalesced_unused;
        edata = extentTryCoalesce(tsdn, pac, ehooks, ecache, edata, &coalesced_unused);
    }
    else if (edata->size() >= SC_LARGE_MINCLASS)
    {
        JE_ASSERT(ecache == &pac->ecache_dirty);
        /// Always coalesce large extents eagerly.
        ///
        /// (fork patch 3c14707b) The maximum size of large extents after coalescing in the dirty cache: if the combined
        /// size of two extents would exceed it, the coalescing is skipped. This improves dirty cache reuse efficiency
        /// by maintaining appropriately sized extents that match common allocation requests, similar to how
        /// `lg_max_fit` is used during extent reuse. During decay/purge, no coalescing restrictions are applied to the
        /// dirty cache, so the final coalescing from dirty to muzzy/retained is not compromised.
        unsigned lg_max_coalesce = unsigned(opt.lg_extent_max_active_fit);
        size_t edata_size = edata->size();
        size_t max_size = (SC_LARGE_MAXCLASS >> lg_max_coalesce) > edata_size ? (edata_size << lg_max_coalesce) : SC_LARGE_MAXCLASS;
        bool coalesced;
        do
        {
            JE_ASSERT(edata->state() == extent_state_active);
            edata = extentTryCoalesceLarge(tsdn, pac, ehooks, ecache, edata, max_size, &coalesced);
        } while (coalesced);
        if (edata->size() >= pac->oversize_threshold.load(std::memory_order_relaxed) && !backgroundThreadEnabled()
            && extentMayForceDecay(pac))
        {
            /// Shortcut to purge the oversize extent eagerly.
            ecache->mtx.unlock(tsdn);
            extentMaximallyPurge(tsdn, pac, ehooks, edata);
            return;
        }
    }
label_skip_coalesce:
    extentDeactivateLocked(tsdn, pac, ecache, edata);

    ecache->mtx.unlock(tsdn);
}

namespace
{

/// The failure path of the DSS allocation (`sbrk` is never used). jemalloc's `extent_alloc_dss` takes a gap `Extent`
/// from the arena's cache (`extent_avail`) before it finds out that it cannot satisfy the request, and puts it back;
/// this is observable through the mutex and base stats. With the default `dss:secondary`, the DSS is only tried when
/// mmap fails, which in practice means a fixed `new_addr` (in-place expansion with `retain:false`). The reference
/// cannot satisfy it either: `new_addr` is never at the edge of the DSS because all extents are mapped with mmap.
///
/// This belongs to the default alloc hook (`ehooks_default_alloc_impl` -> `extent_alloc_core`), but `ExtentHooks` is
/// below the arena layer (the base uses it), so it is done here, in the only caller that passes the arena's hooks
/// with a fixed address. The arena's `edata_cache` is `pac->edata_cache`.
/// jemalloc: extent_alloc_dss (the `label_oom` path)
void extentAllocDssFailed(ThreadState * tsdn, PageAllocator * pac)
{
    Extent * gap = pac->edata_cache->get(tsdn);
    if (gap == nullptr)
        return;
    pac->edata_cache->put(tsdn, gap);
}

/// jemalloc: ehooks_alloc (the DSS part of `extent_alloc_core` around the mmap attempt)
void * extentHooksAllocWithDss(
    ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit)
{
    if constexpr (!config::have_dss)
        return ehooks->alloc(tsdn, new_addr, size, alignment, zero, commit);

    Arena * arena = arenaGet(tsdn, ehooks->indGet(), false);
    /// A null arena indicates `arena_create`.
    DssPrec dss = arena == nullptr ? DssPrec::Disabled : DssPrec(arena->dss_prec.load(std::memory_order_relaxed));
    if (dss == DssPrec::Primary)
        extentAllocDssFailed(tsdn, pac);
    void * ret = ehooks->alloc(tsdn, new_addr, size, alignment, zero, commit);
    if (ret == nullptr && dss == DssPrec::Secondary)
        extentAllocDssFailed(tsdn, pac);
    return ret;
}

}

Extent * extentAllocWrapper(
    ThreadState * tsdn,
    PageAllocator * pac,
    ExtentHooks * ehooks,
    void * new_addr,
    size_t size,
    size_t alignment,
    bool zero,
    bool * commit,
    bool growing_retained)
{
    Extent * edata = pac->edata_cache->get(tsdn);
    if (edata == nullptr)
        return nullptr;
    size_t palignment = alignmentCeiling(alignment, PAGE);
    void * addr = extentHooksAllocWithDss(tsdn, pac, ehooks, new_addr, size, palignment, &zero, commit);
    if (addr == nullptr)
    {
        pac->edata_cache->put(tsdn, edata);
        return nullptr;
    }
    edata->init(
        pac->ecache_dirty.indGet(),
        addr,
        size,
        /* slab */ false,
        SC_NSIZES,
        extentSnNext(pac),
        extent_state_active,
        zero,
        *commit,
        EXTENT_PAI_PAC,
        opt.retain ? EXTENT_IS_HEAD : EXTENT_NOT_HEAD);
    /// Retained memory is not counted towards gdump. Only if an extent is allocated as a separate mapping, i.e.
    /// `growing_retained` is false, then gdump should be updated.
    bool gdump_add = !growing_retained;
    if (extentRegisterImpl(tsdn, pac, edata, gdump_add))
    {
        pac->edata_cache->put(tsdn, edata);
        return nullptr;
    }

    return edata;
}

void extentDallocWrapperPurged(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * edata)
{
    JE_ASSERT(edata->pai() == EXTENT_PAI_PAC);

    /// Verify that will not go down the dalloc / munmap route.
    JE_ASSERT(ehooks->dallocWillFail());

    edata->setZeroed(true);
    extentDallocWrapperFinish(tsdn, pac, ehooks, edata);
}

void extentDallocWrapper(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * edata)
{
    JE_ASSERT(edata->pai() == EXTENT_PAI_PAC);

    /// Avoid calling the default extent dalloc unless we have to.
    if (!ehooks->dallocWillFail())
    {
        /// Remove guard pages for dalloc / unmap.
        if (edata->guarded())
        {
            JE_ASSERT(ehooks->areDefault());
            sanUnguardPagesTwoSided(tsdn, ehooks, edata, pac->emap);
        }
        /// Deregister first to avoid a race with other allocating threads, and reregister if deallocation fails.
        extentDeregister(tsdn, pac, edata);
        if (!extentDallocWrapperTry(tsdn, pac, ehooks, edata))
            return;
        extentReregister(tsdn, pac, edata);
    }

    /// Try to decommit; purge if that fails.
    bool zeroed;
    if (!edata->committed())
        zeroed = true;
    else if (!extentDecommitWrapper(tsdn, ehooks, edata, 0, edata->size()))
        zeroed = true;
    else if (!ehooks->purgeForced(tsdn, edata->base(), edata->size(), 0, edata->size()))
        zeroed = true;
    else if (edata->state() == extent_state_muzzy || !ehooks->purgeLazy(tsdn, edata->base(), edata->size(), 0, edata->size()))
        zeroed = false;
    else
        zeroed = false;
    edata->setZeroed(zeroed);

    extentDallocWrapperFinish(tsdn, pac, ehooks, edata);
}

void extentDestroyWrapper(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * edata)
{
    JE_ASSERT(edata->base() != nullptr);
    JE_ASSERT(edata->size() != 0);
    JE_ASSERT(edata->state() == extent_state_retained || edata->state() == extent_state_active);
    JE_ASSERT(pac->emap->edataIsAcquired(tsdn, edata) || !config::debug);

    if (edata->guarded())
    {
        JE_ASSERT(opt.retain);
        sanUnguardPagesPreDestroy(tsdn, ehooks, edata, pac->emap);
    }
    edata->setAddr(edata->base());

    /// Try to destroy; silently fail otherwise.
    ehooks->destroy(tsdn, edata->base(), edata->size(), edata->committed());

    pac->edata_cache->put(tsdn, edata);
}

bool extentCommitWrapper(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length)
{
    return extentCommitImpl(tsdn, ehooks, edata, offset, length, /* growing_retained */ false);
}

bool extentPurgeLazyWrapper(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length)
{
    return extentPurgeLazyImpl(tsdn, ehooks, edata, offset, length, false);
}

bool extentPurgeForcedWrapper(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length)
{
    return extentPurgeForcedImpl(tsdn, ehooks, edata, offset, length, false);
}

Extent * extentSplitWrapper(
    ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * edata, size_t size_a, size_t size_b, bool holding_core_locks)
{
    return extentSplitImpl(tsdn, pac, ehooks, edata, size_a, size_b, holding_core_locks);
}

bool extentMergeWrapper(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * a, Extent * b)
{
    return extentMergeImpl(tsdn, pac, ehooks, a, b, /* holding_core_locks */ false);
}

bool extentCommitZero(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, bool commit, bool zero, bool growing_retained)
{
    if (commit && !edata->committed())
    {
        if (extentCommitImpl(tsdn, ehooks, edata, 0, edata->size(), growing_retained))
            return true;
    }
    if (zero && !edata->zeroed())
    {
        void * addr = edata->base();
        size_t size = edata->size();
        ehooks->zero(tsdn, addr, size);
    }
    return false;
}

bool extentBoot()
{
    static_assert(sizeof(SlabData) >= sizeof(ExtentProfInfo));
    /// DSS is dropped (`extent_dss_boot` only records `sbrk(0)`).
    return false;
}

size_t extentGdumpCurpages()
{
    return curpages.load(std::memory_order_relaxed);
}

size_t extentGdumpHighpages()
{
    return highpages.load(std::memory_order_relaxed);
}

}
