/// Large allocations (jemalloc: `src/large.c`).

#include <allocator/Arena.h>

#include <allocator/ArenaInlines.h>
#include <allocator/Arenas.h>
#include <allocator/ExtentMap.h>
#include <allocator/Options.h>

#include <cstring>

namespace jemalloc
{

/// jemalloc: large_malloc
void * largeMalloc(ThreadState * tsdn, Arena * arena, size_t usize, bool zero)
{
    JE_ASSERT(usize == sz::s2u(usize));

    return largePalloc(tsdn, arena, usize, CACHELINE, zero);
}

/// jemalloc: large_palloc
void * largePalloc(ThreadState * tsdn, Arena * arena, size_t usize, size_t alignment, bool zero)
{
    Extent * edata;

    JE_ASSERT(tsdn != nullptr || arena != nullptr);

    size_t ausize = sz::sa2u(usize, alignment);
    if (JE_UNLIKELY(ausize == 0 || ausize > SC_LARGE_MAXCLASS))
        return nullptr;

    if (JE_LIKELY(tsdn != nullptr))
        arena = arenaChooseMaybeHuge(*tsdn, arena, usize);
    if (JE_UNLIKELY(arena == nullptr) || (edata = arenaExtentAllocLarge(tsdn, arena, usize, alignment, zero)) == nullptr)
        return nullptr;

    /// See comments in `Bin::slabsFullInsert`.
    if (!arenaIsAuto(arena))
    {
        /// Insert edata into large.
        arena->large_mtx.lock(tsdn);
        arena->large.append(edata);
        arena->large_mtx.unlock(tsdn);
    }

    arenaDecayTick(tsdn, arena);
    return edata->addr();
}

/// jemalloc: large_ralloc_no_move_shrink
static bool largeRallocNoMoveShrink(ThreadState * tsdn, Extent * edata, size_t usize)
{
    Arena * arena = arenaGetFromEdata(edata);
    ExtentHooks * ehooks = arenaGetEhooks(arena);
    size_t old_size = edata->size();
    size_t old_usize = edata->usize();

    JE_ASSERT(old_usize > usize);

    if (ehooks->splitWillFail())
        return true;

    bool deferred_work_generated = false;
    bool err = arena->pa_shard.shrink(tsdn, edata, old_size, usize + sz_large_pad, sz::sizeToIndex(usize), &deferred_work_generated);
    if (err)
        return true;
    if (deferred_work_generated)
        arenaHandleDeferredWork(tsdn, arena);
    arenaExtentRallocLargeShrink(tsdn, arena, edata, old_usize);

    return false;
}

/// jemalloc: large_ralloc_no_move_expand
static bool largeRallocNoMoveExpand(ThreadState * tsdn, Extent * edata, size_t usize, bool zero)
{
    Arena * arena = arenaGetFromEdata(edata);

    size_t old_size = edata->size();
    size_t old_usize = edata->usize();
    size_t new_size = usize + sz_large_pad;

    szind_t szind = sz::sizeToIndex(usize);

    bool deferred_work_generated = false;
    bool err = arena->pa_shard.expand(tsdn, edata, old_size, new_size, szind, zero, &deferred_work_generated);

    if (deferred_work_generated)
        arenaHandleDeferredWork(tsdn, arena);

    if (err)
        return true;

    if (zero)
    {
        if (opt.cache_oblivious)
        {
            JE_ASSERT(sz_large_pad == PAGE);
            /// Zero the trailing bytes of the original allocation's last page, since they are in an indeterminate
            /// state. There will always be trailing bytes, because ptr's offset from the beginning of the extent is a
            /// multiple of CACHELINE in [0 .. PAGE).
            std::byte * zbase = static_cast<std::byte *>(edata->addr()) + old_usize;
            std::byte * zpast = static_cast<std::byte *>(pageAddrToBase(zbase + PAGE));
            size_t nzero = size_t(zpast - zbase);
            JE_ASSERT(nzero > 0);
            memset(zbase, 0, nzero);
        }
    }
    arenaExtentRallocLargeExpand(tsdn, arena, edata, old_usize);

    return false;
}

/// jemalloc: large_ralloc_no_move
bool largeRallocNoMove(ThreadState * tsdn, Extent * edata, size_t usize_min, size_t usize_max, bool zero)
{
    size_t oldusize = edata->usize();

    /// The following should have been caught by callers.
    JE_ASSERT(usize_min > 0 && usize_max <= SC_LARGE_MAXCLASS);
    /// Both allocation sizes must be large to avoid a move.
    JE_ASSERT(oldusize >= SC_LARGE_MINCLASS && usize_max >= SC_LARGE_MINCLASS);

    if (usize_max > oldusize)
    {
        /// Attempt to expand the allocation in-place.
        if (!largeRallocNoMoveExpand(tsdn, edata, usize_max, zero))
        {
            arenaDecayTick(tsdn, arenaGetFromEdata(edata));
            return false;
        }
        /// Try again, this time with usize_min.
        /// jemalloc compatibility: the result of the second expansion attempt is inverted: when it FAILS (returns
        /// true), the reallocation is reported as done in place (returns false) although the extent was not resized;
        /// when it succeeds, we fall through to the checks below. Reproduced as is.
        if (usize_min < usize_max && usize_min > oldusize && largeRallocNoMoveExpand(tsdn, edata, usize_min, zero))
        {
            arenaDecayTick(tsdn, arenaGetFromEdata(edata));
            return false;
        }
    }

    /// Avoid moving the allocation if the existing extent size accommodates the new size.
    if (oldusize >= usize_min && oldusize <= usize_max)
    {
        arenaDecayTick(tsdn, arenaGetFromEdata(edata));
        return false;
    }

    /// Attempt to shrink the allocation in-place.
    if (oldusize > usize_max)
    {
        if (!largeRallocNoMoveShrink(tsdn, edata, usize_max))
        {
            arenaDecayTick(tsdn, arenaGetFromEdata(edata));
            return false;
        }
    }
    return true;
}

/// jemalloc: large_ralloc_move_helper
static void * largeRallocMoveHelper(ThreadState * tsdn, Arena * arena, size_t usize, size_t alignment, bool zero)
{
    if (alignment <= CACHELINE)
        return largeMalloc(tsdn, arena, usize, zero);
    return largePalloc(tsdn, arena, usize, alignment, zero);
}

/// jemalloc: large_ralloc
void * largeRalloc(ThreadState * tsdn, Arena * arena, void * ptr, size_t usize, size_t alignment, bool zero, ThreadCache * tcache)
{
    Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);

    size_t oldusize = edata->usize();
    /// The following should have been caught by callers.
    JE_ASSERT(usize > 0 && usize <= SC_LARGE_MAXCLASS);
    /// Both allocation sizes must be large to avoid a move.
    JE_ASSERT(oldusize >= SC_LARGE_MINCLASS && usize >= SC_LARGE_MINCLASS);

    /// Try to avoid moving the allocation.
    if (!largeRallocNoMove(tsdn, edata, usize, usize, zero))
    {
        /// hook_invoke_expand: hooks are dropped.
        return edata->addr();
    }

    /// usize and old size are different enough that we need to use a different size class. In that case, fall back
    /// to allocating new space and copying.
    void * ret = largeRallocMoveHelper(tsdn, arena, usize, alignment, zero);
    if (ret == nullptr)
        return nullptr;

    /// hook_invoke_alloc, hook_invoke_dalloc: hooks are dropped.

    size_t copysize = (usize < oldusize) ? usize : oldusize;
    memcpy(ret, edata->addr(), copysize);
    /// isdalloct(tsdn, edata_addr_get(edata), oldusize, tcache, NULL, true)
    arenaSdalloc(tsdn, edata->addr(), oldusize, tcache, nullptr, true);
    return ret;
}

/// `locked` indicates whether the arena's `large_mtx` is currently held.
/// jemalloc: large_dalloc_prep_impl
static void largeDallocPrepImpl(ThreadState * tsdn, Arena * arena, Extent * edata, bool locked)
{
    if (!locked)
    {
        /// See comments in `Bin::slabsFullInsert`.
        if (!arenaIsAuto(arena))
        {
            arena->large_mtx.lock(tsdn);
            arena->large.remove(edata);
            arena->large_mtx.unlock(tsdn);
        }
    }
    else
    {
        /// Only hold the large_mtx if necessary.
        if (!arenaIsAuto(arena))
        {
            arena->large_mtx.assertOwner(tsdn);
            arena->large.remove(edata);
        }
    }
    arenaExtentDallocLargePrep(tsdn, arena, edata);
}

/// jemalloc: large_dalloc_finish_impl
static void largeDallocFinishImpl(ThreadState * tsdn, Arena * arena, Extent * edata)
{
    bool deferred_work_generated = false;
    arena->pa_shard.dalloc(tsdn, edata, &deferred_work_generated);
    if (deferred_work_generated)
        arenaHandleDeferredWork(tsdn, arena);
}

/// jemalloc: large_dalloc_prep_locked
void largeDallocPrepLocked(ThreadState * tsdn, Extent * edata)
{
    largeDallocPrepImpl(tsdn, arenaGetFromEdata(edata), edata, true);
}

/// jemalloc: large_dalloc_finish
void largeDallocFinish(ThreadState * tsdn, Extent * edata)
{
    largeDallocFinishImpl(tsdn, arenaGetFromEdata(edata), edata);
}

/// jemalloc: large_dalloc
void largeDalloc(ThreadState * tsdn, Extent * edata)
{
    Arena * arena = arenaGetFromEdata(edata);
    largeDallocPrepImpl(tsdn, arena, edata, false);
    largeDallocFinishImpl(tsdn, arena, edata);
    arenaDecayTick(tsdn, arena);
}

/// jemalloc: large_prof_info_get
void largeProfInfoGet(ThreadState & tsd, Extent * edata, ProfInfo * prof_info, bool reset_recent)
{
    JE_ASSERT(prof_info != nullptr);

    ProfThreadContext * alloc_tctx = edata->profTctx();
    prof_info->alloc_tctx = alloc_tctx;

    if (profTctxIsValid(alloc_tctx))
    {
        prof_info->alloc_time.copy(*edata->profAllocTime());
        prof_info->alloc_size = edata->profAllocSize();
        if (reset_recent)
        {
            profFragUntrack(tsd, edata, alloc_tctx);
            /// Reset the pointer on the recent allocation record, so that this allocation is recorded as released.
            profRecentAllocReset(tsd, edata);
        }
    }
}

/// jemalloc: large_prof_tctx_set
static void largeProfTctxSet(Extent * edata, ProfThreadContext * tctx)
{
    edata->setProfTctx(tctx);
}

/// jemalloc: large_prof_tctx_reset
void largeProfTctxReset(Extent * edata)
{
    largeProfTctxSet(edata, PROF_TCTX_SENTINEL);
}

/// jemalloc: large_prof_info_set
void largeProfInfoSet(Extent * edata, ProfThreadContext * tctx, size_t size)
{
    NsTime t = NsTime::zero();
    t.profInitUpdate();
    edata->setProfAllocTime(&t);
    edata->setProfAllocSize(size);
    /// jemalloc: edata_prof_recent_alloc_init
    edata->setProfRecentAllocDontCallDirectly(nullptr);
    /// The flag may hold garbage from a previous (slab) use of this extent; it must be cleared before the tctx is
    /// published below, which is what makes the allocation reachable by `profFragUntrack`.
    edata->setProfFragTracked(false);
    largeProfTctxSet(edata, tctx);
}

}
