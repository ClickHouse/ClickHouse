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
void * largeMalloc(ThreadState * thread_state, Arena * arena, size_t usable_size, bool zero)
{
    ALLOCATOR_ASSERT(usable_size == size_classes::sizeToUsableSize(usable_size));

    return largeAllocateAligned(thread_state, arena, usable_size, CACHE_LINE, zero);
}

/// jemalloc: large_palloc
void * largeAllocateAligned(ThreadState * thread_state, Arena * arena, size_t usable_size, size_t alignment, bool zero)
{
    Extent * extent;

    ALLOCATOR_ASSERT(thread_state != nullptr || arena != nullptr);

    size_t aligned_usable_size = size_classes::alignedSizeToUsableSize(usable_size, alignment);
    if (ALLOCATOR_UNLIKELY(aligned_usable_size == 0 || aligned_usable_size > SIZE_CLASS_LARGE_MAX_CLASS))
        return nullptr;

    if (ALLOCATOR_LIKELY(thread_state != nullptr))
        arena = arenaChooseMaybeHuge(*thread_state, arena, usable_size);
    if (ALLOCATOR_UNLIKELY(arena == nullptr)
        || (extent = arenaExtentAllocLarge(thread_state, arena, usable_size, alignment, zero)) == nullptr)
        return nullptr;

    /// See comments in `Bin::slabsFullInsert`.
    if (!arenaIsAuto(arena))
    {
        /// Insert edata into large.
        arena->large_mutex.lock(thread_state);
        arena->large.append(extent);
        arena->large_mutex.unlock(thread_state);
    }

    arenaDecayTick(thread_state, arena);
    return extent->addr();
}

/// jemalloc: large_ralloc_no_move_shrink
static bool largeReallocateNoMoveShrink(ThreadState * thread_state, Extent * extent, size_t usable_size)
{
    Arena * arena = arenaGetFromExtent(extent);
    ExtentHooks * extent_hooks = arenaGetExtentHooks(arena);
    size_t old_size = extent->size();
    size_t old_usable_size = extent->usableSize();

    ALLOCATOR_ASSERT(old_usable_size > usable_size);

    if (extent_hooks->splitWillFail())
        return true;

    bool deferred_work_generated = false;
    bool error = arena->page_allocator_shard.shrink(
        thread_state, extent, old_size, usable_size + large_pad, size_classes::sizeToIndex(usable_size), &deferred_work_generated);
    if (error)
        return true;
    if (deferred_work_generated)
        arenaHandleDeferredWork(thread_state, arena);
    arenaExtentReallocateLargeShrink(thread_state, arena, extent, old_usable_size);

    return false;
}

/// jemalloc: large_ralloc_no_move_expand
static bool largeReallocateNoMoveExpand(ThreadState * thread_state, Extent * extent, size_t usable_size, bool zero)
{
    Arena * arena = arenaGetFromExtent(extent);

    size_t old_size = extent->size();
    size_t old_usable_size = extent->usableSize();
    size_t new_size = usable_size + large_pad;

    SizeClassIdx size_class_idx = size_classes::sizeToIndex(usable_size);

    bool deferred_work_generated = false;
    bool error
        = arena->page_allocator_shard.expand(thread_state, extent, old_size, new_size, size_class_idx, zero, &deferred_work_generated);

    if (deferred_work_generated)
        arenaHandleDeferredWork(thread_state, arena);

    if (error)
        return true;

    if (zero)
    {
        if (options.cache_oblivious)
        {
            ALLOCATOR_ASSERT(large_pad == PAGE);
            /// Zero the trailing bytes of the original allocation's last page, since they are in an indeterminate
            /// state. There will always be trailing bytes, because ptr's offset from the beginning of the extent is a
            /// multiple of CACHELINE in [0 .. PAGE).
            std::byte * zero_begin = static_cast<std::byte *>(extent->addr()) + old_usable_size;
            std::byte * zero_end = static_cast<std::byte *>(pageAddrToBase(zero_begin + PAGE));
            size_t zero_size = size_t(zero_end - zero_begin);
            ALLOCATOR_ASSERT(zero_size > 0);
            memset(zero_begin, 0, zero_size);
        }
    }
    arenaExtentReallocateLargeExpand(thread_state, arena, extent, old_usable_size);

    return false;
}

/// jemalloc: large_ralloc_no_move
bool largeReallocateNoMove(ThreadState * thread_state, Extent * extent, size_t usable_size_min, size_t usable_size_max, bool zero)
{
    size_t old_usable_size = extent->usableSize();

    /// The following should have been caught by callers.
    ALLOCATOR_ASSERT(usable_size_min > 0 && usable_size_max <= SIZE_CLASS_LARGE_MAX_CLASS);
    /// Both allocation sizes must be large to avoid a move.
    ALLOCATOR_ASSERT(old_usable_size >= SIZE_CLASS_LARGE_MIN_CLASS && usable_size_max >= SIZE_CLASS_LARGE_MIN_CLASS);

    if (usable_size_max > old_usable_size)
    {
        /// Attempt to expand the allocation in-place.
        if (!largeReallocateNoMoveExpand(thread_state, extent, usable_size_max, zero))
        {
            arenaDecayTick(thread_state, arenaGetFromExtent(extent));
            return false;
        }
        /// Try again, this time with usize_min.
        /// jemalloc compatibility: the result of the second expansion attempt is inverted: when it FAILS (returns
        /// true), the reallocation is reported as done in place (returns false) although the extent was not resized;
        /// when it succeeds, we fall through to the checks below. Reproduced as is.
        if (usable_size_min < usable_size_max && usable_size_min > old_usable_size
            && largeReallocateNoMoveExpand(thread_state, extent, usable_size_min, zero))
        {
            arenaDecayTick(thread_state, arenaGetFromExtent(extent));
            return false;
        }
    }

    /// Avoid moving the allocation if the existing extent size accommodates the new size.
    if (old_usable_size >= usable_size_min && old_usable_size <= usable_size_max)
    {
        arenaDecayTick(thread_state, arenaGetFromExtent(extent));
        return false;
    }

    /// Attempt to shrink the allocation in-place.
    if (old_usable_size > usable_size_max)
    {
        if (!largeReallocateNoMoveShrink(thread_state, extent, usable_size_max))
        {
            arenaDecayTick(thread_state, arenaGetFromExtent(extent));
            return false;
        }
    }
    return true;
}

/// jemalloc: large_ralloc_move_helper
static void * largeReallocateMoveHelper(ThreadState * thread_state, Arena * arena, size_t usable_size, size_t alignment, bool zero)
{
    if (alignment <= CACHE_LINE)
        return largeMalloc(thread_state, arena, usable_size, zero);
    return largeAllocateAligned(thread_state, arena, usable_size, alignment, zero);
}

/// jemalloc: large_ralloc
void * largeReallocate(
    ThreadState * thread_state, Arena * arena, void * ptr, size_t usable_size, size_t alignment, bool zero, ThreadCache * thread_cache)
{
    Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);

    size_t old_usable_size = extent->usableSize();
    /// The following should have been caught by callers.
    ALLOCATOR_ASSERT(usable_size > 0 && usable_size <= SIZE_CLASS_LARGE_MAX_CLASS);
    /// Both allocation sizes must be large to avoid a move.
    ALLOCATOR_ASSERT(old_usable_size >= SIZE_CLASS_LARGE_MIN_CLASS && usable_size >= SIZE_CLASS_LARGE_MIN_CLASS);

    /// Try to avoid moving the allocation.
    if (!largeReallocateNoMove(thread_state, extent, usable_size, usable_size, zero))
    {
        /// hook_invoke_expand: hooks are dropped.
        return extent->addr();
    }

    /// usize and old size are different enough that we need to use a different size class. In that case, fall back
    /// to allocating new space and copying.
    void * result = largeReallocateMoveHelper(thread_state, arena, usable_size, alignment, zero);
    if (result == nullptr)
        return nullptr;

    /// hook_invoke_alloc, hook_invoke_dalloc: hooks are dropped.

    size_t copy_size = (usable_size < old_usable_size) ? usable_size : old_usable_size;
    memcpy(result, extent->addr(), copy_size);
    /// isdalloct(tsdn, edata_addr_get(edata), oldusize, tcache, NULL, true)
    arenaSizedDeallocate(thread_state, extent->addr(), old_usable_size, thread_cache, nullptr, true);
    return result;
}

/// `locked` indicates whether the arena's `large_mutex` is currently held.
/// jemalloc: large_dalloc_prep_impl
static void largeDeallocatePrepareImpl(ThreadState * thread_state, Arena * arena, Extent * extent, bool locked)
{
    if (!locked)
    {
        /// See comments in `Bin::slabsFullInsert`.
        if (!arenaIsAuto(arena))
        {
            arena->large_mutex.lock(thread_state);
            arena->large.remove(extent);
            arena->large_mutex.unlock(thread_state);
        }
    }
    else
    {
        /// Only hold the large_mtx if necessary.
        if (!arenaIsAuto(arena))
        {
            arena->large_mutex.assertOwner(thread_state);
            arena->large.remove(extent);
        }
    }
    arenaExtentDeallocateLargePrepare(thread_state, arena, extent);
}

/// jemalloc: large_dalloc_finish_impl
static void largeDeallocateFinishImpl(ThreadState * thread_state, Arena * arena, Extent * extent)
{
    bool deferred_work_generated = false;
    arena->page_allocator_shard.deallocate(thread_state, extent, &deferred_work_generated);
    if (deferred_work_generated)
        arenaHandleDeferredWork(thread_state, arena);
}

/// jemalloc: large_dalloc_prep_locked
void largeDeallocatePrepareLocked(ThreadState * thread_state, Extent * extent)
{
    largeDeallocatePrepareImpl(thread_state, arenaGetFromExtent(extent), extent, true);
}

/// jemalloc: large_dalloc_finish
void largeDeallocateFinish(ThreadState * thread_state, Extent * extent)
{
    largeDeallocateFinishImpl(thread_state, arenaGetFromExtent(extent), extent);
}

/// jemalloc: large_dalloc
void largeDeallocate(ThreadState * thread_state, Extent * extent)
{
    Arena * arena = arenaGetFromExtent(extent);
    largeDeallocatePrepareImpl(thread_state, arena, extent, false);
    largeDeallocateFinishImpl(thread_state, arena, extent);
    arenaDecayTick(thread_state, arena);
}

/// jemalloc: large_prof_info_get
void largeProfilingInfoGet(ThreadState & thread_state, Extent * extent, ProfilingInfo * profiling_info, bool reset_recent)
{
    ALLOCATOR_ASSERT(profiling_info != nullptr);

    ProfilingThreadContext * alloc_thread_context = extent->profilingThreadContext();
    profiling_info->alloc_thread_context = alloc_thread_context;

    if (profilingThreadContextIsValid(alloc_thread_context))
    {
        profiling_info->alloc_time.copy(*extent->profilingAllocTime());
        profiling_info->alloc_size = extent->profilingAllocSize();
        if (reset_recent)
        {
            profilingFragmentationUntrack(thread_state, extent, alloc_thread_context);
            /// Reset the pointer on the recent allocation record, so that this allocation is recorded as released.
            profilingRecentAllocReset(thread_state, extent);
        }
    }
}

/// jemalloc: large_prof_tctx_set
static void largeProfilingThreadContextSet(Extent * extent, ProfilingThreadContext * thread_context)
{
    extent->setProfilingThreadContext(thread_context);
}

/// jemalloc: large_prof_tctx_reset
void largeProfilingThreadContextReset(Extent * extent)
{
    largeProfilingThreadContextSet(extent, PROFILING_THREAD_CONTEXT_SENTINEL);
}

/// jemalloc: large_prof_info_set
void largeProfilingInfoSet(Extent * extent, ProfilingThreadContext * thread_context, size_t size)
{
    Nanoseconds t = Nanoseconds::zero();
    t.profilingInitUpdate();
    extent->setProfilingAllocTime(&t);
    extent->setProfilingAllocSize(size);
    /// jemalloc: edata_prof_recent_alloc_init
    extent->setProfilingRecentAllocDontCallDirectly(nullptr);
    /// The flag may hold garbage from a previous (slab) use of this extent; it must be cleared before the tctx is
    /// published below, which is what makes the allocation reachable by `profilingFragmentationUntrack`.
    extent->setProfilingFragmentationTracked(false);
    largeProfilingThreadContextSet(extent, thread_context);
}

}
