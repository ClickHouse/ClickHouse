#pragma once

/// The profiling calls made by the allocation front-end (jemalloc: `prof_inlines.h`, the lookahead part of
/// `prof_externs.h`, and the prof-related globals read on the allocation paths).
///
/// The inline functions here are a faithful port of jemalloc's inline profiling logic. The out-of-line functions are
/// implemented by the profiling module (Profiling.cpp, ProfilingData.cpp, ...; see Profiling.h).

#include <allocator/Arena.h>
#include <allocator/ArenaInlines.h>
#include <allocator/Common.h>
#include <allocator/Options.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

#include <cstddef>
#include <cstdint>

namespace jemalloc
{

class Base;

/// --- Globals (prof.c) ---------------------------------------------------------------------------------------------

/// Initialized as `opt.prof_active`, and accessed via `profilingActiveGetUnlocked` (no locking on the fast path).
/// jemalloc: prof_active_state
extern constinit bool profiling_active_state;

/// Initialized as `opt.lg_prof_sample`, and potentially modified during profiling resets.
/// jemalloc: lg_prof_sample
extern constinit size_t log2_profiling_sample;

/// Profile dump interval, measured in bytes allocated (0: disabled). jemalloc: prof_interval
extern constinit uint64_t profiling_interval;

/// jemalloc: PROF_SAMPLE_ALIGNMENT_MASK
inline constexpr size_t PROFILING_SAMPLE_ALIGNMENT_MASK = PROFILING_SAMPLE_ALIGNMENT - 1;

/// --- Out-of-line functions (Profiling.cpp) ----------------------------------------------------------------------------------

/// The part of `prof_sample_should_skip` after the `sample_event` check: `thread_data = prof_tdata_get(thread_state, true)`;
/// returns `thread_data == NULL || !thread_data->active`.
bool profilingSampleShouldSkipSlow(ThreadState & thread_state);

/// jemalloc: prof_tctx_create
ProfilingThreadContext * profilingThreadContextCreate(ThreadState & thread_state);

/// jemalloc: prof_alloc_rollback
void profilingAllocRollback(ThreadState & thread_state, ProfilingThreadContext * thread_context);

/// jemalloc: prof_malloc_sample_object
void profilingMallocSampleObject(
    ThreadState & thread_state, const void * ptr, size_t size, size_t usable_size, ProfilingThreadContext * thread_context);

/// `profilingFreeSampledObject` (jemalloc: prof_free_sampled_object) is declared in Arena.h.
void profilingFreeSampledObject(ThreadState & thread_state, const void * ptr, size_t usable_size, ProfilingInfo * profiling_info);

/// The boot steps of the profiling module. `prof_boot0` is not needed (`opt.prof_prefix` is constant-initialized).
/// jemalloc: prof_boot1, prof_boot2 (returns true on error)
void profilingBoot1();
bool profilingBoot2(ThreadState & thread_state, Base * base);

/// jemalloc: prof_prefork0, prof_prefork1, prof_postfork_parent, prof_postfork_child
void profilingPrefork0(ThreadState * thread_state);
void profilingPrefork1(ThreadState * thread_state);
void profilingPostforkParent(ThreadState * thread_state);
void profilingPostforkChild(ThreadState * thread_state);

/// --- Inline logic (prof_inlines.h, prof_externs.h) -------------------------------------------------------------------

/// jemalloc: prof_active_get_unlocked
ALLOCATOR_ALWAYS_INLINE bool profilingActiveGetUnlocked()
{
    /// If `opt.prof` is off, then `profiling_active` must always be off.
    ALLOCATOR_ASSERT(options.profiling || !profiling_active_state);
    /// Even if `opt.prof` is true, sampling can be temporarily disabled by setting `profiling_active` to false. No locking
    /// is used when reading `profiling_active` in the fast path, so there are no guarantees regarding how long it will take
    /// for all threads to notice state changes.
    return profiling_active_state;
}

/// jemalloc: tsd_prof_sample_event_wait_get
ALLOCATOR_ALWAYS_INLINE uint64_t threadStateProfilingSampleEventWaitGet(ThreadState & thread_state)
{
    return thread_state.thread_event_data.alloc_wait[thread_event_allocation_profiling_sample];
}

/// Returns true if allocation of `usable_size` would go above the next trigger of the prof sample event (without advancing
/// the event counters). If so and `surplus` is not null, it receives the number of bytes beyond that trigger.
/// jemalloc: te_prof_sample_event_lookahead_surplus
ALLOCATOR_ALWAYS_INLINE bool
threadEventProfilingSampleEventLookaheadSurplus(ThreadState & thread_state, size_t usable_size, size_t * surplus)
{
    if (surplus != nullptr)
    {
        /// A dead store: a valid surplus is strictly less than usize.
        *surplus = SIZE_MAX;
    }
    if (ALLOCATOR_UNLIKELY(!thread_state.nominal() || thread_state.reentrancyLevel() > 0))
        return false;
    /// The subtraction is intentionally susceptible to underflow.
    uint64_t accumulated_bytes = thread_state.thread_allocated + usable_size - thread_state.thread_allocated_last_event;
    uint64_t sample_wait = threadStateProfilingSampleEventWaitGet(thread_state);
    if (accumulated_bytes < sample_wait)
        return false;
    ALLOCATOR_ASSERT(accumulated_bytes - sample_wait < uint64_t(usable_size));
    if (surplus != nullptr)
        *surplus = size_t(accumulated_bytes - sample_wait);
    return true;
}

/// jemalloc: te_prof_sample_event_lookahead
ALLOCATOR_ALWAYS_INLINE bool threadEventProfilingSampleEventLookahead(ThreadState & thread_state, size_t usable_size)
{
    return threadEventProfilingSampleEventLookaheadSurplus(thread_state, usable_size, nullptr);
}

/// jemalloc: prof_info_get
ALLOCATOR_ALWAYS_INLINE void
profilingInfoGet(ThreadState & thread_state, const void * ptr, AllocContext * alloc_context, ProfilingInfo * profiling_info)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(profiling_info != nullptr);
    arenaProfilingInfoGet(thread_state, ptr, alloc_context, profiling_info, false);
}

/// jemalloc: prof_info_get_and_reset_recent
ALLOCATOR_ALWAYS_INLINE void
profilingInfoGetAndResetRecent(ThreadState & thread_state, const void * ptr, AllocContext * alloc_context, ProfilingInfo * profiling_info)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(profiling_info != nullptr);
    arenaProfilingInfoGet(thread_state, ptr, alloc_context, profiling_info, true);
}

/// jemalloc: prof_tctx_reset
ALLOCATOR_ALWAYS_INLINE void profilingThreadContextReset(ThreadState & thread_state, const void * ptr, AllocContext * alloc_context)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    arenaProfilingThreadContextReset(thread_state, ptr, alloc_context);
}

/// jemalloc: prof_tctx_reset_sampled
ALLOCATOR_ALWAYS_INLINE void profilingThreadContextResetSampled(ThreadState & thread_state, const void * ptr)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    arenaProfilingThreadContextResetSampled(thread_state, ptr);
}

/// jemalloc: prof_sample_should_skip
ALLOCATOR_ALWAYS_INLINE bool profilingSampleShouldSkip(ThreadState & thread_state, bool sample_event)
{
    /// Fastpath: no need to load tdata.
    if (ALLOCATOR_LIKELY(!sample_event))
        return true;

    /// `sample_event` is always obtained from the thread event module, and whenever it's true, it means that the
    /// thread event module has already checked the reentrancy level.
    ALLOCATOR_ASSERT(thread_state.reentrancyLevel() == 0);

    return profilingSampleShouldSkipSlow(thread_state);
}

/// jemalloc: prof_alloc_prep
ALLOCATOR_ALWAYS_INLINE ProfilingThreadContext * profilingAllocPrepare(ThreadState & thread_state, bool profiling_active, bool sample_event)
{
    if (!profiling_active || ALLOCATOR_LIKELY(profilingSampleShouldSkip(thread_state, sample_event)))
        return PROFILING_THREAD_CONTEXT_SENTINEL;
    return profilingThreadContextCreate(thread_state);
}

/// jemalloc: prof_malloc
ALLOCATOR_ALWAYS_INLINE void profilingMalloc(
    ThreadState & thread_state,
    const void * ptr,
    size_t size,
    size_t usable_size,
    AllocContext * alloc_context,
    ProfilingThreadContext * thread_context)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(usable_size == arenaAllocationSize(&thread_state, ptr));

    if (ALLOCATOR_UNLIKELY(profilingThreadContextIsValid(thread_context)))
        profilingMallocSampleObject(thread_state, ptr, size, usable_size, thread_context);
    else
        profilingThreadContextReset(thread_state, ptr, alloc_context);
}

/// jemalloc: prof_realloc
ALLOCATOR_ALWAYS_INLINE void profilingRealloc(
    ThreadState & thread_state,
    const void * ptr,
    size_t size,
    size_t usable_size,
    ProfilingThreadContext * thread_context,
    bool profiling_active,
    const void * old_ptr,
    size_t old_usable_size,
    ProfilingInfo * old_profiling_info,
    bool sample_event)
{
    ALLOCATOR_ASSERT(ptr != nullptr || !profilingThreadContextIsValid(thread_context));

    if (profiling_active && ptr != nullptr)
    {
        ALLOCATOR_ASSERT(usable_size == arenaAllocationSize(&thread_state, ptr));
        if (profilingSampleShouldSkip(thread_state, sample_event))
        {
            /// Don't sample. The usize passed to `profilingAllocPrepare` was larger than what actually got allocated, so a
            /// backtrace was captured for this allocation, even though its actual usize was insufficient to cross the
            /// sample threshold.
            profilingAllocRollback(thread_state, thread_context);
            thread_context = PROFILING_THREAD_CONTEXT_SENTINEL;
        }
    }

    bool sampled = profilingThreadContextIsValid(thread_context);
    bool old_sampled = profilingThreadContextIsValid(old_profiling_info->alloc_thread_context);
    bool moved = (ptr != old_ptr);

    if (ALLOCATOR_UNLIKELY(sampled))
    {
        profilingMallocSampleObject(thread_state, ptr, size, usable_size, thread_context);
    }
    else if (moved)
    {
        profilingThreadContextReset(thread_state, ptr, nullptr);
    }
    else if (ALLOCATOR_UNLIKELY(old_sampled))
    {
        /// `profilingThreadContextReset` would work for the !moved case as well, but `profilingThreadContextResetSampled` is slightly
        /// cheaper.
        profilingThreadContextResetSampled(thread_state, ptr);
    }
    else
    {
        if constexpr (config::debug)
        {
            ProfilingInfo profiling_info;
            profilingInfoGet(thread_state, ptr, nullptr, &profiling_info);
            ALLOCATOR_ASSERT(profiling_info.alloc_thread_context == PROFILING_THREAD_CONTEXT_SENTINEL);
        }
    }

    /// The `profilingFreeSampledObject` call must come after the `profilingMallocSampleObject` call, because tctx and old_tctx
    /// may be the same, in which case reversing the call order could cause the tctx to be prematurely destroyed as a
    /// side effect of momentarily zeroed counters.
    if (ALLOCATOR_UNLIKELY(old_sampled))
        profilingFreeSampledObject(thread_state, old_ptr, old_usable_size, old_profiling_info);
}

/// Enforce alignment, so that sampled allocations can be identified without metadata lookup.
/// jemalloc: prof_sample_align
ALLOCATOR_ALWAYS_INLINE size_t profilingSampleAlign(size_t usable_size, size_t original_align)
{
    ALLOCATOR_ASSERT(options.profiling);
    return (original_align < PROFILING_SAMPLE_ALIGNMENT && (size_classes::canUseSlab(usable_size) || options.cache_oblivious))
        ? PROFILING_SAMPLE_ALIGNMENT
        : original_align;
}

/// jemalloc: prof_sample_aligned
ALLOCATOR_ALWAYS_INLINE bool profilingSampleAligned(const void * ptr)
{
    return (reinterpret_cast<uintptr_t>(ptr) & PROFILING_SAMPLE_ALIGNMENT_MASK) == 0;
}

/// jemalloc: prof_free
ALLOCATOR_ALWAYS_INLINE void profilingFree(ThreadState & thread_state, const void * ptr, size_t usable_size, AllocContext * alloc_context)
{
    ProfilingInfo profiling_info;
    profilingInfoGetAndResetRecent(thread_state, ptr, alloc_context, &profiling_info);

    ALLOCATOR_ASSERT(usable_size == arenaAllocationSize(&thread_state, ptr));

    if (ALLOCATOR_UNLIKELY(profilingThreadContextIsValid(profiling_info.alloc_thread_context)))
    {
        ALLOCATOR_ASSERT(profilingSampleAligned(ptr));
        profilingFreeSampledObject(thread_state, ptr, usable_size, &profiling_info);
    }
}

}
