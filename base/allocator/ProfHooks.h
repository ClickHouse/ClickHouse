#pragma once

/// The profiling calls made by the allocation front-end (jemalloc: `prof_inlines.h`, the lookahead part of
/// `prof_externs.h`, and the prof-related globals read on the allocation paths).
///
/// The inline functions here are a faithful port of jemalloc's inline profiling logic. The out-of-line functions are
/// implemented by the profiling module (Prof.cpp, ProfData.cpp, ...; see Prof.h).

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

/// Initialized as `opt.prof_active`, and accessed via `profActiveGetUnlocked` (no locking on the fast path).
/// jemalloc: prof_active_state
extern constinit bool prof_active_state;

/// Initialized as `opt.lg_prof_sample`, and potentially modified during profiling resets.
/// jemalloc: lg_prof_sample
extern constinit size_t lg_prof_sample;

/// Profile dump interval, measured in bytes allocated (0: disabled). jemalloc: prof_interval
extern constinit uint64_t prof_interval;

/// jemalloc: PROF_SAMPLE_ALIGNMENT_MASK
inline constexpr size_t PROF_SAMPLE_ALIGNMENT_MASK = PROF_SAMPLE_ALIGNMENT - 1;

/// --- Out-of-line functions (Prof.cpp) ----------------------------------------------------------------------------------

/// The part of `prof_sample_should_skip` after the `sample_event` check: `tdata = prof_tdata_get(tsd, true)`;
/// returns `tdata == NULL || !tdata->active`.
bool profSampleShouldSkipSlow(ThreadState & tsd);

/// jemalloc: prof_tctx_create
ProfThreadContext * profTctxCreate(ThreadState & tsd);

/// jemalloc: prof_alloc_rollback
void profAllocRollback(ThreadState & tsd, ProfThreadContext * tctx);

/// jemalloc: prof_malloc_sample_object
void profMallocSampleObject(ThreadState & tsd, const void * ptr, size_t size, size_t usize, ProfThreadContext * tctx);

/// `profFreeSampledObject` (jemalloc: prof_free_sampled_object) is declared in Arena.h.
void profFreeSampledObject(ThreadState & tsd, const void * ptr, size_t usize, ProfInfo * prof_info);

/// The boot steps of the profiling module. `prof_boot0` is not needed (`opt.prof_prefix` is constant-initialized).
/// jemalloc: prof_boot1, prof_boot2 (returns true on error)
void profBoot1();
bool profBoot2(ThreadState & tsd, Base * base);

/// jemalloc: prof_prefork0, prof_prefork1, prof_postfork_parent, prof_postfork_child
void profPrefork0(ThreadState * tsdn);
void profPrefork1(ThreadState * tsdn);
void profPostforkParent(ThreadState * tsdn);
void profPostforkChild(ThreadState * tsdn);

/// --- Inline logic (prof_inlines.h, prof_externs.h) -------------------------------------------------------------------

/// jemalloc: prof_active_get_unlocked
JE_ALWAYS_INLINE bool profActiveGetUnlocked()
{
    /// If `opt.prof` is off, then `prof_active` must always be off.
    JE_ASSERT(opt.prof || !prof_active_state);
    /// Even if `opt.prof` is true, sampling can be temporarily disabled by setting `prof_active` to false. No locking
    /// is used when reading `prof_active` in the fast path, so there are no guarantees regarding how long it will take
    /// for all threads to notice state changes.
    return prof_active_state;
}

/// jemalloc: tsd_prof_sample_event_wait_get
JE_ALWAYS_INLINE uint64_t tsdProfSampleEventWaitGet(ThreadState & tsd)
{
    return tsd.te_data.alloc_wait[te_alloc_prof_sample];
}

/// Returns true if allocation of `usize` would go above the next trigger of the prof sample event (without advancing
/// the event counters). If so and `surplus` is not null, it receives the number of bytes beyond that trigger.
/// jemalloc: te_prof_sample_event_lookahead_surplus
JE_ALWAYS_INLINE bool teProfSampleEventLookaheadSurplus(ThreadState & tsd, size_t usize, size_t * surplus)
{
    if (surplus != nullptr)
    {
        /// A dead store: a valid surplus is strictly less than usize.
        *surplus = SIZE_MAX;
    }
    if (JE_UNLIKELY(!tsd.nominal() || tsd.reentrancyLevel() > 0))
        return false;
    /// The subtraction is intentionally susceptible to underflow.
    uint64_t accumbytes = tsd.thread_allocated + usize - tsd.thread_allocated_last_event;
    uint64_t sample_wait = tsdProfSampleEventWaitGet(tsd);
    if (accumbytes < sample_wait)
        return false;
    JE_ASSERT(accumbytes - sample_wait < uint64_t(usize));
    if (surplus != nullptr)
        *surplus = size_t(accumbytes - sample_wait);
    return true;
}

/// jemalloc: te_prof_sample_event_lookahead
JE_ALWAYS_INLINE bool teProfSampleEventLookahead(ThreadState & tsd, size_t usize)
{
    return teProfSampleEventLookaheadSurplus(tsd, usize, nullptr);
}

/// jemalloc: prof_info_get
JE_ALWAYS_INLINE void profInfoGet(ThreadState & tsd, const void * ptr, AllocContext * alloc_ctx, ProfInfo * prof_info)
{
    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(prof_info != nullptr);
    arenaProfInfoGet(tsd, ptr, alloc_ctx, prof_info, false);
}

/// jemalloc: prof_info_get_and_reset_recent
JE_ALWAYS_INLINE void profInfoGetAndResetRecent(ThreadState & tsd, const void * ptr, AllocContext * alloc_ctx, ProfInfo * prof_info)
{
    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(prof_info != nullptr);
    arenaProfInfoGet(tsd, ptr, alloc_ctx, prof_info, true);
}

/// jemalloc: prof_tctx_reset
JE_ALWAYS_INLINE void profTctxReset(ThreadState & tsd, const void * ptr, AllocContext * alloc_ctx)
{
    JE_ASSERT(ptr != nullptr);
    arenaProfTctxReset(tsd, ptr, alloc_ctx);
}

/// jemalloc: prof_tctx_reset_sampled
JE_ALWAYS_INLINE void profTctxResetSampled(ThreadState & tsd, const void * ptr)
{
    JE_ASSERT(ptr != nullptr);
    arenaProfTctxResetSampled(tsd, ptr);
}

/// jemalloc: prof_sample_should_skip
JE_ALWAYS_INLINE bool profSampleShouldSkip(ThreadState & tsd, bool sample_event)
{
    /// Fastpath: no need to load tdata.
    if (JE_LIKELY(!sample_event))
        return true;

    /// `sample_event` is always obtained from the thread event module, and whenever it's true, it means that the
    /// thread event module has already checked the reentrancy level.
    JE_ASSERT(tsd.reentrancyLevel() == 0);

    return profSampleShouldSkipSlow(tsd);
}

/// jemalloc: prof_alloc_prep
JE_ALWAYS_INLINE ProfThreadContext * profAllocPrep(ThreadState & tsd, bool prof_active, bool sample_event)
{
    if (!prof_active || JE_LIKELY(profSampleShouldSkip(tsd, sample_event)))
        return PROF_TCTX_SENTINEL;
    return profTctxCreate(tsd);
}

/// jemalloc: prof_malloc
JE_ALWAYS_INLINE void profMalloc(
    ThreadState & tsd, const void * ptr, size_t size, size_t usize, AllocContext * alloc_ctx, ProfThreadContext * tctx)
{
    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(usize == arenaSalloc(&tsd, ptr));

    if (JE_UNLIKELY(profTctxIsValid(tctx)))
        profMallocSampleObject(tsd, ptr, size, usize, tctx);
    else
        profTctxReset(tsd, ptr, alloc_ctx);
}

/// jemalloc: prof_realloc
JE_ALWAYS_INLINE void profRealloc(
    ThreadState & tsd,
    const void * ptr,
    size_t size,
    size_t usize,
    ProfThreadContext * tctx,
    bool prof_active,
    const void * old_ptr,
    size_t old_usize,
    ProfInfo * old_prof_info,
    bool sample_event)
{
    JE_ASSERT(ptr != nullptr || !profTctxIsValid(tctx));

    if (prof_active && ptr != nullptr)
    {
        JE_ASSERT(usize == arenaSalloc(&tsd, ptr));
        if (profSampleShouldSkip(tsd, sample_event))
        {
            /// Don't sample. The usize passed to `profAllocPrep` was larger than what actually got allocated, so a
            /// backtrace was captured for this allocation, even though its actual usize was insufficient to cross the
            /// sample threshold.
            profAllocRollback(tsd, tctx);
            tctx = PROF_TCTX_SENTINEL;
        }
    }

    bool sampled = profTctxIsValid(tctx);
    bool old_sampled = profTctxIsValid(old_prof_info->alloc_tctx);
    bool moved = (ptr != old_ptr);

    if (JE_UNLIKELY(sampled))
    {
        profMallocSampleObject(tsd, ptr, size, usize, tctx);
    }
    else if (moved)
    {
        profTctxReset(tsd, ptr, nullptr);
    }
    else if (JE_UNLIKELY(old_sampled))
    {
        /// `profTctxReset` would work for the !moved case as well, but `profTctxResetSampled` is slightly cheaper.
        profTctxResetSampled(tsd, ptr);
    }
    else
    {
        if constexpr (config::debug)
        {
            ProfInfo prof_info;
            profInfoGet(tsd, ptr, nullptr, &prof_info);
            JE_ASSERT(prof_info.alloc_tctx == PROF_TCTX_SENTINEL);
        }
    }

    /// The `profFreeSampledObject` call must come after the `profMallocSampleObject` call, because tctx and old_tctx
    /// may be the same, in which case reversing the call order could cause the tctx to be prematurely destroyed as a
    /// side effect of momentarily zeroed counters.
    if (JE_UNLIKELY(old_sampled))
        profFreeSampledObject(tsd, old_ptr, old_usize, old_prof_info);
}

/// Enforce alignment, so that sampled allocations can be identified without metadata lookup.
/// jemalloc: prof_sample_align
JE_ALWAYS_INLINE size_t profSampleAlign(size_t usize, size_t orig_align)
{
    JE_ASSERT(opt.prof);
    return (orig_align < PROF_SAMPLE_ALIGNMENT && (sz::canUseSlab(usize) || opt.cache_oblivious)) ? PROF_SAMPLE_ALIGNMENT
                                                                                                  : orig_align;
}

/// jemalloc: prof_sample_aligned
JE_ALWAYS_INLINE bool profSampleAligned(const void * ptr)
{
    return (reinterpret_cast<uintptr_t>(ptr) & PROF_SAMPLE_ALIGNMENT_MASK) == 0;
}

/// jemalloc: prof_free
JE_ALWAYS_INLINE void profFree(ThreadState & tsd, const void * ptr, size_t usize, AllocContext * alloc_ctx)
{
    ProfInfo prof_info;
    profInfoGetAndResetRecent(tsd, ptr, alloc_ctx, &prof_info);

    JE_ASSERT(usize == arenaSalloc(&tsd, ptr));

    if (JE_UNLIKELY(profTctxIsValid(prof_info.alloc_tctx)))
    {
        JE_ASSERT(profSampleAligned(ptr));
        profFreeSampledObject(tsd, ptr, usize, &prof_info);
    }
}

}
