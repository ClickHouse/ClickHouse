#pragma once

/// Thread events: every thread counts the bytes it allocates and deallocates, and runs event handlers when the counts
/// cross the next event threshold (jemalloc: `thread_event.h`, `src/thread_event.c`, `thread_event_registry.h/.c`,
/// `peak_event.h`, `src/peak_event.c`, `counter.h`, `src/counter.c`).
///
/// Events, in the order of jemalloc's handler tables under ClickHouse's configuration:
///   alloc:   prof_sample, stats_interval, tcache_gc, peak;
///   dalloc:  tcache_gc, peak.
/// When triggered, the handlers run in the order tcache_gc, prof_sample, stats_interval, peak. The user events
/// (`experimental.hooks.thread_event`) are dropped: no user event is ever installed.
///
/// The handlers of tcache GC, prof sampling and stats interval printing belong to other modules; their entry points
/// are declared here. The peak handler is implemented here.

#include <allocator/Common.h>
#include <allocator/SizeClassConstants.h>
#include <allocator/ThreadState.h>

#include <atomic>
#include <cstdint>

namespace jemalloc
{

/// Should not exceed the minimal allocation usize.
/// jemalloc: TE_MIN_START_WAIT, TE_MAX_START_WAIT
inline constexpr uint64_t THREAD_EVENT_MIN_START_WAIT = 1;
inline constexpr uint64_t THREAD_EVENT_MAX_START_WAIT = UINT64_MAX;

/// The maximum threshold on `thread_(de)allocated_next_event_fast`, so that there is no need to check overflow in the
/// malloc fast path (whose allocation size never exceeds `SIZE_CLASS_LOOKUP_MAX_CLASS`).
/// jemalloc: TE_NEXT_EVENT_FAST_MAX
inline constexpr uint64_t THREAD_EVENT_NEXT_EVENT_FAST_MAX = UINT64_MAX - SIZE_CLASS_LOOKUP_MAX_CLASS + 1;

/// Makes sure that malloc stays on the fast path in the common case (`thread_allocated <
/// thread_allocated_next_event_fast`): when `thread_allocated` is within an event's distance to
/// `THREAD_EVENT_NEXT_EVENT_FAST_MAX`, the fast threshold is 0 and the medium-fast path is taken; the max interval makes sure we
/// do not stay there for too long, even if there is no active event or all of them have long waits.
/// jemalloc: TE_MAX_INTERVAL
inline constexpr uint64_t THREAD_EVENT_MAX_INTERVAL = uint64_t(4) << 20;

/// Invalid elapsed time, for situations where elapsed time is not needed.
/// jemalloc: TE_INVALID_ELAPSED
inline constexpr uint64_t THREAD_EVENT_INVALID_ELAPSED = UINT64_MAX;

/// Update the peak every 64K. Not a configuration option.
/// jemalloc: PEAK_EVENT_WAIT
inline constexpr uint64_t PEAK_EVENT_WAIT = 64 * 1024;

/// jemalloc: te_ctx_t, te_ctx_get and the `te_ctx_*` accessors
struct ThreadEventContext
{
    bool is_alloc;
    uint64_t * current;
    uint64_t * last_event;
    uint64_t * next_event;
    uint64_t * next_event_fast;

    /// jemalloc: te_ctx_get
    static ALLOCATOR_ALWAYS_INLINE ThreadEventContext get(ThreadState & thread_state, bool is_alloc_)
    {
        if (is_alloc_)
            return {
                true,
                &thread_state.thread_allocated,
                &thread_state.thread_allocated_last_event,
                &thread_state.thread_allocated_next_event,
                &thread_state.thread_allocated_next_event_fast};
        return {
            false,
            &thread_state.thread_deallocated,
            &thread_state.thread_deallocated_last_event,
            &thread_state.thread_deallocated_next_event,
            &thread_state.thread_deallocated_next_event_fast};
    }

    /// jemalloc: te_ctx_is_alloc
    ALLOCATOR_ALWAYS_INLINE bool isAlloc() const { return is_alloc; }
    /// jemalloc: te_ctx_current_bytes_get, te_ctx_current_bytes_set
    ALLOCATOR_ALWAYS_INLINE uint64_t currentBytesGet() const { return *current; }
    ALLOCATOR_ALWAYS_INLINE void currentBytesSet(uint64_t v) { *current = v; }
    /// jemalloc: te_ctx_last_event_get, te_ctx_last_event_set
    ALLOCATOR_ALWAYS_INLINE uint64_t lastEventGet() const { return *last_event; }
    ALLOCATOR_ALWAYS_INLINE void lastEventSet(uint64_t v) { *last_event = v; }

    /// jemalloc: te_ctx_next_event_fast_get
    ALLOCATOR_ALWAYS_INLINE uint64_t nextEventFastGet() const
    {
        uint64_t v = std::atomic_ref<uint64_t>(*next_event_fast).load(std::memory_order_relaxed);
        ALLOCATOR_ASSERT(v <= THREAD_EVENT_NEXT_EVENT_FAST_MAX);
        return v;
    }

    /// jemalloc: te_ctx_next_event_fast_set
    ALLOCATOR_ALWAYS_INLINE void nextEventFastSet(uint64_t v)
    {
        ALLOCATOR_ASSERT(v <= THREAD_EVENT_NEXT_EVENT_FAST_MAX);
        std::atomic_ref<uint64_t>(*next_event_fast).store(v, std::memory_order_relaxed);
    }

    /// jemalloc: te_ctx_next_event_get
    ALLOCATOR_ALWAYS_INLINE uint64_t nextEventGet() const { return *next_event; }

    /// The setter also updates the fast thresholds.
    /// jemalloc: te_ctx_next_event_set
    ALLOCATOR_ALWAYS_INLINE void nextEventSet(ThreadState & thread_state, uint64_t v);
};

/// jemalloc: te_assert_invariants_debug
void threadEventAssertInvariantsDebug(ThreadState & thread_state);
/// Handles the events when the counter of `context` has crossed `next_event`.
/// jemalloc: te_event_trigger
void threadEventEventTrigger(ThreadState & thread_state, ThreadEventContext & context);
/// jemalloc: te_recompute_fast_threshold
void threadEventRecomputeFastThreshold(ThreadState & thread_state);
/// Starts the events from a clean state (called on TSD initialization, after the PRNG is seeded).
/// jemalloc: tsd_te_init
void threadStateThreadEventInit(ThreadState & thread_state);
/// jemalloc: te_adjust_thresholds_helper
void threadEventAdjustThresholdsHelper(ThreadState & thread_state, ThreadEventContext & context, uint64_t wait);

ALLOCATOR_ALWAYS_INLINE void ThreadEventContext::nextEventSet(ThreadState & thread_state, uint64_t v)
{
    *next_event = v;
    threadEventRecomputeFastThreshold(thread_state);
}

/// --- Counters (jemalloc: `ITERATE_OVER_ALL_COUNTERS`) ---------------------------------------------------------------
/// The setters write through the pointers (not the TSD setters), so that the counters can be modified even when the
/// TSD is reincarnated or minimal_initialized: an event triggered then is delayed to the next allocation.

/// jemalloc: thread_allocated_get, thread_allocated_last_event_get, prof_sample_last_event_get,
/// stats_interval_last_event_get (and the `_set` versions)
ALLOCATOR_ALWAYS_INLINE uint64_t threadAllocatedGet(ThreadState & thread_state)
{
    return thread_state.thread_allocated;
}
ALLOCATOR_ALWAYS_INLINE void threadAllocatedSet(ThreadState & thread_state, uint64_t v)
{
    thread_state.thread_allocated = v;
}
ALLOCATOR_ALWAYS_INLINE uint64_t threadAllocatedLastEventGet(ThreadState & thread_state)
{
    return thread_state.thread_allocated_last_event;
}
ALLOCATOR_ALWAYS_INLINE void threadAllocatedLastEventSet(ThreadState & thread_state, uint64_t v)
{
    thread_state.thread_allocated_last_event = v;
}
ALLOCATOR_ALWAYS_INLINE uint64_t profilingSampleLastEventGet(ThreadState & thread_state)
{
    return thread_state.profiling_sample_last_event;
}
ALLOCATOR_ALWAYS_INLINE void profilingSampleLastEventSet(ThreadState & thread_state, uint64_t v)
{
    thread_state.profiling_sample_last_event = v;
}
ALLOCATOR_ALWAYS_INLINE uint64_t statsIntervalLastEventGet(ThreadState & thread_state)
{
    return thread_state.stats_interval_last_event;
}
ALLOCATOR_ALWAYS_INLINE void statsIntervalLastEventSet(ThreadState & thread_state, uint64_t v)
{
    thread_state.stats_interval_last_event = v;
}

/// The malloc and free fast path getters: the TSD may be non-nominal, in which case the fast threshold is 0. This
/// allows checking for events and a non-nominal TSD in a single branch. Only for the fast paths.
/// jemalloc: te_malloc_fastpath_ctx
ALLOCATOR_ALWAYS_INLINE void threadEventMallocFastPathContext(ThreadState & thread_state, uint64_t & allocated, uint64_t & threshold)
{
    allocated = thread_state.thread_allocated;
    threshold = std::atomic_ref<uint64_t>(thread_state.thread_allocated_next_event_fast).load(std::memory_order_relaxed);
    ALLOCATOR_ASSERT(threshold <= THREAD_EVENT_NEXT_EVENT_FAST_MAX);
}

/// This may happen before the TSD is initialized.
/// jemalloc: te_free_fastpath_ctx
ALLOCATOR_ALWAYS_INLINE void threadEventFreeFastPathContext(ThreadState & thread_state, uint64_t & deallocated, uint64_t & threshold)
{
    deallocated = thread_state.thread_deallocated;
    threshold = std::atomic_ref<uint64_t>(thread_state.thread_deallocated_next_event_fast).load(std::memory_order_relaxed);
    ALLOCATOR_ASSERT(threshold <= THREAD_EVENT_NEXT_EVENT_FAST_MAX);
}

/// Sets the fast thresholds to zero when the TSD is non-nominal. May be called during TSD init and cleanup, and from
/// other threads (`ThreadState::globalSlowIncrement`).
/// jemalloc: te_next_event_fast_set_non_nominal
ALLOCATOR_ALWAYS_INLINE void threadEventNextEventFastSetNonNominal(ThreadState & thread_state)
{
    std::atomic_ref<uint64_t>(thread_state.thread_allocated_next_event_fast).store(0, std::memory_order_relaxed);
    std::atomic_ref<uint64_t>(thread_state.thread_deallocated_next_event_fast).store(0, std::memory_order_relaxed);
}

/// Checks in debug mode whether the event counters are in a consistent state (the invariants before and after each
/// round of event handling).
/// jemalloc: te_assert_invariants
ALLOCATOR_ALWAYS_INLINE void threadEventAssertInvariants(ThreadState & thread_state)
{
    if constexpr (config::debug)
        threadEventAssertInvariantsDebug(thread_state);
}

/// jemalloc: te_event_advance
ALLOCATOR_ALWAYS_INLINE void threadEventEventAdvance(ThreadState & thread_state, size_t usable_size, bool is_alloc)
{
    threadEventAssertInvariants(thread_state);

    ThreadEventContext context = ThreadEventContext::get(thread_state, is_alloc);

    uint64_t bytes_before = context.currentBytesGet();
    context.currentBytesSet(bytes_before + usable_size);

    /// The subtraction is intentionally susceptible to underflow.
    if (ALLOCATOR_LIKELY(usable_size < context.nextEventGet() - bytes_before))
        threadEventAssertInvariants(thread_state);
    else
        threadEventEventTrigger(thread_state, context);
}

/// jemalloc: thread_dalloc_event
ALLOCATOR_ALWAYS_INLINE void threadDeallocationEvent(ThreadState & thread_state, size_t usable_size)
{
    threadEventEventAdvance(thread_state, usable_size, false);
}

/// jemalloc: thread_alloc_event
ALLOCATOR_ALWAYS_INLINE void threadAllocationEvent(ThreadState & thread_state, size_t usable_size)
{
    threadEventEventAdvance(thread_state, usable_size, true);
}

/// --- Counter accumulation (counter.h) ---------------------------------------------------------------------------

/// Accumulates bytes and reports when an interval is crossed (prof idump, stats interval). 64-bit atomics are
/// available on every platform, so there is no mutex (`LOCKEDINT_MTX_*` are no-ops).
/// jemalloc: counter_accum_t
class CounterAccumulated
{
public:
    /// jemalloc: locked_u64_t accumbytes
    std::atomic<uint64_t> accumulated_bytes{0};
    uint64_t interval = 0;

    constexpr CounterAccumulated() = default;

    /// Returns true on error (never).
    /// jemalloc: counter_accum_init
    bool init(uint64_t interval_);

    /// If the event moves fast enough (and/or the event handling is slow enough), extreme overflow can cause counter
    /// trigger coalescing. This is an intentional mechanism that avoids rate-limiting allocation.
    /// jemalloc: counter_accum, locked_inc_mod_u64
    ALLOCATOR_ALWAYS_INLINE bool accumulate(ThreadState * /*tsdn*/, uint64_t bytes)
    {
        uint64_t modulus = interval;
        ALLOCATOR_ASSERT(modulus > 0);
        uint64_t before = accumulated_bytes.load(std::memory_order_relaxed);
        uint64_t after;
        bool overflow;
        do
        {
            after = before + bytes;
            ALLOCATOR_ASSERT(after >= before);
            overflow = (after >= modulus);
            if (overflow)
                after %= modulus;
        } while (!accumulated_bytes.compare_exchange_weak(before, after, std::memory_order_relaxed, std::memory_order_relaxed));
        return overflow;
    }

    /// jemalloc: counter_prefork, counter_postfork_parent, counter_postfork_child (no-ops with 64-bit atomics)
    void prefork(ThreadState * /*tsdn*/) { }
    void postforkParent(ThreadState * /*tsdn*/) { }
    void postforkChild(ThreadState * /*tsdn*/) { }
};

/// --- Peak (peak_event.c) ----------------------------------------------------------------------------------------

/// Updates the peak with the current TSD state. jemalloc: peak_event_update
void peakEventUpdate(ThreadState & thread_state);
/// Sets the current state to zero. jemalloc: peak_event_zero
void peakEventZero(ThreadState & thread_state);
/// jemalloc: peak_event_max
uint64_t peakEventMax(ThreadState & thread_state);
/// jemalloc: peak_event_new_event_wait, peak_event_postponed_event_wait
uint64_t peakEventNewEventWait(ThreadState & thread_state);
uint64_t peakEventPostponedEventWait(ThreadState & thread_state);
/// Updates the peak and calls the activity callback. jemalloc: peak_event_handler
void peakEvent(ThreadState & thread_state);

/// --- Handlers of other modules -----------------------------------------------------------------------------------
/// Defined by the owning modules: ThreadCache.cpp (tcache GC), Profiling.cpp (prof sampling), StatsFrontend.cpp (stats
/// interval).

/// jemalloc: tcache_gc_new_event_wait, tcache_gc_postponed_event_wait, tcache_gc_event (`tcache.c`)
uint64_t threadCacheGCNewEventWait(ThreadState & thread_state);
uint64_t threadCacheGCPostponedEventWait(ThreadState & thread_state);
void threadCacheGCEvent(ThreadState & thread_state);

/// The postponed wait of prof sampling is computed as a new wait (to avoid sampling bias).
/// jemalloc: prof_sample_new_event_wait, prof_sample_event_handler (`prof.c`)
uint64_t profilingSampleNewEventWait(ThreadState & thread_state);
uint64_t profilingSamplePostponedEventWait(ThreadState & thread_state);
void profilingSampleEvent(ThreadState & thread_state);

/// jemalloc: stats_interval_new_event_wait, stats_interval_postponed_event_wait, stats_interval_event_handler
/// (`stats.c`)
uint64_t statsIntervalNewEventWait(ThreadState & thread_state);
uint64_t statsIntervalPostponedEventWait(ThreadState & thread_state);
void statsIntervalEvent(ThreadState & thread_state);

/// The wait of the stats interval event, set by `stats_boot`.
/// jemalloc: stats_interval_accum_batch (`stats.c`)
extern uint64_t stats_interval_accumulated_batch;

}
