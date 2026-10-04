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
inline constexpr uint64_t TE_MIN_START_WAIT = 1;
inline constexpr uint64_t TE_MAX_START_WAIT = UINT64_MAX;

/// The maximum threshold on `thread_(de)allocated_next_event_fast`, so that there is no need to check overflow in the
/// malloc fast path (whose allocation size never exceeds `SC_LOOKUP_MAXCLASS`).
/// jemalloc: TE_NEXT_EVENT_FAST_MAX
inline constexpr uint64_t TE_NEXT_EVENT_FAST_MAX = UINT64_MAX - SC_LOOKUP_MAXCLASS + 1;

/// Makes sure that malloc stays on the fast path in the common case (`thread_allocated <
/// thread_allocated_next_event_fast`): when `thread_allocated` is within an event's distance to
/// `TE_NEXT_EVENT_FAST_MAX`, the fast threshold is 0 and the medium-fast path is taken; the max interval makes sure we
/// do not stay there for too long, even if there is no active event or all of them have long waits.
/// jemalloc: TE_MAX_INTERVAL
inline constexpr uint64_t TE_MAX_INTERVAL = uint64_t(4) << 20;

/// Invalid elapsed time, for situations where elapsed time is not needed.
/// jemalloc: TE_INVALID_ELAPSED
inline constexpr uint64_t TE_INVALID_ELAPSED = UINT64_MAX;

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
    static JE_ALWAYS_INLINE ThreadEventContext get(ThreadState & tsd, bool is_alloc_)
    {
        if (is_alloc_)
            return {true, &tsd.thread_allocated, &tsd.thread_allocated_last_event, &tsd.thread_allocated_next_event,
                    &tsd.thread_allocated_next_event_fast};
        return {false, &tsd.thread_deallocated, &tsd.thread_deallocated_last_event, &tsd.thread_deallocated_next_event,
                &tsd.thread_deallocated_next_event_fast};
    }

    /// jemalloc: te_ctx_is_alloc
    JE_ALWAYS_INLINE bool isAlloc() const { return is_alloc; }
    /// jemalloc: te_ctx_current_bytes_get, te_ctx_current_bytes_set
    JE_ALWAYS_INLINE uint64_t currentBytesGet() const { return *current; }
    JE_ALWAYS_INLINE void currentBytesSet(uint64_t v) { *current = v; }
    /// jemalloc: te_ctx_last_event_get, te_ctx_last_event_set
    JE_ALWAYS_INLINE uint64_t lastEventGet() const { return *last_event; }
    JE_ALWAYS_INLINE void lastEventSet(uint64_t v) { *last_event = v; }

    /// jemalloc: te_ctx_next_event_fast_get
    JE_ALWAYS_INLINE uint64_t nextEventFastGet() const
    {
        uint64_t v = *next_event_fast;
        JE_ASSERT(v <= TE_NEXT_EVENT_FAST_MAX);
        return v;
    }

    /// jemalloc: te_ctx_next_event_fast_set
    JE_ALWAYS_INLINE void nextEventFastSet(uint64_t v)
    {
        JE_ASSERT(v <= TE_NEXT_EVENT_FAST_MAX);
        *next_event_fast = v;
    }

    /// jemalloc: te_ctx_next_event_get
    JE_ALWAYS_INLINE uint64_t nextEventGet() const { return *next_event; }

    /// The setter also updates the fast thresholds.
    /// jemalloc: te_ctx_next_event_set
    JE_ALWAYS_INLINE void nextEventSet(ThreadState & tsd, uint64_t v);
};

/// jemalloc: te_assert_invariants_debug
void teAssertInvariantsDebug(ThreadState & tsd);
/// Handles the events when the counter of `ctx` has crossed `next_event`.
/// jemalloc: te_event_trigger
void teEventTrigger(ThreadState & tsd, ThreadEventContext & ctx);
/// jemalloc: te_recompute_fast_threshold
void teRecomputeFastThreshold(ThreadState & tsd);
/// Starts the events from a clean state (called on TSD initialization, after the PRNG is seeded).
/// jemalloc: tsd_te_init
void tsdTeInit(ThreadState & tsd);
/// jemalloc: te_adjust_thresholds_helper
void teAdjustThresholdsHelper(ThreadState & tsd, ThreadEventContext & ctx, uint64_t wait);

JE_ALWAYS_INLINE void ThreadEventContext::nextEventSet(ThreadState & tsd, uint64_t v)
{
    *next_event = v;
    teRecomputeFastThreshold(tsd);
}

/// --- Counters (jemalloc: `ITERATE_OVER_ALL_COUNTERS`) ---------------------------------------------------------------
/// The setters write through the pointers (not the TSD setters), so that the counters can be modified even when the
/// TSD is reincarnated or minimal_initialized: an event triggered then is delayed to the next allocation.

/// jemalloc: thread_allocated_get, thread_allocated_last_event_get, prof_sample_last_event_get,
/// stats_interval_last_event_get (and the `_set` versions)
JE_ALWAYS_INLINE uint64_t threadAllocatedGet(ThreadState & tsd) { return tsd.thread_allocated; }
JE_ALWAYS_INLINE void threadAllocatedSet(ThreadState & tsd, uint64_t v) { tsd.thread_allocated = v; }
JE_ALWAYS_INLINE uint64_t threadAllocatedLastEventGet(ThreadState & tsd) { return tsd.thread_allocated_last_event; }
JE_ALWAYS_INLINE void threadAllocatedLastEventSet(ThreadState & tsd, uint64_t v) { tsd.thread_allocated_last_event = v; }
JE_ALWAYS_INLINE uint64_t profSampleLastEventGet(ThreadState & tsd) { return tsd.prof_sample_last_event; }
JE_ALWAYS_INLINE void profSampleLastEventSet(ThreadState & tsd, uint64_t v) { tsd.prof_sample_last_event = v; }
JE_ALWAYS_INLINE uint64_t statsIntervalLastEventGet(ThreadState & tsd) { return tsd.stats_interval_last_event; }
JE_ALWAYS_INLINE void statsIntervalLastEventSet(ThreadState & tsd, uint64_t v) { tsd.stats_interval_last_event = v; }

/// The malloc and free fast path getters: the TSD may be non-nominal, in which case the fast threshold is 0. This
/// allows checking for events and a non-nominal TSD in a single branch. Only for the fast paths.
/// jemalloc: te_malloc_fastpath_ctx
JE_ALWAYS_INLINE void teMallocFastpathCtx(ThreadState & tsd, uint64_t & allocated, uint64_t & threshold)
{
    allocated = tsd.thread_allocated;
    threshold = tsd.thread_allocated_next_event_fast;
    JE_ASSERT(threshold <= TE_NEXT_EVENT_FAST_MAX);
}

/// This may happen before the TSD is initialized.
/// jemalloc: te_free_fastpath_ctx
JE_ALWAYS_INLINE void teFreeFastpathCtx(ThreadState & tsd, uint64_t & deallocated, uint64_t & threshold)
{
    deallocated = tsd.thread_deallocated;
    threshold = tsd.thread_deallocated_next_event_fast;
    JE_ASSERT(threshold <= TE_NEXT_EVENT_FAST_MAX);
}

/// Sets the fast thresholds to zero when the TSD is non-nominal. May be called during TSD init and cleanup, and from
/// other threads (`ThreadState::globalSlowInc`).
/// jemalloc: te_next_event_fast_set_non_nominal
JE_ALWAYS_INLINE void teNextEventFastSetNonNominal(ThreadState & tsd)
{
    tsd.thread_allocated_next_event_fast = 0;
    tsd.thread_deallocated_next_event_fast = 0;
}

/// Checks in debug mode whether the event counters are in a consistent state (the invariants before and after each
/// round of event handling).
/// jemalloc: te_assert_invariants
JE_ALWAYS_INLINE void teAssertInvariants(ThreadState & tsd)
{
    if constexpr (config::debug)
        teAssertInvariantsDebug(tsd);
}

/// jemalloc: te_event_advance
JE_ALWAYS_INLINE void teEventAdvance(ThreadState & tsd, size_t usize, bool is_alloc)
{
    teAssertInvariants(tsd);

    ThreadEventContext ctx = ThreadEventContext::get(tsd, is_alloc);

    uint64_t bytes_before = ctx.currentBytesGet();
    ctx.currentBytesSet(bytes_before + usize);

    /// The subtraction is intentionally susceptible to underflow.
    if (JE_LIKELY(usize < ctx.nextEventGet() - bytes_before))
        teAssertInvariants(tsd);
    else
        teEventTrigger(tsd, ctx);
}

/// jemalloc: thread_dalloc_event
JE_ALWAYS_INLINE void threadDallocEvent(ThreadState & tsd, size_t usize)
{
    teEventAdvance(tsd, usize, false);
}

/// jemalloc: thread_alloc_event
JE_ALWAYS_INLINE void threadAllocEvent(ThreadState & tsd, size_t usize)
{
    teEventAdvance(tsd, usize, true);
}

/// --- Counter accumulation (counter.h) ---------------------------------------------------------------------------

/// Accumulates bytes and reports when an interval is crossed (prof idump, stats interval). 64-bit atomics are
/// available on every platform, so there is no mutex (`LOCKEDINT_MTX_*` are no-ops).
/// jemalloc: counter_accum_t
class CounterAccum
{
public:
    /// jemalloc: locked_u64_t accumbytes
    std::atomic<uint64_t> accumbytes{0};
    uint64_t interval = 0;

    constexpr CounterAccum() = default;

    /// Returns true on error (never).
    /// jemalloc: counter_accum_init
    bool init(uint64_t interval_);

    /// If the event moves fast enough (and/or the event handling is slow enough), extreme overflow can cause counter
    /// trigger coalescing. This is an intentional mechanism that avoids rate-limiting allocation.
    /// jemalloc: counter_accum, locked_inc_mod_u64
    JE_ALWAYS_INLINE bool accum(ThreadState * /*tsdn*/, uint64_t bytes)
    {
        uint64_t modulus = interval;
        JE_ASSERT(modulus > 0);
        uint64_t before = accumbytes.load(std::memory_order_relaxed);
        uint64_t after;
        bool overflow;
        do
        {
            after = before + bytes;
            JE_ASSERT(after >= before);
            overflow = (after >= modulus);
            if (overflow)
                after %= modulus;
        } while (!accumbytes.compare_exchange_weak(before, after, std::memory_order_relaxed, std::memory_order_relaxed));
        return overflow;
    }

    /// jemalloc: counter_prefork, counter_postfork_parent, counter_postfork_child (no-ops with 64-bit atomics)
    void prefork(ThreadState * /*tsdn*/) {}
    void postforkParent(ThreadState * /*tsdn*/) {}
    void postforkChild(ThreadState * /*tsdn*/) {}
};

/// --- Peak (peak_event.c) ----------------------------------------------------------------------------------------

/// Updates the peak with the current TSD state. jemalloc: peak_event_update
void peakEventUpdate(ThreadState & tsd);
/// Sets the current state to zero. jemalloc: peak_event_zero
void peakEventZero(ThreadState & tsd);
/// jemalloc: peak_event_max
uint64_t peakEventMax(ThreadState & tsd);
/// jemalloc: peak_event_new_event_wait, peak_event_postponed_event_wait
uint64_t peakEventNewEventWait(ThreadState & tsd);
uint64_t peakEventPostponedEventWait(ThreadState & tsd);
/// Updates the peak and calls the activity callback. jemalloc: peak_event_handler
void peakEvent(ThreadState & tsd);

/// --- Handlers of other modules -----------------------------------------------------------------------------------
/// Defined by the owning modules: ThreadCache.cpp (tcache GC), Prof.cpp (prof sampling), StatsFrontend.cpp (stats
/// interval).

/// jemalloc: tcache_gc_new_event_wait, tcache_gc_postponed_event_wait, tcache_gc_event (`tcache.c`)
uint64_t tcacheGcNewEventWait(ThreadState & tsd);
uint64_t tcacheGcPostponedEventWait(ThreadState & tsd);
void tcacheGcEvent(ThreadState & tsd);

/// The postponed wait of prof sampling is computed as a new wait (to avoid sampling bias).
/// jemalloc: prof_sample_new_event_wait, prof_sample_event_handler (`prof.c`)
uint64_t profSampleNewEventWait(ThreadState & tsd);
uint64_t profSamplePostponedEventWait(ThreadState & tsd);
void profSampleEvent(ThreadState & tsd);

/// jemalloc: stats_interval_new_event_wait, stats_interval_postponed_event_wait, stats_interval_event_handler
/// (`stats.c`)
uint64_t statsIntervalNewEventWait(ThreadState & tsd);
uint64_t statsIntervalPostponedEventWait(ThreadState & tsd);
void statsIntervalEvent(ThreadState & tsd);

/// The wait of the stats interval event, set by `stats_boot`.
/// jemalloc: stats_interval_accum_batch (`stats.c`)
extern uint64_t stats_interval_accum_batch;

}
