#include <allocator/ThreadEvent.h>

#include <allocator/Format.h>
#include <allocator/Options.h>

#include <array>
#include <cstdlib>

namespace jemalloc
{

namespace
{

/// The events of the handler tables (jemalloc: `te_alloc_handlers`, `te_dalloc_handlers`, `te_base_cb_t`). jemalloc
/// calls the handlers through function pointers; here the table is fixed, so they are called directly.
enum class ThreadEventHandler : uint8_t
{
    ProfilingSample,
    StatsInterval,
    ThreadCacheGC,
    Peak,
};

/// jemalloc: prof_sample_enabled, stats_interval_enabled, tcache_gc_enabled, peak_event_enabled (`te_enabled_yes`)
bool threadEventHandlerEnabled(ThreadEventHandler handler)
{
    switch (handler)
    {
        case ThreadEventHandler::ProfilingSample: return config::profiling && options.profiling;
        case ThreadEventHandler::StatsInterval: return options.stats_interval >= 0;
        case ThreadEventHandler::ThreadCacheGC: return options.thread_cache_gc_increment_bytes > 0;
        case ThreadEventHandler::Peak: return config::stats;
    }
    ALLOCATOR_NOT_REACHED();
}

/// jemalloc: te_base_cb_t::new_event_wait
uint64_t threadEventHandlerNewEventWait(ThreadState & thread_state, ThreadEventHandler handler)
{
    switch (handler)
    {
        case ThreadEventHandler::ProfilingSample: return profilingSampleNewEventWait(thread_state);
        case ThreadEventHandler::StatsInterval: return statsIntervalNewEventWait(thread_state);
        case ThreadEventHandler::ThreadCacheGC: return threadCacheGCNewEventWait(thread_state);
        case ThreadEventHandler::Peak: return peakEventNewEventWait(thread_state);
    }
    ALLOCATOR_NOT_REACHED();
}

/// jemalloc: te_base_cb_t::postponed_event_wait
uint64_t threadEventHandlerPostponedEventWait(ThreadState & thread_state, ThreadEventHandler handler)
{
    switch (handler)
    {
        case ThreadEventHandler::ProfilingSample: return profilingSamplePostponedEventWait(thread_state);
        case ThreadEventHandler::StatsInterval: return statsIntervalPostponedEventWait(thread_state);
        case ThreadEventHandler::ThreadCacheGC: return threadCacheGCPostponedEventWait(thread_state);
        case ThreadEventHandler::Peak: return peakEventPostponedEventWait(thread_state);
    }
    ALLOCATOR_NOT_REACHED();
}

/// jemalloc: te_base_cb_t::event_handler
void threadEventHandlerEvent(ThreadState & thread_state, ThreadEventHandler handler)
{
    switch (handler)
    {
        case ThreadEventHandler::ProfilingSample: profilingSampleEvent(thread_state); return;
        case ThreadEventHandler::StatsInterval: statsIntervalEvent(thread_state); return;
        case ThreadEventHandler::ThreadCacheGC: threadCacheGCEvent(thread_state); return;
        case ThreadEventHandler::Peak: peakEvent(thread_state); return;
    }
    ALLOCATOR_NOT_REACHED();
}

/// The handler tables in jemalloc's order (`thread_event_registry.c`). The user event slots are never installed.
/// jemalloc: te_alloc_handlers, te_dalloc_handlers
constexpr ThreadEventHandler thread_event_allocation_handlers[]
    = {ThreadEventHandler::ProfilingSample, ThreadEventHandler::StatsInterval, ThreadEventHandler::ThreadCacheGC, ThreadEventHandler::Peak};
constexpr ThreadEventHandler thread_event_deallocation_handlers[] = {ThreadEventHandler::ThreadCacheGC, ThreadEventHandler::Peak};

static_assert(thread_event_allocation_handlers[thread_event_allocation_profiling_sample] == ThreadEventHandler::ProfilingSample);
static_assert(thread_event_allocation_handlers[thread_event_allocation_stats_interval] == ThreadEventHandler::StatsInterval);
static_assert(thread_event_allocation_handlers[thread_event_allocation_thread_cache_gc] == ThreadEventHandler::ThreadCacheGC);
static_assert(thread_event_allocation_handlers[thread_event_allocation_peak] == ThreadEventHandler::Peak);
static_assert(std::size(thread_event_allocation_handlers) == thread_event_allocation_user0);
static_assert(thread_event_deallocation_handlers[thread_event_deallocation_thread_cache_gc] == ThreadEventHandler::ThreadCacheGC);
static_assert(thread_event_deallocation_handlers[thread_event_deallocation_peak] == ThreadEventHandler::Peak);
static_assert(std::size(thread_event_deallocation_handlers) == thread_event_deallocation_user0);

/// jemalloc: te_ctx_has_active_events
[[maybe_unused]] bool threadEventContextHasActiveEvents(const ThreadEventContext & context)
{
    ALLOCATOR_ASSERT(config::debug);
    if (context.is_alloc)
    {
        for (ThreadEventHandler handler : thread_event_allocation_handlers)
            if (threadEventHandlerEnabled(handler))
                return true;
    }
    else
    {
        for (ThreadEventHandler handler : thread_event_deallocation_handlers)
            if (threadEventHandlerEnabled(handler))
                return true;
    }
    return false;
}

/// jemalloc: te_next_event_compute
uint64_t threadEventNextEventCompute(ThreadState & thread_state, bool is_alloc)
{
    const ThreadEventHandler * handlers = is_alloc ? thread_event_allocation_handlers : thread_event_deallocation_handlers;
    const uint64_t * waits = is_alloc ? thread_state.thread_event_data.alloc_wait : thread_state.thread_event_data.deallocation_wait;
    size_t count = is_alloc ? std::size(thread_event_allocation_handlers) : std::size(thread_event_deallocation_handlers);

    uint64_t wait = THREAD_EVENT_MAX_START_WAIT;
    for (size_t i = 0; i < count; ++i)
    {
        if (threadEventHandlerEnabled(handlers[i]))
        {
            uint64_t event_wait = waits[i];
            ALLOCATOR_ASSERT(event_wait <= THREAD_EVENT_MAX_START_WAIT);
            if (event_wait > 0 && event_wait < wait)
                wait = event_wait;
        }
    }
    return wait;
}

/// jemalloc: te_assert_invariants_impl
void threadEventAssertInvariantsImpl(ThreadState & thread_state, const ThreadEventContext & context)
{
    uint64_t current_bytes = context.currentBytesGet();
    uint64_t last_event = context.lastEventGet();
    uint64_t next_event = context.nextEventGet();
    uint64_t next_event_fast = context.nextEventFastGet();

    ALLOCATOR_ASSERT(last_event != next_event);
    if (next_event > THREAD_EVENT_NEXT_EVENT_FAST_MAX || !thread_state.fast())
        ALLOCATOR_ASSERT(next_event_fast == 0);
    else
        ALLOCATOR_ASSERT(next_event_fast == next_event);

    /// The subtraction is intentionally susceptible to underflow.
    uint64_t interval = next_event - last_event;

    /// The subtraction is intentionally susceptible to underflow.
    ALLOCATOR_ASSERT(current_bytes - last_event < interval);

    /// This assumes that no event became active since the last trigger (waits of inactive events are 0 and ignored).
    /// `next_event` should have been pushed up except when no event is on and the TSD is just initialized; the
    /// `last_event == 0` guard is stronger than needed.
    [[maybe_unused]] uint64_t min_wait = threadEventNextEventCompute(thread_state, context.isAlloc());
    ALLOCATOR_ASSERT(
        (!threadEventContextHasActiveEvents(context) && last_event == 0) || interval == min_wait
        || (interval < min_wait && interval == THREAD_EVENT_MAX_INTERVAL));
    (void)current_bytes;
    (void)interval;
    (void)next_event_fast;
}

/// jemalloc: te_ctx_next_event_fast_update
void threadEventContextNextEventFastUpdate(ThreadEventContext & context)
{
    uint64_t next_event = context.nextEventGet();
    uint64_t next_event_fast = (next_event <= THREAD_EVENT_NEXT_EVENT_FAST_MAX) ? next_event : 0;
    context.nextEventFastSet(next_event_fast);
}

/// jemalloc: te_adjust_thresholds_impl
inline void threadEventAdjustThresholdsImpl(ThreadState & thread_state, ThreadEventContext & context, uint64_t wait)
{
    /// The next threshold based on future events can only be adjusted after progressing the last_event counter
    /// (which is set to current).
    ALLOCATOR_ASSERT(context.currentBytesGet() == context.lastEventGet());
    ALLOCATOR_ASSERT(wait <= THREAD_EVENT_MAX_START_WAIT);

    uint64_t next_event = context.lastEventGet() + (wait <= THREAD_EVENT_MAX_INTERVAL ? wait : THREAD_EVENT_MAX_INTERVAL);
    context.nextEventSet(thread_state, next_event);
}

/// jemalloc: te_init_waits
void threadEventInitWaits(ThreadState & thread_state, uint64_t & wait, bool is_alloc)
{
    const ThreadEventHandler * handlers = is_alloc ? thread_event_allocation_handlers : thread_event_deallocation_handlers;
    uint64_t * waits = is_alloc ? thread_state.thread_event_data.alloc_wait : thread_state.thread_event_data.deallocation_wait;
    size_t count = is_alloc ? std::size(thread_event_allocation_handlers) : std::size(thread_event_deallocation_handlers);
    for (size_t i = 0; i < count; ++i)
    {
        if (threadEventHandlerEnabled(handlers[i]))
        {
            uint64_t event_wait = threadEventHandlerNewEventWait(thread_state, handlers[i]);
            ALLOCATOR_ASSERT(event_wait > 0);
            waits[i] = event_wait;
            if (event_wait < wait)
                wait = event_wait;
        }
    }
    /// The user event slots (`thread_event_allocation_user0..3`) are never installed: `te_user_event_enabled` returns
    /// `te_enabled_not_installed`, so they are skipped.
}

/// jemalloc: te_update_wait
inline bool threadEventUpdateWait(
    ThreadState & thread_state,
    uint64_t accumulated_bytes,
    bool allow,
    uint64_t & event_wait,
    uint64_t & wait,
    ThreadEventHandler handler,
    uint64_t new_wait)
{
    bool result = false;
    if (event_wait > accumulated_bytes)
    {
        event_wait -= accumulated_bytes;
    }
    else if (!allow)
    {
        event_wait = threadEventHandlerPostponedEventWait(thread_state, handler);
    }
    else
    {
        result = true;
        event_wait = new_wait == 0 ? threadEventHandlerNewEventWait(thread_state, handler) : new_wait;
    }

    ALLOCATOR_ASSERT(event_wait > 0);
    if (event_wait < wait)
        wait = event_wait;
    return result;
}

/// Returns the number of handlers enqueued into `to_trigger`. Hand-unrolled (not a loop over the table) because this
/// path is relatively hot.
/// jemalloc: te_update_alloc_events
inline size_t threadEventUpdateAllocEvents(
    ThreadState & thread_state, ThreadEventHandler * to_trigger, uint64_t accumulated_bytes, bool allow, uint64_t & wait)
{
    size_t num_to_trigger = 0;
    uint64_t * waits = thread_state.thread_event_data.alloc_wait;
    if (options.thread_cache_gc_increment_bytes > 0)
    {
        ALLOCATOR_ASSERT(threadEventHandlerEnabled(ThreadEventHandler::ThreadCacheGC));
        if (threadEventUpdateWait(
                thread_state,
                accumulated_bytes,
                allow,
                waits[thread_event_allocation_thread_cache_gc],
                wait,
                ThreadEventHandler::ThreadCacheGC,
                options.thread_cache_gc_increment_bytes))
            to_trigger[num_to_trigger++] = ThreadEventHandler::ThreadCacheGC;
    }
    if constexpr (config::profiling)
    {
        if (options.profiling)
        {
            ALLOCATOR_ASSERT(threadEventHandlerEnabled(ThreadEventHandler::ProfilingSample));
            if (threadEventUpdateWait(
                    thread_state,
                    accumulated_bytes,
                    allow,
                    waits[thread_event_allocation_profiling_sample],
                    wait,
                    ThreadEventHandler::ProfilingSample,
                    0))
                to_trigger[num_to_trigger++] = ThreadEventHandler::ProfilingSample;
        }
    }
    if (options.stats_interval >= 0)
    {
        if (threadEventUpdateWait(
                thread_state,
                accumulated_bytes,
                allow,
                waits[thread_event_allocation_stats_interval],
                wait,
                ThreadEventHandler::StatsInterval,
                stats_interval_accumulated_batch))
        {
            ALLOCATOR_ASSERT(threadEventHandlerEnabled(ThreadEventHandler::StatsInterval));
            to_trigger[num_to_trigger++] = ThreadEventHandler::StatsInterval;
        }
    }
    if constexpr (config::stats)
    {
        ALLOCATOR_ASSERT(threadEventHandlerEnabled(ThreadEventHandler::Peak));
        if (threadEventUpdateWait(
                thread_state,
                accumulated_bytes,
                allow,
                waits[thread_event_allocation_peak],
                wait,
                ThreadEventHandler::Peak,
                PEAK_EVENT_WAIT))
            to_trigger[num_to_trigger++] = ThreadEventHandler::Peak;
    }
    /// The user events loop breaks at the first not installed slot, i.e. immediately.
    return num_to_trigger;
}

/// jemalloc: te_update_dalloc_events
inline size_t threadEventUpdateDeallocationEvents(
    ThreadState & thread_state, ThreadEventHandler * to_trigger, uint64_t accumulated_bytes, bool allow, uint64_t & wait)
{
    size_t num_to_trigger = 0;
    uint64_t * waits = thread_state.thread_event_data.deallocation_wait;
    if (options.thread_cache_gc_increment_bytes > 0)
    {
        ALLOCATOR_ASSERT(threadEventHandlerEnabled(ThreadEventHandler::ThreadCacheGC));
        if (threadEventUpdateWait(
                thread_state,
                accumulated_bytes,
                allow,
                waits[thread_event_deallocation_thread_cache_gc],
                wait,
                ThreadEventHandler::ThreadCacheGC,
                options.thread_cache_gc_increment_bytes))
            to_trigger[num_to_trigger++] = ThreadEventHandler::ThreadCacheGC;
    }
    if constexpr (config::stats)
    {
        ALLOCATOR_ASSERT(threadEventHandlerEnabled(ThreadEventHandler::Peak));
        if (threadEventUpdateWait(
                thread_state,
                accumulated_bytes,
                allow,
                waits[thread_event_deallocation_peak],
                wait,
                ThreadEventHandler::Peak,
                PEAK_EVENT_WAIT))
            to_trigger[num_to_trigger++] = ThreadEventHandler::Peak;
    }
    return num_to_trigger;
}

/// jemalloc: te_init
void threadEventInit(ThreadState & thread_state, bool is_alloc)
{
    ThreadEventContext context = ThreadEventContext::get(thread_state, is_alloc);
    /// Reset the last event to current, which starts the events from a clean state. This is necessary when the TSD
    /// event counters are re-initialized (e.g. a reincarnated TSD): the relationship
    /// last_event <= current < next_event must hold, and all events start fresh from the current bytes.
    context.lastEventSet(context.currentBytesGet());

    uint64_t wait = THREAD_EVENT_MAX_START_WAIT;
    threadEventInitWaits(thread_state, wait, is_alloc);

    threadEventAdjustThresholdsImpl(thread_state, context, wait);
}

}

/// jemalloc: te_assert_invariants_debug
void threadEventAssertInvariantsDebug(ThreadState & thread_state)
{
    ThreadEventContext context = ThreadEventContext::get(thread_state, true);
    threadEventAssertInvariantsImpl(thread_state, context);

    context = ThreadEventContext::get(thread_state, false);
    threadEventAssertInvariantsImpl(thread_state, context);
}

/// Synchronization around the fast threshold: a remote thread doing a slow path change (`ThreadState::globalSlowIncrement`)
/// updates the slow path state, issues a SEQ_CST fence, then zeroes `next_event_fast`; the owner thread updates
/// `next_event_fast`, issues a SEQ_CST fence, then checks its state. So a slow path transition cannot be ignored for
/// arbitrarily long, and the owner goes down the slow path on its next operation after the remote thread has
/// communicated the change (see the detailed argument in jemalloc's `thread_event.c`).
/// jemalloc: te_recompute_fast_threshold
void threadEventRecomputeFastThreshold(ThreadState & thread_state)
{
    if (thread_state.stateGet() != thread_state_nominal)
    {
        /// Check first because this is also called on purgatory.
        threadEventNextEventFastSetNonNominal(thread_state);
        return;
    }

    ThreadEventContext context = ThreadEventContext::get(thread_state, true);
    threadEventContextNextEventFastUpdate(context);
    context = ThreadEventContext::get(thread_state, false);
    threadEventContextNextEventFastUpdate(context);

    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (thread_state.stateGet() != thread_state_nominal)
        threadEventNextEventFastSetNonNominal(thread_state);
}

/// jemalloc: te_adjust_thresholds_helper
void threadEventAdjustThresholdsHelper(ThreadState & thread_state, ThreadEventContext & context, uint64_t wait)
{
    threadEventAdjustThresholdsImpl(thread_state, context, wait);
}

/// jemalloc: te_event_trigger
void threadEventEventTrigger(ThreadState & thread_state, ThreadEventContext & context)
{
    /// usize has already been added to the current counter.
    uint64_t bytes_after = context.currentBytesGet();
    /// The subtraction is intentionally susceptible to underflow.
    uint64_t accumulated_bytes = bytes_after - context.lastEventGet();

    context.lastEventSet(bytes_after);

    bool allow_event_trigger = thread_state.nominal() && thread_state.reentrancy_level == 0;
    uint64_t wait = THREAD_EVENT_MAX_START_WAIT;

    static_assert(unsigned(thread_event_allocation_count) >= unsigned(thread_event_deallocation_count));
    ThreadEventHandler to_trigger[thread_event_allocation_count];
    size_t num_to_trigger;
    if (context.is_alloc)
        num_to_trigger = threadEventUpdateAllocEvents(thread_state, to_trigger, accumulated_bytes, allow_event_trigger, wait);
    else
        num_to_trigger = threadEventUpdateDeallocationEvents(thread_state, to_trigger, accumulated_bytes, allow_event_trigger, wait);

    ALLOCATOR_ASSERT(wait <= THREAD_EVENT_MAX_START_WAIT);
    threadEventAdjustThresholdsHelper(thread_state, context, wait);
    threadEventAssertInvariants(thread_state);

    for (size_t i = 0; i < num_to_trigger; ++i)
    {
        ALLOCATOR_ASSERT(allow_event_trigger);
        threadEventHandlerEvent(thread_state, to_trigger[i]);
    }

    threadEventAssertInvariants(thread_state);
}

/// jemalloc: tsd_te_init
void threadStateThreadEventInit(ThreadState & thread_state)
{
    /// Make sure there is no overflow for the bytes accumulated on event trigger.
    static_assert(THREAD_EVENT_MAX_INTERVAL <= UINT64_MAX - SIZE_CLASS_LARGE_MAX_CLASS + 1);
    threadEventInit(thread_state, true);
    threadEventInit(thread_state, false);
    threadEventAssertInvariants(thread_state);
}

/// --- Counter accumulation ---------------------------------------------------------------------------------------

/// jemalloc: counter_accum_init
bool CounterAccumulated::init(uint64_t interval_)
{
    /// `LOCKEDINT_MTX_INIT` is `false` with 64-bit atomics. jemalloc: locked_init_u64_unsynchronized
    accumulated_bytes.store(0, std::memory_order_relaxed);
    interval = interval_;
    return false;
}

/// --- Peak -------------------------------------------------------------------------------------------------------

/// jemalloc: peak_event_update
void peakEventUpdate(ThreadState & thread_state)
{
    uint64_t alloc = thread_state.thread_allocated;
    uint64_t deallocate = thread_state.thread_deallocated;
    thread_state.peak.update(alloc, deallocate);
}

/// jemalloc: peak_event_activity_callback
static void peakEventActivityCallback(ThreadState & thread_state)
{
    ActivityCallbackThunk * thunk = &thread_state.activity_callback_thunk;
    uint64_t alloc = thread_state.thread_allocated;
    uint64_t deallocate = thread_state.thread_deallocated;
    if (thunk->callback != nullptr)
        thunk->callback(thunk->user_context, alloc, deallocate);
}

/// jemalloc: peak_event_zero
void peakEventZero(ThreadState & thread_state)
{
    uint64_t alloc = thread_state.thread_allocated;
    uint64_t deallocate = thread_state.thread_deallocated;
    thread_state.peak.setZero(alloc, deallocate);
}

/// jemalloc: peak_event_max
uint64_t peakEventMax(ThreadState & thread_state)
{
    return thread_state.peak.max();
}

/// jemalloc: peak_event_new_event_wait
uint64_t peakEventNewEventWait(ThreadState & /*tsd*/)
{
    return PEAK_EVENT_WAIT;
}

/// jemalloc: peak_event_postponed_event_wait
uint64_t peakEventPostponedEventWait(ThreadState & /*tsd*/)
{
    return THREAD_EVENT_MIN_START_WAIT;
}

/// jemalloc: peak_event_handler
void peakEvent(ThreadState & thread_state)
{
    peakEventUpdate(thread_state);
    peakEventActivityCallback(thread_state);
}


}
