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
enum class TeHandler : uint8_t
{
    ProfSample,
    StatsInterval,
    TcacheGc,
    Peak,
};

/// jemalloc: prof_sample_enabled, stats_interval_enabled, tcache_gc_enabled, peak_event_enabled (`te_enabled_yes`)
bool teHandlerEnabled(TeHandler handler)
{
    switch (handler)
    {
        case TeHandler::ProfSample:
            return config::prof && opt.prof;
        case TeHandler::StatsInterval:
            return opt.stats_interval >= 0;
        case TeHandler::TcacheGc:
            return opt.tcache_gc_incr_bytes > 0;
        case TeHandler::Peak:
            return config::stats;
    }
    JE_NOT_REACHED();
}

/// jemalloc: te_base_cb_t::new_event_wait
uint64_t teHandlerNewEventWait(ThreadState & tsd, TeHandler handler)
{
    switch (handler)
    {
        case TeHandler::ProfSample:
            return profSampleNewEventWait(tsd);
        case TeHandler::StatsInterval:
            return statsIntervalNewEventWait(tsd);
        case TeHandler::TcacheGc:
            return tcacheGcNewEventWait(tsd);
        case TeHandler::Peak:
            return peakEventNewEventWait(tsd);
    }
    JE_NOT_REACHED();
}

/// jemalloc: te_base_cb_t::postponed_event_wait
uint64_t teHandlerPostponedEventWait(ThreadState & tsd, TeHandler handler)
{
    switch (handler)
    {
        case TeHandler::ProfSample:
            return profSamplePostponedEventWait(tsd);
        case TeHandler::StatsInterval:
            return statsIntervalPostponedEventWait(tsd);
        case TeHandler::TcacheGc:
            return tcacheGcPostponedEventWait(tsd);
        case TeHandler::Peak:
            return peakEventPostponedEventWait(tsd);
    }
    JE_NOT_REACHED();
}

/// jemalloc: te_base_cb_t::event_handler
void teHandlerEvent(ThreadState & tsd, TeHandler handler)
{
    switch (handler)
    {
        case TeHandler::ProfSample:
            profSampleEvent(tsd);
            return;
        case TeHandler::StatsInterval:
            statsIntervalEvent(tsd);
            return;
        case TeHandler::TcacheGc:
            tcacheGcEvent(tsd);
            return;
        case TeHandler::Peak:
            peakEvent(tsd);
            return;
    }
    JE_NOT_REACHED();
}

/// The handler tables in jemalloc's order (`thread_event_registry.c`). The user event slots are never installed.
/// jemalloc: te_alloc_handlers, te_dalloc_handlers
constexpr TeHandler te_alloc_handlers[] = {TeHandler::ProfSample, TeHandler::StatsInterval, TeHandler::TcacheGc, TeHandler::Peak};
constexpr TeHandler te_dalloc_handlers[] = {TeHandler::TcacheGc, TeHandler::Peak};

static_assert(te_alloc_handlers[te_alloc_prof_sample] == TeHandler::ProfSample);
static_assert(te_alloc_handlers[te_alloc_stats_interval] == TeHandler::StatsInterval);
static_assert(te_alloc_handlers[te_alloc_tcache_gc] == TeHandler::TcacheGc);
static_assert(te_alloc_handlers[te_alloc_peak] == TeHandler::Peak);
static_assert(std::size(te_alloc_handlers) == te_alloc_user0);
static_assert(te_dalloc_handlers[te_dalloc_tcache_gc] == TeHandler::TcacheGc);
static_assert(te_dalloc_handlers[te_dalloc_peak] == TeHandler::Peak);
static_assert(std::size(te_dalloc_handlers) == te_dalloc_user0);

/// jemalloc: te_ctx_has_active_events
[[maybe_unused]] bool teCtxHasActiveEvents(const ThreadEventContext & ctx)
{
    JE_ASSERT(config::debug);
    if (ctx.is_alloc)
    {
        for (TeHandler handler : te_alloc_handlers)
            if (teHandlerEnabled(handler))
                return true;
    }
    else
    {
        for (TeHandler handler : te_dalloc_handlers)
            if (teHandlerEnabled(handler))
                return true;
    }
    return false;
}

/// jemalloc: te_next_event_compute
uint64_t teNextEventCompute(ThreadState & tsd, bool is_alloc)
{
    const TeHandler * handlers = is_alloc ? te_alloc_handlers : te_dalloc_handlers;
    const uint64_t * waits = is_alloc ? tsd.te_data.alloc_wait : tsd.te_data.dalloc_wait;
    size_t count = is_alloc ? std::size(te_alloc_handlers) : std::size(te_dalloc_handlers);

    uint64_t wait = TE_MAX_START_WAIT;
    for (size_t i = 0; i < count; ++i)
    {
        if (teHandlerEnabled(handlers[i]))
        {
            uint64_t ev_wait = waits[i];
            JE_ASSERT(ev_wait <= TE_MAX_START_WAIT);
            if (ev_wait > 0 && ev_wait < wait)
                wait = ev_wait;
        }
    }
    return wait;
}

/// jemalloc: te_assert_invariants_impl
void teAssertInvariantsImpl(ThreadState & tsd, const ThreadEventContext & ctx)
{
    uint64_t current_bytes = ctx.currentBytesGet();
    uint64_t last_event = ctx.lastEventGet();
    uint64_t next_event = ctx.nextEventGet();
    uint64_t next_event_fast = ctx.nextEventFastGet();

    JE_ASSERT(last_event != next_event);
    if (next_event > TE_NEXT_EVENT_FAST_MAX || !tsd.fast())
        JE_ASSERT(next_event_fast == 0);
    else
        JE_ASSERT(next_event_fast == next_event);

    /// The subtraction is intentionally susceptible to underflow.
    uint64_t interval = next_event - last_event;

    /// The subtraction is intentionally susceptible to underflow.
    JE_ASSERT(current_bytes - last_event < interval);

    /// This assumes that no event became active since the last trigger (waits of inactive events are 0 and ignored).
    /// `next_event` should have been pushed up except when no event is on and the TSD is just initialized; the
    /// `last_event == 0` guard is stronger than needed.
    [[maybe_unused]] uint64_t min_wait = teNextEventCompute(tsd, ctx.isAlloc());
    JE_ASSERT(
        (!teCtxHasActiveEvents(ctx) && last_event == 0) || interval == min_wait
        || (interval < min_wait && interval == TE_MAX_INTERVAL));
    (void)current_bytes;
    (void)interval;
    (void)next_event_fast;
}

/// jemalloc: te_ctx_next_event_fast_update
void teCtxNextEventFastUpdate(ThreadEventContext & ctx)
{
    uint64_t next_event = ctx.nextEventGet();
    uint64_t next_event_fast = (next_event <= TE_NEXT_EVENT_FAST_MAX) ? next_event : 0;
    ctx.nextEventFastSet(next_event_fast);
}

/// jemalloc: te_adjust_thresholds_impl
inline void teAdjustThresholdsImpl(ThreadState & tsd, ThreadEventContext & ctx, uint64_t wait)
{
    /// The next threshold based on future events can only be adjusted after progressing the last_event counter
    /// (which is set to current).
    JE_ASSERT(ctx.currentBytesGet() == ctx.lastEventGet());
    JE_ASSERT(wait <= TE_MAX_START_WAIT);

    uint64_t next_event = ctx.lastEventGet() + (wait <= TE_MAX_INTERVAL ? wait : TE_MAX_INTERVAL);
    ctx.nextEventSet(tsd, next_event);
}

/// jemalloc: te_init_waits
void teInitWaits(ThreadState & tsd, uint64_t & wait, bool is_alloc)
{
    const TeHandler * handlers = is_alloc ? te_alloc_handlers : te_dalloc_handlers;
    uint64_t * waits = is_alloc ? tsd.te_data.alloc_wait : tsd.te_data.dalloc_wait;
    size_t count = is_alloc ? std::size(te_alloc_handlers) : std::size(te_dalloc_handlers);
    for (size_t i = 0; i < count; ++i)
    {
        if (teHandlerEnabled(handlers[i]))
        {
            uint64_t ev_wait = teHandlerNewEventWait(tsd, handlers[i]);
            JE_ASSERT(ev_wait > 0);
            waits[i] = ev_wait;
            if (ev_wait < wait)
                wait = ev_wait;
        }
    }
    /// The user event slots (`te_alloc_user0..3`) are never installed: `te_user_event_enabled` returns
    /// `te_enabled_not_installed`, so they are skipped.
}

/// jemalloc: te_update_wait
inline bool teUpdateWait(
    ThreadState & tsd, uint64_t accumbytes, bool allow, uint64_t & ev_wait, uint64_t & wait, TeHandler handler, uint64_t new_wait)
{
    bool ret = false;
    if (ev_wait > accumbytes)
    {
        ev_wait -= accumbytes;
    }
    else if (!allow)
    {
        ev_wait = teHandlerPostponedEventWait(tsd, handler);
    }
    else
    {
        ret = true;
        ev_wait = new_wait == 0 ? teHandlerNewEventWait(tsd, handler) : new_wait;
    }

    JE_ASSERT(ev_wait > 0);
    if (ev_wait < wait)
        wait = ev_wait;
    return ret;
}

/// Returns the number of handlers enqueued into `to_trigger`. Hand-unrolled (not a loop over the table) because this
/// path is relatively hot.
/// jemalloc: te_update_alloc_events
inline size_t teUpdateAllocEvents(ThreadState & tsd, TeHandler * to_trigger, uint64_t accumbytes, bool allow, uint64_t & wait)
{
    size_t nto_trigger = 0;
    uint64_t * waits = tsd.te_data.alloc_wait;
    if (opt.tcache_gc_incr_bytes > 0)
    {
        JE_ASSERT(teHandlerEnabled(TeHandler::TcacheGc));
        if (teUpdateWait(tsd, accumbytes, allow, waits[te_alloc_tcache_gc], wait, TeHandler::TcacheGc, opt.tcache_gc_incr_bytes))
            to_trigger[nto_trigger++] = TeHandler::TcacheGc;
    }
    if constexpr (config::prof)
    {
        if (opt.prof)
        {
            JE_ASSERT(teHandlerEnabled(TeHandler::ProfSample));
            if (teUpdateWait(tsd, accumbytes, allow, waits[te_alloc_prof_sample], wait, TeHandler::ProfSample, 0))
                to_trigger[nto_trigger++] = TeHandler::ProfSample;
        }
    }
    if (opt.stats_interval >= 0)
    {
        if (teUpdateWait(
                tsd, accumbytes, allow, waits[te_alloc_stats_interval], wait, TeHandler::StatsInterval, stats_interval_accum_batch))
        {
            JE_ASSERT(teHandlerEnabled(TeHandler::StatsInterval));
            to_trigger[nto_trigger++] = TeHandler::StatsInterval;
        }
    }
    if constexpr (config::stats)
    {
        JE_ASSERT(teHandlerEnabled(TeHandler::Peak));
        if (teUpdateWait(tsd, accumbytes, allow, waits[te_alloc_peak], wait, TeHandler::Peak, PEAK_EVENT_WAIT))
            to_trigger[nto_trigger++] = TeHandler::Peak;
    }
    /// The user events loop breaks at the first not installed slot, i.e. immediately.
    return nto_trigger;
}

/// jemalloc: te_update_dalloc_events
inline size_t teUpdateDallocEvents(ThreadState & tsd, TeHandler * to_trigger, uint64_t accumbytes, bool allow, uint64_t & wait)
{
    size_t nto_trigger = 0;
    uint64_t * waits = tsd.te_data.dalloc_wait;
    if (opt.tcache_gc_incr_bytes > 0)
    {
        JE_ASSERT(teHandlerEnabled(TeHandler::TcacheGc));
        if (teUpdateWait(tsd, accumbytes, allow, waits[te_dalloc_tcache_gc], wait, TeHandler::TcacheGc, opt.tcache_gc_incr_bytes))
            to_trigger[nto_trigger++] = TeHandler::TcacheGc;
    }
    if constexpr (config::stats)
    {
        JE_ASSERT(teHandlerEnabled(TeHandler::Peak));
        if (teUpdateWait(tsd, accumbytes, allow, waits[te_dalloc_peak], wait, TeHandler::Peak, PEAK_EVENT_WAIT))
            to_trigger[nto_trigger++] = TeHandler::Peak;
    }
    return nto_trigger;
}

/// jemalloc: te_init
void teInit(ThreadState & tsd, bool is_alloc)
{
    ThreadEventContext ctx = ThreadEventContext::get(tsd, is_alloc);
    /// Reset the last event to current, which starts the events from a clean state. This is necessary when the TSD
    /// event counters are re-initialized (e.g. a reincarnated TSD): the relationship
    /// last_event <= current < next_event must hold, and all events start fresh from the current bytes.
    ctx.lastEventSet(ctx.currentBytesGet());

    uint64_t wait = TE_MAX_START_WAIT;
    teInitWaits(tsd, wait, is_alloc);

    teAdjustThresholdsImpl(tsd, ctx, wait);
}

}

/// jemalloc: te_assert_invariants_debug
void teAssertInvariantsDebug(ThreadState & tsd)
{
    ThreadEventContext ctx = ThreadEventContext::get(tsd, true);
    teAssertInvariantsImpl(tsd, ctx);

    ctx = ThreadEventContext::get(tsd, false);
    teAssertInvariantsImpl(tsd, ctx);
}

/// Synchronization around the fast threshold: a remote thread doing a slow path change (`ThreadState::globalSlowInc`)
/// updates the slow path state, issues a SEQ_CST fence, then zeroes `next_event_fast`; the owner thread updates
/// `next_event_fast`, issues a SEQ_CST fence, then checks its state. So a slow path transition cannot be ignored for
/// arbitrarily long, and the owner goes down the slow path on its next operation after the remote thread has
/// communicated the change (see the detailed argument in jemalloc's `thread_event.c`).
/// jemalloc: te_recompute_fast_threshold
void teRecomputeFastThreshold(ThreadState & tsd)
{
    if (tsd.stateGet() != tsd_state_nominal)
    {
        /// Check first because this is also called on purgatory.
        teNextEventFastSetNonNominal(tsd);
        return;
    }

    ThreadEventContext ctx = ThreadEventContext::get(tsd, true);
    teCtxNextEventFastUpdate(ctx);
    ctx = ThreadEventContext::get(tsd, false);
    teCtxNextEventFastUpdate(ctx);

    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (tsd.stateGet() != tsd_state_nominal)
        teNextEventFastSetNonNominal(tsd);
}

/// jemalloc: te_adjust_thresholds_helper
void teAdjustThresholdsHelper(ThreadState & tsd, ThreadEventContext & ctx, uint64_t wait)
{
    teAdjustThresholdsImpl(tsd, ctx, wait);
}

/// jemalloc: te_event_trigger
void teEventTrigger(ThreadState & tsd, ThreadEventContext & ctx)
{
    /// usize has already been added to the current counter.
    uint64_t bytes_after = ctx.currentBytesGet();
    /// The subtraction is intentionally susceptible to underflow.
    uint64_t accumbytes = bytes_after - ctx.lastEventGet();

    ctx.lastEventSet(bytes_after);

    bool allow_event_trigger = tsd.nominal() && tsd.reentrancy_level == 0;
    uint64_t wait = TE_MAX_START_WAIT;

    static_assert(unsigned(te_alloc_count) >= unsigned(te_dalloc_count));
    TeHandler to_trigger[te_alloc_count];
    size_t nto_trigger;
    if (ctx.is_alloc)
        nto_trigger = teUpdateAllocEvents(tsd, to_trigger, accumbytes, allow_event_trigger, wait);
    else
        nto_trigger = teUpdateDallocEvents(tsd, to_trigger, accumbytes, allow_event_trigger, wait);

    JE_ASSERT(wait <= TE_MAX_START_WAIT);
    teAdjustThresholdsHelper(tsd, ctx, wait);
    teAssertInvariants(tsd);

    for (size_t i = 0; i < nto_trigger; ++i)
    {
        JE_ASSERT(allow_event_trigger);
        teHandlerEvent(tsd, to_trigger[i]);
    }

    teAssertInvariants(tsd);
}

/// jemalloc: tsd_te_init
void tsdTeInit(ThreadState & tsd)
{
    /// Make sure there is no overflow for the bytes accumulated on event trigger.
    static_assert(TE_MAX_INTERVAL <= UINT64_MAX - SC_LARGE_MAXCLASS + 1);
    teInit(tsd, true);
    teInit(tsd, false);
    teAssertInvariants(tsd);
}

/// --- Counter accumulation ---------------------------------------------------------------------------------------

/// jemalloc: counter_accum_init
bool CounterAccum::init(uint64_t interval_)
{
    /// `LOCKEDINT_MTX_INIT` is `false` with 64-bit atomics. jemalloc: locked_init_u64_unsynchronized
    accumbytes.store(0, std::memory_order_relaxed);
    interval = interval_;
    return false;
}

/// --- Peak -------------------------------------------------------------------------------------------------------

/// jemalloc: peak_event_update
void peakEventUpdate(ThreadState & tsd)
{
    uint64_t alloc = tsd.thread_allocated;
    uint64_t dalloc = tsd.thread_deallocated;
    tsd.peak.update(alloc, dalloc);
}

/// jemalloc: peak_event_activity_callback
static void peakEventActivityCallback(ThreadState & tsd)
{
    ActivityCallbackThunk * thunk = &tsd.activity_callback_thunk;
    uint64_t alloc = tsd.thread_allocated;
    uint64_t dalloc = tsd.thread_deallocated;
    if (thunk->callback != nullptr)
        thunk->callback(thunk->uctx, alloc, dalloc);
}

/// jemalloc: peak_event_zero
void peakEventZero(ThreadState & tsd)
{
    uint64_t alloc = tsd.thread_allocated;
    uint64_t dalloc = tsd.thread_deallocated;
    tsd.peak.setZero(alloc, dalloc);
}

/// jemalloc: peak_event_max
uint64_t peakEventMax(ThreadState & tsd)
{
    return tsd.peak.max();
}

/// jemalloc: peak_event_new_event_wait
uint64_t peakEventNewEventWait(ThreadState & /*tsd*/)
{
    return PEAK_EVENT_WAIT;
}

/// jemalloc: peak_event_postponed_event_wait
uint64_t peakEventPostponedEventWait(ThreadState & /*tsd*/)
{
    return TE_MIN_START_WAIT;
}

/// jemalloc: peak_event_handler
void peakEvent(ThreadState & tsd)
{
    peakEventUpdate(tsd);
    peakEventActivityCallback(tsd);
}


}
