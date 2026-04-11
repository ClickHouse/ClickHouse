#include <Coordination/SessionRequest.h>

#include <Common/CurrentMetrics.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/KeeperSpans.h>

#include <string>
#include <vector>

namespace CurrentMetrics
{
    extern const Metric KeeperRequestsInitial;
    extern const Metric KeeperRequestsReceived;
    extern const Metric KeeperRequestsPendingRaft;
    extern const Metric KeeperRequestsInRaft;
    extern const Metric KeeperRequestsRaftResponseReady;
    extern const Metric KeeperRequestsPendingLocal;
    extern const Metric KeeperRequestsExecutingLocal;
    extern const Metric KeeperRequestsCompleted;
    extern const Metric KeeperRequestsSendingResponse;
    extern const Metric KeeperRequestsSent;
}

namespace ProfileEvents
{
    extern const Event KeeperStateInRaftMicroseconds;
    extern const Event KeeperStatePendingLocalMicroseconds;
    extern const Event KeeperStateCompletedMicroseconds;
}

namespace DB
{

namespace
{

/// Returns the ProfileEvent for time spent in the given state,
/// or std::nullopt for states we don't track.
/// Only the three states with meaningful signal are tracked;
/// the rest are consistently negligible (<10μs).
std::optional<ProfileEvents::Event> profileEventForState(RequestState s)
{
    switch (s)
    {
        case RequestState::InRaft:            return ProfileEvents::KeeperStateInRaftMicroseconds;
        case RequestState::PendingLocal:      return ProfileEvents::KeeperStatePendingLocalMicroseconds;
        case RequestState::Completed:         return ProfileEvents::KeeperStateCompletedMicroseconds;
        default:                              return std::nullopt;
    }
}

/// Common OTel attribute construction — only called for OTel span recording,
/// not for the histogram-only path (maybeFinalize invokes the lambda only when
/// a span object is present).
/// Caller must ensure env.request is not null.
std::vector<OpenTelemetry::SpanAttribute> baseSpanAttributes(const SessionRequest & env)
{
    return {
        {"keeper.operation", Coordination::opNumToString(env.request->getOpNum())},
        {"keeper.session_id", env.session_id},
        {"keeper.xid", env.request->xid},
    };
}

std::vector<OpenTelemetry::SpanAttribute> baseSpanAttributes(
    const SessionRequest & env,
    std::initializer_list<OpenTelemetry::SpanAttribute> extra)
{
    auto attrs = baseSpanAttributes(env);
    attrs.insert(attrs.end(), extra.begin(), extra.end());
    return attrs;
}

}

CurrentMetrics::Metric SessionRequest::metricForState(RequestState s)
{
    switch (s)
    {
        case RequestState::Initial:           return CurrentMetrics::KeeperRequestsInitial;
        case RequestState::Received:          return CurrentMetrics::KeeperRequestsReceived;
        case RequestState::PendingRaft:       return CurrentMetrics::KeeperRequestsPendingRaft;
        case RequestState::InRaft:            return CurrentMetrics::KeeperRequestsInRaft;
        case RequestState::RaftResponseReady: return CurrentMetrics::KeeperRequestsRaftResponseReady;
        case RequestState::PendingLocal:      return CurrentMetrics::KeeperRequestsPendingLocal;
        case RequestState::ExecutingLocal:    return CurrentMetrics::KeeperRequestsExecutingLocal;
        case RequestState::Completed:         return CurrentMetrics::KeeperRequestsCompleted;
        case RequestState::SendingResponse:   return CurrentMetrics::KeeperRequestsSendingResponse;
        case RequestState::Sent:              return CurrentMetrics::KeeperRequestsSent;
    }
    UNREACHABLE();
}

SessionRequest::SessionRequest()
{
    CurrentMetrics::add(metricForState(state));
}

bool SessionRequest::tryTransitionState(
    RequestState expected, RequestState new_state,
    std::initializer_list<OpenTelemetry::SpanAttribute> extra_attrs)
{
    if (!state.compare_exchange_strong(expected, new_state, std::memory_order_relaxed))
        return false;

    CurrentMetrics::sub(metricForState(expected));
    CurrentMetrics::add(metricForState(new_state));

    const UInt64 now_us = clock_gettime_ns() / 1000;
    if (state_entered_us != 0)
    {
        if (auto event = profileEventForState(expected))
            ProfileEvents::increment(*event, now_us - state_entered_us);
    }
    state_entered_us = now_us;

    /// OTel: only PendingRaft → InRaft uses this method currently.
    /// Finalize the queue-time span (matches setState's PendingRaft→InRaft path).
    if (request && expected == RequestState::PendingRaft && new_state == RequestState::InRaft)
    {
        auto make_attrs = [&]
        {
            auto attrs = baseSpanAttributes(*this);
            attrs.insert(attrs.end(), extra_attrs.begin(), extra_attrs.end());
            return attrs;
        };
        ZooKeeperOpentelemetrySpans::maybeFinalize(
            request->spans.dispatcher_requests_queue, make_attrs);
        request->spans.dispatcher_requests_queue.start_time_us = 0;
    }

    return true;
}

void SessionRequest::setState(
    RequestState new_state,
    std::initializer_list<OpenTelemetry::SpanAttribute> extra_attrs,
    OpenTelemetry::SpanStatus status,
    const String & error_message)
{
    auto old_state = state.load(std::memory_order_relaxed);

    /// Validate transitions.
    switch (new_state)
    {
        case RequestState::Received:
            chassert(old_state == RequestState::Initial);
            break;
        case RequestState::PendingLocal:
            chassert(old_state == RequestState::Received || old_state == RequestState::Initial);
            break;
        case RequestState::PendingRaft:
            chassert(old_state == RequestState::Received || old_state == RequestState::Initial);
            break;
        case RequestState::InRaft:
            chassert(old_state == RequestState::PendingRaft);
            break;
        case RequestState::ExecutingLocal:
            chassert(old_state == RequestState::Received || old_state == RequestState::Initial
                || old_state == RequestState::PendingLocal);
            break;
        case RequestState::RaftResponseReady:
            chassert(old_state == RequestState::InRaft);
            break;
        case RequestState::Completed:
            /// Normal: ExecutingLocal → Completed, RaftResponseReady → Completed.
            /// Error/cleanup paths (orphanActiveRequests, soft-limit): InRaft, PendingRaft,
            /// PendingLocal may also transition directly to Completed.
            /// Initial → Completed: synthetic wrappers from deliverDirect (watch
            /// notifications, session expiry responses).
            chassert(old_state == RequestState::ExecutingLocal || old_state == RequestState::RaftResponseReady
                || old_state == RequestState::InRaft || old_state == RequestState::PendingRaft
                || old_state == RequestState::PendingLocal || old_state == RequestState::Initial);
            break;
        case RequestState::SendingResponse:
            chassert(old_state == RequestState::Completed);
            break;
        case RequestState::Sent:
            chassert(old_state == RequestState::SendingResponse);
            break;
        case RequestState::Initial:
            /// PendingRaft → Initial: push to subqueue failed, rolling back.
            chassert(old_state == RequestState::PendingRaft);
            break;
    }

    state.store(new_state, std::memory_order_relaxed);

    /// Update per-state CurrentMetrics gauges.
    CurrentMetrics::sub(metricForState(old_state));
    CurrentMetrics::add(metricForState(new_state));

    /// Track time spent in each state via ProfileEvents.
    const UInt64 now_us = clock_gettime_ns() / 1000;
    if (state_entered_us != 0)
    {
        if (auto event = profileEventForState(old_state))
            ProfileEvents::increment(*event, now_us - state_entered_us);
    }
    state_entered_us = now_us;

    /// OTel hooks — only for transitions that have span activity.
    /// The make_attrs lambda allocates a std::vector, but it is only invoked
    /// when an OTel span is present (not on the histogram-only hot path).
    /// maybeFinalize short-circuits to histogram.observe() when !span.
    if (!request)
        return;

    auto make_attrs = [&]
    {
        auto attrs = baseSpanAttributes(*this);
        attrs.insert(attrs.end(), extra_attrs.begin(), extra_attrs.end());
        return attrs;
    };

    if (old_state == RequestState::Initial && new_state == RequestState::Received)
    {
        /// Request parsed from wire — init with stored start time, finalize now.
        UInt64 start_time = receive_start_time_us ? receive_start_time_us : ZooKeeperOpentelemetrySpans::now();
        ZooKeeperOpentelemetrySpans::maybeInitialize(
            request->spans.receive_request, request->tracing_context, start_time);
        ZooKeeperOpentelemetrySpans::maybeFinalize(
            request->spans.receive_request, make_attrs);
    }
    else if ((old_state == RequestState::Received || old_state == RequestState::Initial) && new_state == RequestState::PendingLocal)
    {
        /// Entering the local wait queue — start timing both spans.
        ZooKeeperOpentelemetrySpans::maybeInitialize(
            request->spans.dispatcher_requests_queue, request->tracing_context);
        ZooKeeperOpentelemetrySpans::maybeInitialize(
            request->spans.read_wait_for_write, request->tracing_context);
    }
    else if ((old_state == RequestState::Received || old_state == RequestState::Initial) && new_state == RequestState::PendingRaft)
    {
        /// Entering the Raft wait queue — start queue-time span.
        ZooKeeperOpentelemetrySpans::maybeInitialize(
            request->spans.dispatcher_requests_queue, request->tracing_context);
    }
    else if (old_state == RequestState::PendingRaft && new_state == RequestState::InRaft)
    {
        /// Pulled by scheduler — finalize queue-time span.
        ZooKeeperOpentelemetrySpans::maybeFinalize(
            request->spans.dispatcher_requests_queue, make_attrs);
        request->spans.dispatcher_requests_queue.start_time_us = 0;
    }
    else if ((old_state == RequestState::Received || old_state == RequestState::Initial) && new_state == RequestState::ExecutingLocal)
    {
        /// Fast-path read on empty queue — init and immediately finalize queue span.
        ZooKeeperOpentelemetrySpans::maybeInitialize(
            request->spans.dispatcher_requests_queue, request->tracing_context);
        ZooKeeperOpentelemetrySpans::maybeFinalize(
            request->spans.dispatcher_requests_queue, make_attrs);
        request->spans.dispatcher_requests_queue.start_time_us = 0;
    }
    else if (old_state == RequestState::PendingLocal && new_state == RequestState::ExecutingLocal)
    {
        /// Read released from wait — finalize both wait spans.
        ZooKeeperOpentelemetrySpans::maybeFinalize(
            request->spans.dispatcher_requests_queue, make_attrs);
        request->spans.dispatcher_requests_queue.start_time_us = 0;
        ZooKeeperOpentelemetrySpans::maybeFinalize(
            request->spans.read_wait_for_write, make_attrs);
        request->spans.read_wait_for_write.start_time_us = 0;
    }
    else if (new_state == RequestState::Completed)
    {
        /// Response ready — start measuring time in the response queue.
        ZooKeeperOpentelemetrySpans::maybeInitialize(
            request->spans.dispatcher_responses_queue, request->tracing_context);
    }
    else if (new_state == RequestState::SendingResponse)
    {
        /// Picked up by TCP handler — finalize responses queue span, init send span.
        ZooKeeperOpentelemetrySpans::maybeFinalize(
            request->spans.dispatcher_responses_queue, make_attrs);
        request->spans.dispatcher_responses_queue.start_time_us = 0;
        ZooKeeperOpentelemetrySpans::maybeInitialize(
            request->spans.send_response, request->tracing_context);
    }
    else if (old_state == RequestState::SendingResponse && new_state == RequestState::Sent)
    {
        /// Response written — finalize send span with OK or ERROR.
        ZooKeeperOpentelemetrySpans::maybeFinalize(
            request->spans.send_response, make_attrs, status, error_message);
    }
}

void SessionRequest::onCommitCompleted(
    uint64_t pc_start, uint64_t pc_end,
    uint64_t c_start, uint64_t c_end) const
{
    if (!request)
        return;

    auto make_attrs = [&] { return baseSpanAttributes(*this); };

    if (pc_start != 0)
    {
        ZooKeeperOpentelemetrySpans::maybeInitialize(
            request->spans.pre_commit, request->tracing_context, pc_start);
        ZooKeeperOpentelemetrySpans::maybeFinalize(
            request->spans.pre_commit, make_attrs,
            OpenTelemetry::SpanStatus::OK, {}, pc_end);
    }
    if (c_start != 0)
    {
        ZooKeeperOpentelemetrySpans::maybeInitialize(
            request->spans.commit, request->tracing_context, c_start);
        ZooKeeperOpentelemetrySpans::maybeFinalize(
            request->spans.commit, make_attrs,
            OpenTelemetry::SpanStatus::OK, {}, c_end);
    }
}

SessionRequest::~SessionRequest()
{
    /// Decrement the metric for whatever state we're dying in.
    CurrentMetrics::sub(metricForState(state.load(std::memory_order_relaxed)));

    if (!request)
        return;

    /// Safety net: finalize any OTel spans that were initialized but not
    /// explicitly finalized via lifecycle methods.
    /// A span is considered unfinalized if it has an active `span` object
    /// (OTel-enabled path) or if `start_time_us != 0` and `span` is empty
    /// (histogram-only path). After `maybeFinalize`, the span object is reset
    /// and `start_time_us` remains set — so we check `span.has_value()` for
    /// OTel spans and use `start_time_us` only for the histogram-only case.
    /// To avoid double-recording histograms, we clear `start_time_us` after
    /// each finalize call.
    auto make_attrs = [&] { return baseSpanAttributes(*this, {{"keeper.leaked", true}}); };

    auto finalize_if_needed = [&](MaybeSpan & s)
    {
        if (s.span.has_value() || s.start_time_us != 0)
        {
            ZooKeeperOpentelemetrySpans::maybeFinalize(
                s, make_attrs,
                OpenTelemetry::SpanStatus::ERROR, "Span not explicitly finalized");
            s.start_time_us = 0;
        }
    };

    /// Only these two spans need a safety net:
    /// - `dispatcher_requests_queue`: initialized on enqueue, finalized on pull/dispatch.
    ///   Can leak if the request is dropped before being pulled (e.g., session cleanup).
    /// - `read_wait_for_write`: initialized for PendingLocal reads, finalized on dispatch.
    ///   Can leak if the read is dropped before being dispatched.
    ///
    /// - `dispatcher_responses_queue`: initialized on Completed, finalized on SendingResponse.
    ///   Can leak if the response is dropped before the TCP handler sends it.
    ///
    /// The remaining spans (`read_process`, `pre_commit`, `commit`, `send_response`)
    /// are always finalized synchronously within the state machine or delivery path.
    finalize_if_needed(request->spans.dispatcher_requests_queue);
    finalize_if_needed(request->spans.dispatcher_responses_queue);
    finalize_if_needed(request->spans.read_wait_for_write);
}

}
