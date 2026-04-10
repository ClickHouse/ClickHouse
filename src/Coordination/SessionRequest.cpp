#include <Coordination/SessionRequest.h>

#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

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

namespace DB
{

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

bool SessionRequest::tryTransitionState(RequestState expected, RequestState new_state)
{
    if (!state.compare_exchange_strong(expected, new_state, std::memory_order_relaxed))
        return false;

    CurrentMetrics::sub(metricForState(expected));
    CurrentMetrics::add(metricForState(new_state));

    const UInt64 now_us = clock_gettime_ns() / 1000;
    state_entered_us = now_us;

    return true;
}

void SessionRequest::setState(RequestState new_state)
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
            /// Normal: ExecutingLocal -> Completed, RaftResponseReady -> Completed.
            /// Error/cleanup paths (orphanActiveRequests, soft-limit): InRaft, PendingRaft,
            /// PendingLocal may also transition directly to Completed.
            /// Initial -> Completed: synthetic wrappers from deliverDirect (watch
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
            /// PendingRaft -> Initial: push to subqueue failed, rolling back.
            chassert(old_state == RequestState::PendingRaft);
            break;
    }

    state.store(new_state, std::memory_order_relaxed);

    /// Update per-state CurrentMetrics gauges.
    CurrentMetrics::sub(metricForState(old_state));
    CurrentMetrics::add(metricForState(new_state));

    /// Record timestamp for the new state.
    const UInt64 now_us = clock_gettime_ns() / 1000;
    state_entered_us = now_us;
}

SessionRequest::~SessionRequest()
{
    /// Decrement the metric for whatever state we're dying in.
    CurrentMetrics::sub(metricForState(state.load(std::memory_order_relaxed)));
}

}
