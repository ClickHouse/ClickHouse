#pragma once

#include <Coordination/KeeperCommon.h>
#include <Common/CurrentMetrics.h>

#include <optional>


namespace DB
{

/// Lifecycle state of the request inside the session queue.
///
///   On creation (TCP handler / KeeperOverDispatcher):
///     Initial -> Received                (request parsed from wire)
///
///   On arrival in session:
///     Received -> ExecutingLocal         (queue empty, session thread)
///     Received -> PendingLocal           (queue non-empty, inherits executor)
///     Received -> PendingRaft            (quorum op, quorum thread)
///
///   Direct requests (Reconfig, SessionID, dead-session Close) bypass
///   the TCP handler's Received state:
///     Initial  -> PendingRaft            (pushed directly to system subqueue)
///
///   Transitions:
///     PendingRaft   -> InRaft            (pulled by `requestThread` from `KeeperRequestsQueue`)
///     PendingLocal  -> ExecutingLocal    (preceding entry completed, dispatch read)
///     ExecutingLocal -> Completed        (read result filled, deliver to client)
///     InRaft -> RaftResponseReady        (Raft commit response attached)
///     RaftResponseReady -> Completed     (delivered to client via advanceQueue)
///     Completed -> SendingResponse       (about to write response to client)
///     SendingResponse -> Sent            (response written -- OK or ERROR)
enum class RequestState : uint8_t
{
    Initial,
    Received,
    PendingRaft,
    InRaft,
    RaftResponseReady,
    PendingLocal,
    ExecutingLocal,
    Completed,
    SendingResponse,
    Sent,
};

/// Whether the request goes through Raft consensus or executes locally.
enum class KeeperRequestMode : uint8_t
{
    /// Processed through Raft consensus (writes, quorum reads, Auth, Heartbeat, Close, Reconfig).
    Quorum,
    /// Executed locally against the state machine (non-quorum reads).
    NonQuorum,
};

/// Which thread group dispatches this request's local execution.
///
/// Two threads can execute operations for a session:
///
/// - SessionThread (TCP handler): one per session, calls `addRequest`
///   sequentially, dispatches local reads inline, finishes each before
///   the next `addRequest` call.
///
/// - QuorumThread (NuRaft commit callback): fires `onRaftResponse`
///   (mark only, no pop) then `onRaftCommitted` (pop + drain). Serialized
///   by NuRaft: the next commit cannot start until the previous
///   `onRaftCommitted` returns.
///
/// Execution order is fully controlled within a single thread (both are
/// FIFO). The executor tag ensures the two threads do not race on the
/// same request group: `advanceQueue` stops at the first entry with
/// a different executor, so the session thread never executes reads that
/// belong to the quorum thread group, and vice versa.
///
/// This eliminates the need for cross-thread synchronization beyond the
/// session mutex: each executor processes its own contiguous group, and
/// `onRaftResponse` (head-only) is guaranteed to find the write at
/// the FIFO head because all preceding reads from the quorum group were
/// dispatched and popped synchronously within `onRaftCommitted`.
enum class KeeperRequestExecutor : uint8_t
{
    SessionThread,  /// TCP handler thread -- dispatches reads inline in `addRequest`.
    QuorumThread,   /// NuRaft commit callback -- dispatches reads in `onRaftCommitted`.
};

/// Unified request type for the Keeper pipeline. Holds both session-level fields
/// (session_id, request, mode, target) and Raft-level fields (zxid, digest, log_idx).
/// Used for all request paths -- session-routed (writes, reads) and direct (Reconfig,
/// SessionID, dead session Close, `KeeperOverDispatcher` reads).
class SessionRequest
{
public:
    SessionRequest();
    ~SessionRequest();

    /// --- State management ---

    RequestState getState() const { return state.load(std::memory_order_relaxed); }

    /// Transition to a new state. Validates the transition with `chassert`
    /// in debug builds. Updates per-state `CurrentMetrics` gauges.
    void setState(RequestState new_state);

    /// Atomically transition from `expected` to `new_state` via CAS.
    /// Returns true if the transition succeeded. If another thread changed
    /// the state concurrently (e.g. `finalizeWithErrors`), returns false
    /// without asserting -- the caller treats the entry as orphaned.
    bool tryTransitionState(RequestState expected, RequestState new_state);

    /// --- Session-level fields (set at creation / classification time) ---

    int64_t session_id{0};
    Coordination::ZooKeeperRequestPtr request;
    KeeperRequestMode mode{KeeperRequestMode::Quorum};
    KeeperRequestExecutor executor{KeeperRequestExecutor::SessionThread};
    int64_t time{0};
    bool use_xid_64{false};

    /// Cached result of `request->bytesSize()`. Avoids repeated virtual calls
    /// during batch collection. Set once after `request` is assigned.
    uint64_t cached_bytes_size{0};

    /// Monotonic timestamp (microseconds) when the current state was entered.
    /// Used by `setState` for future per-state duration `ProfileEvents`.
    UInt64 state_entered_us{0};

private:
    /// Atomic because `orphanActiveRequests` (under session mutex) and
    /// `requestThread` (no session mutex) may access concurrently.
    /// relaxed is sufficient -- inter-thread ordering is provided by
    /// NuRaft's internal sync.
    std::atomic<RequestState> state{RequestState::Initial};

    /// Returns the CurrentMetrics::Metric for the given state.
    static CurrentMetrics::Metric metricForState(RequestState s);

public:

    /// --- Raft-level fields (populated during pipeline processing) ---

    int64_t zxid{0};
    std::optional<KeeperDigest> digest;
    int64_t log_idx{0};

    /// --- Response (set when request completes) ---

    /// Attached by `onRaftResponse` (Raft commit) or `local_read` callback (local read).
    Coordination::ZooKeeperResponsePtr response;

    /// True for synthetic entries created by `onWatchNotification` for watch notifications.
    /// When true, only `response` is meaningful; `request` may be null.
    /// Set at construction, never mutated afterwards.
    bool is_watch_notification{false};


    /// SessionRequest is non-copyable, non-movable (prevent metric double-count).
    SessionRequest(const SessionRequest &) = delete;
    SessionRequest & operator=(const SessionRequest &) = delete;
    SessionRequest(SessionRequest &&) = delete;
    SessionRequest & operator=(SessionRequest &&) = delete;
};

}
