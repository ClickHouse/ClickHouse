#pragma once

#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/SessionRequest.h>

#include <deque>
#include <functional>
#include <mutex>
#include <span>


namespace DB
{

class KeeperSubqueue;
using KeeperSubqueuePtr = std::shared_ptr<KeeperSubqueue>;

class KeeperSessionRegistry;

/// Server-side state for a single Keeper client connection.
///
/// Request paths:
///   Quorum (writes, quorum reads, Auth, Close, Reconfig)
///     -> `active_requests` FIFO -> `queue_handle.push` -> `KeeperRequestsQueue` -> `requestThread` -> Raft
///   Local (non-quorum reads)
///     -> `active_requests` FIFO
///     -> dispatched immediately when queue is empty (ExecutingLocal)
///     -> otherwise waits as PendingLocal until preceding entries complete
///
/// Response flow:
///   Writes: Raft commit -> `response_router` -> `routeResponse` ->
///     `onRaftResponse` (attaches response to head, sets RaftResponseReady) ->
///     `onRaftCommitted` -> `advanceQueue` (pops, delivers, advances queue)
///   Reads: `local_read` fills `req->response` directly ->
///     `advanceQueue` (pops, delivers, advances queue)
///
/// Lock ordering (see also `process_and_responses_lock` in KeeperStateMachine):
///   storage_mutex (shared) -> process_and_responses_lock -> session->mutex -> subqueue->mutex
///   No path holds subqueue->mutex then session->mutex (no reverse nesting).
class KeeperSession
{
public:
    /// Session state machine:
    ///   Active -> Finishing  (Close request admitted to FIFO via addRequest)
    ///   Active -> Closed     (finalizeWithErrors, or Close response delivered)
    ///   Finishing -> Closed  (Close response delivered via popResponseReadyNoLock)
    enum class SessionState : uint8_t
    {
        Active,     /// Normal operation, accepts new requests.
        Finishing,  /// Close submitted or session expired, no new requests accepted.
        Closed,     /// Callback detached, session fully dead.
    };

    enum class DeliveryResult : uint8_t
    {
        NotDelivered,        /// Session Closed, empty FIFO, or no callback.
        Delivered,           /// Response attached / delivered.
        DeliveredAndDetach,  /// Close response — caller must detach from registry.
        FifoMismatch,        /// XID mismatch at FIFO head — bug.
    };

    /// Callback invoked by the session to deliver completed requests to the consumer.
    /// Accepts a batch so that `advanceQueue` can push multiple responses with
    /// a single pipe write (wakeup) instead of one per request.
    using ResponseCallback = std::function<void(std::vector<SessionRequestPtr>)>;

    /// Callback to dispatch a batch of local reads. Must be synchronous —
    /// callers may pass spans over stack locals. Must always fill
    /// `req->response` on each element (result or error). Set once at
    /// construction. The batch may contain a single element.
    using LocalReadCallback = std::function<void(std::span<SessionRequestPtr>)>;

    /// @param registry  Non-owning pointer to the session registry (outlives all sessions).
    ///                  Sessions access admission control and settings.
    /// @param subqueue  The assigned subqueue in `KeeperRequestsQueue` for this session.
    ///                  Used to push quorum requests without holding `mutex`.
    /// @param local_read_dispatch  Callback for local read execution. Owned by the session.
    KeeperSession(int64_t session_id, ResponseCallback callback, KeeperSessionRegistry & registry, KeeperSubqueuePtr subqueue, LocalReadCallback local_read_dispatch);

    int64_t getSessionID() const { return session_id; }

    /// Result of addRequest — lets the caller distinguish rejection reasons
    /// without re-acquiring the session mutex.
    enum class AddResult : uint8_t
    {
        Accepted,        /// Request accepted.
        SessionClosed,   /// Session is not Active (Finishing/Closed).
        QueueFull,       /// Per-session or global queue limit reached.
    };

    /// Classifies the request and routes it into the session FIFO.
    /// The `SessionRequest` must already have `session_id`, `request`, `time`,
    /// and `use_xid_64` set. `addRequest` sets `mode`, `executor`, and state.
    AddResult addRequest(SessionRequestPtr keeper_req);

    /// True only when Active.
    bool canAcceptRequests() const;

    /// Finalize the session and deliver error responses for all in-flight requests.
    /// Atomically (under mutex): collects active_requests, creates error responses
    /// for entries without an existing response, copies callback, clears callback,
    /// sets state to Closed. Then delivers the batch outside the mutex.
    /// Returns true if any responses were delivered.
    bool finalizeWithErrors(Coordination::Error error);

    /// --- Response delivery ---
    ///
    /// Three paths (dispatched by `KeeperDispatcher::routeResponse`):
    /// - `onRaftResponse`: for Raft-committed responses — attaches to FIFO head,
    ///   sets RaftResponseReady. Actual delivery deferred to `onRaftCommitted`.
    /// - `onWatchNotification`: watch notifications (synthetic entries, bypass FIFO)
    /// - `deliverDirect`: fallback for error responses, session expiry, follower path

    /// Deliver a response directly, bypassing the session FIFO. Used for error responses,
    /// session expiry, follower path, and KeeperOverDispatcher local reads.
    /// Creates a wrapper `SessionRequestPtr` and invokes the callback directly.
    /// Returns `delivered=false` if session is Closed or has no callback.
    /// Returns `detach_session=true` for Close responses (caller must detach from registry).
    DeliveryResult deliverDirect(
        const Coordination::ZooKeeperResponsePtr & response);

    /// Attach a Raft commit response to the FIFO head. Does NOT pop or deliver —
    /// that happens in `onRaftCommitted` → `advanceQueue`.
    /// Verifies head XID matches; returns `delivered=false` on mismatch (bug).
    DeliveryResult onRaftResponse(
        Coordination::XID xid,
        const Coordination::ZooKeeperResponsePtr & response);

    /// Deliver a watch notification. Creates a synthetic `SessionRequestPtr` with
    /// `is_watch_notification=true` and invokes the callback (bypasses FIFO).
    ///
    /// Ordering: the watch is delivered immediately without checking whether the
    /// request that SET the watch is still in `active_requests`. Correct ordering
    /// (watch arrives after the setting request's response) is guaranteed by the
    /// global `process_and_responses_lock` in the state machine — the setting
    /// request's response is delivered under the lock before any concurrent commit
    /// can trigger the watch.
    DeliveryResult onWatchNotification(const Coordination::ZooKeeperResponsePtr & watch_response);

    /// Called from the commit callback after a Raft response has been attached
    /// by `onRaftResponse`. Runs outside the state machine locks. Pops the
    /// completed write from the FIFO head, delivers it, then advances the queue:
    /// dispatching PendingLocal reads.
    ///
    /// This blocks the Raft commit thread for the duration of all dispatched
    /// local reads. This is intentional: NuRaft serializes commits, so no other
    /// commit can interleave while local reads execute. Each local read acquires
    /// `process_and_responses_lock` independently in `processReadRequest`.
    ///
    /// Keep the code in the `local_read` callback fast. Lock order during the call:
    /// `storage_mutex` (shared) -> `process_and_responses_lock` -> `session->mutex`
    /// (via response delivery). Never acquire `process_and_responses_lock` or
    /// `storage_mutex` while holding `mutex`.
    void onRaftCommitted();

private:
    /// Shared implementation for deliverDirect and onWatchNotification.
    /// Creates a wrapper SessionRequestPtr, copies callback under lock, invokes outside.
    DeliveryResult deliverDirectResponse(const Coordination::ZooKeeperResponsePtr & response, bool is_watch);

    /// Pop completed heads and dispatch consecutive `PendingLocal` reads
    /// that belong to `current_executor`. Stops at the first entry with a
    /// different executor or a non-`PendingLocal` state (e.g. `InRaft`).
    ///
    /// Called from:
    /// - `addRequest` with `KeeperRequestExecutor::SessionThread` (inline read dispatch)
    /// - `onRaftCommitted` with `KeeperRequestExecutor::QuorumThread` (drain after commit)
    void advanceQueue(KeeperRequestExecutor current_executor);

    /// Pop all completed heads from `active_requests` into `out`.
    /// Returns true if Close was encountered (callback moved, state set to Closed).
    /// Must be called with `mutex` held.
    bool popResponseReadyNoLock(std::vector<SessionRequestPtr> & out);

    /// Collect consecutive PendingLocal reads with matching executor from the
    /// head of `active_requests`. Marks them ExecutingLocal, decrements pending count.
    /// Must be called with `mutex` held.
    void collectPendingReadsNoLock(KeeperRequestExecutor executor, std::vector<SessionRequestPtr> & out);

    /// Deliver popped responses via `cb`. Outside `mutex`.
    void deliverResponses(std::vector<SessionRequestPtr> & responses, const ResponseCallback & cb);

    /// Dispatch a batch of reads via `local_read_dispatch`. Fills ZSYSTEMERROR
    /// on exception. Outside `mutex`.
    void dispatchReads(std::vector<SessionRequestPtr> & reads);

    /// Serialize the first N entries of `active_requests` for log messages.
    /// Must be called with `mutex` held.
    String serializeActiveRequestsNoLock(size_t max_entries = 5) const;

    /// Clear `active_requests`. Must be called with `mutex` held.
    void orphanActiveRequests();

    const int64_t session_id;
    static inline LoggerPtr log = getLogger("KeeperSession");
    SessionState state = SessionState::Active;

    /// Response delivery callback. Set once in the constructor, cleared on Close/shutdown.
    /// Stored as shared_ptr so that copying under mutex (for use outside the lock)
    /// is a cheap refcount bump instead of a std::function heap allocation.
    std::shared_ptr<const ResponseCallback> callback;

    /// Ordered FIFO of all in-flight requests for this session.
    /// Entries progress through states (see RequestState in SessionRequest.h).
    /// Protected by `mutex`.
    std::deque<SessionRequestPtr> active_requests;

    /// LOCK ORDERING (to prevent deadlock with state machine):
    ///   storage_mutex (shared) -> process_and_responses_lock -> session->mutex
    /// Never acquire process_and_responses_lock or storage_mutex while holding mutex.
    /// All callbacks (local_read, callback) are called OUTSIDE mutex.
    mutable std::mutex mutex;

    /// Non-owning pointer to the session registry for admission control and settings.
    KeeperSessionRegistry * registry;

    /// Handle to the assigned subqueue in `KeeperRequestsQueue`.
    /// Quorum requests are pushed here outside `mutex` after `addRequest` enqueues
    /// them in `active_requests`. The queue notifies `requestThread` via its CV.
    KeeperSubqueuePtr subqueue;

    /// Local read dispatch callback. Set once at construction.
    LocalReadCallback local_read_dispatch;
};

}
