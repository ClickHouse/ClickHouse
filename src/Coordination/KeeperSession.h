#pragma once

#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Coordination/KeeperCommon.h>

#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>


namespace DB
{

/// Server-side state for a single Keeper client connection.
///
/// Holds the session ID, response callback, and a simple state machine:
///   Active -> Finishing  (Close submitted or session expired)
///   Active -> Closed     (session fully dead)
///   Finishing -> Closed  (session fully dead)
///
/// This is the initial version: just callback + state. Later steps will add
/// the per-session subqueue, and executor model.
class KeeperSession
{
public:
    enum class SessionState : uint8_t
    {
        Active,     /// Normal operation, accepts new requests.
        Finishing,  /// Close submitted or session expired, no new requests accepted.
        Closed,     /// Callback detached, session fully dead.
    };

    /// Result of `addRequest` — lets the caller distinguish rejection reasons.
    enum class AddResult : uint8_t
    {
        Accepted,        /// Request accepted (may have been handled locally or queued for Raft).
        SessionClosed,   /// Session is not Active (Finishing/Closed).
        QueueFull,       /// Per-session active request limit reached.
    };

    /// Same type as `ZooKeeperResponseCallback` in KeeperSessionRegistry.h.
    using ResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request)>;

    /// Callback to dispatch a local (non-quorum) read request.
    using LocalReadCallback = std::function<void(const KeeperRequestForSession &)>;

    KeeperSession(int64_t session_id, ResponseCallback callback, bool quorum_reads, size_t max_active_requests, LocalReadCallback local_read_callback);

    int64_t getSessionID() const { return session_id; }

    /// True only when Active.
    bool canAcceptRequests() const;

    /// Classify the request and handle accordingly:
    /// - Non-quorum reads with no pending writes: dispatch inline via `local_read_callback_`
    /// - Non-quorum reads with pending writes: defer behind the last enqueued write
    /// - Everything else (writes, quorum reads, reconfig, close, etc.): accepted for Raft queue
    ///
    /// Returns `Accepted` with `goes_to_raft=true` if the caller should push to queue,
    /// `Accepted` with `goes_to_raft=false` if handled locally, or a rejection reason.
    struct AddRequestResult
    {
        AddResult result;
        bool goes_to_raft; /// Only meaningful when result == Accepted.
    };
    AddRequestResult addRequest(const KeeperRequestForSession & request);

    /// Invoke the session's callback to deliver a response.
    /// Returns false if session is Closed or callback is empty.
    bool deliverResponse(const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request = nullptr);

    /// Set state to Closed, clear callback, clear active requests.
    void close();

    SessionState getState() const;

    /// Add a read request that must wait for the write with the given xid to commit.
    void addDeferredRead(Coordination::XID write_xid, const KeeperRequestForSession & read_request);

    /// Release all deferred reads waiting for the given write xid.
    /// Also decrements the pending-writes counter, and clears
    /// `last_enqueued_write_xid_` when no more writes are in flight.
    /// Returns the released reads so the caller can dispatch them.
    KeeperRequestsForSessions releaseDeferredReads(Coordination::XID write_xid);

    /// Clear all deferred reads (e.g. when the session is terminated).
    void clearDeferredReads();

private:
    const int64_t session_id;
    const bool quorum_reads_;
    const size_t max_active_requests_;
    mutable std::mutex mutex;
    SessionState state{SessionState::Active};
    ResponseCallback callback;
    LocalReadCallback local_read_callback_;

    /// Ordered FIFO of all in-flight requests for this session (bookkeeping).
    /// Protected by `mutex`.
    std::deque<KeeperRequestForSession> active_requests;

    /// Tracks in-flight writes enqueued to the Raft queue but not yet committed.
    size_t pending_writes_count_{0};
    /// XID of the most recently enqueued write (used to defer reads behind it).
    std::optional<Coordination::XID> last_enqueued_write_xid_;

    std::unordered_map<Coordination::XID, KeeperRequestsForSessions> deferred_reads_;
    LoggerPtr log = getLogger("KeeperSession");
};

}
