#pragma once

#include <Common/Logger.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Coordination/KeeperCommon.h>

#include <functional>
#include <mutex>
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
/// the per-session FIFO, active_requests, subqueue, and executor model.
class KeeperSession
{
public:
    enum class SessionState : uint8_t
    {
        Active,     /// Normal operation, accepts new requests.
        Finishing,  /// Close submitted or session expired, no new requests accepted.
        Closed,     /// Callback detached, session fully dead.
    };

    /// Same type as `ZooKeeperResponseCallback` in KeeperSessionRegistry.h.
    using ResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request)>;

    KeeperSession(int64_t session_id, ResponseCallback callback);

    int64_t getSessionID() const { return session_id; }

    /// True only when Active.
    bool canAcceptRequests() const;

    /// Invoke the session's callback to deliver a response.
    /// Returns false if session is Closed or callback is empty.
    bool deliverResponse(const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request = nullptr);

    /// Set state to Closed, clear callback.
    void close();

    SessionState getState() const;

    /// Add a read request that must wait for the write with the given xid to commit.
    void addDeferredRead(Coordination::XID write_xid, const KeeperRequestForSession & read_request);

    /// Release all deferred reads waiting for the given write xid.
    /// Returns the released reads so the caller can dispatch them.
    KeeperRequestsForSessions releaseDeferredReads(Coordination::XID write_xid);

    /// Clear all deferred reads (e.g. when the session is terminated).
    void clearDeferredReads();

private:
    const int64_t session_id;
    mutable std::mutex mutex;
    SessionState state{SessionState::Active};
    ResponseCallback callback;
    std::unordered_map<Coordination::XID, KeeperRequestsForSessions> deferred_reads_;
    LoggerPtr log = getLogger("KeeperSession");
};

}
