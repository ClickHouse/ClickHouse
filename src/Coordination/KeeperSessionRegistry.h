#pragma once

#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperSession.h>

#include <atomic>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

/// Callback invoked by `setResponse` and `terminateSession` to deliver responses to clients.
/// Must be safe for concurrent invocation: `setResponse` (from `responseThread`) and
/// `terminateSession` (from dead session cleaner) may invoke copies of the same callback
/// concurrently for the same session.
using ZooKeeperResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request)>;

/// Thin facade over session-related state that used to live directly in `KeeperDispatcher`.
/// Owns the live-session set, response-callback maps, and the internal session-id counter.
/// All methods use the same locking strategy that the dispatcher originally used.
class KeeperSessionRegistry
{
public:
    /// Register a session: creates a `KeeperSession` object, adds to both
    /// `session_to_response_callback` and `live_sessions`, increments `KeeperAliveConnections`.
    /// Throws on duplicate session_id. Returns the new session object.
    KeeperSessionPtr registerSession(
        int64_t session_id,
        ZooKeeperResponseCallback callback,
        bool quorum_reads,
        KeeperSession::LocalReadCallback local_read_callback);

    /// Unregister a session's response callback and close its `KeeperSession`.
    /// Returns the callback so the caller can deliver a final ZSESSIONEXPIRED
    /// response outside the lock. Decrements `KeeperAliveConnections`.
    /// Returns empty callback if not found.
    ZooKeeperResponseCallback unregisterSession(int64_t session_id);

    /// Look up a session by id. Returns nullptr if not found.
    KeeperSessionPtr findSession(int64_t session_id) const;

    /// Check whether a session is in the live set.
    bool isSessionAlive(int64_t session_id) const;

    /// Add a session to the live set.
    void addLiveSession(int64_t session_id);

    /// Remove a session from the live set.
    void removeLiveSession(int64_t session_id);

    /// Look up the response callback for a session.
    /// Returns a copy (the entry stays in the map) or empty callback.
    ZooKeeperResponseCallback getSessionCallback(int64_t session_id) const;

    /// Check if a session has a registered response callback.
    bool hasSessionCallback(int64_t session_id) const;

    /// Move-extract the callback for a session and erase it from the map.
    /// Also decrements `KeeperAliveConnections`. Returns empty callback if not found.
    ZooKeeperResponseCallback extractAndEraseSessionCallback(int64_t session_id);

    /// Register a temporary callback for new session ID allocation.
    void registerNewSessionCallback(int64_t internal_id, ZooKeeperResponseCallback callback);

    /// Extract (and remove) the new-session callback for the given internal_id.
    /// Returns empty callback if not found.
    ZooKeeperResponseCallback extractNewSessionCallback(int64_t internal_id);

    /// Check if a new-session callback exists for the given internal_id.
    bool hasNewSessionCallback(int64_t internal_id) const;

    /// Returns the next internal session ID (atomic increment).
    int64_t nextInternalSessionId();

    /// Build close requests for all sessions that have registered callbacks.
    /// Returns the requests and clears `session_to_response_callback`.
    /// Used during shutdown.
    struct ShutdownResult
    {
        std::vector<std::pair<int64_t, ZooKeeperResponseCallback>> sessions;
    };
    ShutdownResult clearAllSessions();

private:
    mutable std::mutex live_sessions_mutex;
    std::unordered_set<int64_t> live_sessions;

    mutable std::mutex session_to_response_callback_mutex;
    /// Normal session response callbacks (keyed by session_id).
    std::unordered_map<int64_t, ZooKeeperResponseCallback> session_to_response_callback;
    /// Per-session objects (dual-tracked alongside callback map, temporary).
    std::unordered_map<int64_t, KeeperSessionPtr> sessions_;
    /// Temporary callbacks for new session ID requests (keyed by internal_id).
    std::unordered_map<int64_t, ZooKeeperResponseCallback> new_session_id_response_callback;

    std::atomic<int64_t> internal_session_id_counter{0};
};

}
