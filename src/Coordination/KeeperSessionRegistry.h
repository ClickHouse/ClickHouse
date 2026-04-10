#pragma once

#include <Coordination/KeeperSession.h>
#include <Coordination/KeeperContext.h>

#include <atomic>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

namespace DB
{

class KeeperRequestsQueue;

class KeeperSessionRegistry
{
public:
    /// @param keeper_context  Extracts coordination settings (quorum_reads, max_session_active_requests).
    /// @param requests_queue  Shared request queue — sessions obtain subqueue handles from it.
    /// @param local_read_dispatch  Callback for local read execution, passed to every session.
    KeeperSessionRegistry(
        KeeperContextPtr keeper_context,
        KeeperRequestsQueue & requests_queue,
        KeeperSession::LocalReadCallback local_read_dispatch);

    /// Whether reads go through Raft (quorum_reads setting).
    bool quorumReads() const { return quorum_reads; }

    /// Per-session limit on active_requests (0 = unlimited).
    size_t maxSessionActiveRequests() const { return max_session_active_requests; }

    /// Creates a KeeperSession, inserts into the active map, and returns it.
    /// Throws LOGICAL_ERROR on duplicate session_id.
    KeeperSessionPtr registerSession(int64_t session_id, KeeperSession::ResponseCallback callback);

    /// Returns the session or nullptr if not found.
    /// Registry mutex is released before returning -- the caller uses
    /// the session's own mutex for further operations.
    KeeperSessionPtr findSession(int64_t session_id) const;

    /// Removes session from the active map and decrements KeeperAliveConnections.
    /// Returns the detached session (still alive via shared_ptr) or nullptr.
    KeeperSessionPtr detachSession(int64_t session_id);

    /// Registers a temporary callback for new session ID allocation.
    void registerNewSessionCallback(int64_t internal_id, ZooKeeperResponseCallback callback);

    /// Extracts (and removes) the new-session callback for the given internal_id.
    /// Returns empty callback if not found or if server_id != our_server_id.
    ZooKeeperResponseCallback extractNewSessionCallback(int64_t internal_id, int32_t server_id, int32_t our_server_id);

    /// Returns the next internal session ID (atomic increment).
    int64_t nextInternalSessionId();

    /// Atomically removes all sessions and new-session callbacks.
    /// Returns the detached sessions so the caller can send Close requests.
    std::vector<KeeperSessionPtr> shutdown();

private:
    mutable std::shared_mutex mutex;

    /// `sessions` owns the sessions and defines the iteration set; order is stable between register/detach operations but not globally predictable due to swap-and-pop removal.
    /// `session_index` is a derived O(1) lookup from session_id to position in `sessions`.
    /// Both must be kept in sync under `mutex`.
    std::vector<KeeperSessionPtr> sessions;
    std::unordered_map<int64_t, size_t> session_index;

    std::unordered_map<int64_t, ZooKeeperResponseCallback> new_session_callbacks;
    std::atomic<int64_t> internal_session_id_counter{0};

    bool quorum_reads = false;
    size_t max_session_active_requests = 0;

    /// Local read dispatch callback, passed to every session at construction.
    KeeperSession::LocalReadCallback local_read_dispatch;

    /// Non-owning pointer to the shared request queue. Set via constructor.
    KeeperRequestsQueue * requests_queue = nullptr;
};

}
