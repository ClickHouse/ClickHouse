#pragma once

#include <Coordination/KeeperSessionReadBarrier_fwd.h>
#include <Common/logger_useful.h>

#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>


namespace DB
{

/// Per-session read barrier for ClickHouse Keeper.
///
/// Tracks pending writes per session and defers read requests until
/// the writes they depend on commit. Thread-safe: a lightweight global
/// mutex protects map-level operations; per-session mutation uses
/// per-session mutexes. The two locks are never held simultaneously.
class KeeperSessionReadBarrier
{
public:
    struct Settings
    {
        uint64_t max_pending_writes_per_session;
        uint64_t max_deferred_reads_per_session;
    };

    explicit KeeperSessionReadBarrier(const Settings & settings, LoggerPtr log);

    /// Register pending writes from a batch before sending to Raft.
    /// Only processes barrier-participating write requests.
    void registerPendingWrites(const KeeperRequestsForSessions & batch);

    /// Try to defer a read for the given session.
    DeferReadResult tryDeferRead(const KeeperRequestForSession & request);

    /// Resolve a committed write. Returns reads to execute (normal) and reads to fail (degraded mode).
    CommitResolveResult resolveCommit(int64_t session_id, Coordination::OpNum op_num, Coordination::XID xid);

    /// Resolve a rolled-back write. All collected reads should fail.
    DeferredReadRequests resolveRollback(int64_t session_id, Coordination::OpNum op_num, Coordination::XID xid);

    /// Resolve all writes in a failed batch. All collected reads should fail.
    DeferredReadRequests failBatch(const KeeperRequestsForSessions & batch);

    /// Close a session: remove its state and return all deferred reads for failure.
    DeferredReadRequests closeSession(int64_t session_id);

    /// Shutdown: drain all sessions and return all deferred reads for failure.
    DeferredReadRequests shutdown();

private:
    /// Identity key for matching commit/rollback callbacks to the correct pending write.
    /// Using (op_num, xid) because Auth requests share a fixed AUTH_XID that could collide
    /// with regular write xids; the op_num discriminator prevents cross-type mismatches.
    struct CallbackKey
    {
        Coordination::OpNum op_num;
        Coordination::XID xid;
        bool operator==(const CallbackKey &) const = default;
    };

    struct PendingWrite
    {
        uint64_t write_seq;
        CallbackKey key;
    };

    struct SessionState
    {
        std::mutex mutex;
        std::deque<PendingWrite> pending_writes;
        std::unordered_map<uint64_t, std::deque<DeferredReadRequest>> deferred_reads;
        uint64_t next_write_seq = 1;
        bool barrier_degraded = false;
        size_t deferred_reads_count = 0;
    };

    using SessionStatePtr = std::shared_ptr<SessionState>;

    /// Acquire per-session state pointer under the global map lock. Returns nullptr if absent.
    SessionStatePtr findSession(int64_t session_id) const;

    /// Acquire or create per-session state pointer under the global map lock.
    SessionStatePtr findOrCreateSession(int64_t session_id);

    /// Remove a session from the map and return its state pointer. Returns nullptr if absent.
    SessionStatePtr removeSession(int64_t session_id);

    /// Drain all deferred reads from a session state into the output vector.
    /// Decrements `CurrentMetrics`. Caller must hold `state.mutex`.
    static void drainAllDeferredReads(SessionState & state, DeferredReadRequests & out);

    /// Clear pending writes from a session state, decrementing `CurrentMetrics`.
    /// Caller must hold `state.mutex`.
    static void clearPendingWrites(SessionState & state);

    /// Resolve a single write by callback key. Shared by `resolveCommit` and `resolveRollback`.
    /// `scan_from_back`: true for rollback (reverse NuRaft order), false for commit.
    /// Appends collected reads to `out`. Returns true if degraded mode was entered.
    bool resolveWrite(
        SessionState & state,
        int64_t session_id,
        const CallbackKey & key,
        bool scan_from_back,
        DeferredReadRequests & out);

    /// Enter degraded mode: drain all reads, clear pending state.
    /// Caller must hold `state.mutex`.
    void enterDegradedMode(
        SessionState & state,
        int64_t session_id,
        const CallbackKey & key,
        DeferredReadRequests & out);

    Settings settings_;
    LoggerPtr log_;

    mutable std::mutex map_mutex_;
    std::unordered_map<int64_t, SessionStatePtr> sessions_;
};

}
