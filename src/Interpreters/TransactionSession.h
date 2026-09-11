#pragma once

#include <atomic>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <base/types.h>
#include <Common/TransactionID.h>
#include <Common/ZooKeeperNodeVersion.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>

namespace DB
{

/// Answers "is this TID still alive?". A TID dies either because its replica's session identity
/// moved on — the Keeper version of `<uuid>_session`, embedded in every TID that replica mints,
/// bumped by a restart or by a peer's `markDeadReplicas` — or because it was invalidated one
/// `(tid, job_id)` at a time through `invalid_tids/<key>`.
///
/// Owned by `TransactionManager`, which passes the Keeper handle to each method.
class TransactionSession
{
public:
    TransactionSession(
        String replicas_path_, String invalid_tids_path_, UUID my_replica_id_, Int64 dead_replica_threshold_ms_, LoggerPtr log_);

    /// Which call site is asking, so the log line can name the situation.
    enum class SessionCheck : uint8_t { AtStartup, AfterReconnect };

    ZooKeeperNodeVersion getVersion() const { return session_node_version.load(); }
    void setVersion(ZooKeeperNodeVersion version) { session_node_version.store(version); }

    /// Create or bump `_session` and adopt its version as our session identity.
    void initSessionNode(const zkutil::ZooKeeperPtr & zookeeper);
    void initInvalidTidsNode(const zkutil::ZooKeeperPtr & zookeeper);

    /// Create the ephemeral `_active`, dropping any node a previous session left behind.
    void createActiveNode(const zkutil::ZooKeeperPtr & zookeeper);
    /// Call before releasing the Keeper handle: the holder stores a raw `ZooKeeper &`.
    void releaseActiveNode();

    /// After a reconnect: re-advertise `_active`, then reconcile `_session`.
    std::optional<ZooKeeperNodeVersion> renewSession(const zkutil::ZooKeeperPtr & zookeeper);

    /// Take `_session` back if a peer bumped it to "DEAD". Returns the new version, or nullopt when
    /// it still matches ours. Only call once `_active` exists, or a peer can mark us dead again
    /// right after.
    std::optional<ZooKeeperNodeVersion> updateSessionVersionIfChanged(const zkutil::ZooKeeperPtr & zookeeper, SessionCheck check);

    void loadReplicaMap(const zkutil::ZooKeeperPtr & zookeeper);
    /// Bump `_session` to "DEAD" for peers whose `_active` has been absent past the threshold.
    void markDeadReplicas(const zkutil::ZooKeeperPtr & zookeeper);

    /// True if this `(tid, job_id)` is done for and its locks and parts can be reclaimed: either it
    /// is in `invalid_tids` — this exact job, or the whole transaction (`job_id = 0`), which covers
    /// every job of it — or the owning replica's `_session` version has moved past the TID.
    bool isTIDInvalid(const TransactionID & tid, JobId job_id) const;

    /// Only the invalidation records — no session check. `getCSN` needs exactly this: a
    /// `RolledBackCSN` from a live replica's own rollback must not imply a dead session.
    bool isInvalidated(TIDHash tid_hash) const;
    const String & invalidTidsPath() const { return invalid_tids_path; }

    /// Never throws: the Keeper write is left to the worker and retried, so this is safe on the
    /// noexcept rollback and merge-cleanup paths.
    void invalidateTID(const TransactionID & tid, JobId job_id, const String & reason) noexcept;
    /// In-memory half of `invalidateTID`, for a caller that wrote the record itself.
    void markTIDInvalidInMemory(const TransactionID & tid, JobId job_id) noexcept;
    /// A Multi is all-or-nothing, so a caller batching takeovers dedupes on this path.
    String getInvalidTIDRecordPath(const TransactionID & tid, JobId job_id) const;
    /// The `Create` the worker would write, for a caller needing the record to land atomically with
    /// its own write. Pair with `markTIDInvalidInMemory` once the Multi succeeds.
    Coordination::RequestPtr makeInvalidateTIDRequest(const TransactionID & tid, JobId job_id, const String & reason) const;

    void storePendingInvalidTids(const zkutil::ZooKeeperPtr & zookeeper);
    /// Rebuild the in-memory copy from Keeper, plus entries not yet written, so a local
    /// invalidation is never forgotten before it reaches Keeper.
    void loadInvalidTids(const zkutil::ZooKeeperPtr & zookeeper);
    /// Drop a record once the owning replica's session has moved past its TID: `isTIDInvalid` then
    /// reports it via the dead-session path anyway.
    void evictInvalidTids(const zkutil::ZooKeeperPtr & zookeeper);

    const String & replicasPath() const { return replicas_path; }
    String replicaTailPtrPath() const;
    String replicaActivePath() const;
    String replicaSessionPath() const;
    String replicaTailPtrPath(const UUID & id) const;
    String replicaActivePath(const UUID & id) const;
    String replicaSessionPath(const UUID & id) const;

private:
    const String replicas_path;
    const String invalid_tids_path;
    const UUID my_replica_id;
    /// Tunable via `transaction_log.dead_replica_threshold_ms`. Default 30s.
    const Int64 dead_replica_threshold_ms;
    LoggerPtr log;

    AtomicZooKeeperNodeVersion session_node_version;

    /// No lock: only the updating thread touches it, and `shutdown` releases it after joining.
    zkutil::EphemeralNodeHolderPtr active_node_holder;

    /// Own mutex so `invalidateTID`, called from noexcept paths, never waits on the locks the
    /// worker holds.
    mutable std::mutex invalid_tids_mutex;
    /// Keyed by `invalidationKey(tid.getHash(), job_id)`. `pending_invalid_store` holds what the
    /// worker has not written to Keeper yet, with the full TID so the record can carry it.
    std::unordered_set<TIDHash> invalid_tids TSA_GUARDED_BY(invalid_tids_mutex);
    struct PendingInvalidation
    {
        TransactionID tid;
        JobId job_id = Tx::MainJobId;
        String reason;  /// Diagnosis only, never parsed back.
    };
    std::vector<PendingInvalidation> pending_invalid_store TSA_GUARDED_BY(invalid_tids_mutex);

    mutable std::mutex replicas_mutex;
    /// Per-peer liveness, refreshed each iteration of the updating thread.
    struct ReplicaInfo
    {
        ZooKeeperNodeVersion session_node_version;  /// Last observed `_session` Keeper version.
        Int64 last_active_ts_ms{0};     /// Monotonic ms when `_active` was last observed.
        /// Peer's `_session` holds the "DEAD" sentinel, so `markDeadReplicas` skips it instead of
        /// re-firing the bump every iteration while the replica stays down.
        bool already_marked_dead{false};
    };
    std::unordered_map<UUID, ReplicaInfo> replica_info_map TSA_GUARDED_BY(replicas_mutex);
    /// Replicas named by a TID but missing from `replica_info_map` at lookup time. `loadReplicaMap`
    /// re-checks after the next scan and warns only if still absent. Value is a sample TID.
    mutable std::unordered_map<UUID, TransactionID> replicas_pending_session_check TSA_GUARDED_BY(replicas_mutex);
};

}
