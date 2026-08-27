#pragma once

#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperSnapshotManagerS3.h>
#include <Coordination/KeeperContext.h>
#include <Common/SharedMutex.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

#include <base/defines.h>
#include <libnuraft/nuraft.hxx>
#include <Common/ConcurrentBoundedQueue.h>
#include <limits>
#include <optional>

namespace DB
{
class ResponseForSession;

struct CoordinationSettings;
using CoordinationSettingsPtr = std::shared_ptr<CoordinationSettings>;
//TODO(keeper-batch2) Change to take a batch of responses (KeeperResponsesForSessions), so queue pushes and flow-control accounting in onResponse happen once per batch instead of once per response.
using KeeperResponseCallback = std::function<void(KeeperResponseForSession)>; // noexcept
using SnapshotsQueue = ConcurrentBoundedQueue<CreateSnapshotTask>;

struct KeeperStorageStats;
class KeeperLogStore;

struct AsynchronousMetricValue;
using AsynchronousMetricValues = std::unordered_map<std::string, AsynchronousMetricValue>;

struct ISnapshotLoader;

struct KeeperSnapshotStatus
{
    uint64_t last_log_index;
    String path;
    DiskPtr disk;
    SnapshotFileInfoPtr pin;
    bool is_received;
};

/// ClickHouse Keeper state machine. Wrapper for KeeperStorage.
/// Responsible for entries commit, snapshots creation and so on.
class KeeperStateMachine : public nuraft::state_machine
{
public:
    using CommitCallback = std::function<void(uint64_t, const KeeperRequestForSession &)>;

    KeeperStateMachine(
        KeeperResponseCallback response_callback_,
        SnapshotsQueue & snapshots_queue_,
        const KeeperContextPtr & keeper_context_,
        KeeperSnapshotManagerS3 * snapshot_manager_s3_,
        CommitCallback commit_callback_ = {},
        const std::string & superdigest_ = "");

    /// Read state from the latest snapshot
    void init();

    void setLogStore(KeeperLogStore * log_store_);
    KeeperLogStore * getLogStore() { return log_store; }

    enum ZooKeeperLogSerializationVersion
    {
        /// Versions of the legacy single-request format, sniffed from the buffer length by
        /// parseRequest. New entries are only written in the REQUEST_BATCH format.
        INITIAL = 0,
        WITH_TIME = 1,
        WITH_ZXID_DIGEST = 2,
        WITH_XID_64 = 3,
        WITH_OPTIONAL_TRACING_CONTEXT = 4,
        /// Format where one log entry contains a batch of keeper requests.
        REQUEST_BATCH = 5,
    };

    /// Batch log entry format. Distinguished from legacy single-request entries by the first
    /// int64: legacy entries start with a session id, which is never INT64_MIN.
    static constexpr int64_t BATCH_ENTRY_MARKER = std::numeric_limits<int64_t>::min();
    static constexpr size_t BATCH_ENTRY_PATCHABLE_FIELDS_OFFSET = sizeof(int64_t) + sizeof(uint32_t);

    /// Serialize the batch into log entry buffers. With use_batched_format the whole batch
    /// becomes one entry in the batch format; otherwise one entry per request in the legacy
    /// single-request format (for compatibility with clusters that have older servers).
    static std::vector<nuraft::ptr<nuraft::buffer>> serializeRequestBatch(const KeeperRequestBatch & batch, bool use_batched_format);

    /// Overwrite the patchable header fields of a batch log entry produced by serializeRequestBatch.
    static void patchSerializedRequestBatch(nuraft::buffer & data, ZooKeeperLogSerializationVersion serialization_version, size_t patched_fields_offset, const KeeperRequestBatch & batch);

    /// Parses both a batch entry and a legacy single-request entry (returned as a batch of one).
    ///
    /// Lifetime of a parsed batch is:
    /// [preprocess/PreAppendLog -> commit]
    /// [preprocess/PreAppendLog -> rollback]
    /// on events like commit and rollback we can remove the parsed batch from cache to keep the
    /// memory usage at minimum; the cache is also cleaned on session close in case something
    /// strange happened.
    /// final - whether it's the final time we will fetch the batch so we can safely remove it from cache
    /// serialization_version - which format/fields were parsed from the buffer so we can modify the buffer accordingly
    /// patched_fields_offset - where the leader-patched fields live: for batch entries it is
    /// BATCH_ENTRY_PATCHABLE_FIELDS_OFFSET; for legacy entries it is the request end position
    /// (which of the fields are present there follows from `serialization_version`).
    std::shared_ptr<KeeperRequestBatch> parseRequestBatch(
        nuraft::buffer & data,
        bool final,
        ZooKeeperLogSerializationVersion * serialization_version = nullptr,
        size_t * patched_fields_offset = nullptr);

    /// Preprocess all requests of the batch in order. The batch must have first_zxid assigned.
    /// Returns the digest after the last request, or nullopt if the storage is finalized.
    /// Idempotent on the leader: if the batch was already preprocessed (in the PreAppendLogLeader
    /// callback), only stamps batch.log_idx on its transactions.
    std::optional<KeeperDigest> preprocessBatch(const KeeperRequestBatch & batch, bool lock_mutex);

    nuraft::ptr<nuraft::buffer> pre_commit(uint64_t log_idx, nuraft::buffer & data) override;

    nuraft::ptr<nuraft::buffer> commit(const uint64_t log_idx, nuraft::buffer & data) override; /// NOLINT

    void commit_config(const uint64_t log_idx, nuraft::ptr<nuraft::cluster_config> & new_conf) override; /// NOLINT

    void rollback(uint64_t log_idx, nuraft::buffer & data) override;

    // Roll back the batch's requests in reverse zxid order.
    // allow_missing - whether the transactions we want to rollback can be missing from storage
    // (can happen in case of exception during preprocessing)
    void rollbackRequestBatch(const KeeperRequestBatch & batch, bool allow_missing);

    uint64_t last_commit_index() override { return keeper_context->lastCommittedIndex(); }

    nuraft::ptr<nuraft::snapshot> last_snapshot() override;

    /// Apply preliminarily saved (save_logical_snp_obj) snapshot to our state.
    bool apply_snapshot(nuraft::snapshot & s) override;

    /// Create new snapshot from current state.
    void create_snapshot(nuraft::snapshot & s, nuraft::async_result<bool>::handler_type & when_done) override;

    /// Save snapshot which was send by leader to us. After that we will apply it in apply_snapshot.
    void save_logical_snp_obj(nuraft::snapshot & s, uint64_t & obj_id, nuraft::buffer & data, bool is_first_obj, bool is_last_obj) override;

    int read_logical_snp_obj(
        nuraft::snapshot & s, void *& user_snp_ctx, uint64_t obj_id, nuraft::ptr<nuraft::buffer> & data_out, bool & is_last_obj) override;

    void free_user_snp_ctx(void *& user_snp_ctx) override;

    // This should be used only for tests or keeper-data-dumper because it violates
    // TSA -- we can't acquire the lock outside of this class or return a storage under lock
    // in a reasonable way.
    KeeperStorage & getStorageUnsafe()
    {
        chassert(storage);
        return *storage;
    }

    /// Issues a lock-free MVCC-style read view of the storage container.
    std::unique_ptr<KeeperNodesReadView> getStorageReadView() const;

    void shutdownStorage();

    ClusterConfigPtr getClusterConfig() const;

    /// Process local read requests
    void processReadRequests(const KeeperRequestsForSessions & requests);

    std::vector<int64_t> getDeadSessions();

    int64_t getNextZxid() const;

    KeeperDigest getNodesDigest() const;

    /// Introspection functions for 4lw commands
    int64_t getLastProcessedZxid() const;

    KeeperStorageStats getStorageStats() const;

    /// Like getStorageStats, but also populates `new_values` with node-storage-specific metrics
    /// (see KeeperNodesStorage::fillAsynchronousMetrics).
    KeeperStorageStats getStorageStatsAndAsynchronousMetrics(AsynchronousMetricValues & new_values) const;

    void dumpWatches(WriteBufferFromOwnString & buf) const;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const;
    void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const;

    uint64_t getLatestSnapshotSize() const;

    void recalculateStorageStats();

    void reconfigure(const KeeperRequestForSession& request_for_session);
    std::vector<std::pair<std::string, Int32>> getExpiredTTLPathsForGarbageCollector(size_t batch_size) const;
    std::vector<std::pair<std::string, Int32>> getContainerCandidatesForGarbageCollector(size_t batch_size, UInt64 max_never_used_interval_ms) const;

    std::vector<KeeperSnapshotStatus> getSnapshotsStatus() const;

    /// Cancel an in-progress snapshot receive: remove partial files and reset the context.
    void cancelIfHasUnfinishedSnapshotReceive() TSA_REQUIRES(snapshots_lock);

    /// Return a pin for `log_idx`, or `nullptr` if absent. The pin defers
    /// unlink and cross-disk moves until the transfer releases it.
    /// Caller must hold `snapshots_lock`.
    SnapshotFileInfoPtr getSnapshotPinUnlocked(uint64_t log_idx) const TSA_REQUIRES(snapshots_lock);

    /// Call after loading `storage` from snapshot.
    /// Does preprocessRequest on log entries to populate storage's UncommittedState.
    void preprocessUncommittedLogEntries(uint64_t start_idx, uint64_t end_idx, bool lock_mutex);

private:
    /// Advance the mark (no-op if older; LOGICAL_ERROR backstop on equal index with a
    /// different term) and re-point retention protection at its backing snapshot file.
    void advanceLatestSnapshotMeta(const SnapshotMetadataPtr & candidate) TSA_REQUIRES(snapshots_lock);

    KeeperResponseForSession processReconfiguration(const KeeperRequestForSession & request_for_session);

    CommitCallback commit_callback;

    /// Monotonic high-water mark reported to NuRaft via `last_snapshot`. Advanced only via
    /// `advanceLatestSnapshotMeta`; never regresses; retention pins its registry entry.
    /// A saved-but-not-applied install does not advance it, so the manager's map max may
    /// exceed it; `init` adopts the newest disk snapshot.
    SnapshotMetadataPtr latest_snapshot_meta TSA_GUARDED_BY(snapshots_lock) = nullptr;

    /// Per-install context: stamped by the `save_logical_snp_obj` tail, validated and
    /// consumed by the matching `apply_snapshot` on every exit (identity =
    /// (last_log_idx, last_log_term)). May move to ANY index — a lower-index re-install
    /// after leadership churn is valid. A lingering value is harmless; the next stamp
    /// overwrites it.
    SnapshotMetadataPtr pending_snapshot_to_apply TSA_GUARDED_BY(snapshots_lock) = nullptr;

    /// Follower snapshot receive context.
    /// Kept for the duration of snapshot transfer, reset on completion/error.
    std::unique_ptr<SnapshotReceiveCtx> snapshot_receive_ctx TSA_GUARDED_BY(snapshots_lock);

    /// Leader snapshot loader info, stored only in case of remote disk.
    /// Shared across concurrent followers transferring the same snapshot.
    /// Reset when a new snapshot is created or when the loader encounters an error.
    std::shared_ptr<ISnapshotLoader> snapshot_loader_info TSA_GUARDED_BY(snapshots_lock);

    /// `log_idx` for the cached remote `snapshot_loader_info`.
    /// Requests for a different retained snapshot reset the cache.
    uint64_t snapshot_loader_info_log_idx TSA_GUARDED_BY(snapshots_lock) = 0;

    /// Cached size of the newest REGISTERED snapshot (the manager's map max). Updated under
    /// `snapshots_lock` only when the written index is the map max; read lock-free by
    /// `getLatestSnapshotSize` (`mntr`). Stale values self-correct on the next snapshot.
    std::atomic<uint64_t> latest_snapshot_size{0};

    CoordinationSettingsPtr coordination_settings;

    /// Function to put processed responses into a queue for sending to the client.
    KeeperResponseCallback response_callback;

    /// Snapshots to create by snapshot thread
    SnapshotsQueue & snapshots_queue;

    /// Mutex for snapshots
    mutable std::mutex snapshots_lock;

    /// Lock for the storage
    /// Storage works in thread-safe way ONLY for preprocessing/processing
    /// In any other case, unique storage lock needs to be taken
    mutable SharedMutex state_machine_storage_mutex;
    /// Lock for processing and response_callback. It's important to process requests
    /// and push them to the responses queue while holding this lock. Otherwise
    /// we can get strange cases when, for example client send read request with
    /// watch and after that receive watch response and only receive response
    /// for request.
    mutable std::mutex process_and_responses_lock;

    /// Serialize the request as a log entry in the legacy single-request format.
    /// Used only by serializeRequestBatch when the batched format is disabled.
    static nuraft::ptr<nuraft::buffer> serializeRequestInOldFormat(const KeeperRequestForSession & request_for_session);

    /// Parse a legacy single-request log entry as a batch of one, going through
    /// parsed_batch_cache. Used only by parseRequestBatch.
    std::shared_ptr<KeeperRequestBatch> parseRequestInOldFormat(
        nuraft::buffer & data,
        bool final,
        ZooKeeperLogSerializationVersion * serialization_version,
        size_t * patched_fields_offset);

    /// Whether a parsed log entry should go through parsed_batch_cache;
    /// (session_id, xid) are of the entry's first request.
    bool shouldCacheParsedEntry(int64_t session_id, int64_t xid, size_t entry_size) const;

    /// Cache of parsed batch log entries, keyed by (session_id, xid) of the batch's first request.
    /// Entries of closed sessions are erased on Close commit in case something strange happened.
    //TODO(keeper-batch2) Replace this cache by plumbing the parsed batch through nuraft as an opaque pointer attached to the log entry, instead of re-parsing (and caching) the exact buffer we serialized microseconds earlier in the same process.
    std::unordered_map<int64_t, std::unordered_map<Coordination::XID, std::shared_ptr<KeeperRequestBatch>>> parsed_batch_cache;
    uint64_t min_request_size_to_cache{0};
    /// we only need to protect the access to the map itself
    /// batches can be modified from anywhere without lock because a single batch
    /// can be processed only in 1 thread at any point
    std::mutex request_cache_mutex;

    LoggerPtr log;

    /// Cluster config for our quorum.
    /// It's a copy of config stored in StateManager, but here
    /// we also write it to disk during snapshot. Must be used with lock.
    mutable std::mutex cluster_config_lock;
    ClusterConfigPtr cluster_config;

    ThreadPool read_pool;
    /// Special part of ACL system -- superdigest specified in server config.
    const std::string superdigest;

    KeeperContextPtr keeper_context;

    KeeperSnapshotManagerS3 * snapshot_manager_s3;

    KeeperLogStore * log_store = nullptr;

    struct DetachedSnapshotReceiveFiles
    {
        DiskPtr disk;
        std::string snapshot_file_name;
        uint64_t log_idx = 0;
    };

    std::optional<DetachedSnapshotReceiveFiles> detachUnfinishedSnapshotReceiveForCleanup() TSA_REQUIRES(snapshots_lock);
    void cleanupDetachedSnapshotReceive(const DetachedSnapshotReceiveFiles & files);
    void runSnapshotMaintenance(SnapshotMaintenanceTasks && tasks);

    /// Phase 2 of the create task: write+sync under a fresh unique name, outside `snapshots_lock`.
    SnapshotFileInfoPtr writeSnapshotToDisk(const KeeperStorageSnapshot & snapshot);

    struct LocalSnapshotPublishOutcome
    {
        SnapshotFileInfoPtr published;       /// entry to report to NuRaft / S3
        SnapshotFileInfoPtr loser_to_remove; /// retired; unlinked outside the lock when the last ref drops
        bool won = false;                    /// true iff our written file became the registered entry
    };

    /// Phase 3: metadata-only publication with adopt-on-conflict. No disk IO.
    /// Adopt-on-conflict: the caller unlinks loser_to_remove outside the lock.
    LocalSnapshotPublishOutcome publishWrittenSnapshot(
        const SnapshotFileInfoPtr & written_file_info,
        const SnapshotMetadataPtr & written_snapshot_meta,
        std::optional<uint64_t> written_size) TSA_REQUIRES(snapshots_lock);

    /// Main state machine logic. Swapped by `apply_snapshot` under the exclusive
    /// storage lock; in-flight create tasks capture their own reference.
    std::shared_ptr<KeeperStorage> storage;

    /// Save/Load and Serialize/Deserialize logic for snapshots.
    KeeperSnapshotManager snapshot_manager;
};

}
