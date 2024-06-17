#pragma once

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperSnapshotManagerS3.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperStorage.h>

#include <libnuraft/nuraft.hxx>
#include <Common/ConcurrentBoundedQueue.h>


namespace DB
{
using ResponsesQueue = ConcurrentBoundedQueue<KeeperStorage::ResponseForSession>;
using SnapshotsQueue = ConcurrentBoundedQueue<CreateSnapshotTask>;

/// ClickHouse Keeper state machine. Wrapper for KeeperStorage.
/// Responsible for entries commit, snapshots creation and so on.
class KeeperStateMachine : public nuraft::state_machine
{
public:
    using CommitCallback = std::function<void(uint64_t, const KeeperStorage::RequestForSession &)>;

    KeeperStateMachine(
        ResponsesQueue & responses_queue_,
        SnapshotsQueue & snapshots_queue_,
        const KeeperContextPtr & keeper_context_,
        KeeperSnapshotManagerS3 * snapshot_manager_s3_,
        CommitCallback commit_callback_ = {},
        const std::string & superdigest_ = "");

    /// Read state from the latest snapshot
    void init();

    enum ZooKeeperLogSerializationVersion
    {
        INITIAL = 0,
        WITH_TIME = 1,
        WITH_ZXID_DIGEST = 2,
    };

    /// lifetime of a parsed request is:
    /// [preprocess/PreAppendLog -> commit]
    /// [preprocess/PreAppendLog -> rollback]
    /// on events like commit and rollback we can remove the parsed request to keep the memory usage at minimum
    /// request cache is also cleaned on session close in case something strange happened
    ///
    /// final - whether it's the final time we will fetch the request so we can safely remove it from cache
    /// serialization_version - information about which fields were parsed from the buffer so we can modify the buffer accordingly
    std::shared_ptr<KeeperStorage::RequestForSession> parseRequest(nuraft::buffer & data, bool final, ZooKeeperLogSerializationVersion * serialization_version = nullptr);

    bool preprocess(const KeeperStorage::RequestForSession & request_for_session);

    nuraft::ptr<nuraft::buffer> pre_commit(uint64_t log_idx, nuraft::buffer & data) override;

    nuraft::ptr<nuraft::buffer> commit(const uint64_t log_idx, nuraft::buffer & data) override; /// NOLINT

    /// Save new cluster config to our snapshot (copy of the config stored in StateManager)
    void commit_config(const uint64_t log_idx, nuraft::ptr<nuraft::cluster_config> & new_conf) override; /// NOLINT

    void rollback(uint64_t log_idx, nuraft::buffer & data) override;

    // allow_missing - whether the transaction we want to rollback can be missing from storage
    // (can happen in case of exception during preprocessing)
    void rollbackRequest(const KeeperStorage::RequestForSession & request_for_session, bool allow_missing);

    void rollbackRequestNoLock(
        const KeeperStorage::RequestForSession & request_for_session,
        bool allow_missing) TSA_NO_THREAD_SAFETY_ANALYSIS;

    uint64_t last_commit_index() override { return keeper_context->lastCommittedIndex(); }

    /// Apply preliminarily saved (save_logical_snp_obj) snapshot to our state.
    bool apply_snapshot(nuraft::snapshot & s) override;

    nuraft::ptr<nuraft::snapshot> last_snapshot() override;

    /// Create new snapshot from current state.
    void create_snapshot(nuraft::snapshot & s, nuraft::async_result<bool>::handler_type & when_done) override;

    /// Save snapshot which was send by leader to us. After that we will apply it in apply_snapshot.
    void save_logical_snp_obj(nuraft::snapshot & s, uint64_t & obj_id, nuraft::buffer & data, bool is_first_obj, bool is_last_obj) override;

    /// Better name is `serialize snapshot` -- save existing snapshot (created by create_snapshot) into
    /// in-memory buffer data_out.
    int read_logical_snp_obj(
        nuraft::snapshot & s, void *& user_snp_ctx, uint64_t obj_id, nuraft::ptr<nuraft::buffer> & data_out, bool & is_last_obj) override;

    // This should be used only for tests or keeper-data-dumper because it violates
    // TSA -- we can't acquire the lock outside of this class or return a storage under lock
    // in a reasonable way.
    KeeperStorage & getStorageUnsafe() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return *storage;
    }

    void shutdownStorage();

    ClusterConfigPtr getClusterConfig() const;

    /// Process local read request
    void processReadRequest(const KeeperStorage::RequestForSession & request_for_session);

    std::vector<int64_t> getDeadSessions();

    int64_t getNextZxid() const;

    KeeperStorage::Digest getNodesDigest() const;

    /// Introspection functions for 4lw commands
    uint64_t getLastProcessedZxid() const;

    uint64_t getNodesCount() const;
    uint64_t getTotalWatchesCount() const;
    uint64_t getWatchedPathsCount() const;
    uint64_t getSessionsWithWatchesCount() const;

    void dumpWatches(WriteBufferFromOwnString & buf) const;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const;
    void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const;

    uint64_t getSessionWithEphemeralNodesCount() const;
    uint64_t getTotalEphemeralNodesCount() const;
    uint64_t getApproximateDataSize() const;
    uint64_t getKeyArenaSize() const;
    uint64_t getLatestSnapshotSize() const;

    void recalculateStorageStats();

    void reconfigure(const KeeperStorage::RequestForSession& request_for_session);

private:
    CommitCallback commit_callback;
    /// In our state machine we always have a single snapshot which is stored
    /// in memory in compressed (serialized) format.
    SnapshotMetadataPtr latest_snapshot_meta = nullptr;
    std::shared_ptr<SnapshotFileInfo> latest_snapshot_info;
    nuraft::ptr<nuraft::buffer> latest_snapshot_buf = nullptr;

    /// Main state machine logic
    KeeperStoragePtr storage TSA_PT_GUARDED_BY(storage_and_responses_lock);

    /// Save/Load and Serialize/Deserialize logic for snapshots.
    KeeperSnapshotManager snapshot_manager;

    /// Put processed responses into this queue
    ResponsesQueue & responses_queue;

    /// Snapshots to create by snapshot thread
    SnapshotsQueue & snapshots_queue;

    /// Mutex for snapshots
    mutable std::mutex snapshots_lock;

    /// Lock for storage and responses_queue. It's important to process requests
    /// and push them to the responses queue while holding this lock. Otherwise
    /// we can get strange cases when, for example client send read request with
    /// watch and after that receive watch response and only receive response
    /// for request.
    mutable std::mutex storage_and_responses_lock;

    std::unordered_map<int64_t, std::unordered_map<Coordination::XID, std::shared_ptr<KeeperStorage::RequestForSession>>> parsed_request_cache;
    uint64_t min_request_size_to_cache{0};
    /// we only need to protect the access to the map itself
    /// requests can be modified from anywhere without lock because a single request
    /// can be processed only in 1 thread at any point
    std::mutex request_cache_mutex;

    LoggerPtr log;

    /// Cluster config for our quorum.
    /// It's a copy of config stored in StateManager, but here
    /// we also write it to disk during snapshot. Must be used with lock.
    mutable std::mutex cluster_config_lock;
    ClusterConfigPtr cluster_config;

    /// Special part of ACL system -- superdigest specified in server config.
    const std::string superdigest;

    KeeperContextPtr keeper_context;

    KeeperSnapshotManagerS3 * snapshot_manager_s3;

    KeeperStorage::ResponseForSession processReconfiguration(
        const KeeperStorage::RequestForSession& request_for_session)
        TSA_REQUIRES(storage_and_responses_lock);
};
}
