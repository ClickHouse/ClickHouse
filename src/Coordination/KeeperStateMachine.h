#pragma once

#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperSnapshotManagerS3.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperStorage.h>

#include <libnuraft/nuraft.hxx>
#include <Common/ConcurrentBoundedQueue.h>


namespace DB
{
struct CoordinationSettings;
using CoordinationSettingsPtr = std::shared_ptr<CoordinationSettings>;
using ResponsesQueue = ConcurrentBoundedQueue<KeeperStorageBase::ResponseForSession>;
using SnapshotsQueue = ConcurrentBoundedQueue<CreateSnapshotTask>;

class IKeeperStateMachine : public nuraft::state_machine
{
public:
    using CommitCallback = std::function<void(uint64_t, const KeeperStorageBase::RequestForSession &)>;

    IKeeperStateMachine(
        ResponsesQueue & responses_queue_,
        SnapshotsQueue & snapshots_queue_,
        const KeeperContextPtr & keeper_context_,
        KeeperSnapshotManagerS3 * snapshot_manager_s3_,
        CommitCallback commit_callback_,
        const std::string & superdigest_);

    /// Read state from the latest snapshot
    virtual void init() = 0;

    enum ZooKeeperLogSerializationVersion
    {
        INITIAL = 0,
        WITH_TIME = 1,
        WITH_ZXID_DIGEST = 2,
        WITH_XID_64 = 3,
    };

    /// lifetime of a parsed request is:
    /// [preprocess/PreAppendLog -> commit]
    /// [preprocess/PreAppendLog -> rollback]
    /// on events like commit and rollback we can remove the parsed request to keep the memory usage at minimum
    /// request cache is also cleaned on session close in case something strange happened
    ///
    /// final - whether it's the final time we will fetch the request so we can safely remove it from cache
    /// serialization_version - information about which fields were parsed from the buffer so we can modify the buffer accordingly
    std::shared_ptr<KeeperStorageBase::RequestForSession>
    parseRequest(nuraft::buffer & data, bool final, ZooKeeperLogSerializationVersion * serialization_version = nullptr);

    static nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(const KeeperStorageBase::RequestForSession & request_for_session);

    virtual bool preprocess(const KeeperStorageBase::RequestForSession & request_for_session) = 0;

    void commit_config(const uint64_t log_idx, nuraft::ptr<nuraft::cluster_config> & new_conf) override; /// NOLINT

    void rollback(uint64_t log_idx, nuraft::buffer & data) override;

    // allow_missing - whether the transaction we want to rollback can be missing from storage
    // (can happen in case of exception during preprocessing)
    virtual void rollbackRequest(const KeeperStorageBase::RequestForSession & request_for_session, bool allow_missing) = 0;

    uint64_t last_commit_index() override { return keeper_context->lastCommittedIndex(); }

    nuraft::ptr<nuraft::snapshot> last_snapshot() override;

    /// Create new snapshot from current state.
    void create_snapshot(nuraft::snapshot & s, nuraft::async_result<bool>::handler_type & when_done) override = 0;

    /// Save snapshot which was send by leader to us. After that we will apply it in apply_snapshot.
    void save_logical_snp_obj(nuraft::snapshot & s, uint64_t & obj_id, nuraft::buffer & data, bool is_first_obj, bool is_last_obj) override = 0;

    int read_logical_snp_obj(
        nuraft::snapshot & s, void *& user_snp_ctx, uint64_t obj_id, nuraft::ptr<nuraft::buffer> & data_out, bool & is_last_obj) override;

    virtual void shutdownStorage() = 0;

    ClusterConfigPtr getClusterConfig() const;

    virtual void processReadRequest(const KeeperStorageBase::RequestForSession & request_for_session) = 0;

    virtual std::vector<int64_t> getDeadSessions() = 0;

    virtual int64_t getNextZxid() const = 0;

    virtual KeeperStorageBase::Digest getNodesDigest() const = 0;

    /// Introspection functions for 4lw commands
    virtual uint64_t getLastProcessedZxid() const = 0;

    virtual const KeeperStorageBase::Stats & getStorageStats() const = 0;

    virtual uint64_t getNodesCount() const = 0;
    virtual uint64_t getTotalWatchesCount() const = 0;
    virtual uint64_t getWatchedPathsCount() const = 0;
    virtual uint64_t getSessionsWithWatchesCount() const = 0;

    virtual void dumpWatches(WriteBufferFromOwnString & buf) const = 0;
    virtual void dumpWatchesByPath(WriteBufferFromOwnString & buf) const = 0;
    virtual void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const = 0;

    virtual uint64_t getSessionWithEphemeralNodesCount() const = 0;
    virtual uint64_t getTotalEphemeralNodesCount() const = 0;
    virtual uint64_t getApproximateDataSize() const = 0;
    virtual uint64_t getKeyArenaSize() const = 0;
    virtual uint64_t getLatestSnapshotSize() const = 0;

    virtual void recalculateStorageStats() = 0;

    virtual void reconfigure(const KeeperStorageBase::RequestForSession& request_for_session) = 0;

protected:
    CommitCallback commit_callback;
    /// In our state machine we always have a single snapshot which is stored
    /// in memory in compressed (serialized) format.
    SnapshotMetadataPtr latest_snapshot_meta = nullptr;
    std::shared_ptr<SnapshotFileInfo> latest_snapshot_info;
    nuraft::ptr<nuraft::buffer> latest_snapshot_buf = nullptr;

    CoordinationSettingsPtr coordination_settings;

    /// Save/Load and Serialize/Deserialize logic for snapshots.
    /// Put processed responses into this queue
    ResponsesQueue & responses_queue;

    /// Snapshots to create by snapshot thread
    SnapshotsQueue & snapshots_queue;

    /// Mutex for snapshots
    mutable std::mutex snapshots_lock;

    /// Lock for the storage
    /// Storage works in thread-safe way ONLY for preprocessing/processing
    /// In any other case, unique storage lock needs to be taken
    mutable SharedMutex storage_mutex;
    /// Lock for processing and responses_queue. It's important to process requests
    /// and push them to the responses queue while holding this lock. Otherwise
    /// we can get strange cases when, for example client send read request with
    /// watch and after that receive watch response and only receive response
    /// for request.
    mutable std::mutex process_and_responses_lock;

    std::unordered_map<int64_t, std::unordered_map<Coordination::XID, std::shared_ptr<KeeperStorageBase::RequestForSession>>> parsed_request_cache;
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

    ThreadPool read_pool;
    /// Special part of ACL system -- superdigest specified in server config.
    const std::string superdigest;

    KeeperContextPtr keeper_context;

    KeeperSnapshotManagerS3 * snapshot_manager_s3;

    virtual KeeperStorageBase::ResponseForSession processReconfiguration(const KeeperStorageBase::RequestForSession & request_for_session)
        = 0;
};

/// ClickHouse Keeper state machine. Wrapper for KeeperStorage.
/// Responsible for entries commit, snapshots creation and so on.
template<typename Storage>
class KeeperStateMachine : public IKeeperStateMachine
{
public:
    /// using CommitCallback = std::function<void(uint64_t, const KeeperStorage::RequestForSession &)>;

    KeeperStateMachine(
        ResponsesQueue & responses_queue_,
        SnapshotsQueue & snapshots_queue_,
        /// const CoordinationSettingsPtr & coordination_settings_,
        const KeeperContextPtr & keeper_context_,
        KeeperSnapshotManagerS3 * snapshot_manager_s3_,
        CommitCallback commit_callback_ = {},
        const std::string & superdigest_ = "");

    /// Read state from the latest snapshot
    void init() override;

    bool preprocess(const KeeperStorageBase::RequestForSession & request_for_session) override;

    nuraft::ptr<nuraft::buffer> pre_commit(uint64_t log_idx, nuraft::buffer & data) override;

    nuraft::ptr<nuraft::buffer> commit(const uint64_t log_idx, nuraft::buffer & data) override; /// NOLINT

    // allow_missing - whether the transaction we want to rollback can be missing from storage
    // (can happen in case of exception during preprocessing)
    void rollbackRequest(const KeeperStorageBase::RequestForSession & request_for_session, bool allow_missing) override;

    /// Apply preliminarily saved (save_logical_snp_obj) snapshot to our state.
    bool apply_snapshot(nuraft::snapshot & s) override;

    /// Create new snapshot from current state.
    void create_snapshot(nuraft::snapshot & s, nuraft::async_result<bool>::handler_type & when_done) override;

    /// Save snapshot which was send by leader to us. After that we will apply it in apply_snapshot.
    void save_logical_snp_obj(nuraft::snapshot & s, uint64_t & obj_id, nuraft::buffer & data, bool is_first_obj, bool is_last_obj) override;

    // This should be used only for tests or keeper-data-dumper because it violates
    // TSA -- we can't acquire the lock outside of this class or return a storage under lock
    // in a reasonable way.
    Storage & getStorageUnsafe()
    {
        return *storage;
    }

    void shutdownStorage() override;

    /// Process local read request
    void processReadRequest(const KeeperStorageBase::RequestForSession & request_for_session) override;

    std::vector<int64_t> getDeadSessions() override;

    int64_t getNextZxid() const override;

    KeeperStorageBase::Digest getNodesDigest() const override;

    /// Introspection functions for 4lw commands
    uint64_t getLastProcessedZxid() const override;

    const KeeperStorageBase::Stats & getStorageStats() const override;

    uint64_t getNodesCount() const override;
    uint64_t getTotalWatchesCount() const override;
    uint64_t getWatchedPathsCount() const override;
    uint64_t getSessionsWithWatchesCount() const override;

    void dumpWatches(WriteBufferFromOwnString & buf) const override;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const override;
    void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const override;

    uint64_t getSessionWithEphemeralNodesCount() const override;
    uint64_t getTotalEphemeralNodesCount() const override;
    uint64_t getApproximateDataSize() const override;
    uint64_t getKeyArenaSize() const override;
    uint64_t getLatestSnapshotSize() const override;

    void recalculateStorageStats() override;

    void reconfigure(const KeeperStorageBase::RequestForSession& request_for_session) override;

private:
    /// Main state machine logic
    std::unique_ptr<Storage> storage;

    /// Save/Load and Serialize/Deserialize logic for snapshots.
    KeeperSnapshotManager<Storage> snapshot_manager;

    KeeperStorageBase::ResponseForSession processReconfiguration(const KeeperStorageBase::RequestForSession & request_for_session) override;
};

}
