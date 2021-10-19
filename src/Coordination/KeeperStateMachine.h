#pragma once

#include <Coordination/KeeperStorage.h>
#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <common/logger_useful.h>
#include <Coordination/ThreadSafeQueue.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperSnapshotManager.h>

namespace DB
{

using ResponsesQueue = ThreadSafeQueue<KeeperStorage::ResponseForSession>;
using SnapshotsQueue = ConcurrentBoundedQueue<CreateSnapshotTask>;

/// ClickHouse Keeper state machine. Wrapper for KeeperStorage.
/// Responsible for entries commit, snapshots creation and so on.
class KeeperStateMachine : public nuraft::state_machine
{
public:
    KeeperStateMachine(
        ResponsesQueue & responses_queue_, SnapshotsQueue & snapshots_queue_,
        const std::string & snapshots_path_, const CoordinationSettingsPtr & coordination_settings_,
        const std::string & superdigest_ = "");

    /// Read state from the latest snapshot
    void init();

    /// Currently not supported
    nuraft::ptr<nuraft::buffer> pre_commit(const uint64_t /*log_idx*/, nuraft::buffer & /*data*/) override { return nullptr; }

    nuraft::ptr<nuraft::buffer> commit(const uint64_t log_idx, nuraft::buffer & data) override;

    /// Currently not supported
    void rollback(const uint64_t /*log_idx*/, nuraft::buffer & /*data*/) override {}

    uint64_t last_commit_index() override { return last_committed_idx; }

    /// Apply preliminarily saved (save_logical_snp_obj) snapshot to our state.
    bool apply_snapshot(nuraft::snapshot & s) override;

    nuraft::ptr<nuraft::snapshot> last_snapshot() override;

    /// Create new snapshot from current state.
    void create_snapshot(
        nuraft::snapshot & s,
        nuraft::async_result<bool>::handler_type & when_done) override;

    /// Save snapshot which was send by leader to us. After that we will apply it in apply_snapshot.
    void save_logical_snp_obj(
        nuraft::snapshot & s,
        uint64_t & obj_id,
        nuraft::buffer & data,
        bool is_first_obj,
        bool is_last_obj) override;

    /// Better name is `serialize snapshot` -- save existing snapshot (created by create_snapshot) into
    /// in-memory buffer data_out.
    int read_logical_snp_obj(
        nuraft::snapshot & s,
        void* & user_snp_ctx,
        uint64_t obj_id,
        nuraft::ptr<nuraft::buffer> & data_out,
        bool & is_last_obj) override;

    KeeperStorage & getStorage()
    {
        return *storage;
    }

    /// Process local read request
    void processReadRequest(const KeeperStorage::RequestForSession & request_for_session);

    std::vector<int64_t> getDeadSessions();

    void shutdownStorage();

private:

    /// In our state machine we always have a single snapshot which is stored
    /// in memory in compressed (serialized) format.
    SnapshotMetadataPtr latest_snapshot_meta = nullptr;
    nuraft::ptr<nuraft::buffer> latest_snapshot_buf = nullptr;

    CoordinationSettingsPtr coordination_settings;

    /// Main state machine logic
    KeeperStoragePtr storage;

    /// Save/Load and Serialize/Deserialize logic for snapshots.
    KeeperSnapshotManager snapshot_manager;

    /// Put processed responses into this queue
    ResponsesQueue & responses_queue;

    /// Snapshots to create by snapshot thread
    SnapshotsQueue & snapshots_queue;

    /// Mutex for snapshots
    std::mutex snapshots_lock;

    /// Lock for storage and responses_queue. It's important to process requests
    /// and push them to the responses queue while holding this lock. Otherwise
    /// we can get strange cases when, for example client send read request with
    /// watch and after that receive watch response and only receive response
    /// for request.
    std::mutex storage_and_responses_lock;

    /// Last committed Raft log number.
    std::atomic<uint64_t> last_committed_idx;
    Poco::Logger * log;

    /// Special part of ACL system -- superdigest specified in server config.
    const std::string superdigest;
};

}
