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

class KeeperStateMachine : public nuraft::state_machine
{
public:
    KeeperStateMachine(
        ResponsesQueue & responses_queue_, SnapshotsQueue & snapshots_queue_,
        const std::string & snapshots_path_, const CoordinationSettingsPtr & coordination_settings_,
        const std::string & superdigest_ = "");

    void init();

    nuraft::ptr<nuraft::buffer> pre_commit(const uint64_t /*log_idx*/, nuraft::buffer & /*data*/) override { return nullptr; }

    nuraft::ptr<nuraft::buffer> commit(const uint64_t log_idx, nuraft::buffer & data) override;

    void rollback(const uint64_t /*log_idx*/, nuraft::buffer & /*data*/) override {}

    uint64_t last_commit_index() override { return last_committed_idx; }

    bool apply_snapshot(nuraft::snapshot & s) override;

    nuraft::ptr<nuraft::snapshot> last_snapshot() override;

    void create_snapshot(
        nuraft::snapshot & s,
        nuraft::async_result<bool>::handler_type & when_done) override;

    void save_logical_snp_obj(
        nuraft::snapshot & s,
        uint64_t & obj_id,
        nuraft::buffer & data,
        bool is_first_obj,
        bool is_last_obj) override;

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

    void processReadRequest(const KeeperStorage::RequestForSession & request_for_session);

    std::vector<int64_t> getDeadSessions();

    void shutdownStorage();

private:

    SnapshotMetadataPtr latest_snapshot_meta = nullptr;
    nuraft::ptr<nuraft::buffer> latest_snapshot_buf = nullptr;

    CoordinationSettingsPtr coordination_settings;

    KeeperStoragePtr storage;

    KeeperSnapshotManager snapshot_manager;

    ResponsesQueue & responses_queue;

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

    const std::string superdigest;
};

}
