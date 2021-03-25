#pragma once

#include <Coordination/NuKeeperStorage.h>
#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <common/logger_useful.h>
#include <Coordination/ThreadSafeQueue.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/NuKeeperSnapshotManager.h>

namespace DB
{

using ResponsesQueue = ThreadSafeQueue<NuKeeperStorage::ResponseForSession>;
using SnapshotsQueue = ConcurrentBoundedQueue<CreateSnapshotTask>;

class NuKeeperStateMachine : public nuraft::state_machine
{
public:
    NuKeeperStateMachine(ResponsesQueue & responses_queue_, SnapshotsQueue & snapshots_queue_, const std::string & snapshots_path_, const CoordinationSettingsPtr & coordination_settings_);

    void init();

    nuraft::ptr<nuraft::buffer> pre_commit(const size_t /*log_idx*/, nuraft::buffer & /*data*/) override { return nullptr; }

    nuraft::ptr<nuraft::buffer> commit(const size_t log_idx, nuraft::buffer & data) override;

    void rollback(const size_t /*log_idx*/, nuraft::buffer & /*data*/) override {}

    size_t last_commit_index() override { return last_committed_idx; }

    bool apply_snapshot(nuraft::snapshot & s) override;

    nuraft::ptr<nuraft::snapshot> last_snapshot() override;

    void create_snapshot(
        nuraft::snapshot & s,
        nuraft::async_result<bool>::handler_type & when_done) override;

    void save_logical_snp_obj(
        nuraft::snapshot & s,
        size_t & obj_id,
        nuraft::buffer & data,
        bool is_first_obj,
        bool is_last_obj) override;

    int read_logical_snp_obj(
        nuraft::snapshot & s,
        void* & user_snp_ctx,
        ulong obj_id,
        nuraft::ptr<nuraft::buffer> & data_out,
        bool & is_last_obj) override;

    NuKeeperStorage & getStorage()
    {
        return storage;
    }

    void processReadRequest(const NuKeeperStorage::RequestForSession & request_for_session);

    std::unordered_set<int64_t> getDeadSessions();

    void shutdownStorage();

private:

    SnapshotMetadataPtr latest_snapshot_meta = nullptr;
    nuraft::ptr<nuraft::buffer> latest_snapshot_buf = nullptr;

    CoordinationSettingsPtr coordination_settings;

    NuKeeperStorage storage;

    NuKeeperSnapshotManager snapshot_manager;

    ResponsesQueue & responses_queue;

    SnapshotsQueue & snapshots_queue;
    /// Mutex for snapshots
    std::mutex snapshots_lock;

    /// Lock for storage
    std::mutex storage_lock;

    /// Last committed Raft log number.
    std::atomic<size_t> last_committed_idx;
    Poco::Logger * log;
};

}
