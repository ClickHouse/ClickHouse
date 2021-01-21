#pragma once

#include <Coordination/TestKeeperStorage.h>
#include <libnuraft/nuraft.hxx>
#include <common/logger_useful.h>

namespace DB
{

class NuKeeperStateMachine : public nuraft::state_machine
{
public:
    NuKeeperStateMachine();

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

    zkutil::TestKeeperStorage & getStorage()
    {
        return storage;
    }

private:
    zkutil::TestKeeperStorage storage;
    // Mutex for `snapshots_`.
    std::mutex snapshots_lock;

    /// Fake snapshot storage
    std::map<uint64_t, nuraft::ptr<nuraft::snapshot>> snapshots;

    /// Last committed Raft log number.
    std::atomic<size_t> last_committed_idx;
    Poco::Logger * log;
};

}
