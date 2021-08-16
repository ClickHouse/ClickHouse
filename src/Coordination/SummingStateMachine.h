#pragma once

#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <Core/Types.h>
#include <atomic>
#include <map>
#include <mutex>

namespace DB
{

/// Example trivial state machine.
class SummingStateMachine : public nuraft::state_machine
{
public:
    SummingStateMachine();

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

    int64_t getValue() const { return value; }

private:
    struct SingleValueSnapshotContext
    {
        SingleValueSnapshotContext(nuraft::ptr<nuraft::snapshot> & s, int64_t v)
            : snapshot(s)
            , value(v)
        {}

        nuraft::ptr<nuraft::snapshot> snapshot;
        int64_t value;
    };

    void createSnapshotInternal(nuraft::snapshot & s);

    // State machine's current value.
    std::atomic<int64_t> value;

    // Last committed Raft log number.
    std::atomic<uint64_t> last_committed_idx;

    // Keeps the last 3 snapshots, by their Raft log numbers.
    std::map<uint64_t, nuraft::ptr<SingleValueSnapshotContext>> snapshots;

    // Mutex for `snapshots_`.
    std::mutex snapshots_lock;

};

}
