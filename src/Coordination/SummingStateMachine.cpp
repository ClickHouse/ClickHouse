#include <Coordination/SummingStateMachine.h>
#include <cstring>

namespace DB
{

static constexpr int MAX_SNAPSHOTS = 3;

static int64_t deserializeValue(nuraft::buffer & buffer)
{
    nuraft::buffer_serializer bs(buffer);
    int64_t result;
    memcpy(&result, bs.get_raw(buffer.size()), sizeof(result));
    return result;
}

SummingStateMachine::SummingStateMachine()
    : value(0)
    , last_committed_idx(0)
{
}

nuraft::ptr<nuraft::buffer> SummingStateMachine::commit(const uint64_t log_idx, nuraft::buffer & data)
{
    int64_t value_to_add = deserializeValue(data);

    value += value_to_add;
    last_committed_idx = log_idx;

    // Return Raft log number as a return result.
    nuraft::ptr<nuraft::buffer> ret = nuraft::buffer::alloc(sizeof(log_idx));
    nuraft::buffer_serializer bs(ret);
    bs.put_u64(log_idx);
    return ret;
}

bool SummingStateMachine::apply_snapshot(nuraft::snapshot & s)
{
    std::lock_guard ll(snapshots_lock);
    auto entry = snapshots.find(s.get_last_log_idx());
    if (entry == snapshots.end())
        return false;

    auto ctx = entry->second;
    value = ctx->value;
    return true;
}

nuraft::ptr<nuraft::snapshot> SummingStateMachine::last_snapshot()
{
    // Just return the latest snapshot.
    std::lock_guard ll(snapshots_lock);
    auto entry = snapshots.rbegin();
    if (entry == snapshots.rend())
        return nullptr;

    auto ctx = entry->second;
    return ctx->snapshot;
}


void SummingStateMachine::createSnapshotInternal(nuraft::snapshot & s)
{
    // Clone snapshot from `s`.
    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    nuraft::ptr<nuraft::snapshot> ss = nuraft::snapshot::deserialize(*snp_buf);

    // Put into snapshot map.
    auto ctx = cs_new<SingleValueSnapshotContext>(ss, value);
    snapshots[s.get_last_log_idx()] = ctx;

    // Maintain last 3 snapshots only.
    ssize_t num = snapshots.size();
    auto entry = snapshots.begin();

    for (ssize_t ii = 0; ii < num - MAX_SNAPSHOTS; ++ii)
    {
        if (entry == snapshots.end())
            break;
        entry = snapshots.erase(entry);
    }
}

void SummingStateMachine::save_logical_snp_obj(
    nuraft::snapshot & s,
    uint64_t & obj_id,
    nuraft::buffer & data,
    bool /*is_first_obj*/,
    bool /*is_last_obj*/)
{
    if (obj_id == 0)
    {
        // Object ID == 0: it contains dummy value, create snapshot context.
        createSnapshotInternal(s);
    }
    else
    {
        // Object ID > 0: actual snapshot value.
        nuraft::buffer_serializer bs(data);
        int64_t local_value = static_cast<int64_t>(bs.get_u64());

        std::lock_guard ll(snapshots_lock);
        auto entry = snapshots.find(s.get_last_log_idx());
        assert(entry != snapshots.end());
        entry->second->value = local_value;
    }
    // Request next object.
    obj_id++;
}

int SummingStateMachine::read_logical_snp_obj(
    nuraft::snapshot & s,
    void* & /*user_snp_ctx*/,
    uint64_t obj_id,
    nuraft::ptr<nuraft::buffer> & data_out,
    bool & is_last_obj)
{
    nuraft::ptr<SingleValueSnapshotContext> ctx = nullptr;
    {
        std::lock_guard ll(snapshots_lock);
        auto entry = snapshots.find(s.get_last_log_idx());
        if (entry == snapshots.end())
        {
            // Snapshot doesn't exist.
            data_out = nullptr;
            is_last_obj = true;
            return 0;
        }
        ctx = entry->second;
    }

    if (obj_id == 0)
    {
        // Object ID == 0: first object, put dummy data.
        data_out = nuraft::buffer::alloc(sizeof(Int32));
        nuraft::buffer_serializer bs(data_out);
        bs.put_i32(0);
        is_last_obj = false;

    }
    else
    {
        // Object ID > 0: second object, put actual value.
        data_out = nuraft::buffer::alloc(sizeof(uint64_t));
        nuraft::buffer_serializer bs(data_out);
        bs.put_u64(ctx->value);
        is_last_obj = true;
    }
    return 0;
}

void SummingStateMachine::create_snapshot(
    nuraft::snapshot & s,
    nuraft::async_result<bool>::handler_type & when_done)
{
    {
        std::lock_guard ll(snapshots_lock);
        createSnapshotInternal(s);
    }
    nuraft::ptr<std::exception> except(nullptr);
    bool ret = true;
    when_done(ret, except);
}


}
