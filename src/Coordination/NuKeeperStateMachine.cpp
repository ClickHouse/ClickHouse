#include <Coordination/NuKeeperStateMachine.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>

namespace DB
{

zkutil::TestKeeperStorage::RequestForSession parseRequest(nuraft::buffer & data)
{
    ReadBufferFromNuraftBuffer buffer(data);
    zkutil::TestKeeperStorage::RequestForSession request_for_session;
    readIntBinary(request_for_session.session_id, buffer);

    int32_t length;
    Coordination::read(length, buffer);

    int32_t xid;
    Coordination::read(xid, buffer);

    Coordination::OpNum opnum;
    Coordination::read(opnum, buffer);

    request_for_session.request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request_for_session.request->xid = xid;
    request_for_session.request->readImpl(buffer);
    return request_for_session;
}

nuraft::ptr<nuraft::buffer> writeResponses(zkutil::TestKeeperStorage::ResponsesForSessions & responses)
{
    WriteBufferFromNuraftBuffer buffer;
    for (const auto & response_and_session : responses)
    {
        writeIntBinary(response_and_session.session_id, buffer);
        response_and_session.response->write(buffer);
    }
    return buffer.getBuffer();
}


NuKeeperStateMachine::NuKeeperStateMachine()
    : last_committed_idx(0)
    , log(&Poco::Logger::get("NuRaftStateMachine"))
{
    LOG_DEBUG(log, "Created nukeeper state machine");
}

nuraft::ptr<nuraft::buffer> NuKeeperStateMachine::commit(const size_t log_idx, nuraft::buffer & data)
{
    LOG_DEBUG(log, "Commiting logidx {}", log_idx);
    auto request_for_session = parseRequest(data);
    auto responses_with_sessions = storage.processRequest(request_for_session.request, request_for_session.session_id);

    last_committed_idx = log_idx;
    return writeResponses(responses_with_sessions);
}

bool NuKeeperStateMachine::apply_snapshot(nuraft::snapshot & s)
{
    LOG_DEBUG(log, "Applying snapshot {}", s.get_last_log_idx());
    std::lock_guard<std::mutex> lock(snapshots_lock);
    auto entry = snapshots.find(s.get_last_log_idx());
    if (entry == snapshots.end())
    {
        return false;
    }

    /// TODO
    return true;
}

nuraft::ptr<nuraft::snapshot> NuKeeperStateMachine::last_snapshot()
{

    LOG_DEBUG(log, "Trying to get last snapshot");
   // Just return the latest snapshot.
    std::lock_guard<std::mutex> lock(snapshots_lock);
    auto entry = snapshots.rbegin();
    if (entry == snapshots.rend())
        return nullptr;

    return entry->second;
}

void NuKeeperStateMachine::create_snapshot(
    nuraft::snapshot & s,
    nuraft::async_result<bool>::handler_type & when_done)
{

    LOG_DEBUG(log, "Creating snapshot {}", s.get_last_log_idx());
    {
        std::lock_guard<std::mutex> lock(snapshots_lock);
        nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
        nuraft::ptr<nuraft::snapshot> ss = nuraft::snapshot::deserialize(*snp_buf);
        snapshots[s.get_last_log_idx()] = ss;
        const int MAX_SNAPSHOTS = 3;
        int num = snapshots.size();
        auto entry = snapshots.begin();

        for (int i = 0; i < num - MAX_SNAPSHOTS; ++i)
        {
            if (entry == snapshots.end())
                break;
            entry = snapshots.erase(entry);
        }
    }
    nuraft::ptr<std::exception> except(nullptr);
    bool ret = true;
    when_done(ret, except);
}

void NuKeeperStateMachine::save_logical_snp_obj(
    nuraft::snapshot & s,
    size_t & obj_id,
    nuraft::buffer & /*data*/,
    bool /*is_first_obj*/,
    bool /*is_last_obj*/)
{
    LOG_DEBUG(log, "Saving snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);
    if (obj_id == 0)
    {
        std::lock_guard<std::mutex> lock(snapshots_lock);
        nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
        nuraft::ptr<nuraft::snapshot> ss = nuraft::snapshot::deserialize(*snp_buf);
        snapshots[s.get_last_log_idx()] = ss;
        const int MAX_SNAPSHOTS = 3;
        int num = snapshots.size();
        auto entry = snapshots.begin();

        for (int i = 0; i < num - MAX_SNAPSHOTS; ++i)
        {
            if (entry == snapshots.end())
                break;
            entry = snapshots.erase(entry);
        }
    }
    else
    {
        std::lock_guard<std::mutex> lock(snapshots_lock);
        auto entry = snapshots.find(s.get_last_log_idx());
        assert(entry != snapshots.end());
    }

    obj_id++;
}

int NuKeeperStateMachine::read_logical_snp_obj(
    nuraft::snapshot & s,
    void* & /*user_snp_ctx*/,
    ulong obj_id,
    nuraft::ptr<nuraft::buffer> & data_out,
    bool & is_last_obj)
{

    LOG_DEBUG(log, "Reading snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);
    {
        std::lock_guard<std::mutex> ll(snapshots_lock);
        auto entry = snapshots.find(s.get_last_log_idx());
        if (entry == snapshots.end())
        {
            // Snapshot doesn't exist.
            data_out = nullptr;
            is_last_obj = true;
            return 0;
        }
    }

    if (obj_id == 0)
    {
        // Object ID == 0: first object, put dummy data.
        data_out = nuraft::buffer::alloc(sizeof(size_t));
        nuraft::buffer_serializer bs(data_out);
        bs.put_i32(0);
        is_last_obj = false;

    }
    else
    {
        // Object ID > 0: second object, put actual value.
        data_out = nuraft::buffer::alloc(sizeof(size_t));
        nuraft::buffer_serializer bs(data_out);
        bs.put_u64(1);
        is_last_obj = true;
    }
    return 0;
}

}
