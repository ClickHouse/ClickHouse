#include <Coordination/NuKeeperStateMachine.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Coordination/TestKeeperStorageSerializer.h>

namespace DB
{

static constexpr int MAX_SNAPSHOTS = 3;

TestKeeperStorage::RequestForSession parseRequest(nuraft::buffer & data)
{
    ReadBufferFromNuraftBuffer buffer(data);
    TestKeeperStorage::RequestForSession request_for_session;
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

nuraft::ptr<nuraft::buffer> writeResponses(TestKeeperStorage::ResponsesForSessions & responses)
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
    if (data.size() == sizeof(size_t))
    {
        LOG_DEBUG(log, "Session ID response {}", log_idx);
        auto response = nuraft::buffer::alloc(sizeof(size_t));
        nuraft::buffer_serializer bs(response);
        {
            std::lock_guard lock(storage_lock);
            bs.put_i64(storage.getSessionID());
        }
        last_committed_idx = log_idx;
        return response;
    }
    else
    {
        auto request_for_session = parseRequest(data);
        TestKeeperStorage::ResponsesForSessions responses_for_sessions;
        {
            std::lock_guard lock(storage_lock);
            responses_for_sessions = storage.processRequest(request_for_session.request, request_for_session.session_id);
        }

        last_committed_idx = log_idx;
        return writeResponses(responses_for_sessions);
    }
}

bool NuKeeperStateMachine::apply_snapshot(nuraft::snapshot & s)
{
    LOG_DEBUG(log, "Applying snapshot {}", s.get_last_log_idx());
    StorageSnapshotPtr snapshot;
    {
        std::lock_guard<std::mutex> lock(snapshots_lock);
        auto entry = snapshots.find(s.get_last_log_idx());
        if (entry == snapshots.end())
            return false;
        snapshot = entry->second;
    }
    std::lock_guard lock(storage_lock);
    storage = snapshot->storage;
    last_committed_idx = s.get_last_log_idx();
    return true;
}

nuraft::ptr<nuraft::snapshot> NuKeeperStateMachine::last_snapshot()
{
   // Just return the latest snapshot.
    std::lock_guard<std::mutex> lock(snapshots_lock);
    auto entry = snapshots.rbegin();
    if (entry == snapshots.rend())
        return nullptr;

    return entry->second->snapshot;
}

NuKeeperStateMachine::StorageSnapshotPtr NuKeeperStateMachine::createSnapshotInternal(nuraft::snapshot & s)
{
    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    nuraft::ptr<nuraft::snapshot> ss = nuraft::snapshot::deserialize(*snp_buf);
    std::lock_guard lock(storage_lock);
    return std::make_shared<NuKeeperStateMachine::StorageSnapshot>(ss, storage);
}

NuKeeperStateMachine::StorageSnapshotPtr NuKeeperStateMachine::readSnapshot(nuraft::snapshot & s, nuraft::buffer & in)
{
    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    nuraft::ptr<nuraft::snapshot> ss = nuraft::snapshot::deserialize(*snp_buf);
    TestKeeperStorageSerializer serializer;

    ReadBufferFromNuraftBuffer reader(in);
    TestKeeperStorage new_storage;
    serializer.deserialize(new_storage, reader);
    return std::make_shared<StorageSnapshot>(ss, new_storage);
}


void NuKeeperStateMachine::writeSnapshot(const NuKeeperStateMachine::StorageSnapshotPtr & snapshot, nuraft::ptr<nuraft::buffer> & out)
{
    TestKeeperStorageSerializer serializer;

    WriteBufferFromNuraftBuffer writer;
    serializer.serialize(snapshot->storage, writer);
    out = writer.getBuffer();
}

void NuKeeperStateMachine::create_snapshot(
    nuraft::snapshot & s,
    nuraft::async_result<bool>::handler_type & when_done)
{
    LOG_DEBUG(log, "Creating snapshot {}", s.get_last_log_idx());
    auto snapshot = createSnapshotInternal(s);
    {
        std::lock_guard<std::mutex> lock(snapshots_lock);
        snapshots[s.get_last_log_idx()] = snapshot;
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
    nuraft::buffer & data,
    bool /*is_first_obj*/,
    bool /*is_last_obj*/)
{
    LOG_DEBUG(log, "Saving snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);

    if (obj_id == 0)
    {
        auto new_snapshot = createSnapshotInternal(s);
        std::lock_guard<std::mutex> lock(snapshots_lock);
        snapshots.try_emplace(s.get_last_log_idx(), std::move(new_snapshot));
    }
    else
    {
        auto received_snapshot = readSnapshot(s, data);

        std::lock_guard<std::mutex> lock(snapshots_lock);
        snapshots[s.get_last_log_idx()] = std::move(received_snapshot);
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
    StorageSnapshotPtr required_snapshot;
    {
        std::lock_guard<std::mutex> lock(snapshots_lock);
        auto entry = snapshots.find(s.get_last_log_idx());
        if (entry == snapshots.end())
        {
            // Snapshot doesn't exist.
            data_out = nullptr;
            is_last_obj = true;
            return 0;
        }
        required_snapshot = entry->second;
    }

    if (obj_id == 0)
    {
        auto new_snapshot = createSnapshotInternal(s);
        writeSnapshot(new_snapshot, data_out);
        is_last_obj = false;
    }
    else
    {
        writeSnapshot(required_snapshot, data_out);
        is_last_obj = true;
    }
    return 0;
}

}
