#include <Coordination/NuKeeperStateMachine.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Coordination/NuKeeperStorageSerializer.h>

namespace DB
{

NuKeeperStorage::RequestForSession parseRequest(nuraft::buffer & data)
{
    ReadBufferFromNuraftBuffer buffer(data);
    NuKeeperStorage::RequestForSession request_for_session;
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

nuraft::ptr<nuraft::buffer> writeResponses(NuKeeperStorage::ResponsesForSessions & responses)
{
    WriteBufferFromNuraftBuffer buffer;
    for (const auto & response_and_session : responses)
    {
        writeIntBinary(response_and_session.session_id, buffer);
        response_and_session.response->write(buffer);
    }
    return buffer.getBuffer();
}


NuKeeperStateMachine::NuKeeperStateMachine(ResponsesQueue & responses_queue_, const CoordinationSettingsPtr & coordination_settings_)
    : coordination_settings(coordination_settings_)
    , storage(coordination_settings->dead_session_check_period_ms.totalMilliseconds())
    , responses_queue(responses_queue_)
    , last_committed_idx(0)
    , log(&Poco::Logger::get("NuKeeperStateMachine"))
{
    LOG_DEBUG(log, "Created nukeeper state machine");
}

nuraft::ptr<nuraft::buffer> NuKeeperStateMachine::commit(const size_t log_idx, nuraft::buffer & data)
{
    if (data.size() == sizeof(int64_t))
    {
        nuraft::buffer_serializer timeout_data(data);
        int64_t session_timeout_ms = timeout_data.get_i64();
        auto response = nuraft::buffer::alloc(sizeof(int64_t));
        int64_t session_id;
        nuraft::buffer_serializer bs(response);
        {
            std::lock_guard lock(storage_lock);
            session_id = storage.getSessionID(session_timeout_ms);
            bs.put_i64(session_id);
        }
        LOG_DEBUG(log, "Session ID response {} with timeout {}", session_id, session_timeout_ms);
        last_committed_idx = log_idx;
        return response;
    }
    else
    {
        auto request_for_session = parseRequest(data);
        NuKeeperStorage::ResponsesForSessions responses_for_sessions;
        {
            std::lock_guard lock(storage_lock);
            responses_for_sessions = storage.processRequest(request_for_session.request, request_for_session.session_id);
            for (auto & response_for_session : responses_for_sessions)
                responses_queue.push(response_for_session);
        }

        last_committed_idx = log_idx;
        return nullptr;
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
    NuKeeperStorageSerializer serializer;

    ReadBufferFromNuraftBuffer reader(in);
    NuKeeperStorage new_storage(coordination_settings->dead_session_check_period_ms.totalMilliseconds());
    serializer.deserialize(new_storage, reader);
    return std::make_shared<StorageSnapshot>(ss, new_storage);
}


void NuKeeperStateMachine::writeSnapshot(const NuKeeperStateMachine::StorageSnapshotPtr & snapshot, nuraft::ptr<nuraft::buffer> & out)
{
    NuKeeperStorageSerializer serializer;

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
        size_t num = snapshots.size();
        if (num > coordination_settings->max_stored_snapshots)
        {
            auto entry = snapshots.begin();

            for (size_t i = 0; i < num - coordination_settings->max_stored_snapshots; ++i)
            {
                if (entry == snapshots.end())
                    break;
                entry = snapshots.erase(entry);
            }
        }

    }

    LOG_DEBUG(log, "Created snapshot {}", s.get_last_log_idx());
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

void NuKeeperStateMachine::processReadRequest(const NuKeeperStorage::RequestForSession & request_for_session)
{
    NuKeeperStorage::ResponsesForSessions responses;
    {
        std::lock_guard lock(storage_lock);
        responses = storage.processRequest(request_for_session.request, request_for_session.session_id);
    }
    for (const auto & response : responses)
        responses_queue.push(response);
}

std::unordered_set<int64_t> NuKeeperStateMachine::getDeadSessions()
{
    std::lock_guard lock(storage_lock);
    return storage.getDeadSessions();
}

void NuKeeperStateMachine::shutdownStorage()
{
    std::lock_guard lock(storage_lock);
    storage.finalize();
}

}
