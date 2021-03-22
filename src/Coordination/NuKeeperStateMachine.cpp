#include <Coordination/NuKeeperStateMachine.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Coordination/NuKeeperSnapshotManager.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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

NuKeeperStateMachine::NuKeeperStateMachine(ResponsesQueue & responses_queue_, SnapshotsQueue & snapshots_queue_, const std::string & snapshots_path_, const CoordinationSettingsPtr & coordination_settings_)
    : coordination_settings(coordination_settings_)
    , storage(coordination_settings->dead_session_check_period_ms.totalMilliseconds())
    , snapshot_manager(snapshots_path_, coordination_settings->snapshots_to_keep)
    , responses_queue(responses_queue_)
    , snapshots_queue(snapshots_queue_)
    , last_committed_idx(0)
    , log(&Poco::Logger::get("NuKeeperStateMachine"))
{
}

void NuKeeperStateMachine::init()
{
    /// Do everything without mutexes, no other threads exist.
    LOG_DEBUG(log, "Totally have {} snapshots", snapshot_manager.totalSnapshots());
    bool loaded = false;
    bool has_snapshots = snapshot_manager.totalSnapshots() != 0;
    while (snapshot_manager.totalSnapshots() != 0)
    {
        size_t latest_log_index = snapshot_manager.getLatestSnapshotIndex();
        LOG_DEBUG(log, "Trying to load state machine from snapshot up to log index {}", latest_log_index);

        try
        {
            latest_snapshot_buf = snapshot_manager.deserializeSnapshotBufferFromDisk(latest_log_index);
            latest_snapshot_meta = snapshot_manager.deserializeSnapshotFromBuffer(&storage, latest_snapshot_buf);
            last_committed_idx = latest_snapshot_meta->get_last_log_idx();
            loaded = true;
            break;
        }
        catch (const DB::Exception & ex)
        {
            LOG_WARNING(log, "Failed to load from snapshot with index {}, with error {}, will remove it from disk", latest_log_index, ex.displayText());
            snapshot_manager.removeSnapshot(latest_log_index);
        }
    }

    if (has_snapshots)
    {
        if (loaded)
            LOG_DEBUG(log, "Loaded snapshot with last committed log index {}", last_committed_idx);
        else
            LOG_WARNING(log, "All snapshots broken, last committed log index {}", last_committed_idx);
    }
    else
    {
        LOG_DEBUG(log, "No existing snapshots, last committed log index {}", last_committed_idx);
    }
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
            responses_for_sessions = storage.processRequest(request_for_session.request, request_for_session.session_id, log_idx);
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
    nuraft::ptr<nuraft::buffer> latest_snapshot_ptr;
    {
        std::lock_guard lock(snapshots_lock);
        if (s.get_last_log_idx() != latest_snapshot_meta->get_last_log_idx())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Required to apply snapshot with last log index {}, but our last log index is {}",
                            s.get_last_log_idx(), latest_snapshot_meta->get_last_log_idx());
        latest_snapshot_ptr = latest_snapshot_buf;
    }

    {
        std::lock_guard lock(storage_lock);
        snapshot_manager.deserializeSnapshotFromBuffer(&storage, latest_snapshot_ptr);
    }
    last_committed_idx = s.get_last_log_idx();
    return true;
}

nuraft::ptr<nuraft::snapshot> NuKeeperStateMachine::last_snapshot()
{
    /// Just return the latest snapshot.
    std::lock_guard<std::mutex> lock(snapshots_lock);
    return latest_snapshot_meta;
}

void NuKeeperStateMachine::create_snapshot(
    nuraft::snapshot & s,
    nuraft::async_result<bool>::handler_type & when_done)
{
    LOG_DEBUG(log, "Creating snapshot {}", s.get_last_log_idx());

    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    auto snapshot_meta_copy = nuraft::snapshot::deserialize(*snp_buf);
    CreateSnapshotTask snapshot_task;
    {
        std::lock_guard lock(storage_lock);
        snapshot_task.snapshot = std::make_shared<NuKeeperStorageSnapshot>(&storage, snapshot_meta_copy);
    }

    snapshot_task.create_snapshot = [this, when_done] (NuKeeperStorageSnapshotPtr && snapshot)
    {
        nuraft::ptr<std::exception> exception(nullptr);
        bool ret = true;
        try
        {
            auto snapshot_buf = snapshot_manager.serializeSnapshotToBuffer(*snapshot);
            auto result_path = snapshot_manager.serializeSnapshotBufferToDisk(*snapshot_buf, snapshot->snapshot_meta->get_last_log_idx());
            {
                std::lock_guard lock(snapshots_lock);
                latest_snapshot_buf = snapshot_buf;
                latest_snapshot_meta = snapshot->snapshot_meta;
            }

            LOG_DEBUG(log, "Created persistent snapshot {} with path {}", latest_snapshot_meta->get_last_log_idx(), result_path);

            {
                /// Must do it with lock (clearing elements from list)
                std::lock_guard lock(storage_lock);
                storage.clearGarbageAfterSnapshot();
                /// Destroy snapshot with lock
                snapshot.reset();
                LOG_TRACE(log, "Cleared garbage after snapshot");

            }
        }
        catch (...)
        {
            LOG_TRACE(log, "Exception happened during snapshot");
            tryLogCurrentException(log);
            ret = false;
        }

        when_done(ret, exception);
    };

    LOG_DEBUG(log, "In memory snapshot {} created, queueing task to flash to disk", s.get_last_log_idx());
    snapshots_queue.push(std::move(snapshot_task));
}

void NuKeeperStateMachine::save_logical_snp_obj(
    nuraft::snapshot & s,
    size_t & obj_id,
    nuraft::buffer & data,
    bool /*is_first_obj*/,
    bool /*is_last_obj*/)
{
    LOG_DEBUG(log, "Saving snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);

    nuraft::ptr<nuraft::buffer> cloned_buffer;
    nuraft::ptr<nuraft::snapshot> cloned_meta;
    if (obj_id == 0)
    {
        std::lock_guard lock(storage_lock);
        NuKeeperStorageSnapshot snapshot(&storage, s.get_last_log_idx());
        cloned_buffer = snapshot_manager.serializeSnapshotToBuffer(snapshot);
    }
    else
    {
        cloned_buffer = nuraft::buffer::clone(data);
    }

    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    cloned_meta = nuraft::snapshot::deserialize(*snp_buf);

    auto result_path = snapshot_manager.serializeSnapshotBufferToDisk(*cloned_buffer, s.get_last_log_idx());

    {
        std::lock_guard lock(snapshots_lock);
        latest_snapshot_buf = cloned_buffer;
        latest_snapshot_meta = cloned_meta;
    }

    LOG_DEBUG(log, "Created snapshot {} with path {}", s.get_last_log_idx(), result_path);

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
    if (obj_id == 0)
    {
        data_out = nuraft::buffer::alloc(sizeof(int32_t));
        nuraft::buffer_serializer bs(data_out);
        bs.put_i32(0);
        is_last_obj = false;
    }
    else
    {
        std::lock_guard lock(snapshots_lock);
        if (s.get_last_log_idx() != latest_snapshot_meta->get_last_log_idx())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Required to apply snapshot with last log index {}, but our last log index is {}",
                            s.get_last_log_idx(), latest_snapshot_meta->get_last_log_idx());
        data_out = nuraft::buffer::clone(*latest_snapshot_buf);
        is_last_obj = true;
    }
    return 0;
}

void NuKeeperStateMachine::processReadRequest(const NuKeeperStorage::RequestForSession & request_for_session)
{
    NuKeeperStorage::ResponsesForSessions responses;
    {
        std::lock_guard lock(storage_lock);
        responses = storage.processRequest(request_for_session.request, request_for_session.session_id, std::nullopt);
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
