#include <Coordination/KeeperStateMachine.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <future>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

KeeperStorage::RequestForSession parseRequest(nuraft::buffer & data)
{
    ReadBufferFromNuraftBuffer buffer(data);
    KeeperStorage::RequestForSession request_for_session;
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

    KeeperStateMachine::KeeperStateMachine(
        ResponsesQueue & responses_queue_,
        SnapshotsQueue & snapshots_queue_,
        const std::string & snapshots_path_,
        const CoordinationSettingsPtr & coordination_settings_,
        const std::string & superdigest_)
    : coordination_settings(coordination_settings_)
    , snapshot_manager(snapshots_path_, coordination_settings->snapshots_to_keep, superdigest_, coordination_settings->dead_session_check_period_ms.totalMicroseconds())
    , responses_queue(responses_queue_)
    , snapshots_queue(snapshots_queue_)
    , last_committed_idx(0)
    , log(&Poco::Logger::get("KeeperStateMachine"))
    , superdigest(superdigest_)
{
}

void KeeperStateMachine::init()
{
    /// Do everything without mutexes, no other threads exist.
    LOG_DEBUG(log, "Totally have {} snapshots", snapshot_manager.totalSnapshots());
    bool loaded = false;
    bool has_snapshots = snapshot_manager.totalSnapshots() != 0;
    while (snapshot_manager.totalSnapshots() != 0)
    {
        uint64_t latest_log_index = snapshot_manager.getLatestSnapshotIndex();
        LOG_DEBUG(log, "Trying to load state machine from snapshot up to log index {}", latest_log_index);

        try
        {
            latest_snapshot_buf = snapshot_manager.deserializeSnapshotBufferFromDisk(latest_log_index);
            std::tie(latest_snapshot_meta, storage) = snapshot_manager.deserializeSnapshotFromBuffer(latest_snapshot_buf);
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

    if (!storage)
        storage = std::make_unique<KeeperStorage>(coordination_settings->dead_session_check_period_ms.totalMilliseconds(), superdigest);
}

nuraft::ptr<nuraft::buffer> KeeperStateMachine::commit(const uint64_t log_idx, nuraft::buffer & data)
{
    auto request_for_session = parseRequest(data);
    if (request_for_session.request->getOpNum() == Coordination::OpNum::SessionID)
    {
        const Coordination::ZooKeeperSessionIDRequest & session_id_request = dynamic_cast<const Coordination::ZooKeeperSessionIDRequest &>(*request_for_session.request);
        int64_t session_id;
        std::shared_ptr<Coordination::ZooKeeperSessionIDResponse> response = std::make_shared<Coordination::ZooKeeperSessionIDResponse>();
        response->internal_id = session_id_request.internal_id;
        response->server_id = session_id_request.server_id;
        KeeperStorage::ResponseForSession response_for_session;
        response_for_session.session_id = -1;
        response_for_session.response = response;
        {
            std::lock_guard lock(storage_and_responses_lock);
            session_id = storage->getSessionID(session_id_request.session_timeout_ms);
            LOG_DEBUG(log, "Session ID response {} with timeout {}", session_id, session_id_request.session_timeout_ms);
            response->session_id = session_id;
            responses_queue.push(response_for_session);
        }
    }
    else
    {
        std::lock_guard lock(storage_and_responses_lock);
        KeeperStorage::ResponsesForSessions responses_for_sessions = storage->processRequest(request_for_session.request, request_for_session.session_id, log_idx);
        for (auto & response_for_session : responses_for_sessions)
            responses_queue.push(response_for_session);
    }

    last_committed_idx = log_idx;
    return nullptr;
}

bool KeeperStateMachine::apply_snapshot(nuraft::snapshot & s)
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

    { /// deserialize and apply snapshot to storage
        std::lock_guard lock(storage_and_responses_lock);
        std::tie(latest_snapshot_meta, storage) = snapshot_manager.deserializeSnapshotFromBuffer(latest_snapshot_ptr);
    }
    last_committed_idx = s.get_last_log_idx();
    return true;
}

nuraft::ptr<nuraft::snapshot> KeeperStateMachine::last_snapshot()
{
    /// Just return the latest snapshot.
    std::lock_guard<std::mutex> lock(snapshots_lock);
    return latest_snapshot_meta;
}

void KeeperStateMachine::create_snapshot(
    nuraft::snapshot & s,
    nuraft::async_result<bool>::handler_type & when_done)
{
    LOG_DEBUG(log, "Creating snapshot {}", s.get_last_log_idx());

    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    auto snapshot_meta_copy = nuraft::snapshot::deserialize(*snp_buf);
    CreateSnapshotTask snapshot_task;
    { /// lock storage for a short period time to turn on "snapshot mode". After that we can read consistent storage state without locking.
        std::lock_guard lock(storage_and_responses_lock);
        snapshot_task.snapshot = std::make_shared<KeeperStorageSnapshot>(storage.get(), snapshot_meta_copy);
    }

    snapshot_task.create_snapshot = [this, when_done] (KeeperStorageSnapshotPtr && snapshot)
    {
        nuraft::ptr<std::exception> exception(nullptr);
        bool ret = true;
        try
        {
            {
                std::lock_guard lock(snapshots_lock);
                auto snapshot_buf = snapshot_manager.serializeSnapshotToBuffer(*snapshot);
                auto result_path = snapshot_manager.serializeSnapshotBufferToDisk(*snapshot_buf, snapshot->snapshot_meta->get_last_log_idx());
                latest_snapshot_buf = snapshot_buf;
                latest_snapshot_meta = snapshot->snapshot_meta;

                LOG_DEBUG(log, "Created persistent snapshot {} with path {}", latest_snapshot_meta->get_last_log_idx(), result_path);
            }

            {
                /// Must do it with lock (clearing elements from list)
                std::lock_guard lock(storage_and_responses_lock);
                /// Turn off "snapshot mode" and clear outdate part of storage state
                storage->clearGarbageAfterSnapshot();
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

void KeeperStateMachine::save_logical_snp_obj(
    nuraft::snapshot & s,
    uint64_t & obj_id,
    nuraft::buffer & data,
    bool /*is_first_obj*/,
    bool /*is_last_obj*/)
{
    LOG_DEBUG(log, "Saving snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);

    nuraft::ptr<nuraft::buffer> cloned_buffer;
    nuraft::ptr<nuraft::snapshot> cloned_meta;
    if (obj_id == 0)
    {
        std::lock_guard lock(storage_and_responses_lock);
        KeeperStorageSnapshot snapshot(storage.get(), s.get_last_log_idx());
        cloned_buffer = snapshot_manager.serializeSnapshotToBuffer(snapshot);
    }
    else
    {
        cloned_buffer = nuraft::buffer::clone(data);
    }

    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    cloned_meta = nuraft::snapshot::deserialize(*snp_buf);

    try
    {
        std::lock_guard lock(snapshots_lock);
        auto result_path = snapshot_manager.serializeSnapshotBufferToDisk(*cloned_buffer, s.get_last_log_idx());
        latest_snapshot_buf = cloned_buffer;
        latest_snapshot_meta = cloned_meta;
        LOG_DEBUG(log, "Saved snapshot {} to path {}", s.get_last_log_idx(), result_path);
        obj_id++;
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

int KeeperStateMachine::read_logical_snp_obj(
    nuraft::snapshot & s,
    void* & /*user_snp_ctx*/,
    uint64_t obj_id,
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
        {
            LOG_WARNING(log, "Required to apply snapshot with last log index {}, but our last log index is {}. Will ignore this one and retry",
                            s.get_last_log_idx(), latest_snapshot_meta->get_last_log_idx());
            return -1;
        }
        data_out = nuraft::buffer::clone(*latest_snapshot_buf);
        is_last_obj = true;
    }
    return 1;
}

void KeeperStateMachine::processReadRequest(const KeeperStorage::RequestForSession & request_for_session)
{
    /// Pure local request, just process it with storage
    std::lock_guard lock(storage_and_responses_lock);
    auto responses = storage->processRequest(request_for_session.request, request_for_session.session_id, std::nullopt);
    for (const auto & response : responses)
        responses_queue.push(response);
}

std::vector<int64_t> KeeperStateMachine::getDeadSessions()
{
    std::lock_guard lock(storage_and_responses_lock);
    return storage->getDeadSessions();
}

void KeeperStateMachine::shutdownStorage()
{
    std::lock_guard lock(storage_and_responses_lock);
    storage->finalize();
}

}
