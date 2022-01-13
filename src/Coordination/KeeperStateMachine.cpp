#include <Coordination/KeeperStateMachine.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <IO/ReadHelpers.h>
#include "Common/ThreadPool.h"
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include "base/logger_useful.h"
#include <Coordination/KeeperSnapshotManager.h>
#include <libnuraft/state_machine.hxx>
#if defined(OS_LINUX)
#include <sys/sysinfo.h>
#endif
#include <cstddef>
#include <future>
#include <thread>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYSTEM_ERROR;
}

namespace
{
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
}

void KeeperStateMachine::asyncCommitThread()
{
    setThreadName("AsyncCommitT");
#if defined(OS_LINUX)
    if (coordination_settings->cpu_affinity)
    {
        int cpuid = 2 % get_nprocs();
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(cpuid, &cpuset);
        int rc = pthread_setaffinity_np(pthread_self(),
                                        sizeof(cpu_set_t), &cpuset);
        if (rc != 0)
        {
            LOG_WARNING(log, "Error calling pthread_setaffinity_np, return code {}.", rc);
        }
    }
#endif
    while (!shutdown)
    {
        QueueParam param(0, nullptr);
        if (async_commit_queue.pop(param))
        {
            auto request_for_session = parseRequest(*param.ptr_data);
            auto log_idx = param.idx;
            /// Special processing of session_id request
            if (request_for_session.request->getOpNum() == Coordination::OpNum::SessionID)
            {
                const Coordination::ZooKeeperSessionIDRequest & session_id_request
                    = dynamic_cast<const Coordination::ZooKeeperSessionIDRequest &>(*request_for_session.request);
                int64_t session_id;
                std::shared_ptr<Coordination::ZooKeeperSessionIDResponse> response
                    = std::make_shared<Coordination::ZooKeeperSessionIDResponse>();
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
                    if (!responses_queue.push(response_for_session))
                        throw Exception(
                            ErrorCodes::SYSTEM_ERROR, "Could not push response with session id {} into responses queue", session_id);
                }
            }
            else
            {
                std::lock_guard lock(storage_and_responses_lock);
                KeeperStorage::ResponsesForSessions responses_for_sessions
                    = storage->processRequest(request_for_session.request, request_for_session.session_id, log_idx);
                for (auto & response_for_session : responses_for_sessions)
                    if (!responses_queue.push(response_for_session))
                        throw Exception(
                            ErrorCodes::SYSTEM_ERROR,
                            "Could not push response with session id {} into responses queue",
                            response_for_session.session_id);
            }

            last_committed_idx = log_idx;
        }
        else
        {
            //std::this_thread::sleep_for(std::chrono::microseconds(1));
            std::this_thread::yield();
        }
    }
}

KeeperStateMachine::KeeperStateMachine(
        ResponsesQueue & responses_queue_,
        SnapshotsQueue & snapshots_queue_,
        const std::string & snapshots_path_,
        const CoordinationSettingsPtr & coordination_settings_,
        const std::string & superdigest_)
    : coordination_settings(coordination_settings_)
    , snapshot_manager(
        snapshots_path_, coordination_settings->snapshots_to_keep,
        coordination_settings->compress_snapshots_with_zstd_format, superdigest_,
        coordination_settings->dead_session_check_period_ms.totalMicroseconds())
    , responses_queue(responses_queue_)
    , snapshots_queue(snapshots_queue_)
    , last_committed_idx(0)
    , log(&Poco::Logger::get("KeeperStateMachine"))
    , superdigest(superdigest_)
{
    async_commit_thread = ThreadFromGlobalPool([this] { asyncCommitThread(); });
}

void KeeperStateMachine::init()
{
    /// Do everything without mutexes, no other threads exist.
    LOG_DEBUG(log, "Totally have {} snapshots", snapshot_manager.totalSnapshots());
    bool loaded = false;
    bool has_snapshots = snapshot_manager.totalSnapshots() != 0;
    /// Deserialize latest snapshot from disk
    while (snapshot_manager.totalSnapshots() != 0)
    {
        uint64_t latest_log_index = snapshot_manager.getLatestSnapshotIndex();
        LOG_DEBUG(log, "Trying to load state machine from snapshot up to log index {}", latest_log_index);

        try
        {
            //latest_snapshot_buf = snapshot_manager.deserializeSnapshotBufferFromDisk(latest_log_index);
            //auto snapshot_deserialization_result = snapshot_manager.deserializeSnapshotFromBuffer(latest_snapshot_buf);
            auto snapshot_deserialization_result = snapshot_manager.deserializeSnapshotFromBuffer(snapshot_manager.deserializeSnapshotBufferFromDisk(latest_log_index));
            storage = std::move(snapshot_deserialization_result.storage);
            latest_snapshot_meta = snapshot_deserialization_result.snapshot_meta;
            cluster_config = snapshot_deserialization_result.cluster_config;
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

nuraft::ptr<nuraft::buffer> KeeperStateMachine::commit_ext(const nuraft::state_machine::ext_op_params & params)
{
    QueueParam param(params.log_idx, params.data);
    while (!async_commit_queue.push(param))
    {
        //LOG_INFO(log, "Commit queue full {}/{}", async_commit_queue.read_available(), async_commit_queue.write_available());
        std::this_thread::yield();
    }
    return nullptr;
}

#if 1
nuraft::ptr<nuraft::buffer> KeeperStateMachine::commit(const uint64_t log_idx, nuraft::buffer & data)
{
    auto request_for_session = parseRequest(data);
    /// Special processing of session_id request
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
            if (!responses_queue.push(response_for_session))
                throw Exception(ErrorCodes::SYSTEM_ERROR, "Could not push response with session id {} into responses queue", session_id);
        }
    }
    else
    {
        std::lock_guard lock(storage_and_responses_lock);
        KeeperStorage::ResponsesForSessions responses_for_sessions = storage->processRequest(request_for_session.request, request_for_session.session_id, log_idx);
        for (auto & response_for_session : responses_for_sessions)
            if (!responses_queue.push(response_for_session))
                throw Exception(ErrorCodes::SYSTEM_ERROR, "Could not push response with session id {} into responses queue", response_for_session.session_id);
    }

    last_committed_idx = log_idx;
    return nullptr;
}
#endif

bool KeeperStateMachine::apply_snapshot(nuraft::snapshot & s)
{
    LOG_DEBUG(log, "Applying snapshot {}", s.get_last_log_idx());
    nuraft::ptr<nuraft::buffer> latest_snapshot_ptr;
    { /// save snapshot into memory
        std::lock_guard lock(snapshots_lock);
        if (s.get_last_log_idx() != latest_snapshot_meta->get_last_log_idx())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Required to apply snapshot with last log index {}, but our last log index is {}",
                            s.get_last_log_idx(), latest_snapshot_meta->get_last_log_idx());
        latest_snapshot_ptr = latest_snapshot_buf;
    }

    { /// deserialize and apply snapshot to storage
        std::lock_guard lock(storage_and_responses_lock);
        //auto snapshot_deserialization_result = snapshot_manager.deserializeSnapshotFromBuffer(latest_snapshot_buf);
        auto snapshot_deserialization_result = snapshot_manager.deserializeSnapshotFromBuffer(snapshot_manager.deserializeSnapshotBufferFromDisk(s.get_last_log_idx()));
        storage = std::move(snapshot_deserialization_result.storage);
        latest_snapshot_meta = snapshot_deserialization_result.snapshot_meta;
        cluster_config = snapshot_deserialization_result.cluster_config;
    }

    last_committed_idx = s.get_last_log_idx();
    return true;
}


void KeeperStateMachine::commit_config(const uint64_t /*log_idx*/, nuraft::ptr<nuraft::cluster_config> & new_conf)
{
    std::lock_guard lock(cluster_config_lock);
    auto tmp = new_conf->serialize();
    cluster_config = ClusterConfig::deserialize(*tmp);
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
        snapshot_task.snapshot = std::make_shared<KeeperStorageSnapshot>(storage.get(), snapshot_meta_copy, getClusterConfig());
    }

    /// create snapshot task for background execution (in snapshot thread)
    snapshot_task.create_snapshot = [this, when_done] (KeeperStorageSnapshotPtr && snapshot)
    {
        nuraft::ptr<std::exception> exception(nullptr);
        bool ret = true;
        try
        {
            {   /// Read storage data without locks and create snapshot
                std::lock_guard lock(snapshots_lock);
                //auto snapshot_buf = snapshot_manager.serializeSnapshotToBuffer(*snapshot);
                //auto result_path = snapshot_manager.serializeSnapshotBufferToDisk(*snapshot_buf, snapshot->snapshot_meta->get_last_log_idx());
                auto result = snapshot_manager.serializeSnapshotToDisk(*snapshot);
                if (result.second)
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Snapshot {} was created failed, error: {}",
                            snapshot->snapshot_meta->get_last_log_idx(), result.second.message());
                }
                //latest_snapshot_buf = snapshot_buf;
                latest_snapshot_meta = snapshot->snapshot_meta;

                LOG_DEBUG(log, "Created persistent snapshot {} with path {}", latest_snapshot_meta->get_last_log_idx(), result.first);
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
    /// Flush snapshot to disk in a separate thread.
    if (!snapshots_queue.push(std::move(snapshot_task)))
        LOG_WARNING(log, "Cannot push snapshot task into queue");
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
    if (obj_id == 0) /// Fake snapshot required by NuRaft at startup
    {
        std::lock_guard lock(storage_and_responses_lock);
        KeeperStorageSnapshot snapshot(storage.get(), s.get_last_log_idx(), getClusterConfig());
        cloned_buffer = snapshot_manager.serializeSnapshotToBuffer(snapshot);
    }
    else
    {
        /// copy snapshot into memory
        //cloned_buffer = nuraft::buffer::clone(data);
        //cloned_buffer = data;
    }

    /// copy snapshot meta into memory
    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    cloned_meta = nuraft::snapshot::deserialize(*snp_buf);

    try
    {
        std::lock_guard lock(snapshots_lock);
        /// Serialize snapshot to disk and switch in memory pointers.
        //auto result_path = snapshot_manager.serializeSnapshotBufferToDisk(*cloned_buffer, s.get_last_log_idx());
        auto result_path = snapshot_manager.serializeSnapshotBufferToDisk(data, s.get_last_log_idx());
        //latest_snapshot_buf = cloned_buffer;
        latest_snapshot_meta = cloned_meta;
        latest_snapshot_path = result_path;
        LOG_DEBUG(log, "Saved snapshot {} to path {}", s.get_last_log_idx(), result_path);
        obj_id++;
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}
#include <sys/mman.h>
int KeeperStateMachine::read_logical_snp_obj(
    nuraft::snapshot & s,
    void* & /*user_snp_ctx*/,
    uint64_t obj_id,
    nuraft::ptr<nuraft::buffer> & data_out,
    bool & is_last_obj)
{

    LOG_DEBUG(log, "Reading snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);
    if (obj_id == 0) /// Fake snapshot required by NuRaft at startup
    {
        data_out = nuraft::buffer::alloc(sizeof(int32_t));
        nuraft::buffer_serializer bs(data_out);
        bs.put_i32(0);
        is_last_obj = false;
    }
    else
    {
        std::lock_guard lock(snapshots_lock);
        /// Our snapshot is not equal to required. Maybe we still creating it in the background.
        /// Let's wait and NuRaft will retry this call.
        if (s.get_last_log_idx() != latest_snapshot_meta->get_last_log_idx())
        {
            LOG_WARNING(log, "Required to apply snapshot with last log index {}, but our last log index is {}. Will ignore this one and retry",
                            s.get_last_log_idx(), latest_snapshot_meta->get_last_log_idx());
            return -1;
        }
        int fd = ::open(latest_snapshot_path.c_str(), O_RDONLY);
        if (fd < 0)
        {
            LOG_WARNING(log, "Error opening {}.", latest_snapshot_path);
            return -1;
        }
        auto file_size = ::lseek(fd, 0, SEEK_END);
        ::lseek(fd, 0, SEEK_SET);
        auto* chunk = reinterpret_cast<nuraft::byte*>(::mmap(nullptr, file_size, PROT_READ, MAP_FILE | MAP_SHARED, fd, 0));
        if (chunk == MAP_FAILED)
        {
            LOG_WARNING(log, "Error mmapping {}.", latest_snapshot_path);
            ::close(fd);
            return -1;
        }
        data_out = nuraft::buffer::alloc(file_size);
        data_out->put_raw(chunk, file_size);
        ::munmap(chunk, file_size);
        ::close(fd);
        //data_out = nuraft::buffer::clone(*latest_snapshot_buf);
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
        if (!responses_queue.push(response))
            throw Exception(ErrorCodes::SYSTEM_ERROR, "Could not push response with session id {} into responses queue", response.session_id);
}

void KeeperStateMachine::shutdownStorage()
{
    std::lock_guard lock(storage_and_responses_lock);
    storage->finalize();
}

std::vector<int64_t> KeeperStateMachine::getDeadSessions()
{
    std::lock_guard lock(storage_and_responses_lock);
    return storage->getDeadSessions();
}

uint64_t KeeperStateMachine::getLastProcessedZxid() const
{
    std::lock_guard lock(storage_and_responses_lock);
    return storage->getZXID();
}

uint64_t KeeperStateMachine::getNodesCount() const
{
    std::lock_guard lock(storage_and_responses_lock);
    return storage->getNodesCount();
}

uint64_t KeeperStateMachine::getTotalWatchesCount() const
{
    std::lock_guard lock(storage_and_responses_lock);
    return storage->getTotalWatchesCount();
}

uint64_t KeeperStateMachine::getWatchedPathsCount() const
{
    std::lock_guard lock(storage_and_responses_lock);
    return storage->getWatchedPathsCount();
}

uint64_t KeeperStateMachine::getSessionsWithWatchesCount() const
{
    std::lock_guard lock(storage_and_responses_lock);
    return storage->getSessionsWithWatchesCount();
}

uint64_t KeeperStateMachine::getTotalEphemeralNodesCount() const
{
    std::lock_guard lock(storage_and_responses_lock);
    return storage->getTotalEphemeralNodesCount();
}

uint64_t KeeperStateMachine::getSessionWithEphemeralNodesCount() const
{
    std::lock_guard lock(storage_and_responses_lock);
    return storage->getSessionWithEphemeralNodesCount();
}

void KeeperStateMachine::dumpWatches(WriteBufferFromOwnString & buf) const
{
    std::lock_guard lock(storage_and_responses_lock);
    storage->dumpWatches(buf);
}

void KeeperStateMachine::dumpWatchesByPath(WriteBufferFromOwnString & buf) const
{
    std::lock_guard lock(storage_and_responses_lock);
    storage->dumpWatchesByPath(buf);
}

void KeeperStateMachine::dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const
{
    std::lock_guard lock(storage_and_responses_lock);
    storage->dumpSessionsAndEphemerals(buf);
}

uint64_t KeeperStateMachine::getApproximateDataSize() const
{
    std::lock_guard lock(storage_and_responses_lock);
    return storage->getApproximateDataSize();
}

ClusterConfigPtr KeeperStateMachine::getClusterConfig() const
{
    std::lock_guard lock(cluster_config_lock);
    if (cluster_config)
    {
        /// dumb way to return copy...
        auto tmp = cluster_config->serialize();
        return ClusterConfig::deserialize(*tmp);
    }
    return nullptr;
}

}
