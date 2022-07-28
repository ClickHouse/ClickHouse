#include <cerrno>
#include <future>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <IO/ReadHelpers.h>
#include <sys/mman.h>
#include "Common/ZooKeeper/ZooKeeperCommon.h"
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/ProfileEvents.h>
#include "Coordination/KeeperStorage.h"

namespace ProfileEvents
{
    extern const Event KeeperCommits;
    extern const Event KeeperCommitsFailed;
    extern const Event KeeperSnapshotCreations;
    extern const Event KeeperSnapshotCreationsFailed;
    extern const Event KeeperSnapshotApplys;
    extern const Event KeeperSnapshotApplysFailed;
    extern const Event KeeperReadSnapshot;
    extern const Event KeeperSaveSnapshot;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYSTEM_ERROR;
}

namespace
{
}

KeeperStateMachine::KeeperStateMachine(
    ResponsesQueue & responses_queue_,
    SnapshotsQueue & snapshots_queue_,
    const std::string & snapshots_path_,
    const CoordinationSettingsPtr & coordination_settings_,
    const KeeperContextPtr & keeper_context_,
    const std::string & superdigest_)
    : coordination_settings(coordination_settings_)
    , snapshot_manager(
          snapshots_path_,
          coordination_settings->snapshots_to_keep,
          keeper_context_,
          coordination_settings->compress_snapshots_with_zstd_format,
          superdigest_,
          coordination_settings->dead_session_check_period_ms.totalMilliseconds())
    , responses_queue(responses_queue_)
    , snapshots_queue(snapshots_queue_)
    , last_committed_idx(0)
    , log(&Poco::Logger::get("KeeperStateMachine"))
    , superdigest(superdigest_)
    , keeper_context(keeper_context_)
{
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
            auto snapshot_deserialization_result
                = snapshot_manager.deserializeSnapshotFromBuffer(snapshot_manager.deserializeSnapshotBufferFromDisk(latest_log_index));
            latest_snapshot_path = snapshot_manager.getLatestSnapshotPath();
            storage = std::move(snapshot_deserialization_result.storage);
            latest_snapshot_meta = snapshot_deserialization_result.snapshot_meta;
            cluster_config = snapshot_deserialization_result.cluster_config;
            last_committed_idx = latest_snapshot_meta->get_last_log_idx();
            loaded = true;
            break;
        }
        catch (const DB::Exception & ex)
        {
            LOG_WARNING(
                log,
                "Failed to load from snapshot with index {}, with error {}, will remove it from disk",
                latest_log_index,
                ex.displayText());
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
        storage = std::make_unique<KeeperStorage>(
            coordination_settings->dead_session_check_period_ms.totalMilliseconds(), superdigest, keeper_context);
}

namespace
{

void assertDigest(
    const KeeperStorage::Digest & first,
    const KeeperStorage::Digest & second,
    const Coordination::ZooKeeperRequest & request,
    bool committing)
{
    if (!KeeperStorage::checkDigest(first, second))
    {
        LOG_FATAL(
            &Poco::Logger::get("KeeperStateMachine"),
            "Digest for nodes is not matching after {} request of type '{}'.\nExpected digest - {}, actual digest - {} (digest version "
            "{}). Keeper will "
            "terminate to avoid inconsistencies.\nExtra information about the request:\n{}",
            committing ? "committing" : "preprocessing",
            request.getOpNum(),
            first.value,
            second.value,
            first.version,
            request.toString());
        std::terminate();
    }
}

}

nuraft::ptr<nuraft::buffer> KeeperStateMachine::pre_commit(uint64_t log_idx, nuraft::buffer & data)
{
    auto request_for_session = parseRequest(data);
    if (!request_for_session.zxid)
        request_for_session.zxid = log_idx;

    preprocess(request_for_session);
    return nullptr;
}

KeeperStorage::RequestForSession KeeperStateMachine::parseRequest(nuraft::buffer & data)
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

    if (!buffer.eof())
        readIntBinary(request_for_session.time, buffer);
    else /// backward compatibility
        request_for_session.time
            = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    if (!buffer.eof())
        readIntBinary(request_for_session.zxid, buffer);

    if (!buffer.eof())
    {
        request_for_session.digest.emplace();
        readIntBinary(request_for_session.digest->version, buffer);
        if (request_for_session.digest->version != KeeperStorage::DigestVersion::NO_DIGEST)
            readIntBinary(request_for_session.digest->value, buffer);
    }

    return request_for_session;
}

void KeeperStateMachine::preprocess(const KeeperStorage::RequestForSession & request_for_session)
{
    if (request_for_session.request->getOpNum() == Coordination::OpNum::SessionID)
        return;

    std::lock_guard lock(storage_and_responses_lock);
    storage->preprocessRequest(
        request_for_session.request,
        request_for_session.session_id,
        request_for_session.time,
        request_for_session.zxid,
        true /* check_acl */,
        request_for_session.digest);

    if (keeper_context->digest_enabled && request_for_session.digest)
        assertDigest(*request_for_session.digest, storage->getNodesDigest(false), *request_for_session.request, false);
}

nuraft::ptr<nuraft::buffer> KeeperStateMachine::commit(const uint64_t log_idx, nuraft::buffer & data)
{
    auto request_for_session = parseRequest(data);
    if (!request_for_session.zxid)
        request_for_session.zxid = log_idx;

    /// Special processing of session_id request
    if (request_for_session.request->getOpNum() == Coordination::OpNum::SessionID)
    {
        const Coordination::ZooKeeperSessionIDRequest & session_id_request
            = dynamic_cast<const Coordination::ZooKeeperSessionIDRequest &>(*request_for_session.request);
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
            {
                ProfileEvents::increment(ProfileEvents::KeeperCommitsFailed);
                throw Exception(ErrorCodes::SYSTEM_ERROR, "Could not push response with session id {} into responses queue", session_id);
            }
        }
    }
    else
    {
        std::lock_guard lock(storage_and_responses_lock);
        KeeperStorage::ResponsesForSessions responses_for_sessions = storage->processRequest(
            request_for_session.request, request_for_session.session_id, request_for_session.zxid);
        for (auto & response_for_session : responses_for_sessions)
            if (!responses_queue.push(response_for_session))
            {
                ProfileEvents::increment(ProfileEvents::KeeperCommitsFailed);
                throw Exception(
                    ErrorCodes::SYSTEM_ERROR,
                    "Could not push response with session id {} into responses queue",
                    response_for_session.session_id);
            }

        if (keeper_context->digest_enabled && request_for_session.digest)
            assertDigest(*request_for_session.digest, storage->getNodesDigest(true), *request_for_session.request, true);
    }

    ProfileEvents::increment(ProfileEvents::KeeperCommits);
    last_committed_idx = log_idx;
    return nullptr;
}

bool KeeperStateMachine::apply_snapshot(nuraft::snapshot & s)
{
    LOG_DEBUG(log, "Applying snapshot {}", s.get_last_log_idx());
    nuraft::ptr<nuraft::buffer> latest_snapshot_ptr;
    { /// save snapshot into memory
        std::lock_guard lock(snapshots_lock);
        if (s.get_last_log_idx() != latest_snapshot_meta->get_last_log_idx())
        {
            ProfileEvents::increment(ProfileEvents::KeeperSnapshotApplysFailed);
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Required to apply snapshot with last log index {}, but our last log index is {}",
                s.get_last_log_idx(),
                latest_snapshot_meta->get_last_log_idx());
        }
        latest_snapshot_ptr = latest_snapshot_buf;
    }

    { /// deserialize and apply snapshot to storage
        std::lock_guard lock(storage_and_responses_lock);
        auto snapshot_deserialization_result
            = snapshot_manager.deserializeSnapshotFromBuffer(snapshot_manager.deserializeSnapshotBufferFromDisk(s.get_last_log_idx()));
        storage = std::move(snapshot_deserialization_result.storage);
        latest_snapshot_meta = snapshot_deserialization_result.snapshot_meta;
        cluster_config = snapshot_deserialization_result.cluster_config;
    }

    ProfileEvents::increment(ProfileEvents::KeeperSnapshotApplys);
    last_committed_idx = s.get_last_log_idx();
    return true;
}


void KeeperStateMachine::commit_config(const uint64_t /* log_idx */, nuraft::ptr<nuraft::cluster_config> & new_conf)
{
    std::lock_guard lock(cluster_config_lock);
    auto tmp = new_conf->serialize();
    cluster_config = ClusterConfig::deserialize(*tmp);
}

void KeeperStateMachine::rollback(uint64_t log_idx, nuraft::buffer & data)
{
    auto request_for_session = parseRequest(data);
    // If we received a log from an older node, use the log_idx as the zxid
    // log_idx will always be larger or equal to the zxid so we can safely do this
    // (log_idx is increased for all logs, while zxid is only increased for requests)
    if (!request_for_session.zxid)
        request_for_session.zxid = log_idx;

    rollbackRequest(request_for_session);
}

void KeeperStateMachine::rollbackRequest(const KeeperStorage::RequestForSession & request_for_session)
{
    if (request_for_session.request->getOpNum() == Coordination::OpNum::SessionID)
        return;

    std::lock_guard lock(storage_and_responses_lock);
    storage->rollbackRequest(request_for_session.zxid);
}

nuraft::ptr<nuraft::snapshot> KeeperStateMachine::last_snapshot()
{
    /// Just return the latest snapshot.
    std::lock_guard<std::mutex> lock(snapshots_lock);
    return latest_snapshot_meta;
}

void KeeperStateMachine::create_snapshot(nuraft::snapshot & s, nuraft::async_result<bool>::handler_type & when_done)
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
    snapshot_task.create_snapshot = [this, when_done](KeeperStorageSnapshotPtr && snapshot)
    {
        nuraft::ptr<std::exception> exception(nullptr);
        bool ret = true;
        try
        {
            { /// Read storage data without locks and create snapshot
                std::lock_guard lock(snapshots_lock);
                auto [path, error_code] = snapshot_manager.serializeSnapshotToDisk(*snapshot);
                if (error_code)
                {
                    throw Exception(
                        ErrorCodes::SYSTEM_ERROR,
                        "Snapshot {} was created failed, error: {}",
                        snapshot->snapshot_meta->get_last_log_idx(),
                        error_code.message());
                }
                latest_snapshot_path = path;
                latest_snapshot_meta = snapshot->snapshot_meta;
                ProfileEvents::increment(ProfileEvents::KeeperSnapshotCreations);
                LOG_DEBUG(log, "Created persistent snapshot {} with path {}", latest_snapshot_meta->get_last_log_idx(), path);
            }

            {
                /// Destroy snapshot with lock
                std::lock_guard lock(storage_and_responses_lock);
                LOG_TRACE(log, "Clearing garbage after snapshot");
                /// Turn off "snapshot mode" and clear outdate part of storage state
                storage->clearGarbageAfterSnapshot();
                LOG_TRACE(log, "Cleared garbage after snapshot");
                snapshot.reset();
            }
        }
        catch (...)
        {
            ProfileEvents::increment(ProfileEvents::KeeperSnapshotCreationsFailed);
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
    nuraft::snapshot & s, uint64_t & obj_id, nuraft::buffer & data, bool /*is_first_obj*/, bool /*is_last_obj*/)
{
    LOG_DEBUG(log, "Saving snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);

    /// copy snapshot meta into memory
    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    nuraft::ptr<nuraft::snapshot> cloned_meta = nuraft::snapshot::deserialize(*snp_buf);

    try
    {
        std::lock_guard lock(snapshots_lock);
        /// Serialize snapshot to disk
        auto result_path = snapshot_manager.serializeSnapshotBufferToDisk(data, s.get_last_log_idx());
        latest_snapshot_path = result_path;
        latest_snapshot_meta = cloned_meta;
        LOG_DEBUG(log, "Saved snapshot {} to path {}", s.get_last_log_idx(), result_path);
        obj_id++;
        ProfileEvents::increment(ProfileEvents::KeeperSaveSnapshot);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

static int bufferFromFile(Poco::Logger * log, const std::string & path, nuraft::ptr<nuraft::buffer> & data_out)
{
    if (path.empty() || !std::filesystem::exists(path))
    {
        LOG_WARNING(log, "Snapshot file {} does not exist", path);
        return -1;
    }

    int fd = ::open(path.c_str(), O_RDONLY);
    LOG_INFO(log, "Opening file {} for read_logical_snp_obj", path);
    if (fd < 0)
    {
        LOG_WARNING(log, "Error opening {}, error: {}, errno: {}", path, std::strerror(errno), errno);
        return errno;
    }
    auto file_size = ::lseek(fd, 0, SEEK_END);
    ::lseek(fd, 0, SEEK_SET);
    auto * chunk = reinterpret_cast<nuraft::byte *>(::mmap(nullptr, file_size, PROT_READ, MAP_FILE | MAP_SHARED, fd, 0));
    if (chunk == MAP_FAILED)
    {
        LOG_WARNING(log, "Error mmapping {}, error: {}, errno: {}", path, std::strerror(errno), errno);
        ::close(fd);
        return errno;
    }
    data_out = nuraft::buffer::alloc(file_size);
    data_out->put_raw(chunk, file_size);
    ::munmap(chunk, file_size);
    ::close(fd);
    return 0;
}

int KeeperStateMachine::read_logical_snp_obj(
    nuraft::snapshot & s, void *& /*user_snp_ctx*/, uint64_t obj_id, nuraft::ptr<nuraft::buffer> & data_out, bool & is_last_obj)
{
    LOG_DEBUG(log, "Reading snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);

    std::lock_guard lock(snapshots_lock);
    /// Our snapshot is not equal to required. Maybe we still creating it in the background.
    /// Let's wait and NuRaft will retry this call.
    if (s.get_last_log_idx() != latest_snapshot_meta->get_last_log_idx())
    {
        LOG_WARNING(
            log,
            "Required to apply snapshot with last log index {}, but our last log index is {}. Will ignore this one and retry",
            s.get_last_log_idx(),
            latest_snapshot_meta->get_last_log_idx());
        return -1;
    }
    if (bufferFromFile(log, latest_snapshot_path, data_out))
    {
        LOG_WARNING(log, "Error reading snapshot {} from {}", s.get_last_log_idx(), latest_snapshot_path);
        return -1;
    }
    is_last_obj = true;
    ProfileEvents::increment(ProfileEvents::KeeperReadSnapshot);

    return 1;
}

void KeeperStateMachine::processReadRequest(const KeeperStorage::RequestForSession & request_for_session)
{
    /// Pure local request, just process it with storage
    std::lock_guard lock(storage_and_responses_lock);
    auto responses = storage->processRequest(
        request_for_session.request,
        request_for_session.session_id,
        std::nullopt,
        true /*check_acl*/,
        true /*is_local*/);
    for (const auto & response : responses)
        if (!responses_queue.push(response))
            throw Exception(
                ErrorCodes::SYSTEM_ERROR, "Could not push response with session id {} into responses queue", response.session_id);
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

int64_t KeeperStateMachine::getNextZxid() const
{
    std::lock_guard lock(storage_and_responses_lock);
    return storage->getNextZXID();
}

KeeperStorage::Digest KeeperStateMachine::getNodesDigest() const
{
    std::lock_guard lock(storage_and_responses_lock);
    return storage->getNodesDigest(false);
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

uint64_t KeeperStateMachine::getKeyArenaSize() const
{
    std::lock_guard lock(storage_and_responses_lock);
    return storage->getArenaDataSize();
}

uint64_t KeeperStateMachine::getLatestSnapshotBufSize() const
{
    std::lock_guard lock(snapshots_lock);
    if (latest_snapshot_buf)
        return latest_snapshot_buf->size();
    return 0;
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
