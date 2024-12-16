#include <atomic>
#include <cerrno>
#include <chrono>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperDispatcher.h>
#include <Coordination/KeeperReconfiguration.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Disks/DiskLocal.h>
#include <IO/ReadHelpers.h>
#include <base/defines.h>
#include <base/errnoToString.h>
#include <base/move_extend.h>
#include <sys/mman.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event KeeperCommits;
    extern const Event KeeperReconfigRequest;
    extern const Event KeeperCommitsFailed;
    extern const Event KeeperSnapshotCreations;
    extern const Event KeeperSnapshotCreationsFailed;
    extern const Event KeeperSnapshotApplys;
    extern const Event KeeperSnapshotApplysFailed;
    extern const Event KeeperReadSnapshot;
    extern const Event KeeperSaveSnapshot;
    extern const Event KeeperStorageLockWaitMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric KeeperAliveConnections;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IKeeperStateMachine::IKeeperStateMachine(
    ResponsesQueue & responses_queue_,
    SnapshotsQueue & snapshots_queue_,
    const KeeperContextPtr & keeper_context_,
    KeeperSnapshotManagerS3 * snapshot_manager_s3_,
    CommitCallback commit_callback_,
    const std::string & superdigest_)
    : commit_callback(commit_callback_)
    , responses_queue(responses_queue_)
    , snapshots_queue(snapshots_queue_)
    , min_request_size_to_cache(keeper_context_->getCoordinationSettings()->min_request_size_for_cache)
    , log(getLogger("KeeperStateMachine"))
    , read_pool(CurrentMetrics::KeeperAliveConnections, CurrentMetrics::KeeperAliveConnections, CurrentMetrics::KeeperAliveConnections, 100, 10000, 10000)
    , superdigest(superdigest_)
    , keeper_context(keeper_context_)
    , snapshot_manager_s3(snapshot_manager_s3_)
{
}

template<typename Storage>
KeeperStateMachine<Storage>::KeeperStateMachine(
    ResponsesQueue & responses_queue_,
    SnapshotsQueue & snapshots_queue_,
    // const CoordinationSettingsPtr & coordination_settings_,
    const KeeperContextPtr & keeper_context_,
    KeeperSnapshotManagerS3 * snapshot_manager_s3_,
    IKeeperStateMachine::CommitCallback commit_callback_,
    const std::string & superdigest_)
    : IKeeperStateMachine(
        responses_queue_,
        snapshots_queue_,
        /// coordination_settings_,
        keeper_context_,
        snapshot_manager_s3_,
        commit_callback_,
        superdigest_),
        snapshot_manager(
          keeper_context_->getCoordinationSettings()->snapshots_to_keep,
          keeper_context_,
          keeper_context_->getCoordinationSettings()->compress_snapshots_with_zstd_format,
          superdigest_,
          keeper_context_->getCoordinationSettings()->dead_session_check_period_ms.totalMilliseconds())
{
}

namespace
{

bool isLocalDisk(const IDisk & disk)
{
    return dynamic_cast<const DiskLocal *>(&disk) != nullptr;
}

}

template<typename Storage>
void KeeperStateMachine<Storage>::init()
{
    /// Do everything without mutexes, no other threads exist.
    LOG_DEBUG(log, "Totally have {} snapshots", snapshot_manager.totalSnapshots());
    bool has_snapshots = snapshot_manager.totalSnapshots() != 0;
    /// Deserialize latest snapshot from disk
    uint64_t latest_log_index = snapshot_manager.getLatestSnapshotIndex();
    LOG_DEBUG(log, "Trying to load state machine from snapshot up to log index {}", latest_log_index);

    if (has_snapshots)
    {
        try
        {
            latest_snapshot_buf = snapshot_manager.deserializeSnapshotBufferFromDisk(latest_log_index);
            auto snapshot_deserialization_result = snapshot_manager.deserializeSnapshotFromBuffer(latest_snapshot_buf);
            latest_snapshot_info = snapshot_manager.getLatestSnapshotInfo();
            chassert(latest_snapshot_info);

            if (isLocalDisk(*latest_snapshot_info->disk))
                latest_snapshot_buf = nullptr;

            storage = std::move(snapshot_deserialization_result.storage);
            latest_snapshot_meta = snapshot_deserialization_result.snapshot_meta;
            cluster_config = snapshot_deserialization_result.cluster_config;
            keeper_context->setLastCommitIndex(latest_snapshot_meta->get_last_log_idx());
        }
        catch (...)
        {
            tryLogCurrentException(
                log,
                fmt::format(
                    "Aborting because of failure to load from latest snapshot with index {}. Problematic snapshot can be removed but it will "
                    "lead to data loss",
                    latest_log_index));
            std::abort();
        }
    }

    auto last_committed_idx = keeper_context->lastCommittedIndex();
    if (has_snapshots)
        LOG_DEBUG(log, "Loaded snapshot with last committed log index {}", last_committed_idx);
    else
        LOG_DEBUG(log, "No existing snapshots, last committed log index {}", last_committed_idx);

    if (!storage)
        storage = std::make_unique<Storage>(
            keeper_context->getCoordinationSettings()->dead_session_check_period_ms.totalMilliseconds(), superdigest, keeper_context);
}

namespace
{

void assertDigest(
    const KeeperStorageBase::Digest & expected,
    const KeeperStorageBase::Digest & actual,
    const Coordination::ZooKeeperRequest & request,
    uint64_t log_idx,
    bool committing)
{
    if (!KeeperStorageBase::checkDigest(expected, actual))
    {
        LOG_FATAL(
            getLogger("KeeperStateMachine"),
            "Digest for nodes is not matching after {} request of type '{}' at log index {}.\nExpected digest - {}, actual digest - {} "
            "(digest {}). Keeper will terminate to avoid inconsistencies.\nExtra information about the request:\n{}",
            committing ? "committing" : "preprocessing",
            request.getOpNum(),
            log_idx,
            expected.value,
            actual.value,
            expected.version,
            request.toString());
        std::terminate();
    }
}

template <bool shared = false>
struct LockGuardWithStats final
{
    using LockType = std::conditional_t<shared, std::shared_lock<SharedMutex>, std::unique_lock<SharedMutex>>;
    LockType lock;
    explicit LockGuardWithStats(SharedMutex & mutex)
    {
        Stopwatch watch;
        LockType l(mutex);
        ProfileEvents::increment(ProfileEvents::KeeperStorageLockWaitMicroseconds, watch.elapsedMicroseconds());
        lock = std::move(l);
    }

    ~LockGuardWithStats() = default;
};

}

template<typename Storage>
nuraft::ptr<nuraft::buffer> KeeperStateMachine<Storage>::pre_commit(uint64_t log_idx, nuraft::buffer & data)
{
    auto result = nuraft::buffer::alloc(sizeof(log_idx));
    nuraft::buffer_serializer ss(result);
    ss.put_u64(log_idx);

    /// Don't preprocess anything until the first commit when we will manually pre_commit and commit
    /// all needed logs
    if (!keeper_context->localLogsPreprocessed())
        return result;

    auto request_for_session = parseRequest(data, /*final=*/false);
    if (!request_for_session->zxid)
        request_for_session->zxid = log_idx;

    request_for_session->log_idx = log_idx;

    preprocess(*request_for_session);
    return result;
}

std::shared_ptr<KeeperStorageBase::RequestForSession> IKeeperStateMachine::parseRequest(nuraft::buffer & data, bool final, ZooKeeperLogSerializationVersion * serialization_version)
{
    ReadBufferFromNuraftBuffer buffer(data);
    auto request_for_session = std::make_shared<KeeperStorageBase::RequestForSession>();
    readIntBinary(request_for_session->session_id, buffer);

    int32_t length;
    Coordination::read(length, buffer);

    int32_t xid;
    Coordination::read(xid, buffer);

    static constexpr std::array non_cacheable_xids{
        Coordination::WATCH_XID,
        Coordination::PING_XID,
        Coordination::AUTH_XID,
        Coordination::CLOSE_XID,
    };

    const bool should_cache
        = min_request_size_to_cache != 0 && request_for_session->session_id != -1 && data.size() >= min_request_size_to_cache
        && std::all_of(
              non_cacheable_xids.begin(), non_cacheable_xids.end(), [&](const auto non_cacheable_xid) { return xid != non_cacheable_xid; });

    if (should_cache)
    {
        std::lock_guard lock(request_cache_mutex);
        if (auto xid_to_request_it = parsed_request_cache.find(request_for_session->session_id);
            xid_to_request_it != parsed_request_cache.end())
        {
            auto & xid_to_request = xid_to_request_it->second;
            if (auto request_it = xid_to_request.find(xid); request_it != xid_to_request.end())
            {
                if (final)
                {
                    auto request = std::move(request_it->second);
                    xid_to_request.erase(request_it);
                    return request;
                }
                else
                    return request_it->second;
            }
        }
    }


    Coordination::OpNum opnum;

    Coordination::read(opnum, buffer);

    request_for_session->request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request_for_session->request->xid = xid;
    request_for_session->request->readImpl(buffer);

    using enum ZooKeeperLogSerializationVersion;
    ZooKeeperLogSerializationVersion version = INITIAL;

    if (!buffer.eof())
    {
        version = WITH_TIME;
        readIntBinary(request_for_session->time, buffer);
    }
    else
        request_for_session->time
            = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    if (!buffer.eof())
    {
        version = WITH_ZXID_DIGEST;

        readIntBinary(request_for_session->zxid, buffer);

        chassert(!buffer.eof());

        request_for_session->digest.emplace();
        readIntBinary(request_for_session->digest->version, buffer);
        if (request_for_session->digest->version != KeeperStorageBase::DigestVersion::NO_DIGEST || !buffer.eof())
            readIntBinary(request_for_session->digest->value, buffer);
    }

    if (serialization_version)
        *serialization_version = version;

    if (should_cache && !final)
    {
        std::lock_guard lock(request_cache_mutex);
        parsed_request_cache[request_for_session->session_id].emplace(xid, request_for_session);
    }

    return request_for_session;
}

template<typename Storage>
bool KeeperStateMachine<Storage>::preprocess(const KeeperStorageBase::RequestForSession & request_for_session)
{
    const auto op_num = request_for_session.request->getOpNum();
    if (op_num == Coordination::OpNum::SessionID || op_num == Coordination::OpNum::Reconfig)
        return true;

    if (storage->isFinalized())
        return false;

    try
    {
        LockGuardWithStats<true> lock(storage_mutex);
        storage->preprocessRequest(
            request_for_session.request,
            request_for_session.session_id,
            request_for_session.time,
            request_for_session.zxid,
            true /* check_acl */,
            request_for_session.digest,
            request_for_session.log_idx);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("Failed to preprocess stored log at index {}, aborting to avoid inconsistent state", request_for_session.log_idx));
        std::abort();
    }

    if (keeper_context->digestEnabled() && request_for_session.digest)
        assertDigest(
            *request_for_session.digest,
            storage->getNodesDigest(false, /*lock_transaction_mutex=*/true),
            *request_for_session.request,
            request_for_session.log_idx,
            false);

    return true;
}

template<typename Storage>
void KeeperStateMachine<Storage>::reconfigure(const KeeperStorageBase::RequestForSession& request_for_session)
{
    LockGuardWithStats lock(storage_mutex);
    KeeperStorageBase::ResponseForSession response = processReconfiguration(request_for_session);
    if (!responses_queue.push(response))
    {
        ProfileEvents::increment(ProfileEvents::KeeperCommitsFailed);
        LOG_WARNING(log,
            "Failed to push response with session id {} to the queue, probably because of shutdown",
            response.session_id);
    }
}

template<typename Storage>
KeeperStorageBase::ResponseForSession KeeperStateMachine<Storage>::processReconfiguration(
    const KeeperStorageBase::RequestForSession & request_for_session)
{
    ProfileEvents::increment(ProfileEvents::KeeperReconfigRequest);

    const auto & request = static_cast<const Coordination::ZooKeeperReconfigRequest&>(*request_for_session.request);
    const int64_t session_id = request_for_session.session_id;
    const int64_t zxid = request_for_session.zxid;

    using enum Coordination::Error;
    auto bad_request = [&](Coordination::Error code = ZBADARGUMENTS) -> KeeperStorageBase::ResponseForSession
    {
        auto res = std::make_shared<Coordination::ZooKeeperReconfigResponse>();
        res->xid = request.xid;
        res->zxid = zxid;
        res->error = code;
        return { session_id, std::move(res) };
    };

    if (!storage->checkACL(keeper_config_path, Coordination::ACL::Write, session_id, true))
        return bad_request(ZNOAUTH);

    KeeperDispatcher& dispatcher = *keeper_context->getDispatcher();
    if (!dispatcher.reconfigEnabled())
        return bad_request(ZUNIMPLEMENTED);
    if (request.version != -1)
        return bad_request(ZBADVERSION);

    const bool has_new_members = !request.new_members.empty();
    const bool has_joining = !request.joining.empty();
    const bool has_leaving = !request.leaving.empty();
    const bool incremental_reconfig = (has_joining || has_leaving) && !has_new_members;
    if (!incremental_reconfig)
        return bad_request();

    const ClusterConfigPtr config = getClusterConfig();
    if (!config) // Server can be uninitialized yet
        return bad_request();

    ClusterUpdateActions updates;

    if (has_joining)
    {
        if (auto join_updates = joiningToClusterUpdates(config, request.joining); !join_updates.empty())
            moveExtend(updates, std::move(join_updates));
        else
            return bad_request();
    }

    if (has_leaving)
    {
        if (auto leave_updates = leavingToClusterUpdates(config, request.leaving); !leave_updates.empty())
            moveExtend(updates, std::move(leave_updates));
        else
            return bad_request();
    }

    auto response = std::make_shared<Coordination::ZooKeeperReconfigResponse>();
    response->xid = request.xid;
    response->zxid = zxid;
    response->error = Coordination::Error::ZOK;
    response->value = serializeClusterConfig(config, updates);

    dispatcher.pushClusterUpdates(std::move(updates));
    return { session_id, std::move(response) };
}

template<typename Storage>
nuraft::ptr<nuraft::buffer> KeeperStateMachine<Storage>::commit(const uint64_t log_idx, nuraft::buffer & data)
{
    auto request_for_session = parseRequest(data, true);
    if (!request_for_session->zxid)
        request_for_session->zxid = log_idx;

    request_for_session->log_idx = log_idx;

    if (!keeper_context->localLogsPreprocessed() && !preprocess(*request_for_session))
        return nullptr;

    auto try_push = [&](const KeeperStorageBase::ResponseForSession & response)
    {
        if (!responses_queue.push(response))
        {
            ProfileEvents::increment(ProfileEvents::KeeperCommitsFailed);
            LOG_WARNING(log,
                "Failed to push response with session id {} to the queue, probably because of shutdown",
                response.session_id);
        }
    };

    try
    {
        const auto op_num = request_for_session->request->getOpNum();
        if (op_num == Coordination::OpNum::SessionID)
        {
            const Coordination::ZooKeeperSessionIDRequest & session_id_request
                = dynamic_cast<const Coordination::ZooKeeperSessionIDRequest &>(*request_for_session->request);
            int64_t session_id;
            std::shared_ptr<Coordination::ZooKeeperSessionIDResponse> response = std::make_shared<Coordination::ZooKeeperSessionIDResponse>();
            response->internal_id = session_id_request.internal_id;
            response->server_id = session_id_request.server_id;
            KeeperStorageBase::ResponseForSession response_for_session;
            response_for_session.session_id = -1;
            response_for_session.response = response;
            response_for_session.request = request_for_session->request;

            LockGuardWithStats lock(storage_mutex);
            session_id = storage->getSessionID(session_id_request.session_timeout_ms);
            LOG_DEBUG(log, "Session ID response {} with timeout {}", session_id, session_id_request.session_timeout_ms);
            response->session_id = session_id;
            try_push(response_for_session);
        }
        else
        {
            if (op_num == Coordination::OpNum::Close)

            {
                std::lock_guard cache_lock(request_cache_mutex);
                parsed_request_cache.erase(request_for_session->session_id);
            }

            {
                LockGuardWithStats<true> lock(storage_mutex);
                std::lock_guard response_lock(process_and_responses_lock);
                KeeperStorageBase::ResponsesForSessions responses_for_sessions
                    = storage->processRequest(request_for_session->request, request_for_session->session_id, request_for_session->zxid);
                for (auto & response_for_session : responses_for_sessions)
                {
                    if (response_for_session.response->xid != Coordination::WATCH_XID)
                        response_for_session.request = request_for_session->request;

                    try_push(response_for_session);
                }
            }

            if (keeper_context->digestEnabled() && request_for_session->digest)
                assertDigest(
                    *request_for_session->digest,
                    storage->getNodesDigest(true, /*lock_transaction_mutex=*/true),
                    *request_for_session->request,
                    request_for_session->log_idx,
                    true);
        }

        ProfileEvents::increment(ProfileEvents::KeeperCommits);

        if (commit_callback)
            commit_callback(log_idx, *request_for_session);

        keeper_context->setLastCommitIndex(log_idx);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to commit stored log at index {}", log_idx));
        throw;
    }

    return nullptr;
}

template<typename Storage>
bool KeeperStateMachine<Storage>::apply_snapshot(nuraft::snapshot & s)
{
    LOG_DEBUG(log, "Applying snapshot {}", s.get_last_log_idx());
    nuraft::ptr<nuraft::buffer> latest_snapshot_ptr;
    { /// save snapshot into memory
        std::lock_guard lock(snapshots_lock);
        if (s.get_last_log_idx() > latest_snapshot_meta->get_last_log_idx())
        {
            ProfileEvents::increment(ProfileEvents::KeeperSnapshotApplysFailed);
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Required to apply snapshot with last log index {}, but last created snapshot was for smaller log index {}",
                s.get_last_log_idx(),
                latest_snapshot_meta->get_last_log_idx());
        }
        else if (s.get_last_log_idx() < latest_snapshot_meta->get_last_log_idx())
        {
            LOG_INFO(log, "A snapshot with a larger last log index ({}) was created, skipping applying this snapshot", latest_snapshot_meta->get_last_log_idx());
            return true;
        }

        latest_snapshot_ptr = latest_snapshot_buf;
    }

    { /// deserialize and apply snapshot to storage
        SnapshotDeserializationResult<Storage> snapshot_deserialization_result;
        if (latest_snapshot_ptr)
            snapshot_deserialization_result = snapshot_manager.deserializeSnapshotFromBuffer(latest_snapshot_ptr);
        else
            snapshot_deserialization_result
                = snapshot_manager.deserializeSnapshotFromBuffer(snapshot_manager.deserializeSnapshotBufferFromDisk(s.get_last_log_idx()));

        LockGuardWithStats storage_lock(storage_mutex);
        /// maybe some logs were preprocessed with log idx larger than the snapshot idx
        /// we have to apply them to the new storage
        storage->applyUncommittedState(*snapshot_deserialization_result.storage, snapshot_deserialization_result.snapshot_meta->get_last_log_idx());
        storage = std::move(snapshot_deserialization_result.storage);
        latest_snapshot_meta = snapshot_deserialization_result.snapshot_meta;
        cluster_config = snapshot_deserialization_result.cluster_config;
    }

    ProfileEvents::increment(ProfileEvents::KeeperSnapshotApplys);
    keeper_context->setLastCommitIndex(s.get_last_log_idx());
    return true;
}


void IKeeperStateMachine::commit_config(const uint64_t log_idx, nuraft::ptr<nuraft::cluster_config> & new_conf)
{
    std::lock_guard lock(cluster_config_lock);
    auto tmp = new_conf->serialize();
    cluster_config = ClusterConfig::deserialize(*tmp);
    keeper_context->setLastCommitIndex(log_idx);
}

void IKeeperStateMachine::rollback(uint64_t log_idx, nuraft::buffer & data)
{
    /// Don't rollback anything until the first commit because nothing was preprocessed
    if (!keeper_context->localLogsPreprocessed())
        return;

    auto request_for_session = parseRequest(data, true);
    // If we received a log from an older node, use the log_idx as the zxid
    // log_idx will always be larger or equal to the zxid so we can safely do this
    // (log_idx is increased for all logs, while zxid is only increased for requests)
    if (!request_for_session->zxid)
        request_for_session->zxid = log_idx;

    rollbackRequest(*request_for_session, false);
}

template<typename Storage>
void KeeperStateMachine<Storage>::rollbackRequest(const KeeperStorageBase::RequestForSession & request_for_session, bool allow_missing)
{
    if (request_for_session.request->getOpNum() == Coordination::OpNum::SessionID)
        return;

    LockGuardWithStats lock(storage_mutex);
    storage->rollbackRequest(request_for_session.zxid, allow_missing);
}

nuraft::ptr<nuraft::snapshot> IKeeperStateMachine::last_snapshot()
{
    /// Just return the latest snapshot.
    std::lock_guard lock(snapshots_lock);
    return latest_snapshot_meta;
}

template<typename Storage>
void KeeperStateMachine<Storage>::create_snapshot(nuraft::snapshot & s, nuraft::async_result<bool>::handler_type & when_done)
{
    LOG_DEBUG(log, "Creating snapshot {}", s.get_last_log_idx());

    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    auto snapshot_meta_copy = nuraft::snapshot::deserialize(*snp_buf);
    CreateSnapshotTask snapshot_task;
    { /// lock storage for a short period time to turn on "snapshot mode". After that we can read consistent storage state without locking.
        LockGuardWithStats lock(storage_mutex);
        snapshot_task.snapshot = std::make_shared<KeeperStorageSnapshot<Storage>>(storage.get(), snapshot_meta_copy, getClusterConfig());
    }

    /// create snapshot task for background execution (in snapshot thread)
    snapshot_task.create_snapshot = [this, when_done](KeeperStorageSnapshotPtr && snapshot_, bool execute_only_cleanup)
    {
        nuraft::ptr<std::exception> exception(nullptr);
        bool ret = false;
        auto && snapshot = std::get<std::shared_ptr<KeeperStorageSnapshot<Storage>>>(std::move(snapshot_));
        if (!execute_only_cleanup)
        {
            try
            {
                { /// Read storage data without locks and create snapshot
                    std::lock_guard lock(snapshots_lock);

                    if (latest_snapshot_meta && snapshot->snapshot_meta->get_last_log_idx() <= latest_snapshot_meta->get_last_log_idx())
                    {
                        LOG_INFO(
                            log,
                            "Will not create a snapshot with last log idx {} because a snapshot with bigger last log idx ({}) is already "
                            "created",
                            snapshot->snapshot_meta->get_last_log_idx(),
                            latest_snapshot_meta->get_last_log_idx());
                    }
                    else
                    {
                        latest_snapshot_meta = snapshot->snapshot_meta;
                        /// we rely on the fact that the snapshot disk cannot be changed during runtime
                        if (isLocalDisk(*keeper_context->getLatestSnapshotDisk()))
                        {
                            auto snapshot_info = snapshot_manager.serializeSnapshotToDisk(*snapshot);
                            latest_snapshot_info = std::move(snapshot_info);
                            latest_snapshot_buf = nullptr;
                        }
                        else
                        {
                            auto snapshot_buf = snapshot_manager.serializeSnapshotToBuffer(*snapshot);
                            auto snapshot_info = snapshot_manager.serializeSnapshotBufferToDisk(
                                *snapshot_buf, snapshot->snapshot_meta->get_last_log_idx());
                            latest_snapshot_info = std::move(snapshot_info);
                            latest_snapshot_buf = std::move(snapshot_buf);
                        }

                        ProfileEvents::increment(ProfileEvents::KeeperSnapshotCreations);
                        LOG_DEBUG(
                            log,
                            "Created persistent snapshot {} with path {}",
                            latest_snapshot_meta->get_last_log_idx(),
                            latest_snapshot_info->path);
                    }
                }

                ret = true;
            }
            catch (...)
            {
                ProfileEvents::increment(ProfileEvents::KeeperSnapshotCreationsFailed);
                LOG_TRACE(log, "Exception happened during snapshot");
                tryLogCurrentException(log);
            }
        }
        {
            /// Destroy snapshot with lock
            LockGuardWithStats lock(storage_mutex);
            LOG_TRACE(log, "Clearing garbage after snapshot");
            /// Turn off "snapshot mode" and clear outdate part of storage state
            storage->clearGarbageAfterSnapshot();
            LOG_TRACE(log, "Cleared garbage after snapshot");
            snapshot.reset();
        }

        when_done(ret, exception);

        return ret ? latest_snapshot_info : nullptr;
    };

    if (keeper_context->getServerState() == KeeperContext::Phase::SHUTDOWN)
    {
        LOG_INFO(log, "Creating a snapshot during shutdown because 'create_snapshot_on_exit' is enabled.");
        auto snapshot_file_info = snapshot_task.create_snapshot(std::move(snapshot_task.snapshot), /*execute_only_cleanup=*/false);

        if (snapshot_file_info && snapshot_manager_s3)
        {
            LOG_INFO(log, "Uploading snapshot {} during shutdown because 'upload_snapshot_on_exit' is enabled.", snapshot_file_info->path);
            snapshot_manager_s3->uploadSnapshot(snapshot_file_info, /* asnyc_upload */ false);
        }

        return;
    }

    LOG_DEBUG(log, "In memory snapshot {} created, queueing task to flush to disk", s.get_last_log_idx());
    /// Flush snapshot to disk in a separate thread.
    if (!snapshots_queue.push(std::move(snapshot_task)))
        LOG_WARNING(log, "Cannot push snapshot task into queue");
}

template<typename Storage>
void KeeperStateMachine<Storage>::save_logical_snp_obj(
    nuraft::snapshot & s, uint64_t & obj_id, nuraft::buffer & data, bool /*is_first_obj*/, bool /*is_last_obj*/)
{
    LOG_DEBUG(log, "Saving snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);

    /// copy snapshot meta into memory
    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    nuraft::ptr<nuraft::snapshot> cloned_meta = nuraft::snapshot::deserialize(*snp_buf);

    nuraft::ptr<nuraft::buffer> cloned_buffer;

    /// we rely on the fact that the snapshot disk cannot be changed during runtime
    if (!isLocalDisk(*keeper_context->getSnapshotDisk()))
        cloned_buffer = nuraft::buffer::clone(data);

    try
    {
        std::lock_guard lock(snapshots_lock);
        /// Serialize snapshot to disk
        latest_snapshot_info = snapshot_manager.serializeSnapshotBufferToDisk(data, s.get_last_log_idx());
        latest_snapshot_meta = cloned_meta;
        latest_snapshot_buf = std::move(cloned_buffer);
        LOG_DEBUG(log, "Saved snapshot {} to path {}", s.get_last_log_idx(), latest_snapshot_info->path);
        obj_id++;
        ProfileEvents::increment(ProfileEvents::KeeperSaveSnapshot);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

static int bufferFromFile(LoggerPtr log, const std::string & path, nuraft::ptr<nuraft::buffer> & data_out)
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
        LOG_WARNING(log, "Error opening {}, error: {}, errno: {}", path, errnoToString(), errno);
        return errno;
    }
    auto file_size = ::lseek(fd, 0, SEEK_END);
    ::lseek(fd, 0, SEEK_SET);
    auto * chunk = reinterpret_cast<nuraft::byte *>(::mmap(nullptr, file_size, PROT_READ, MAP_FILE | MAP_SHARED, fd, 0));
    if (chunk == MAP_FAILED)
    {
        LOG_WARNING(log, "Error mmapping {}, error: {}, errno: {}", path, errnoToString(), errno);
        int err = ::close(fd);
        chassert(!err || errno == EINTR);
        return errno;
    }
    data_out = nuraft::buffer::alloc(file_size);
    data_out->put_raw(chunk, file_size);
    ::munmap(chunk, file_size);
    int err = ::close(fd);
    chassert(!err || errno == EINTR);
    return 0;
}

int IKeeperStateMachine::read_logical_snp_obj(
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

    const auto & [path, disk, size] = *latest_snapshot_info;
    if (isLocalDisk(*disk))
    {
        auto full_path = fs::path(disk->getPath()) / path;
        if (bufferFromFile(log, full_path, data_out))
        {
            LOG_WARNING(log, "Error reading snapshot {} from {}", s.get_last_log_idx(), full_path);
            return -1;
        }
    }
    else
    {
        chassert(latest_snapshot_buf);
        data_out = nuraft::buffer::clone(*latest_snapshot_buf);
    }

    is_last_obj = true;
    ProfileEvents::increment(ProfileEvents::KeeperReadSnapshot);

    return 1;
}

template<typename Storage>
void KeeperStateMachine<Storage>::processReadRequest(const KeeperStorageBase::RequestForSession & request_for_session)
{
    /// Pure local request, just process it with storage
    LockGuardWithStats<true> storage_lock(storage_mutex);
    std::lock_guard response_lock(process_and_responses_lock);
    auto responses = storage->processRequest(
        request_for_session.request, request_for_session.session_id, std::nullopt, true /*check_acl*/, true /*is_local*/);
    for (auto & response_for_session : responses)
    {
        if (response_for_session.response->xid != Coordination::WATCH_XID)
            response_for_session.request = request_for_session.request;
        if (!responses_queue.push(response_for_session))
            LOG_WARNING(log, "Failed to push response with session id {} to the queue, probably because of shutdown", response_for_session.session_id);
    }
}

template<typename Storage>
void KeeperStateMachine<Storage>::shutdownStorage()
{
    LockGuardWithStats lock(storage_mutex);
    storage->finalize();
}

template<typename Storage>
std::vector<int64_t> KeeperStateMachine<Storage>::getDeadSessions()
{
    LockGuardWithStats lock(storage_mutex);
    return storage->getDeadSessions();
}

template<typename Storage>
int64_t KeeperStateMachine<Storage>::getNextZxid() const
{
    return storage->getNextZXID();
}

template<typename Storage>
KeeperStorageBase::Digest KeeperStateMachine<Storage>::getNodesDigest() const
{
    LockGuardWithStats lock(storage_mutex);
    return storage->getNodesDigest(false, /*lock_transaction_mutex=*/true);
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getLastProcessedZxid() const
{
    return storage->getZXID();
}

template<typename Storage>
const KeeperStorageBase::Stats & KeeperStateMachine<Storage>::getStorageStats() const TSA_NO_THREAD_SAFETY_ANALYSIS
{
    return storage->getStorageStats();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getNodesCount() const
{
    LockGuardWithStats lock(storage_mutex);
    return storage->getNodesCount();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getTotalWatchesCount() const
{
    LockGuardWithStats lock(storage_mutex);
    return storage->getTotalWatchesCount();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getWatchedPathsCount() const
{
    LockGuardWithStats lock(storage_mutex);
    return storage->getWatchedPathsCount();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getSessionsWithWatchesCount() const
{
    LockGuardWithStats lock(storage_mutex);
    return storage->getSessionsWithWatchesCount();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getTotalEphemeralNodesCount() const
{
    LockGuardWithStats lock(storage_mutex);
    return storage->getTotalEphemeralNodesCount();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getSessionWithEphemeralNodesCount() const
{
    LockGuardWithStats lock(storage_mutex);
    return storage->getSessionWithEphemeralNodesCount();
}

template<typename Storage>
void KeeperStateMachine<Storage>::dumpWatches(WriteBufferFromOwnString & buf) const
{
    LockGuardWithStats lock(storage_mutex);
    storage->dumpWatches(buf);
}

template<typename Storage>
void KeeperStateMachine<Storage>::dumpWatchesByPath(WriteBufferFromOwnString & buf) const
{
    LockGuardWithStats lock(storage_mutex);
    storage->dumpWatchesByPath(buf);
}

template<typename Storage>
void KeeperStateMachine<Storage>::dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const
{
    LockGuardWithStats lock(storage_mutex);
    storage->dumpSessionsAndEphemerals(buf);
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getApproximateDataSize() const
{
    LockGuardWithStats lock(storage_mutex);
    return storage->getApproximateDataSize();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getKeyArenaSize() const
{
    LockGuardWithStats lock(storage_mutex);
    return storage->getArenaDataSize();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getLatestSnapshotSize() const
{
    auto snapshot_info = [&]
    {
        std::lock_guard lock(snapshots_lock);
        return latest_snapshot_info;
    }();

    if (snapshot_info == nullptr || snapshot_info->disk == nullptr)
        return 0;

    /// there is a possibility multiple threads can try to get size
    /// this can happen in rare cases while it's not a heavy operation
    size_t size = snapshot_info->size.load(std::memory_order_relaxed);
    if (size == 0)
    {
        size = snapshot_info->disk->getFileSize(snapshot_info->path);
        snapshot_info->size.store(size, std::memory_order_relaxed);
    }

    return size;
}

ClusterConfigPtr IKeeperStateMachine::getClusterConfig() const
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

template<typename Storage>
void KeeperStateMachine<Storage>::recalculateStorageStats()
{
    LockGuardWithStats lock(storage_mutex);
    LOG_INFO(log, "Recalculating storage stats");
    storage->recalculateStats();
    LOG_INFO(log, "Done recalculating storage stats");
}

template class KeeperStateMachine<KeeperMemoryStorage>;
#if USE_ROCKSDB
template class KeeperStateMachine<KeeperRocksStorage>;
#endif

}
