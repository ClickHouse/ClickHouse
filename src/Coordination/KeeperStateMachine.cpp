#include <atomic>
#include <cerrno>
#include <chrono>
#include <exception>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Disks/IDisk.h>
#include <Coordination/KeeperDispatcher.h>
#include <Coordination/KeeperReconfiguration.h>
#include <Common/ZooKeeper/KeeperSpans.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperStateMachine.h>
#include <Common/ProfiledLocks.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <boost/noncopyable.hpp>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/copyData.h>
#include <base/defines.h>
#include <base/errnoToString.h>
#include <base/move_extend.h>
#include <sys/mman.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>


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
    extern const Event KeeperReadSnapshotObject;
    extern const Event KeeperReadSnapshotFailed;
    extern const Event KeeperSnapshotRemoteLoaderErrors;
    extern const Event KeeperSaveSnapshotObject;
    extern const Event KeeperSaveSnapshotFailed;
    extern const Event KeeperSaveSnapshot;
    extern const Event KeeperStorageLockWaitMicroseconds;
    extern const Event KeeperStorageSharedLockWaitMicroseconds;
    extern const Event KeeperProcessAndResponsesLockWaitMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric KeeperAliveConnections;
}

namespace DB
{

namespace FailPoints
{
    extern const char keeper_save_snapshot_pause_mid_transfer[];
}

namespace CoordinationSetting
{
    extern const CoordinationSettingsBool compress_snapshots_with_zstd_format;
    extern const CoordinationSettingsMilliseconds dead_session_check_period_ms;
    extern const CoordinationSettingsUInt64 min_request_size_for_cache;
    extern const CoordinationSettingsUInt64 snapshots_to_keep;
    extern const CoordinationSettingsUInt64 snapshot_transfer_chunk_size;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
}

/// nuraft::snapshot holds only Raft metadata (last_log_idx, last_log_term, size, cluster_config).
static nuraft::ptr<nuraft::snapshot> cloneSnapshotMeta(nuraft::snapshot & s)
{
    auto buf = s.serialize();
    return nuraft::snapshot::deserialize(*buf);
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
    , min_request_size_to_cache(keeper_context_->getCoordinationSettings()[CoordinationSetting::min_request_size_for_cache])
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
    const KeeperContextPtr & keeper_context_,
    KeeperSnapshotManagerS3 * snapshot_manager_s3_,
    IKeeperStateMachine::CommitCallback commit_callback_,
    const std::string & superdigest_)
    : IKeeperStateMachine(
        responses_queue_,
        snapshots_queue_,
        keeper_context_,
        snapshot_manager_s3_,
        commit_callback_,
        superdigest_),
        snapshot_manager(
          keeper_context_->getCoordinationSettings()[CoordinationSetting::snapshots_to_keep],
          keeper_context_,
          keeper_context_->getCoordinationSettings()[CoordinationSetting::compress_snapshots_with_zstd_format],
          superdigest_,
          keeper_context_->getCoordinationSettings()[CoordinationSetting::dead_session_check_period_ms].totalMilliseconds())
{
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
            std::lock_guard lock(snapshots_lock);

            auto snapshot_buf = snapshot_manager.deserializeSnapshotBufferFromDisk(latest_log_index);
            auto snapshot_deserialization_result = snapshot_manager.deserializeSnapshotFromBuffer(snapshot_buf);
            latest_snapshot_info = snapshot_manager.getLatestSnapshotInfo();
            chassert(latest_snapshot_info);

            try
            {
                latest_snapshot_size.store(
                    latest_snapshot_info->disk->getFileSize(latest_snapshot_info->path),
                    std::memory_order_relaxed);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to get snapshot size during init");
            }

            storage = std::move(snapshot_deserialization_result.storage);
            latest_snapshot_meta = snapshot_deserialization_result.snapshot_meta;
            cluster_config = snapshot_deserialization_result.cluster_config;
            keeper_context->setLastCommitIndex(latest_snapshot_meta->get_last_log_idx());
        }
        catch (...)
        {
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Failure to load from latest snapshot with index {}: {}. Manual intervention is necessary for recovery. Problematic "
                "snapshot can be removed but it will lead to data loss",
                latest_log_index,
                getCurrentExceptionMessage(/* with_stacktrace= */ false, /* check_embedded_stacktrace= */ true));
            }
    }

    auto last_committed_idx = keeper_context->lastCommittedIndex();
    if (has_snapshots)
        LOG_DEBUG(log, "Loaded snapshot with last committed log index {}", last_committed_idx);
    else
        LOG_DEBUG(log, "No existing snapshots, last committed log index {}", last_committed_idx);

    if (!storage)
        storage = std::make_unique<Storage>(
            keeper_context->getCoordinationSettings()[CoordinationSetting::dead_session_check_period_ms].totalMilliseconds(), superdigest, keeper_context);
}

namespace
{

void assertDigest(
    const KeeperDigest & expected,
    const KeeperDigest & actual,
    const Coordination::ZooKeeperRequest & request,
    uint64_t log_idx,
    uint64_t session_id,
    bool committing)
{
    if (!KeeperStorageBase::checkDigest(expected, actual))
    {
        LOG_FATAL(
            getLogger("KeeperStateMachine"),
            "Digest for nodes is not matching after {} request of type '{}' at log index {} for session {}.\nExpected digest - {}, actual digest - {} "
            "(digest {}). Keeper will terminate to avoid inconsistencies.\nExtra information about the request:\n{}",
            committing ? "committing" : "preprocessing",
            request.getOpNum(),
            log_idx,
            session_id,
            expected.value,
            actual.value,
            expected.version,
            request.toString());
        std::terminate();
    }
}

/// Macros to construct timed lock guards for state_machine_storage_mutex with appropriate ProfileEvents.
/// We cannot use a factory function because TSA does not track lock ownership across function boundaries.
#define KEEPER_STORAGE_LOCK_EXCLUSIVE(name) \
    ProfiledExclusiveLock name(state_machine_storage_mutex, ProfileEvents::KeeperStorageLockWaitMicroseconds)

#define KEEPER_STORAGE_LOCK_SHARED(name) \
    ProfiledSharedLock name(state_machine_storage_mutex, ProfileEvents::KeeperStorageSharedLockWaitMicroseconds)

union XidHelper
{
    struct
    {
        uint32_t lower;
        uint32_t upper;
    } parts;
    int64_t xid;
};

}

template<typename Storage>
nuraft::ptr<nuraft::buffer> KeeperStateMachine<Storage>::pre_commit(uint64_t log_idx, nuraft::buffer & data)
{
    const UInt64 start_time_us = ZooKeeperOpentelemetrySpans::now();

    double sleep_probability = keeper_context->getPrecommitSleepProbabilityForTesting();
    int64_t sleep_ms = keeper_context->getPrecommitSleepMillisecondsForTesting();
    if (sleep_ms != 0 && sleep_probability != 0)
    {
        std::uniform_real_distribution<double> distribution{0., 1.};
        if (distribution(thread_local_rng) > (1 - sleep_probability))
        {
            LOG_WARNING(log, "Precommit sleep enabled, will pause for {} ms", sleep_ms);
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        }
    }

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

    const auto maybe_log_opentelemetry_span = [&](OpenTelemetry::SpanStatus status, const std::string & error_message)
    {
        request_for_session->request->spans.maybeInitialize(
            KeeperSpan::PreCommit,
            request_for_session->request->tracing_context.get(),
            start_time_us);

        request_for_session->request->spans.maybeFinalize(
            KeeperSpan::PreCommit,
            [&]
            {
                return std::vector<OpenTelemetry::SpanAttribute>{
                    {"keeper.operation", Coordination::opNumToString(request_for_session->request->getOpNum())},
                    {"keeper.session_id", request_for_session->session_id},
                    {"keeper.xid", request_for_session->request->xid},
                    {"raft.log_idx", log_idx},
                };
            },
            status,
            error_message);
    };

    try
    {
        preprocess(*request_for_session);
    }
    catch (...)
    {
        maybe_log_opentelemetry_span(OpenTelemetry::SpanStatus::ERROR, getCurrentExceptionMessage(true));
        throw;
    }

    maybe_log_opentelemetry_span(OpenTelemetry::SpanStatus::OK, "");

    return result;
}

// Serialize the request for the log entry
nuraft::ptr<nuraft::buffer> IKeeperStateMachine::getZooKeeperLogEntry(const KeeperRequestForSession & request_for_session)
{
    DB::WriteBufferFromNuraftBuffer write_buf;
    DB::writeIntBinary(request_for_session.session_id, write_buf);

    const auto & request = request_for_session.request;
    size_t request_size = sizeof(uint32_t) + Coordination::size(request->getOpNum()) + request->sizeImpl();
    Coordination::write(static_cast<int32_t>(request_size), write_buf);

    XidHelper xid_helper{.xid = request->xid};
    Coordination::write(xid_helper.parts.lower, write_buf);

    Coordination::write(request->getOpNum(), write_buf);
    request->writeImpl(write_buf);

    DB::writeIntBinary(request_for_session.time, write_buf);
    /// we fill with dummy values to eliminate unnecessary copy later on when we will write correct values
    DB::writeIntBinary(static_cast<int64_t>(0), write_buf); /// zxid
    DB::writeIntBinary(static_cast<uint8_t>(KeeperDigestVersion::NO_DIGEST), write_buf); /// digest version or NO_DIGEST flag
    DB::writeIntBinary(static_cast<uint64_t>(0), write_buf); /// digest value

    /// Write upper part of XID if either:
    /// 1. use_xid_64
    /// 2. !use_xid_64 && request->tracing_context -> pass zeroes
    if (request_for_session.use_xid_64 || request->tracing_context)
    {
        Coordination::write(xid_helper.parts.upper, write_buf);
    }

    if (request->tracing_context)
    {
        request->tracing_context->serialize(write_buf);
    }

    /// if new fields are added, update KeeperStateMachine::ZooKeeperLogSerializationVersion along with parseRequest function and PreAppendLog callback handler
    return write_buf.getBuffer();
}

std::shared_ptr<KeeperRequestForSession> IKeeperStateMachine::parseRequest(
    nuraft::buffer & data, bool final, ZooKeeperLogSerializationVersion * serialization_version, size_t * request_end_position)
{
    ReadBufferFromNuraftBuffer buffer(data);
    auto request_for_session = std::make_shared<KeeperRequestForSession>();
    readIntBinary(request_for_session->session_id, buffer);

    int32_t length;
    Coordination::read(length, buffer);
    /// Request should not exceed max_request_size (this is verified in KeeperTCPHandler)
    if (length < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid request length: {}", length);

    /// because of backwards compatibility, only 32bit xid could be written
    /// for that reason we serialize XID in 2 parts:
    /// - lower: 32 least significant bits of 64bit XID OR 32bit XID
    /// - upper: 32 most significant bits of 64bit XID
    XidHelper xid_helper;
    Coordination::read(xid_helper.parts.lower, buffer);

    /// go to end of the buffer and read extra information including second part of XID
    auto buffer_position = buffer.getPosition();
    buffer.seek(length - sizeof(uint32_t), SEEK_CUR);

    if (request_end_position)
        *request_end_position = buffer.getPosition();

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
        if (request_for_session->digest->version != KeeperDigestVersion::NO_DIGEST || !buffer.eof())
            readIntBinary(request_for_session->digest->value, buffer);
    }

    if (!buffer.eof())
    {
        version = WITH_XID_64;
        Coordination::read(xid_helper.parts.upper, buffer);
    }
    else
    {
        xid_helper.xid = static_cast<int32_t>(xid_helper.parts.lower);
    }

    std::shared_ptr<OpenTelemetry::TracingContext> tracing_context;
    if (!buffer.eof())
    {
        version = WITH_OPTIONAL_TRACING_CONTEXT;

        tracing_context = std::make_shared<OpenTelemetry::TracingContext>();
        tracing_context->deserialize(buffer);
    }

    if (serialization_version)
        *serialization_version = version;

    int64_t xid = xid_helper.xid;

    buffer.seek(buffer_position, SEEK_SET);

    static constexpr std::array non_cacheable_xids{
        Coordination::WATCH_XID,
        Coordination::PING_XID,
        Coordination::AUTH_XID,
        Coordination::CLOSE_XID,
        Coordination::CLOSE_XID_64,
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
                return request_it->second;
            }
        }
    }

    Coordination::OpNum opnum;
    Coordination::read(opnum, buffer);

    request_for_session->request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request_for_session->request->xid = xid;
    request_for_session->request->readImpl(buffer);

    if (tracing_context)
        request_for_session->request->tracing_context = std::move(tracing_context);

    if (should_cache && !final)
    {
        std::lock_guard lock(request_cache_mutex);
        parsed_request_cache[request_for_session->session_id].emplace(xid, request_for_session);
    }

    return request_for_session;
}

template<typename Storage>
std::optional<KeeperDigest> KeeperStateMachine<Storage>::preprocess(const KeeperRequestForSession & request_for_session)
{
    const auto op_num = request_for_session.request->getOpNum();
    if (op_num == Coordination::OpNum::SessionID || op_num == Coordination::OpNum::Reconfig)
        return storage->getNodesDigest(false, /*lock_transaction_mutex=*/true);

    if (storage->isFinalized())
        return std::nullopt;

    KeeperDigest digest_after_preprocessing;
    try
    {
        KEEPER_STORAGE_LOCK_SHARED(lock);
        digest_after_preprocessing = storage->preprocessRequest(
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
        LOG_FATAL(
            log,
            "Failed to preprocess stored log at index {}: {}",
            request_for_session.log_idx,
            getCurrentExceptionMessage(true, true, false));
        LOG_FATAL(log, "Aborting to avoid inconsistent state");
        abort();
    }

    if (keeper_context->digestEnabled() && request_for_session.digest)
        assertDigest(
            *request_for_session.digest,
            digest_after_preprocessing,
            *request_for_session.request,
            request_for_session.log_idx,
            request_for_session.session_id,
            false);

    return digest_after_preprocessing;
}

template<typename Storage>
void KeeperStateMachine<Storage>::reconfigure(const KeeperRequestForSession & request_for_session)
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    KeeperResponseForSession response = processReconfiguration(request_for_session);
    response.response->enqueue_ts = std::chrono::steady_clock::now();
    if (!responses_queue.push(response))
    {
        ProfileEvents::increment(ProfileEvents::KeeperCommitsFailed);
        LOG_WARNING(log,
            "Failed to push response with session id {} to the queue, probably because of shutdown",
            response.session_id);
    }
}

template<typename Storage>
KeeperResponseForSession KeeperStateMachine<Storage>::processReconfiguration(
    const KeeperRequestForSession & request_for_session)
{
    ProfileEvents::increment(ProfileEvents::KeeperReconfigRequest);

    const auto & request = static_cast<const Coordination::ZooKeeperReconfigRequest &>(*request_for_session.request);
    const int64_t session_id = request_for_session.session_id;
    const int64_t zxid = request_for_session.zxid;

    using enum Coordination::Error;
    auto bad_request = [&](Coordination::Error code = ZBADARGUMENTS) -> KeeperResponseForSession
    {
        auto res = std::make_shared<Coordination::ZooKeeperReconfigResponse>();
        res->xid = request.xid;
        res->zxid = zxid;
        res->error = code;
        return { session_id, std::move(res) };
    };

    if (!storage->checkACL(keeper_config_path, Coordination::ACL::Write, session_id, /*is_local=*/ true, /*should_lock_storage=*/ true))
        return bad_request(ZNOAUTH);

    KeeperDispatcher & dispatcher = *keeper_context->getDispatcher();
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
    const UInt64 start_time_us = ZooKeeperOpentelemetrySpans::now();

    auto request_for_session = parseRequest(data, true);
    if (!request_for_session->zxid)
        request_for_session->zxid = log_idx;

    request_for_session->log_idx = log_idx;

    if (!keeper_context->localLogsPreprocessed() && !preprocess(*request_for_session))
        return nullptr;

    auto try_push = [&](KeeperResponseForSession & response)
    {
        response.response->enqueue_ts = std::chrono::steady_clock::now();
        if (response.request)
            response.request->spans.maybeInitialize(KeeperSpan::DispatcherResponsesQueue, response.request->tracing_context.get());
        if (!responses_queue.push(response))
        {
            ProfileEvents::increment(ProfileEvents::KeeperCommitsFailed);
            LOG_WARNING(log,
                "Failed to push response with session id {} to the queue, probably because of shutdown",
                response.session_id);
        }
    };

    const auto maybe_log_opentelemetry_span = [&](OpenTelemetry::SpanStatus status, const std::string & error_message)
    {
        request_for_session->request->spans.maybeInitialize(
            KeeperSpan::Commit,
            request_for_session->request->tracing_context.get(),
            start_time_us);

        request_for_session->request->spans.maybeFinalize(
            KeeperSpan::Commit,
            [&]
            {
                return std::vector<OpenTelemetry::SpanAttribute>{
                    {"keeper.operation", Coordination::opNumToString(request_for_session->request->getOpNum())},
                    {"keeper.session_id", request_for_session->session_id},
                    {"keeper.xid", request_for_session->request->xid},
                    {"raft.log_idx", log_idx},
                };
            },
            status,
            error_message);
    };

    try
    {
        const auto op_num = request_for_session->request->getOpNum();
        if (op_num == Coordination::OpNum::SessionID)
        {
            const Coordination::ZooKeeperSessionIDRequest & session_id_request
                = dynamic_cast<const Coordination::ZooKeeperSessionIDRequest &>(*request_for_session->request);
            int64_t session_id;
            std::shared_ptr<Coordination::ZooKeeperSessionIDResponse> response = std::dynamic_pointer_cast<Coordination::ZooKeeperSessionIDResponse>(session_id_request.makeResponse());
            KeeperResponseForSession response_for_session;
            response_for_session.session_id = -1;
            response_for_session.response = response;
            response_for_session.request = request_for_session->request;

            KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
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
                KEEPER_STORAGE_LOCK_SHARED(lock);
                ProfiledMutexLock response_lock(process_and_responses_lock, ProfileEvents::KeeperProcessAndResponsesLockWaitMicroseconds);
                KeeperResponsesForSessions responses_for_sessions
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
                    request_for_session->session_id,
                    true);
        }

        ProfileEvents::increment(ProfileEvents::KeeperCommits);

        if (commit_callback)
            commit_callback(log_idx, *request_for_session);

        keeper_context->setLastCommitIndex(log_idx);
    }
    catch (...)
    {
        maybe_log_opentelemetry_span(OpenTelemetry::SpanStatus::ERROR, getCurrentExceptionMessage(true));
        tryLogCurrentException(log, fmt::format("Failed to commit stored log at index {}", log_idx));
        throw;
    }

    maybe_log_opentelemetry_span(OpenTelemetry::SpanStatus::OK, "");

    return nullptr;
}

template<typename Storage>
bool KeeperStateMachine<Storage>::apply_snapshot(nuraft::snapshot & s)
{
    LOG_DEBUG(log, "Applying snapshot {}", s.get_last_log_idx());

    { /// deserialize and apply snapshot to storage
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
        if (s.get_last_log_idx() < latest_snapshot_meta->get_last_log_idx())
        {
            LOG_INFO(
                log,
                "A snapshot with a larger last log index ({}) was created, skipping applying this snapshot",
                latest_snapshot_meta->get_last_log_idx());
            return true;
        }

        SnapshotDeserializationResult<Storage> snapshot_deserialization_result
            = snapshot_manager.deserializeSnapshotFromBuffer(snapshot_manager.deserializeSnapshotBufferFromDisk(s.get_last_log_idx()));

        KEEPER_STORAGE_LOCK_EXCLUSIVE(storage_lock);
        /// maybe some logs were preprocessed with log idx larger than the snapshot idx
        /// we have to apply them to the new storage
        storage->applyUncommittedState(*snapshot_deserialization_result.storage, snapshot_deserialization_result.snapshot_meta->get_last_log_idx());
        storage = std::move(snapshot_deserialization_result.storage);
        latest_snapshot_meta = snapshot_deserialization_result.snapshot_meta;
        cluster_config = snapshot_deserialization_result.cluster_config;

        snapshot_loader_info.reset();
        cancelIfHasUnfinishedSnapshotReceive();
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
void KeeperStateMachine<Storage>::rollbackRequest(const KeeperRequestForSession & request_for_session, bool allow_missing)
{
    if (request_for_session.request->getOpNum() == Coordination::OpNum::SessionID)
        return;

    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
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

    auto snapshot_meta_copy = cloneSnapshotMeta(s);
    CreateSnapshotTask snapshot_task;
    { /// lock storage for a short period time to turn on "snapshot mode". After that we can read consistent storage state without locking.
        KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
        snapshot_task.snapshot = std::make_shared<KeeperStorageSnapshot<Storage>>(
            storage.get(), snapshot_meta_copy, getClusterConfig(), keeper_context->getWriteSnapshotVersion());
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
                            latest_snapshot_info = snapshot_manager.serializeSnapshotToDisk(*snapshot);
                        }
                        else
                        {
                            auto snapshot_buf = snapshot_manager.serializeSnapshotToBuffer(*snapshot);
                            auto snapshot_info = snapshot_manager.serializeSnapshotBufferToDisk(
                                *snapshot_buf, snapshot->snapshot_meta->get_last_log_idx());
                            latest_snapshot_info = std::move(snapshot_info);
                        }
                        snapshot_loader_info.reset();
                        cancelIfHasUnfinishedSnapshotReceive();

                        ProfileEvents::increment(ProfileEvents::KeeperSnapshotCreations);
                        LOG_DEBUG(
                            log,
                            "Created persistent snapshot {} with path {}",
                            latest_snapshot_meta->get_last_log_idx(),
                            latest_snapshot_info->path);

                        try
                        {
                            latest_snapshot_size.store(
                                latest_snapshot_info->disk->getFileSize(latest_snapshot_info->path),
                                std::memory_order_relaxed);
                        }
                        catch (...)
                        {
                            tryLogCurrentException(log, "Failed to get snapshot size after creation");
                        }
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
            KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
            LOG_TRACE(log, "Clearing garbage after snapshot");
            /// Turn off "snapshot mode" and clear outdate part of storage state
            snapshot.reset();
            storage->clearGarbageAfterSnapshot();
            LOG_TRACE(log, "Cleared garbage after snapshot");
        }

        when_done(ret, exception);

        std::lock_guard lock(snapshots_lock);
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
    nuraft::snapshot & s, uint64_t & obj_id, nuraft::buffer & data, bool is_first_obj, bool is_last_obj)
{
    LOG_DEBUG(log, "Saving snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);

    std::lock_guard lock(snapshots_lock);
    try
    {

        if (is_first_obj && is_last_obj)
        {
            /// If there is non-finalized state from previous call - clean it up.
            cancelIfHasUnfinishedSnapshotReceive();
            latest_snapshot_info = snapshot_manager.serializeSnapshotBufferToDisk(data, s.get_last_log_idx());
            ++obj_id;
        }
        else
        {
            if (is_first_obj)
            {
                /// If there is non-finalized state from previous call - clean it up.
                cancelIfHasUnfinishedSnapshotReceive();
                snapshot_receive_ctx = snapshot_manager.beginSnapshotReceiveToDisk(s.get_last_log_idx());
            }

            if (!snapshot_receive_ctx || snapshot_receive_ctx->log_idx != s.get_last_log_idx())
            {
                /// Stale context — ask leader to restart.
                LOG_WARNING(
                    log,
                    "Snapshot receive context is missing or stale for snapshot {} obj_id {} "
                    "(context log_idx: {}). Resetting to obj_id=0 to restart transfer.",
                    s.get_last_log_idx(),
                    obj_id,
                    snapshot_receive_ctx ? snapshot_receive_ctx->log_idx : 0);
                cancelIfHasUnfinishedSnapshotReceive();
                obj_id = 0;
                return;
            }

            if (!is_first_obj && obj_id != snapshot_receive_ctx->expected_obj_id)
            {
                if (obj_id < snapshot_receive_ctx->expected_obj_id)
                {
                    /// Duplicate — skip, advance leader.
                    LOG_WARNING(
                        log,
                        "Snapshot {} received duplicate chunk {} (expected {}), skipping.",
                        s.get_last_log_idx(),
                        obj_id,
                        snapshot_receive_ctx->expected_obj_id);
                    obj_id = snapshot_receive_ctx->expected_obj_id;
                }
                else
                {
                    /// Gap — restart.
                    LOG_WARNING(
                        log,
                        "Snapshot {} received out-of-order chunk {} (expected {}), restarting.",
                        s.get_last_log_idx(),
                        obj_id,
                        snapshot_receive_ctx->expected_obj_id);
                    obj_id = 0;
                }
                if (!obj_id)
                    cancelIfHasUnfinishedSnapshotReceive();
                return;
            }

            ReadBufferFromNuraftBuffer reader(data);
            copyData(reader, *snapshot_receive_ctx->write_buf);
            /// Advance obj_id to the next chunk we want; NuRaft forwards it to the leader as the next offset.
            obj_id = ++snapshot_receive_ctx->expected_obj_id;

            if (!is_first_obj && !is_last_obj)
                FailPointInjection::pauseFailPoint(FailPoints::keeper_save_snapshot_pause_mid_transfer);

            if (is_last_obj)
                latest_snapshot_info = snapshot_manager.finalizeSnapshotReceiveToDisk(*snapshot_receive_ctx);
        }

        ProfileEvents::increment(ProfileEvents::KeeperSaveSnapshotObject);
        if (is_last_obj)
        {
            latest_snapshot_meta = cloneSnapshotMeta(s);
            snapshot_receive_ctx.reset();
            snapshot_loader_info.reset();

            uint64_t snp_size = 0;
            try
            {
                if (latest_snapshot_info)
                {
                    snp_size = latest_snapshot_info->disk->getFileSize(latest_snapshot_info->path);
                    latest_snapshot_size.store(snp_size, std::memory_order_relaxed);
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to get snapshot size after save");
            }

            ProfileEvents::increment(ProfileEvents::KeeperSaveSnapshot);
            LOG_DEBUG(log, "Saved snapshot {} ({} chunks, {} bytes)", s.get_last_log_idx(), obj_id, snp_size);
        }
    }
    catch (...)
    {
        cancelIfHasUnfinishedSnapshotReceive();
        tryLogCurrentException(log);
        ProfileEvents::increment(ProfileEvents::KeeperSaveSnapshotFailed);
        if (is_last_obj)
            throw; /// NuRaft would call apply_snapshot regardless of obj_id; re-throw so it aborts instead.
        obj_id = 0; /// Ask leader to restart the transfer from the beginning.
    }
}


/// Shared buffer for serving a remote-disk snapshot to lagging
/// followers without re-reading from remote storage.
/// Abstract interface for reading snapshot chunks. Implementations for local (mmap) and remote (buffered read) disks.
struct ISnapshotLoader
{
    virtual ~ISnapshotLoader() = default;

    /// Initialize the loader. Safe to call concurrently — implementations serialize internally.
    /// Returns false on any error; the caller is responsible for invalidating the cached loader.
    virtual bool init(uint64_t log_idx, const SnapshotFileInfo & info, LoggerPtr log) = 0;

    virtual uint64_t fileSize() const = 0;

    /// Return a stable pointer to bytes [offset, offset+length).
    /// For remote disk: loads data on demand under an internal mutex.
    /// Returns nullptr on any error; call getLastError() to retrieve the exception.
    virtual nuraft::byte * getChunk(uint64_t offset, uint64_t length, LoggerPtr log) = 0;

    /// Returns the stored exception if a previous getChunk call failed, nullptr otherwise.
    virtual std::exception_ptr getLastError() const { return nullptr; }
};

/// Remote disk loader: reads the snapshot file from remote storage into a shared in-memory buffer,
/// serving chunks to concurrent followers without re-reading from remote storage.
/// All fields are protected by load_mutex; init() and getChunk() acquire it internally.
struct RemoteSnapshotLoader : public ISnapshotLoader
{
    uint64_t snapshot_id = 0;
    uint64_t file_size = 0;
    nuraft::ptr<nuraft::buffer> buf; /// pre-allocated full-size buffer; chunks are served from it
    std::unique_ptr<ReadBufferFromFileBase> reader; /// null until initialized, null again once fully loaded
    std::atomic<uint64_t> loaded_bytes = 0;
    std::atomic<bool> has_error = false; /// set on readStrict failure; prevents retrying a broken reader
    std::exception_ptr last_error;
    mutable std::mutex load_mutex;

    void onException(LoggerPtr log_, const std::string & message)
    {
        last_error = std::current_exception();
        has_error.store(true, std::memory_order_release);
        tryLogCurrentException(log_, message);
    }

    bool init(uint64_t log_idx, const SnapshotFileInfo & info, LoggerPtr log_) override
    {
        if (has_error.load(std::memory_order_relaxed))
            return false;
        std::lock_guard lock(load_mutex);
        if (has_error.load(std::memory_order_relaxed))
            return false;
        if (buf)
        {
            /// Already initialized by a previous follower — just verify it's the same snapshot.
            chassert(snapshot_id == log_idx);
            return true;
        }
        snapshot_id = log_idx;
        try
        {
            file_size = info.disk->getFileSize(info.path);
        }
        catch (...) /// Ok: exception is saved and logged via onException
        {
            onException(log_, "Failed to get snapshot size for transfer");
            return false;
        }
        if (file_size == 0)
        {
            LOG_WARNING(log_, "Snapshot {} on remote disk has zero size", log_idx);
            return false;
        }
        try
        {
            reader = info.disk->readFile(info.path, getReadSettings(), file_size);
        }
        catch (...) /// Ok: exception is saved and logged via onException
        {
            onException(log_, "Failed to open snapshot for transfer");
            return false;
        }
        try
        {
            buf = nuraft::buffer::alloc(file_size);
        }
        catch (...) /// Ok: exception is saved and logged via onException
        {
            reader.reset();
            onException(log_, fmt::format("Failed to allocate {} bytes for snapshot {} transfer", file_size, log_idx));
            return false;
        }
        return true;
    }

    uint64_t fileSize() const override
    {
        std::lock_guard lock(load_mutex);
        return file_size;
    }

    std::exception_ptr getLastError() const override
    {
        if (has_error.load(std::memory_order_acquire))
            return last_error;
        return nullptr;
    }

    nuraft::byte * getChunk(uint64_t offset, uint64_t length, LoggerPtr log_) override
    {
        if (has_error.load(std::memory_order_relaxed))
            return nullptr;

        const uint64_t needed = offset + length;

        /// Once bytes [0, needed) are written into buf they are never modified,
        /// so an acquire-load of loaded_bytes is sufficient to observe them without the mutex.
        if (loaded_bytes.load(std::memory_order_acquire) >= needed)
        {
            LOG_TEST(log_, "Snapshot at offset {} is already loaded (lock-free)", offset);
            return buf->data_begin() + offset;
        }

        std::lock_guard lock(load_mutex);
        if (has_error.load(std::memory_order_relaxed))
            return nullptr;
        const uint64_t current = loaded_bytes.load(std::memory_order_relaxed);
        if (current < needed)
        {
            chassert(reader != nullptr);
            try
            {
                LOG_TEST(log_, "Loading at offset {} size {}", offset, length);
                reader->readStrict(
                    reinterpret_cast<char *>(buf->data_begin()) + current,
                    needed - current);
            }
            catch (...) /// Ok: exception is saved and logged via onException
            {
                onException(log_, "Failed to read snapshot chunk");
                ProfileEvents::increment(ProfileEvents::KeeperSnapshotRemoteLoaderErrors);
                return nullptr;
            }
            loaded_bytes.store(needed, std::memory_order_release);
            if (needed == file_size)
            {
                LOG_DEBUG(log_, "Snapshot {} fully loaded into memory, closing reader", snapshot_id);
                reader.reset();
            }
        }
        else
            LOG_TEST(log_, "Snapshot at offset {} is already loaded", offset);
        return buf->data_begin() + offset;
    }
};

namespace
{

/// Local disk loader: owns fd + mmap for a single follower, RAII cleanup on destruction.
struct LocalSnapshotLoader : private boost::noncopyable, public ISnapshotLoader
{
    int fd = -1;
    nuraft::byte * mmap_ptr = nullptr;
    uint64_t file_size = 0;

    LocalSnapshotLoader() = default;

    bool init(uint64_t /*log_idx*/, const SnapshotFileInfo & info, LoggerPtr log_) override
    {
        const auto full_path = fs::path(info.disk->getPath()) / info.path;
        if (!std::filesystem::exists(full_path))
        {
            LOG_WARNING(log_, "Snapshot file {} does not exist", full_path);
            return false;
        }

        LOG_INFO(log_, "Opening snapshot file {} for chunked transfer", full_path);
        fd = ::open(full_path.string().c_str(), O_RDONLY);
        if (fd < 0)
        {
            LOG_WARNING(log_, "Error opening {}, error: {}, errno: {}", full_path, errnoToString(), errno);
            return false;
        }

        auto raw_file_size = ::lseek(fd, 0, SEEK_END);
        if (raw_file_size < 0)
        {
            LOG_WARNING(log_, "Error getting size of {}, error: {}, errno: {}", full_path, errnoToString(), errno);
            [[maybe_unused]] int err = ::close(fd);
            chassert(!err || errno == EINTR);
            fd = -1;
            return false;
        }

        file_size = static_cast<uint64_t>(raw_file_size);
        if (file_size == 0)
        {
            LOG_WARNING(log_, "Snapshot file {} is empty", full_path);
            [[maybe_unused]] int err = ::close(fd);
            chassert(!err || errno == EINTR);
            fd = -1;
            return false;
        }

        mmap_ptr = reinterpret_cast<nuraft::byte *>(::mmap(nullptr, file_size, PROT_READ, MAP_FILE | MAP_SHARED, fd, 0));
        if (mmap_ptr == MAP_FAILED)
        {
            LOG_WARNING(log_, "Error mmapping {}, error: {}, errno: {}", full_path, errnoToString(), errno);
            [[maybe_unused]] int err = ::close(fd);
            chassert(!err || errno == EINTR);
            fd = -1;
            mmap_ptr = nullptr;
            return false;
        }

        return true;
    }

    uint64_t fileSize() const override { return file_size; }

    nuraft::byte * getChunk(uint64_t offset, uint64_t /*length*/, LoggerPtr /*log_*/) override
    {
        return mmap_ptr + offset;
    }

    ~LocalSnapshotLoader() override
    {
        if (mmap_ptr)
        {
            ::munmap(mmap_ptr, file_size);
            [[maybe_unused]] int err = ::close(fd);
            chassert(!err || errno == EINTR);
        }
    }

};

/// Per-follower state kept alive across chunked snapshot transfer on the leader.
struct SnapshotTransferCtx
{
    uint64_t chunk_size = 0;
    std::shared_ptr<ISnapshotLoader> loader;
};

}

int IKeeperStateMachine::read_logical_snp_obj(
    nuraft::snapshot & s, void *& user_snp_ctx, uint64_t obj_id, nuraft::ptr<nuraft::buffer> & data_out, bool & is_last_obj)
{
    LOG_DEBUG(log, "Reading snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);

    bool success = false;
    SCOPE_EXIT({
        if (!success)
            ProfileEvents::increment(ProfileEvents::KeeperReadSnapshotFailed);
    });

    std::optional<SnapshotFileInfo> snapshot_info;
    std::shared_ptr<ISnapshotLoader> remote_loader;
    const uint64_t configured_chunk_size = keeper_context->getCoordinationSettings()[CoordinationSetting::snapshot_transfer_chunk_size];

    {
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
            if (user_snp_ctx)
                free_user_snp_ctx(user_snp_ctx);
            return -1;
        }

        snapshot_info.emplace(latest_snapshot_info->path, latest_snapshot_info->disk);

        if (obj_id == 0)
        {
            /// Free leftover context if NuRaft retries from obj_id=0 without calling free_user_snp_ctx.
            if (user_snp_ctx)
                free_user_snp_ctx(user_snp_ctx);

            /// Remote loader is shared across followers — create once under lock and copy the shared_ptr.
            /// Local loader is per-follower and created below outside the lock.
            if (!isLocalDisk(*snapshot_info->disk))
            {
                if (!snapshot_loader_info)
                    snapshot_loader_info = std::make_shared<RemoteSnapshotLoader>();
                remote_loader = snapshot_loader_info;
            }
        }
    }

    if (obj_id == 0)
    {
        const bool is_local_disk = !remote_loader;
        LOG_DEBUG(log, "Opening snapshot {} on {} disk for transfer", s.get_last_log_idx(), is_local_disk ? "local" : "remote");

        std::shared_ptr<ISnapshotLoader> loader;
        if (is_local_disk)
            loader = std::make_shared<LocalSnapshotLoader>();
        else
            loader = remote_loader;

        if (!loader->init(s.get_last_log_idx(), *snapshot_info, log))
        {
            if (remote_loader)
            {
                std::lock_guard lock(snapshots_lock);
                if (snapshot_loader_info == remote_loader)
                    snapshot_loader_info.reset();
            }
            return -1;
        }

        const uint64_t file_size = loader->fileSize();
        /// chunk_size == 0 means disabled: send the whole file in one object (backward-compatible behavior).
        const uint64_t effective_chunk_size = configured_chunk_size == 0 ? file_size : configured_chunk_size;
        user_snp_ctx = new SnapshotTransferCtx{
            .chunk_size = effective_chunk_size,
            .loader = std::move(loader),
        };
    }

    auto * ctx = reinterpret_cast<SnapshotTransferCtx *>(user_snp_ctx);
    if (!ctx)
    {
        LOG_WARNING(log, "Snapshot transfer context is null for obj_id {}", obj_id);
        free_user_snp_ctx(user_snp_ctx);
        chassert(false); /// This should not happen, catch in CI.
        return -1;
    }

    const uint64_t file_size = ctx->loader->fileSize();

    /// Guard against multiplication overflow before computing offset.
    if (ctx->chunk_size != 0 && obj_id > file_size / ctx->chunk_size)
    {
        LOG_WARNING(log, "Snapshot obj_id {} would overflow offset computation (file_size={}, chunk_size={})",
            obj_id, file_size, ctx->chunk_size);
        free_user_snp_ctx(user_snp_ctx);
        return -1;
    }
    const auto offset = obj_id * ctx->chunk_size;
    if (offset >= file_size)
    {
        LOG_WARNING(log, "Snapshot obj_id {} is out of range (file_size={})", obj_id, file_size);
        free_user_snp_ctx(user_snp_ctx);
        chassert(false); /// This should not happen, catch in CI.
        return -1;
    }

    chassert(ctx->chunk_size != 0);
    const auto chunk_size = std::min(ctx->chunk_size, file_size - offset);
    is_last_obj = (offset + chunk_size == file_size);

    nuraft::byte * src_ptr = ctx->loader->getChunk(offset, chunk_size, log);
    if (!src_ptr)
    {
        {
            std::lock_guard lock(snapshots_lock);
            /// Only reset if this is the same loader instance that's currently cached
            /// — a concurrent follower may have already replaced snapshot_loader_info.
            if (snapshot_loader_info == ctx->loader)
                snapshot_loader_info.reset();
        }
        auto err = ctx->loader->getLastError();
        free_user_snp_ctx(user_snp_ctx);
        if (err)
            LOG_ERROR(log, "RemoteSnapshotLoader failed to read chunk: {}", getExceptionMessage(err, /*with_stacktrace=*/false));
        return -1;
    }

    try
    {
        data_out = nuraft::buffer::alloc(chunk_size);
        data_out->put_raw(src_ptr, chunk_size);
    }
    catch (...)
    {
        tryLogCurrentException(log);
        {
            std::lock_guard lock(snapshots_lock);
            if (snapshot_loader_info == ctx->loader)
                snapshot_loader_info.reset();
        }
        free_user_snp_ctx(user_snp_ctx);
        return -1;
    }

    success = true;
    ProfileEvents::increment(ProfileEvents::KeeperReadSnapshotObject);
    if (is_last_obj)
        ProfileEvents::increment(ProfileEvents::KeeperReadSnapshot);
    return 1;
}

void IKeeperStateMachine::free_user_snp_ctx(void *& user_snp_ctx)
{
    if (!user_snp_ctx)
        return;

    delete reinterpret_cast<SnapshotTransferCtx *>(user_snp_ctx);
    user_snp_ctx = nullptr;
}

template<typename Storage>
void KeeperStateMachine<Storage>::processReadRequests(const KeeperRequestsForSessions & requests)
{
    /// Pure local request, just process them with storage
    KEEPER_STORAGE_LOCK_SHARED(storage_lock);
    ProfiledMutexLock response_lock(process_and_responses_lock, ProfileEvents::KeeperProcessAndResponsesLockWaitMicroseconds);

    auto responses = storage->processLocalRequests(requests, /*check_acl=*/ true);

    for (auto & response_for_session : responses)
    {
        response_for_session.response->enqueue_ts = std::chrono::steady_clock::now();
        if (response_for_session.response->xid != Coordination::WATCH_XID)
        {
            chassert(response_for_session.request);
            response_for_session.request->spans.maybeInitialize(KeeperSpan::DispatcherResponsesQueue, response_for_session.request->tracing_context.get());
        }
        if (!responses_queue.push(response_for_session))
            LOG_WARNING(log, "Failed to push response with session id {} to the queue, probably because of shutdown", response_for_session.session_id);
    }
}

template<typename Storage>
void KeeperStateMachine<Storage>::shutdownStorage()
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    if (!storage)
        return;
    storage->finalize();
}

template<typename Storage>
std::vector<int64_t> KeeperStateMachine<Storage>::getDeadSessions()
{
    KEEPER_STORAGE_LOCK_SHARED(lock);
    return storage->getDeadSessions();
}

template<typename Storage>
int64_t KeeperStateMachine<Storage>::getNextZxid() const
{
    return storage->getNextZXID();
}

template<typename Storage>
KeeperDigest KeeperStateMachine<Storage>::getNodesDigest() const
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    return storage->getNodesDigest(false, /*lock_transaction_mutex=*/true);
}

template<typename Storage>
int64_t KeeperStateMachine<Storage>::getLastProcessedZxid() const
{
    KEEPER_STORAGE_LOCK_SHARED(lock);
    return storage->getZXID();
}

template<typename Storage>
const KeeperStorageStats & KeeperStateMachine<Storage>::getStorageStats() const TSA_NO_THREAD_SAFETY_ANALYSIS
{
    return storage->getStorageStats();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getNodesCount() const
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    return storage->getNodesCount();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getTotalWatchesCount() const
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    return storage->getTotalWatchesCount();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getWatchedPathsCount() const
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    return storage->getWatchedPathsCount();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getSessionsWithWatchesCount() const
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    return storage->getSessionsWithWatchesCount();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getTotalEphemeralNodesCount() const
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    return storage->getTotalEphemeralNodesCount();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getSessionWithEphemeralNodesCount() const
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    return storage->getSessionWithEphemeralNodesCount();
}

template<typename Storage>
void KeeperStateMachine<Storage>::dumpWatches(WriteBufferFromOwnString & buf) const
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    storage->dumpWatches(buf);
}

template<typename Storage>
void KeeperStateMachine<Storage>::dumpWatchesByPath(WriteBufferFromOwnString & buf) const
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    storage->dumpWatchesByPath(buf);
}

template<typename Storage>
void KeeperStateMachine<Storage>::dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    storage->dumpSessionsAndEphemerals(buf);
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getApproximateDataSize() const
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    return storage->getApproximateDataSize();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getKeyArenaSize() const
{
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    return storage->getArenaDataSize();
}

template<typename Storage>
uint64_t KeeperStateMachine<Storage>::getLatestSnapshotSize() const
{
    return latest_snapshot_size.load(std::memory_order_relaxed);
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
    KEEPER_STORAGE_LOCK_EXCLUSIVE(lock);
    LOG_INFO(log, "Recalculating storage stats");
    storage->recalculateStats();
    LOG_INFO(log, "Done recalculating storage stats");
}

template<typename Storage>
void KeeperStateMachine<Storage>::cancelIfHasUnfinishedSnapshotReceive()
{
    if (!snapshot_receive_ctx)
        return;

    /// Cancel the write buffer before removing the file.
    const auto disk = snapshot_receive_ctx->disk;
    const auto snapshot_file_name = snapshot_receive_ctx->snapshot_file_name;
    snapshot_receive_ctx.reset();

    try
    {
        const auto tmp_snapshot_file_name = "tmp_" + snapshot_file_name;
        LOG_INFO(log, "Canceling unfinished snapshot receive, removing partial files {} and {}", snapshot_file_name, tmp_snapshot_file_name);
        disk->removeFileIfExists(snapshot_file_name);
        disk->removeFileIfExists(tmp_snapshot_file_name);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to remove partial snapshot files");
    }
}

template class KeeperStateMachine<KeeperMemoryStorage>;
#if USE_ROCKSDB
template class KeeperStateMachine<KeeperRocksStorage>;
#endif

}
