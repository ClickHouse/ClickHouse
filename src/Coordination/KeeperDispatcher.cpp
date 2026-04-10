#include <Coordination/KeeperDispatcher.h>
#include <libnuraft/async.hxx>

#include <Poco/Path.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <base/hex.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/OpenTelemetryTracingContext.h>
#include <Common/HistogramMetrics.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/setThreadName.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/checkStackSize.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>
#include <Coordination/KeeperCommon.h>
#include <Common/ZooKeeper/KeeperSpans.h>
#include <Interpreters/Context.h>
#include <Common/thread_local_rng.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperReconfiguration.h>

#include <IO/ReadHelpers.h>
#include <Disks/IDisk.h>

#include <atomic>
#include <future>
#include <chrono>
#include <limits>
#include <string>
#include <unordered_set>
#include <thread>

#include <fmt/ranges.h>

#if USE_JEMALLOC
#include <Common/Jemalloc.h>
#endif

namespace CurrentMetrics
{
    extern const Metric KeeperAliveConnections;
    extern const Metric KeeperOutstandingRequests;
}

namespace ProfileEvents
{
    extern const Event KeeperCommitWaitElapsedMicroseconds;
    extern const Event KeeperBatchMaxCount;
    extern const Event KeeperBatchMaxTotalSize;
    extern const Event KeeperRequestRejectedDueToSoftMemoryLimitCount;
    extern const Event KeeperStaleRequestsSkipped;
    extern const Event KeeperRaftPrecommitMicroseconds;
    extern const Event KeeperDispatcherInflightWaitMicroseconds;
}

namespace HistogramMetrics
{
    extern Metric & KeeperCurrentBatchSizeElements;
    extern Metric & KeeperCurrentBatchSizeBytes;
}

using namespace std::chrono_literals;

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsMilliseconds dead_session_check_period_ms;
    extern const CoordinationSettingsUInt64 max_request_queue_size;
    extern const CoordinationSettingsNonZeroUInt64 request_queue_num_shards;
    extern const CoordinationSettingsUInt64 max_requests_batch_bytes_size;
    extern const CoordinationSettingsUInt64 max_requests_batch_size;
    extern const CoordinationSettingsUInt64 max_batch_time_microseconds;
    extern const CoordinationSettingsMilliseconds operation_timeout_ms;
    extern const CoordinationSettingsBool quorum_reads;
    extern const CoordinationSettingsMilliseconds session_shutdown_timeout;
    extern const CoordinationSettingsMilliseconds sleep_before_leader_change_ms;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int SYSTEM_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int ABORTED;
}

namespace
{

bool checkIfRequestIncreaseMem(const Coordination::ZooKeeperRequestPtr & request)
{
    if (request->getOpNum() == Coordination::OpNum::Create
        || request->getOpNum() == Coordination::OpNum::Create2
        || request->getOpNum() == Coordination::OpNum::CreateIfNotExists
        || request->getOpNum() == Coordination::OpNum::Set)
    {
        return true;
    }
    if (request->getOpNum() == Coordination::OpNum::Multi)
    {
        Coordination::ZooKeeperMultiRequest & multi_req = dynamic_cast<Coordination::ZooKeeperMultiRequest &>(*request);
        Int64 memory_delta = 0;
        for (const auto & sub_req : multi_req.requests)
        {
            auto sub_zk_request = std::dynamic_pointer_cast<Coordination::ZooKeeperRequest>(sub_req);
            switch (sub_zk_request->getOpNum())
            {
                case Coordination::OpNum::Create:
                case Coordination::OpNum::Create2:
                case Coordination::OpNum::CreateIfNotExists: {
                    Coordination::ZooKeeperCreateRequest & create_req
                        = dynamic_cast<Coordination::ZooKeeperCreateRequest &>(*sub_zk_request);
                    memory_delta += create_req.bytesSize();
                    break;
                }
                case Coordination::OpNum::Set: {
                    Coordination::ZooKeeperSetRequest & set_req = dynamic_cast<Coordination::ZooKeeperSetRequest &>(*sub_zk_request);
                    memory_delta += set_req.bytesSize();
                    break;
                }
                case Coordination::OpNum::Remove:
                case Coordination::OpNum::TryRemove: {
                    Coordination::ZooKeeperRemoveRequest & remove_req
                        = dynamic_cast<Coordination::ZooKeeperRemoveRequest &>(*sub_zk_request);
                    memory_delta -= remove_req.bytesSize();
                    break;
                }
                case Coordination::OpNum::RemoveRecursive: {
                    Coordination::ZooKeeperRemoveRecursiveRequest & remove_req
                        = dynamic_cast<Coordination::ZooKeeperRemoveRecursiveRequest &>(*sub_zk_request);
                    memory_delta -= remove_req.bytesSize();
                    break;
                }
                default:
                    break;
            }
        }
        return memory_delta > 0;
    }

    return false;
}

}

KeeperDispatcher::KeeperDispatcher()
    : configuration_and_settings(std::make_shared<KeeperConfigurationAndSettings>())
    , log(getLogger("KeeperDispatcher"))
{}


void KeeperDispatcher::requestThread()
{
    DB::setThreadName(ThreadName::KEEPER_REQUEST);

    /// In-flight Raft append result and its corresponding batch.
    /// Kept alive so we can send errors to clients if the append fails.
    RaftAppendResult inflight_result = nullptr;
    KeeperRequestsForSessions inflight_batch;

    const auto & shutdown_called = keeper_context->isShutdownCalled();

    while (!shutdown_called)
    {
        const auto & coordination_settings = configuration_and_settings->coordination_settings;
        uint64_t max_batch_bytes_size = coordination_settings[CoordinationSetting::max_requests_batch_bytes_size];
        size_t max_batch_size = coordination_settings[CoordinationSetting::max_requests_batch_size];
        uint64_t max_batch_time_us = coordination_settings[CoordinationSetting::max_batch_time_microseconds];

        try
        {
            /// Poll requests from the queue, transition PendingRaft → InRaft,
            /// and extract lightweight KeeperRequestForSession structs.
            /// Accumulates entries within a bounded time window (max_batch_time_us)
            /// to build bigger batches — more entries per Raft append → more per fsync.
            /// Will not return until the previous in-flight batch is finalized.
            auto batch = requests_queue->waitAndPullRaftBatch(
                max_batch_size,
                max_batch_bytes_size,
                max_batch_time_us,
                [&]
                {
                    if (inflight_result && inflight_result->has_result())
                    {
                        Stopwatch w;
                        forceWaitAndProcessResult(inflight_result, inflight_batch, /*clear_requests_on_success=*/true);
                        ProfileEvents::increment(ProfileEvents::KeeperDispatcherInflightWaitMicroseconds, w.elapsedMicroseconds());
                        return false; /// nothing in flight anymore
                    }
                    return inflight_result != nullptr; /// still in flight
                });

            if (shutdown_called)
                break;

            /// waitAndPullRaftBatch guarantees non-empty batch (unless shutdown).
            nuraft::ptr<nuraft::buffer> result_buf = nullptr;

            if (!batch.entries.empty())
            {
                if (batch.entries.size() == max_batch_size)
                    ProfileEvents::increment(ProfileEvents::KeeperBatchMaxCount, 1);

                if (batch.bytes >= max_batch_bytes_size)
                    ProfileEvents::increment(ProfileEvents::KeeperBatchMaxTotalSize, 1);

                LOG_TEST(log, "Processing requests batch, size: {}, bytes: {}", batch.entries.size(), batch.bytes);

                HistogramMetrics::observe(HistogramMetrics::KeeperCurrentBatchSizeElements, batch.entries.size());
                HistogramMetrics::observe(HistogramMetrics::KeeperCurrentBatchSizeBytes, batch.bytes);

                Stopwatch raft_precommit_watch;
                auto result = server->putRequestBatch(batch.entries);
                ProfileEvents::increment(ProfileEvents::KeeperRaftPrecommitMicroseconds, raft_precommit_watch.elapsedMicroseconds());

                if (!result)
                {
                    failBatchSessions(batch.entries, Coordination::Error::ZCONNECTIONLOSS);
                    batch.entries.clear();
                }

                inflight_batch = std::move(batch.entries);
                inflight_result = result;
            }

            /// If Reconfig follows, we have to wait for the preceding batch
            /// to commit, then execute the reconfig synchronously.
            if (batch.has_reconfig)
            {
                {
                    Stopwatch watch;
                    SCOPE_EXIT(ProfileEvents::increment(ProfileEvents::KeeperCommitWaitElapsedMicroseconds, watch.elapsedMicroseconds()));
                    if (inflight_result)
                        result_buf = forceWaitAndProcessResult(
                            inflight_result, inflight_batch, /*clear_requests_on_success=*/false);

                    if (result_buf)
                    {
                        nuraft::buffer_serializer bs(result_buf);
                        auto log_idx = bs.get_u64();

                        if (!keeper_context->waitCommittedUpto(log_idx, coordination_settings[CoordinationSetting::operation_timeout_ms].totalMilliseconds()))
                            failBatchSessions(inflight_batch, Coordination::Error::ZOPERATIONTIMEOUT);

                        if (shutdown_called)
                            return;
                    }

                    inflight_batch.clear();
                }

                /// reconfig_session may be null if session expired during batch processing.
                /// Reconfig still executes (cluster-level op) but deferred read release is skipped.
                auto reconfig_session = session_registry->findSession(batch.reconfig.session_id);

                try
                {
                    server->getKeeperStateMachine()->reconfigure(batch.reconfig);
                }
                catch (...)
                {
                    /// Reconfig is a cluster-level operation — its failure should
                    /// not terminate an otherwise healthy client session. Deliver
                    /// an error response for just this request, then drain the
                    /// FIFO head so later requests on this session are not stuck.
                    addErrorResponses(
                        {batch.reconfig},
                        Coordination::Error::ZSYSTEMERROR);
                    if (reconfig_session)
                        reconfig_session->onRaftCommitted();
                    throw;
                }

                /// Pop the completed reconfig, deliver, and advance the queue.
                /// Must happen AFTER `reconfigure` returns because it holds the
                /// exclusive storage lock.
                if (reconfig_session)
                    reconfig_session->onRaftCommitted();
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void KeeperDispatcher::snapshotThread()
{
    DB::setThreadName(ThreadName::KEEPER_SNAPSHOT);

    const auto & shutdown_called = keeper_context->isShutdownCalled();
    CreateSnapshotTask task;
    while (snapshots_queue.pop(task))
    {
        try
        {
            auto snapshot_file_info = task.create_snapshot(std::move(task.snapshot), /*execute_only_cleanup=*/shutdown_called);

            if (!snapshot_file_info)
                continue;

            chassert(snapshot_file_info->disk != nullptr);
            if (isLeader())
                snapshot_s3.uploadSnapshot(snapshot_file_info);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

bool KeeperDispatcher::routeResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr parsed_request)
{
    /// SessionID responses are matched by temporary internal IDs because the
    /// client does not have a real session yet.
    if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::SessionID)
    {
        const auto & session_id_resp = dynamic_cast<const Coordination::ZooKeeperSessionIDResponse &>(*response);
        auto callback = session_registry->extractNewSessionCallback(session_id_resp.internal_id, session_id_resp.server_id, server->getServerID());
        if (!callback)
            return false;

        try
        {
            callback(response, std::move(parsed_request));
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to deliver SessionID response");
        }
        return true;
    }

    /// Normal response -- deliver directly to the session.
    auto session = session_registry->findSession(session_id);
    if (!session)
    {
        LOG_TRACE(log, "routeResponse: session {} not found for xid {}",
            session_id, response->xid);
        return false;
    }

    using DR = KeeperSession::DeliveryResult;
    DR result;

    if (response->xid == Coordination::WATCH_XID)
    {
        result = session->onWatchNotification(response);
    }
    else
    {
        LOG_TRACE(log, "routeResponse: session {} trying onRaftResponse xid {} op {}",
            session_id, response->xid, Coordination::opNumToString(response->getOpNum()));
        result = session->onRaftResponse(response->xid, response);
        if (result == DR::FifoMismatch)
        {
            /// XID mismatch is a bug — deliverDirect would leave the FIFO head
            /// unresolved, permanently stalling the session. Kill it instead.
            LOG_ERROR(log, "routeResponse: session {} FIFO head mismatch for xid {} — terminating session",
                session_id, response->xid);
            chassert(false && "FIFO head xid mismatch in routeResponse — this is a bug");
            terminateSession(session_id, Coordination::Error::ZSYSTEMERROR);
            return false;
        }
        if (result == DR::NotDelivered)
        {
            /// Session Closed or empty FIFO — deliverDirect is the correct fallback.
            /// Note: if this is a Close response, the result may be DeliveredAndDetach.
            /// The commit_callback may also call detachSession. This is safe —
            /// detachSession is idempotent (second call returns nullptr).
            result = session->deliverDirect(response);
        }
    }

    if (result == DR::NotDelivered)
    {
        /// Session was found but delivery failed (session Closed or no delivery mechanism).
        /// This can happen if the session was closed between findSession and delivery.
        LOG_WARNING(log, "Response delivery failed for session {} xid {} — session found but delivery rejected (likely closed)",
            session_id, response->xid);
        return false;
    }

    if (result == DR::DeliveredAndDetach)
        session_registry->detachSession(session_id);

    return true;
}

Coordination::Error KeeperDispatcher::putRequest(SessionRequestPtr keeper_req)
{
    auto session = session_registry->findSession(keeper_req->session_id);
    if (!session)
    {
        LOG_WARNING(log,
            "putRequest: session {} not found for xid {} op {}",
            keeper_req->session_id, keeper_req->request->xid,
            Coordination::opNumToString(keeper_req->request->getOpNum()));
        return Coordination::Error::ZSESSIONEXPIRED;
    }
    return putRequest(std::move(keeper_req), session);
}

Coordination::Error KeeperDispatcher::putRequest(SessionRequestPtr keeper_req, const KeeperSessionPtr & session)
{
    if (keeper_context->isShutdownCalled())
        return Coordination::Error::ZSESSIONEXPIRED;

    /// Backpressure: reject if the global pending request count exceeds the
    /// configured limit. Close bypasses (ephemeral cleanup must not be delayed).
    /// Local reads bypass (they never enter the Raft queue — rejecting them
    /// on Raft backlog is a false positive that blocks unrelated read traffic).
    bool is_close = keeper_req->request->getOpNum() == Coordination::OpNum::Close;
    bool is_local_read = !session_registry->quorumReads() && keeper_req->request->isReadRequest();
    const auto max_queue_size = configuration_and_settings->coordination_settings[CoordinationSetting::max_request_queue_size];
    if (!is_close && !is_local_read && max_queue_size > 0 && requests_queue->totalSize() >= max_queue_size)
    {
        LOG_WARNING(log,
            "Request rejected for session {} xid {} op {}: pending count {} >= limit {}",
            keeper_req->session_id,
            keeper_req->request->xid,
            Coordination::opNumToString(keeper_req->request->getOpNum()),
            requests_queue->totalSize(),
            static_cast<uint64_t>(max_queue_size));
        return Coordination::Error::ZCONNECTIONLOSS;
    }

    /// Memory soft limit: reject write-like requests before they enter
    /// the session FIFO. The request never touches active_requests or the
    /// subqueue, so no cleanup is needed on rejection.
    Int64 mem_soft_limit = keeper_context->getKeeperMemorySoftLimit();
    if (configuration_and_settings->standalone_keeper
        && isExceedingMemorySoftLimit()
        && checkIfRequestIncreaseMem(keeper_req->request))
    {
        ProfileEvents::increment(ProfileEvents::KeeperRequestRejectedDueToSoftMemoryLimitCount, 1);
        LOG_WARNING(log,
            "Request rejected due to memory soft limit {} for session {} op {}, allocated {}, RSS {}",
            ReadableSize(mem_soft_limit),
            keeper_req->session_id,
            Coordination::opNumToString(keeper_req->request->getOpNum()),
            ReadableSize(total_memory_tracker.get()),
            ReadableSize(total_memory_tracker.getRSS()));
        return Coordination::Error::ZOUTOFMEMORY;
    }

    auto add_result = session->addRequest(std::move(keeper_req));
    switch (add_result)
    {
        case KeeperSession::AddResult::Accepted:
            return Coordination::Error::ZOK;
        case KeeperSession::AddResult::SessionClosed:
            return Coordination::Error::ZSESSIONEXPIRED;
        case KeeperSession::AddResult::QueueFull:
            return Coordination::Error::ZCONNECTIONLOSS;
    }
    UNREACHABLE();
}

/// NOTE: This method is called directly by `KeeperOverDispatcher` (in-process keeper
/// access used by ClickHouse server for internal ZooKeeper operations such as metadata
/// reads and DDL coordination). That code path bypasses `putRequest` and session-level
/// classification entirely -- reads go directly to the state machine without per-session
/// barrier checks.
///
/// This is safe because `KeeperOverDispatcher` is used for server-internal operations
/// that don't require per-session ordering guarantees with client writes.
///
/// Eventually, `KeeperOverDispatcher` should be migrated to use `putRequest` so that
/// all read paths go through session classification and get proper per-session barrier
/// semantics. Until then, this method must remain as a public API.
bool KeeperDispatcher::putLocalReadRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id)
{
    auto session = session_registry->findSession(session_id);
    if (!session || !session->canAcceptRequests())
    {
        ProfileEvents::increment(ProfileEvents::KeeperStaleRequestsSkipped);
        return false;
    }

    if (keeper_context->isShutdownCalled())
        return false;

    using namespace std::chrono;
    auto keeper_req = std::make_shared<SessionRequest>();
    keeper_req->session_id = session_id;
    keeper_req->request = request;
    keeper_req->cached_bytes_size = request->bytesSize();
    keeper_req->mode = KeeperRequestMode::NonQuorum;
    keeper_req->setState(RequestState::ExecutingLocal);
    keeper_req->time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    KeeperRequestForSession req_for_session;
    req_for_session.session_id = session_id;
    req_for_session.request = request;

    auto response = server->putLocalReadRequest(req_for_session);

    if (response)
    {
        keeper_req->response = response;
        keeper_req->setState(RequestState::Completed);
        session->deliverDirect(response);
    }

    return true;
}

void KeeperDispatcher::initialize(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper, bool start_async, const MultiVersion<Macros>::Version & macros)
{
    LOG_DEBUG(log, "Initializing storage dispatcher");

    configuration_and_settings = KeeperConfigurationAndSettings::loadFromConfig(config, standalone_keeper);
    keeper_context = std::make_shared<KeeperContext>(standalone_keeper, std::make_shared<CoordinationSettings>(configuration_and_settings->coordination_settings));

    keeper_context->initialize(config, this);

    /// Build the sharded request queue. One subqueue per hardware thread is a
    /// reasonable default; the session-to-subqueue mapping is deterministic so
    /// the number of subqueues does not need to match the number of sessions.
    const auto max_queue_size = configuration_and_settings->coordination_settings[CoordinationSetting::max_request_queue_size];
    const auto num_shards = configuration_and_settings->coordination_settings[CoordinationSetting::request_queue_num_shards];
    requests_queue = std::make_unique<KeeperRequestsQueue>(
        /*num_subqueues=*/num_shards,
        /*max_queue_size=*/max_queue_size);

    session_registry = std::make_unique<KeeperSessionRegistry>(
        keeper_context,
        *requests_queue,
        [this](std::span<SessionRequestPtr> batch) { dispatchLocalReads(batch); });

    /// System handle for session-less requests (SessionID, dead-session Close).
    system_subqueue = requests_queue->getSubqueue(0);

    snapshot_thread = ThreadFromGlobalPool([this] { snapshotThread(); });

    snapshot_s3.startup(config, macros);

    server = std::make_unique<KeeperServer>(
        configuration_and_settings,
        config,
        [this](int64_t id, Coordination::ZooKeeperResponsePtr resp, Coordination::ZooKeeperRequestPtr req) -> bool
        {
            return routeResponse(id, std::move(resp), std::move(req));
        },
        snapshots_queue,
        keeper_context,
        snapshot_s3,
        [this](uint64_t idx, const KeeperRequestForSession & req) { onRaftCommit(idx, req); });

    try
    {
        LOG_DEBUG(log, "Waiting server to initialize");
        server->startup(config, configuration_and_settings->enable_ipv6);
        LOG_DEBUG(log, "Server initialized, waiting for quorum");

        if (!start_async)
        {
            server->waitInit();
            LOG_DEBUG(log, "Quorum initialized");
        }
        else
        {
            LOG_INFO(log, "Starting Keeper asynchronously, server will accept connections to Keeper when it will be ready");
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    /// Start threads after keeper server is constructed and initialized.
    request_thread = ThreadFromGlobalPool([this] { requestThread(); });
    session_cleaner_thread = ThreadFromGlobalPool([this] { sessionCleanerTask(); });

    update_configuration_thread = reconfigEnabled()
        ? ThreadFromGlobalPool([this] { clusterUpdateThread(); })
        : ThreadFromGlobalPool([this] { clusterUpdateWithReconfigDisabledThread(); });

    LOG_DEBUG(log, "Dispatcher initialized");
}

void KeeperDispatcher::shutdown()
{
    try
    {
        {
            if (!keeper_context || !keeper_context->setShutdownCalled())
                return;

            LOG_DEBUG(log, "Shutting down storage dispatcher");

            if (session_cleaner_thread.joinable())
                session_cleaner_thread.join();

            if (requests_queue)
                requests_queue->signalShutdown();

            if (request_thread.joinable())
                request_thread.join();

            snapshots_queue.finish();
            if (snapshot_thread.joinable())
                snapshot_thread.join();

            cluster_update_queue.finish();
            if (update_configuration_thread.joinable())
                update_configuration_thread.join();
        }

        KeeperRequestsForSessions close_requests;
        auto sessions = session_registry->shutdown();

        if (server && hasLeader())
        {
            close_requests.reserve(sessions.size());
            for (const auto & session : sessions)
            {
                auto request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
                request->xid = Coordination::CLOSE_XID;
                using namespace std::chrono;
                KeeperRequestForSession request_info;
                request_info.session_id = session->getSessionID();
                request_info.time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                request_info.request = std::move(request);

                close_requests.push_back(std::move(request_info));
            }
        }

        /// Deliver ZSESSIONEXPIRED errors + nullptr sentinel so clients
        /// disconnect promptly instead of waiting for socket read timeout.
        for (const auto & session : sessions)
            session->finalizeWithErrors(Coordination::Error::ZSESSIONEXPIRED);

        if (server && !close_requests.empty())
        {
            // if there is no leader, there is no reason to do CLOSE because it's a write request
            if (hasLeader())
            {
                LOG_INFO(log, "Trying to close {} session(s)", close_requests.size());
                const auto raft_result = server->putRequestBatch(close_requests);
                auto sessions_closing_done_promise = std::make_shared<std::promise<void>>();
                auto sessions_closing_done = sessions_closing_done_promise->get_future();
                raft_result->when_ready([my_sessions_closing_done_promise = std::move(sessions_closing_done_promise)](
                                            nuraft::cmd_result<nuraft::ptr<nuraft::buffer>> & /*result*/,
                                            nuraft::ptr<std::exception> & /*exception*/) { my_sessions_closing_done_promise->set_value(); });

                auto session_shutdown_timeout = configuration_and_settings->coordination_settings[CoordinationSetting::session_shutdown_timeout].totalMilliseconds();
                if (sessions_closing_done.wait_for(std::chrono::milliseconds(session_shutdown_timeout)) != std::future_status::ready)
                    LOG_WARNING(
                        log,
                        "Failed to close sessions in {}ms. If they are not closed, they will be closed after session timeout.",
                        session_shutdown_timeout);
            }
            else
            {
                LOG_INFO(log, "Sessions cannot be closed during shutdown because there is no active leader");
            }
        }

        if (server)
            server->shutdown();

        snapshot_s3.shutdown();

        CurrentMetrics::set(CurrentMetrics::KeeperAliveConnections, 0);

    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    LOG_DEBUG(log, "Dispatcher shut down");
}

void KeeperDispatcher::forceRecovery()
{
    server->forceRecovery();
}

KeeperDispatcher::~KeeperDispatcher()
{
    shutdown();
}

KeeperSessionPtr KeeperDispatcher::registerSession(int64_t session_id, KeeperSession::ResponseCallback callback)
{
    return session_registry->registerSession(session_id, std::move(callback));
}

void KeeperDispatcher::sessionCleanerTask()
{
    const auto & shutdown_called = keeper_context->isShutdownCalled();
    while (true)
    {
        if (shutdown_called)
            return;

        try
        {
            /// Only leader node must check dead sessions
            if (server->checkInit() && isLeader())
            {
                auto dead_sessions = server->getDeadSessions();

                for (int64_t dead_session : dead_sessions)
                {
                    LOG_INFO(log, "Found dead session {}, will try to close it", dead_session);

                    /// Close session == send close request to raft server
                    auto request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
                    request->xid = Coordination::CLOSE_XID;

                    ZooKeeperOpentelemetrySpans::maybeInitialize(request->spans.dispatcher_requests_queue, request->tracing_context);

                    using namespace std::chrono;
                    auto request_info = std::make_shared<SessionRequest>();
                    request_info->session_id = dead_session;
                    request_info->time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                    request_info->request = std::move(request);
                    request_info->setState(RequestState::PendingRaft);
                    /// Detach session from registry before pushing Close to the queue.
                    /// Close requests are exempt from stale filtering, so the
                    /// Close will still pass through RAFT for ephemeral cleanup.
                    terminateSession(dead_session);

                    if (!system_subqueue->push(std::move(request_info), /*bypass_limit=*/true))
                    {
                        LOG_WARNING(log, "Dead session close request for session {} dropped — queue full", dead_session);
                    }
                    else
                    {
                        LOG_INFO(log, "Dead session close request pushed for session {}", dead_session);
                    }
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        auto time_to_sleep = configuration_and_settings->coordination_settings[CoordinationSetting::dead_session_check_period_ms].totalMilliseconds();
        std::this_thread::sleep_for(std::chrono::milliseconds(time_to_sleep));
    }
}

void KeeperDispatcher::terminateSession(int64_t session_id, Coordination::Error error)
{
    /// shutdown() method will cleanup sessions if needed
    if (keeper_context->isShutdownCalled())
        return;

    /// Find the session; keep it alive via shared_ptr while we finalize and deliver.
    /// Returns nullptr if session was already finished by another path.
    ///
    /// Note: between finalizeWithErrors (sets Closed) and detachSession below,
    /// a concurrent onRaftCommit could find the session via findSession and see
    /// state == Closed. All paths check state and return early, so this is safe.
    auto session = session_registry->findSession(session_id);
    if (!session)
        return;

    LOG_INFO(log, "Terminating session {} ({})", session_id, Coordination::errorMessage(error));
    /// Deliver error responses for all in-flight requests before closing.
    /// Atomically collects active_requests, creates error responses, clears
    /// callback, and sets state to Closed. Raft commit callbacks arriving
    /// after this will see Closed state and return early.
    session->finalizeWithErrors(error);

    LOG_INFO(log, "Detaching session {} from registry", session_id);
    /// Detach AFTER delivering the Close response so that in-flight Raft commit
    /// callbacks can still find the session during the delivery window.
    session_registry->detachSession(session_id);
}

void KeeperDispatcher::dispatchLocalReads(std::span<SessionRequestPtr> batch)
{
    if (server->isLeaderAlive())
    {
        /// Build lightweight request structs for the server layer.
        std::vector<KeeperRequestForSession> read_requests(batch.size());
        for (size_t i = 0; i < batch.size(); ++i)
        {
            read_requests[i].session_id = batch[i]->session_id;
            read_requests[i].request = batch[i]->request;
        }

        if (batch.size() == 1)
        {
            batch[0]->response = server->putLocalReadRequest(read_requests[0]);
        }
        else
        {
            auto responses = server->putLocalReadRequests(read_requests);
            for (size_t i = 0; i < batch.size(); ++i)
                batch[i]->response = std::move(responses[i]);
        }
    }
    else
    {
        for (auto & req : batch)
        {
            auto err = req->request->makeResponse();
            err->xid = req->request->xid;
            err->error = Coordination::Error::ZCONNECTIONLOSS;
            err->enqueue_ts = std::chrono::steady_clock::now();
            req->response = err;
        }
    }
}

void KeeperDispatcher::onRaftCommit(uint64_t log_idx, const KeeperRequestForSession & request_for_session)
{
    LOG_TRACE(log, "commit_callback: session {} xid {} op {} log_idx {}",
        request_for_session.session_id,
        request_for_session.request->xid,
        Coordination::opNumToString(request_for_session.request->getOpNum()),
        log_idx);

    if (auto session = session_registry->findSession(request_for_session.session_id))
    {
        bool is_close = (request_for_session.request->getOpNum() == Coordination::OpNum::Close);

        if (is_close)
        {
            /// Two steps, both needed:
            /// 1. `onRaftCommitted` → `advanceQueue` → `popResponseReadyNoLock`
            ///    delivers the Close response to the client (sets Closed).
            /// 2. `terminateSession` → `finalizeWithErrors` (no-op if already
            ///    Closed) → `detachSession` removes from registry.
            /// For cross-node session expiry Close, the FIFO may contain
            /// stale entries — `terminateSession` clears them via finalizeWithErrors.
            session->onRaftCommitted();
            terminateSession(request_for_session.session_id);
        }
        else
        {
            session->onRaftCommitted();
        }
    }
    else
    {
        LOG_TRACE(log, "commit_callback: session {} not found locally (follower?)",
            request_for_session.session_id);
    }
}

void KeeperDispatcher::addErrorResponses(const KeeperRequestsForSessions & requests_for_sessions, Coordination::Error error)
{
    for (const auto & request_for_session : requests_for_sessions)
    {
        auto response = request_for_session.request->makeResponse();
        response->xid = request_for_session.request->xid;
        response->zxid = 0;
        response->error = error;
        response->enqueue_ts = std::chrono::steady_clock::now();
        if (!routeResponse(request_for_session.session_id, response))
            LOG_WARNING(log,
                "Could not deliver error response xid {} error {} -- session not found",
                response->xid,
                error);
    }
}

void KeeperDispatcher::failBatchSessions(const KeeperRequestsForSessions & batch, Coordination::Error error)
{
    /// Context: this is called when a Raft batch append fails transiently (leader change,
    /// slow fsync, network hiccup, quorum instability). The error is typically
    /// `ZCONNECTIONLOSS` or `ZOPERATIONTIMEOUT` — NOT `ZSESSIONEXPIRED`.
    ///
    /// Error semantics (ZooKeeper protocol):
    ///   `ZCONNECTIONLOSS` / `ZOPERATIONTIMEOUT` are retryable operation-level errors.
    ///   They signal ambiguous outcome: the operation may or may not have committed.
    ///   Upstream ZooKeeper docs, the Java/C client examples, and `KeeperException` all
    ///   treat these as fundamentally different from `ZSESSIONEXPIRED`, which is a terminal
    ///   session-level event that destroys ephemerals, drops watches, and invalidates
    ///   the session.
    ///
    /// ClickHouse ZooKeeper client behavior:
    ///   `ZooKeeperImpl` treats `ZCONNECTIONLOSS` and `ZOPERATIONTIMEOUT` as hardware
    ///   errors (`Coordination::isHardwareError`). On such errors the client abandons
    ///   the current connection object and reconnects with a new session. This is
    ///   connection-level recovery, not server-side session expiration. The client
    ///   does not expect these errors to imply that the server destroyed the logical
    ///   session.
    ///
    /// Why we expire the session immediately anyway:
    ///   ClickHouse Keeper does not support session restore (`KeeperTCPHandler.cpp` —
    ///   `previous_session_id` is always zero). The client always reconnects with a
    ///   new session_id. So keeping the local session attachment alive after a transient
    ///   failure would serve no purpose: the client cannot reattach to it. The session
    ///   is effectively dead locally the moment we clear the callback and active_requests.
    ///
    ///   Storage-side session expiry (ephemerals, watches) is independent from the
    ///   local session registry. It is driven by `session_expiry_queue` /
    ///   `getDeadSessions` in `sessionCleanerTask`. Even after `terminateSession` detaches
    ///   the local session, the storage-side session stays alive until its timeout fires
    ///   and a Close request is committed through Raft.
    ///
    /// What we do:
    ///   - Deliver per-request `ZCONNECTIONLOSS` / `ZOPERATIONTIMEOUT` errors for all
    ///     in-flight requests via `finalizeWithErrors`
    ///   - `finalizeWithErrors` also pushes a nullptr sentinel that causes the TCP
    ///     handler to close the socket immediately after sending the error responses
    ///   - Detach the local session from the registry via `terminateSession`
    ///   - Storage-side cleanup happens later through the normal timeout / Close path
    ///
    /// The per-request error code is the caller's `error` (typically `ZCONNECTIONLOSS`),
    /// NOT `ZSESSIONEXPIRED`. This is important: it tells the client "the operation may
    /// have committed, check and retry" — not "your session is gone, rebuild everything."

    LOG_WARNING(log,
        "Failing batch of {} requests with error {}",
        batch.size(), Coordination::errorMessage(error));

    std::unordered_set<int64_t> finished_sessions;
    for (const auto & req : batch)
    {
        if (req.request->getOpNum() == Coordination::OpNum::SessionID)
        {
            addErrorResponses({req}, error);
            continue;
        }

        /// Deduplicate — one batch may contain many requests from the same session.
        /// `terminateSession` is idempotent (second call returns early), but dedup
        /// avoids repeated lookups and log noise.
        if (!finished_sessions.emplace(req.session_id).second)
            continue;

        terminateSession(req.session_id, error);
    }
}

nuraft::ptr<nuraft::buffer> KeeperDispatcher::forceWaitAndProcessResult(
    RaftAppendResult & result, KeeperRequestsForSessions & requests_for_sessions, bool clear_requests_on_success)
{
    if (!result->has_result())
    {
        auto start = std::chrono::steady_clock::now();
        result->get();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        if (elapsed_ms >= 5000)
            LOG_WARNING(log, "Raft append result took {}ms", elapsed_ms);
    }

    /// If we get some errors, then send them to clients
    if (!result->get_accepted() || result->get_result_code() == nuraft::cmd_result_code::TIMEOUT)
        failBatchSessions(requests_for_sessions, Coordination::Error::ZOPERATIONTIMEOUT);
    else if (result->get_result_code() != nuraft::cmd_result_code::OK)
        failBatchSessions(requests_for_sessions, Coordination::Error::ZCONNECTIONLOSS);

    auto result_buf = result->get();

    result = nullptr;

    /// On error: `result_buf` is null (failBatchSessions already delivered errors), always clear.
    /// On success: clear only if `clear_requests_on_success` is true.
    /// When false (Reconfig follows), `requests_for_sessions` is kept so the
    /// caller can wait for the log index from `result_buf` before processing Reconfig.
    if (!result_buf || clear_requests_on_success)
        requests_for_sessions.clear();

    return result_buf;
}

int64_t KeeperDispatcher::getSessionID(int64_t session_timeout_ms)
{
    /// New session id allocation is a special request, because we cannot process it in normal
    /// way: get request -> put to raft -> set response for registered callback.
    auto request_info = std::make_shared<SessionRequest>();
    std::shared_ptr<Coordination::ZooKeeperSessionIDRequest> request = std::make_shared<Coordination::ZooKeeperSessionIDRequest>();
    /// Internal session id. It's a temporary number which is unique for each client on this server
    /// but can be same on different servers.
    request->internal_id = session_registry->nextInternalSessionId();
    request->session_timeout_ms = session_timeout_ms;
    request->server_id = server->getServerID();

    request_info->request = request;
    request_info->setState(RequestState::PendingRaft);
    using namespace std::chrono;
    request_info->time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    request_info->session_id = -1;

    auto promise = std::make_shared<std::promise<int64_t>>();
    auto future = promise->get_future();

    session_registry->registerNewSessionCallback(
        request->internal_id,
        [promise, internal_id = request->internal_id](
              const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr /*request*/)
        {
            if (response->getOpNum() != Coordination::OpNum::SessionID)
            {
                promise->set_exception(std::make_exception_ptr(Exception(
                    ErrorCodes::LOGICAL_ERROR, "Incorrect response of type {} instead of SessionID response", response->getOpNum())));
                return;
            }

            auto session_id_response = dynamic_cast<const Coordination::ZooKeeperSessionIDResponse &>(*response);
            if (session_id_response.internal_id != internal_id)
            {
                promise->set_exception(std::make_exception_ptr(Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Incorrect response with internal id {} instead of {}",
                    session_id_response.internal_id,
                    internal_id)));
                return;
            }

            if (response->error != Coordination::Error::ZOK)
            {
                promise->set_exception(
                    std::make_exception_ptr(zkutil::KeeperException::fromMessage(response->error, "SessionID request failed with error")));
                return;
            }

            promise->set_value(session_id_response.session_id);
        });

    ZooKeeperOpentelemetrySpans::maybeInitialize(request->spans.dispatcher_requests_queue, request->tracing_context);

    /// Push new session request to the system queue. Bounded by max_request_queue_size
    /// to prevent unbounded growth under connection storms.
    const auto max_queue_size = configuration_and_settings->coordination_settings[CoordinationSetting::max_request_queue_size];
    bool pushed = true;
    if (max_queue_size > 0 && requests_queue->totalSize() >= max_queue_size)
    {
        LOG_WARNING(log,
            "Session allocation rejected: pending count {} >= limit {}",
            requests_queue->totalSize(), static_cast<uint64_t>(max_queue_size));
        pushed = false;
    }

    if (pushed)
    {
        /// Count system requests in the registry pending count so that
        /// `requestThread` can decrement it uniformly for all `PendingRaft` requests.
        pushed = system_subqueue->push(std::move(request_info));
    }

    if (!pushed)
    {
        /// Clean up the registered callback — the request was never enqueued,
        /// so no response will arrive to extract it.
        /// Pass server_id as both arguments so the equality check passes —
        /// we want to unconditionally extract our own callback on cleanup.
        session_registry->extractNewSessionCallback(request->internal_id, request->server_id, request->server_id);
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Cannot allocate session: system queue is full");
    }

    if (future.wait_for(std::chrono::milliseconds(session_timeout_ms)) != std::future_status::ready)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Cannot receive session id within session timeout");

    /// Forcefully wait for request execution because we cannot process any other
    /// requests for this client until it get new session id.
    return future.get();
}

void KeeperDispatcher::clusterUpdateWithReconfigDisabledThread()
{
    const auto & shutdown_called = keeper_context->isShutdownCalled();
    while (!shutdown_called)
    {
        try
        {
            if (!server->checkInit())
            {
                LOG_INFO(log, "Server still not initialized, will not apply configuration until initialization finished");
                std::this_thread::sleep_for(5000ms);
                continue;
            }

            if (server->isRecovering())
            {
                LOG_INFO(log, "Server is recovering, will not apply configuration until recovery is finished");
                std::this_thread::sleep_for(5000ms);
                continue;
            }

            ClusterUpdateAction action;
            if (!cluster_update_queue.pop(action))
                break;

            /// We must wait this update from leader or apply it ourself (if we are leader)
            bool done = false;
            while (!done)
            {
                if (server->isRecovering())
                    break;

                if (shutdown_called)
                    return;

                if (isLeader())
                {
                    server->applyConfigUpdateWithReconfigDisabled(action);
                    done = true;
                }
                else if (done = server->waitForConfigUpdateWithReconfigDisabled(action); !done)
                    LOG_INFO(log,
                        "Cannot wait for configuration update, maybe we became leader "
                        "or maybe update is invalid, will try to wait one more time");
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void KeeperDispatcher::clusterUpdateThread()
{
    using enum KeeperServer::ConfigUpdateState;
    bool last_command_was_leader_change = false;
    const auto & shutdown_called = keeper_context->isShutdownCalled();
    while (!shutdown_called)
    {
        ClusterUpdateAction action;
        if (!cluster_update_queue.pop(action))
            return;

        if (const auto res = server->applyConfigUpdate(action, last_command_was_leader_change); res == Accepted)
            LOG_DEBUG(log, "Processing config update {}: accepted", action);
        else
        {
            last_command_was_leader_change = res == WaitBeforeChangingLeader;

            (void)cluster_update_queue.pushFront(action);
            LOG_DEBUG(log, "Processing config update {}: declined, backoff", action);

            std::this_thread::sleep_for(last_command_was_leader_change
                ? std::chrono::milliseconds(configuration_and_settings->coordination_settings[CoordinationSetting::sleep_before_leader_change_ms].totalMilliseconds())
                : 50ms);
        }
    }
}

void KeeperDispatcher::pushClusterUpdates(ClusterUpdateActions && actions)
{
    if (keeper_context->isShutdownCalled()) return;
    for (auto && action : actions)
    {
        LOG_DEBUG(log, "Processing config update {}: pushing", action);
        if (!cluster_update_queue.push(std::move(action)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot push configuration update");
    }
}

bool KeeperDispatcher::reconfigEnabled() const
{
    return server->reconfigEnabled();
}

bool KeeperDispatcher::isServerActive() const
{
    return checkInit() && hasLeader() && !server->isRecovering();
}

void KeeperDispatcher::updateConfiguration(const Poco::Util::AbstractConfiguration & config, const MultiVersion<Macros>::Version & macros)
{
    auto diff = server->getRaftConfigurationDiff(config);

    if (diff.empty())
        LOG_TRACE(log, "Configuration update triggered, but nothing changed for Raft");
    else if (reconfigEnabled())
        LOG_WARNING(log,
            "Raft configuration changed, but keeper_server.enable_reconfiguration is on. "
            "This update will be ignored. Use \"reconfig\" instead");
    else if (diff.size() > 1)
        LOG_WARNING(log,
            "Configuration changed for more than one server ({}) from cluster, "
            "it's strictly not recommended", diff.size());
    else
        LOG_DEBUG(log, "Configuration change size ({})", diff.size());

    if (!reconfigEnabled())
        for (auto & change : diff)
            if (!cluster_update_queue.push(change))
                throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push configuration update to queue");

    snapshot_s3.updateS3Configuration(config, macros);

    keeper_context->updateKeeperMemorySoftLimit(config);
}

void KeeperDispatcher::updateKeeperStatLatency(uint64_t process_time_ms, uint64_t subrequests)
{
    keeper_stats.updateLatency(process_time_ms, subrequests);
}

static uint64_t getTotalSize(const DiskPtr & disk, const std::string & path = "")
{
    checkStackSize();

    uint64_t size = 0;
    for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
    {
        if (disk->existsFile(it->path()))
            size += disk->getFileSize(it->path());
        else
            size += getTotalSize(disk, it->path());
    }

    return size;
}

uint64_t KeeperDispatcher::getLogDirSize() const
{
    auto log_disk = keeper_context->getLogDisk();
    auto size = getTotalSize(log_disk);

    auto latest_log_disk = keeper_context->getLatestLogDisk();
    if (log_disk != latest_log_disk)
        size += getTotalSize(latest_log_disk);

    return size;
}

uint64_t KeeperDispatcher::getSnapDirSize() const
{
    return getTotalSize(keeper_context->getSnapshotDisk());
}

Keeper4LWInfo KeeperDispatcher::getKeeper4LWInfo() const
{
    Keeper4LWInfo result = server->getPartiallyFilled4LWInfo();
    result.outstanding_requests_count
        = CurrentMetrics::values[CurrentMetrics::KeeperOutstandingRequests].load(std::memory_order_relaxed);
    result.alive_connections_count
        = CurrentMetrics::values[CurrentMetrics::KeeperAliveConnections].load(std::memory_order_relaxed);
    return result;
}


void KeeperDispatcher::executeClusterUpdateActionAndWaitConfigChange(const ClusterUpdateAction & action, KeeperDispatcher::ConfigCheckCallback check_callback, size_t max_action_wait_time_ms, int64_t retry_count)
{
    for (int64_t attempt = 0; attempt <= retry_count; ++attempt)
    {
        if (check_callback(server.get()))
        {
            LOG_DEBUG(log, "Configuration update {} already applied, nothing to do", action);
            return;
        }

        pushClusterUpdates({action});
        Stopwatch watch;
        LOG_DEBUG(log, "Waiting for configuration update {} to be applied, will wait for {} ms", action, max_action_wait_time_ms);
        while (watch.elapsedMilliseconds() < max_action_wait_time_ms)
        {
            if (keeper_context->isShutdownCalled())
                throw Exception(ErrorCodes::ABORTED, "Shutdown called, aborting configuration update");

            if (check_callback(server.get()))
            {
                LOG_DEBUG(log, "Configuration update {} applied successfully", action);
                return;
            }

            std::this_thread::sleep_for(1000ms);
        }
        LOG_INFO(log, "Timeout exceeded waiting for configuration update {} to be applied, attempt {}/{}", action, attempt + 1, retry_count + 1);
    }
    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded (with retries count {}) waiting for configuration update {} to happen", retry_count, action);
}

void KeeperDispatcher::checkReconfigCommandPreconditions(Poco::JSON::Object::Ptr reconfig_command)
{
    if (reconfig_command->has("preconditions"))
    {
        auto latest_config = server->getKeeperStateMachine()->getClusterConfig();
        auto preconditions = reconfig_command->getObject("preconditions");
        if (preconditions->has("members"))
        {
            std::unordered_set<int32_t> nodes;
            auto members = preconditions->getArray("members");
            for (unsigned int i = 0; i < members->size(); ++i)
                nodes.insert(members->getElement<int32_t>(i));

            auto servers_in_config = latest_config->get_servers();
            if (nodes.size() != servers_in_config.size())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Precondition failed: expected members size {}, actual {}",
                    nodes.size(),
                    servers_in_config.size());

            for (const auto & participant : servers_in_config)
            {
                if (!nodes.contains(participant->get_id()))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Precondition failed: member with id {} not found in precondition set {}",
                        participant->get_id(), fmt::join(nodes, ", "));
            }
        }

        if (preconditions->has("leaders"))
        {
            std::unordered_set<int32_t> leaders;
            auto leaders_array = preconditions->getArray("leaders");
            for (unsigned int i = 0; i < leaders_array->size(); ++i)
                leaders.insert(leaders_array->getElement<int32_t>(i));

            if (!server->isLeaderAlive())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Precondition failed: expected leader id {} but there is no leader currently",
                    fmt::join(leaders, ", "));

            if (!leaders.contains(static_cast<int32_t>(server->getLeaderID())))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Precondition failed: expected leader id {} does not match actual leader id {}",
                    fmt::join(leaders, ", "),
                    server->getLeaderID());
        }
    }

}
void KeeperDispatcher::checkReconfigCommandActions(Poco::JSON::Object::Ptr reconfig_command)
{
    if (!reconfig_command->has("actions"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Reconfigure command must have 'actions' field");

    auto actions = reconfig_command->getArray("actions");
    /// sever_id -> leader
    std::unordered_map<int32_t, bool> state_model;

    auto config = server->getKeeperStateMachine()->getClusterConfig();
    auto leader_id = server->getLeaderID();
    for (const auto & srv : config->get_servers())
    {
        int32_t server_id = srv->get_id();
        state_model[server_id] = server_id == leader_id;
    }
    auto is_leader = [&state_model](int32_t server_id) -> bool
    {
        auto it = state_model.find(server_id);
        if (it == state_model.end())
            return false;
        return it->second;
    };

    for (const auto & action_json : *actions)
    {
        const auto & action_obj = action_json.extract<Poco::JSON::Object::Ptr>();
        if (action_obj->has("remove_members"))
        {
            auto remove_members = action_obj->getArray("remove_members");
            for (const auto & member_id_json : *remove_members)
            {
                int32_t member_id = member_id_json.convert<int32_t>();
                if (member_id == server->getServerID())
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Reconfigure command cannot remove current server id {} from cluster because it would make other servers ignore all the commands from this one",
                        member_id);
                }
                if (is_leader(member_id))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Reconfigure command cannot remove leader server id {} from cluster, transfer leadership first and than remove it",
                        member_id);
                }
                state_model.erase(member_id);
            }
        }
        else if (action_obj->has("add_members"))
        {
            auto add_members = action_obj->getArray("add_members");
            for (const auto & member_id_json : *add_members)
            {
                const auto & member_obj = member_id_json.extract<Poco::JSON::Object::Ptr>();
                int32_t member_id = member_obj->getValue<int32_t>("id");
                if (state_model.contains(member_id))
                {
                    LOG_INFO(log,
                        "Reconfigure command cannot add server id {} to cluster because it's already present, will do nothing",
                        member_id);
                }
                state_model[member_id] = false;
            }
        }
        else if (action_obj->has("transfer_leadership"))
        {
            bool found = false;
            std::vector<int32_t> leader_ids;
            const auto & leader_ids_array = action_obj->getArray("transfer_leadership");
            if (leader_ids_array->size() == 0)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Reconfigure command 'transfer_leadership' action must have non-empty list of server ids");
            }

            /// Leader is going to be transferred, reset state model
            for (auto & [server_id, _] : state_model)
            {
                state_model[server_id] = false;
            }

            for (const auto & leader_id_json : *leader_ids_array)
            {
                int32_t current_leader_id = leader_id_json.convert<int32_t>();
                leader_ids.push_back(current_leader_id);
                if (state_model.contains(current_leader_id))
                {
                    found = true;
                    state_model[current_leader_id] = true;
                }
            }

            if (!found)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Reconfigure command cannot transfer leadership to any of server ids [{}] because they are not present in cluster",
                    fmt::join(leader_ids, ", "));
            }
        }
        else if (action_obj->has("set_priority"))
        {
            const auto & set_priority_obj = action_obj->getArray("set_priority");
            for (const auto & priority_change: *set_priority_obj)
            {
                const auto & priority_change_obj = priority_change.extract<Poco::JSON::Object::Ptr>();
                int member_id = priority_change_obj->getValue<int>("id");
                int priority = priority_change_obj->getValue<int>("priority");
                if (is_leader(member_id) && priority == 0)
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Reconfigure command cannot set priority 0 for server {}, because it can be potentially a leader", member_id);
                }
            }
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown action in reconfigure command");
        }
    }
}

Poco::JSON::Object::Ptr KeeperDispatcher::reconfigureClusterFromReconfigureCommand(Poco::JSON::Object::Ptr reconfig_command)
try
{
    if (!server->isLeaderAlive())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot reconfigure cluster because there is no active leader");

    checkReconfigCommandPreconditions(reconfig_command);
    checkReconfigCommandActions(reconfig_command);

    LOG_DEBUG(log, "Preconditions passed, executing reconfiguration");
    size_t max_action_wait_time_ms = reconfig_command->has("max_action_wait_time_ms")
        ? reconfig_command->getValue<size_t>("max_action_wait_time_ms")
        : 60000;

    size_t max_total_wait_time_ms = reconfig_command->has("max_total_wait_time_ms")
        ? reconfig_command->getValue<size_t>("max_total_wait_time_ms")
        : 300000;

    auto actions = reconfig_command->getArray("actions");
    Stopwatch total_watch;
    for (const auto & action_json : *actions)
    {

        UInt64 time_left_for_action = max_action_wait_time_ms;
        UInt64 time_spent = total_watch.elapsedMilliseconds();
        UInt64 time_left_total = max_total_wait_time_ms > time_spent ? max_total_wait_time_ms - time_spent : 0;
        UInt64 time_left = std::min(time_left_for_action, time_left_total);

        const auto & action_obj = action_json.extract<Poco::JSON::Object::Ptr>();

        int64_t retry_count = 1;
        if (action_obj->has("retry"))
            retry_count = action_obj->getValue<int64_t>("retry");
        /// Ensure at least one attempt even if an invalid (negative) value is configured.
        retry_count = std::max(int64_t(0), retry_count);

        if (action_obj->has("remove_members"))
        {
            auto remove_members = action_obj->getArray("remove_members");
            for (const auto & member_id_json : *remove_members)
            {
                int32_t member_id = member_id_json.convert<int32_t>();
                RemoveRaftServer remove_action{member_id};
                auto remove_callback = [this, member_id](KeeperServer * server_) -> bool
                {
                    auto config = server_->getKeeperStateMachine()->getClusterConfig();

                    if (config->get_server(member_id) == nullptr)
                    {
                        LOG_INFO(log, "Skip removing server id {} from cluster because it's not present in current configuration: {}",
                            member_id, serializeClusterConfig(config));
                        return true;
                    }
                    else
                    {
                        LOG_INFO(
                            log, "Waiting for removing server id {} from cluster configuration: {}",
                            member_id, serializeClusterConfig(config));
                        return false;
                    }
                };
                executeClusterUpdateActionAndWaitConfigChange(remove_action, std::move(remove_callback), time_left, retry_count);
            }
        }
        else if (action_obj->has("add_members"))
        {
            auto add_members = action_obj->getArray("add_members");
            for (const auto & member_id_json : *add_members)
            {
                const auto & member_obj = member_id_json.extract<Poco::JSON::Object::Ptr>();
                int member_id = member_obj->getValue<int32_t>("id");
                std::string endpoint = member_obj->getValue<std::string>("endpoint");
                bool learner = member_obj->has("learner") ? member_obj->getValue<bool>("learner") : false;
                int priority = member_obj->has("priority") ? member_obj->getValue<int>("priority") : 1;
                AddRaftServer add_action({RaftServerConfig{member_id, endpoint, learner, priority}});
                auto add_callback = [this, member_id](KeeperServer * server_) -> bool
                {
                    auto config = server_->getKeeperStateMachine()->getClusterConfig();
                    if (config->get_server(member_id) != nullptr)
                    {
                        LOG_INFO(
                            log, "Skip adding server id {} to cluster because it's already present in current configuration",
                            member_id);
                        return true;
                    }
                    else
                    {
                        LOG_INFO(
                            log, "Waiting for adding server id {} to cluster configuration",
                            member_id);
                        return false;
                    }
                };
                executeClusterUpdateActionAndWaitConfigChange(add_action, std::move(add_callback), time_left, retry_count);
            }
        }
        else if (action_obj->has("transfer_leadership"))
        {
            auto potential_leaders = action_obj->getArray("transfer_leadership");
            std::vector<int32_t> leader_ids;
            for (const auto & leader_id_json : *potential_leaders)
                leader_ids.push_back(leader_id_json.convert<int32_t>());
            std::shuffle(leader_ids.begin(), leader_ids.end(), thread_local_rng);

            int32_t new_leader_id = leader_ids.front();
            TransferLeadership transfer_action{new_leader_id};
            auto check_callback = [this, new_leader_id](KeeperServer * server_) -> bool
            {
                if (server_->getLeaderID() == new_leader_id)
                {
                    LOG_INFO(
                        log, "Leadership successfully transferred to server id {}",
                        new_leader_id);
                    return true;
                }
                else
                {
                LOG_INFO(
                    log, "Waiting for leadership transfer to server id {}. Current leader id is {}",
                    new_leader_id,
                    server_->getLeaderID());

                    return false;
                }
            };
            executeClusterUpdateActionAndWaitConfigChange(transfer_action, std::move(check_callback), time_left, retry_count);
        }
        else if (action_obj->has("set_priority"))
        {
            const auto & set_priority_obj = action_obj->getArray("set_priority");
            for (const auto & priority_change: *set_priority_obj)
            {
                const auto & priority_change_obj = priority_change.extract<Poco::JSON::Object::Ptr>();
                int member_id = priority_change_obj->getValue<int>("id");
                int priority = priority_change_obj->getValue<int>("priority");
                UpdateRaftServerPriority update_priority_action{member_id, priority};
                auto priority_callback = [this, member_id, priority](KeeperServer * server_) -> bool
                {
                    auto config = server_->getKeeperStateMachine()->getClusterConfig();
                    auto server_in_config = config->get_server(member_id);
                    if (server_in_config == nullptr)
                    {
                        LOG_INFO(
                            log, "Cannot set priority for server id {} because it's not present in current configuration",
                            member_id);
                        return true;
                    }
                    else if (server_in_config->get_priority() == priority)
                    {
                        LOG_INFO(
                            log, "Priority for server id {} successfully changed to {}", member_id, priority);
                        return true;
                    }
                    else
                    {
                        LOG_INFO(
                            log,
                            "Waiting for setting priority {} for server id {} in cluster configuration, current {}", priority, member_id, server_in_config->get_priority());
                        return false;
                    }
                };
                executeClusterUpdateActionAndWaitConfigChange(update_priority_action, std::move(priority_callback), time_left, retry_count);
            }
        }
        else
        {
            std::stringstream ss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            action_obj->stringify(ss);
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown reconfigure action {}", ss.str());
        }
    }

    LOG_DEBUG(log, "Reconfiguration successfully applied");
    Poco::JSON::Object::Ptr result = new Poco::JSON::Object();
    result->set("status", "ok");
    result->set("message", "Reconfiguration successfully applied");
    return result;
}
catch (...)
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
    Poco::JSON::Object::Ptr result = new Poco::JSON::Object();
    result->set("status", "error");
    result->set("message", getCurrentExceptionMessage(false));
    return result;
}


void KeeperDispatcher::cleanResources()
{
#if USE_JEMALLOC
    Jemalloc::purgeArenas();
#endif
}

}
