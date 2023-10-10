#include <Coordination/KeeperDispatcher.h>
#include <libnuraft/async.hxx>

#include <Poco/Path.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <base/hex.h>
#include <Common/setThreadName.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/checkStackSize.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <future>
#include <chrono>
#include <filesystem>
#include <iterator>
#include <limits>

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)

#endif

namespace CurrentMetrics
{
    extern const Metric KeeperAliveConnections;
    extern const Metric KeeperOutstandingRequets;
}

namespace ProfileEvents
{
    extern const Event MemoryAllocatorPurge;
    extern const Event MemoryAllocatorPurgeTimeMicroseconds;
}

using namespace std::chrono_literals;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int SYSTEM_ERROR;
}


KeeperDispatcher::KeeperDispatcher()
    : responses_queue(std::numeric_limits<size_t>::max())
    , configuration_and_settings(std::make_shared<KeeperConfigurationAndSettings>())
    , log(&Poco::Logger::get("KeeperDispatcher"))
{}

void KeeperDispatcher::requestThread()
{
    setThreadName("KeeperReqT");

    /// Result of requests batch from previous iteration
    RaftAppendResult prev_result = nullptr;
    /// Requests from previous iteration. We store them to be able
    /// to send errors to the client.
    KeeperStorage::RequestsForSessions prev_batch;

    while (!shutdown_called)
    {
        KeeperStorage::RequestForSession request;

        auto coordination_settings = configuration_and_settings->coordination_settings;
        uint64_t max_wait = coordination_settings->operation_timeout_ms.totalMilliseconds();
        uint64_t max_batch_size = coordination_settings->max_requests_batch_size;
        uint64_t max_batch_bytes_size = coordination_settings->max_requests_batch_bytes_size;

        /// The code below do a very simple thing: batch all write (quorum) requests into vector until
        /// previous write batch is not finished or max_batch size achieved. The main complexity goes from
        /// the ability to process read requests without quorum (from local state). So when we are collecting
        /// requests into a batch we must check that the new request is not read request. Otherwise we have to
        /// process all already accumulated write requests, wait them synchronously and only after that process
        /// read request. So reads are some kind of "separator" for writes.
        /// Also there is a special reconfig request also being a separator.
        try
        {
            if (requests_queue->tryPop(request, max_wait))
            {
                CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequets);
                if (shutdown_called)
                    break;

                KeeperStorage::RequestsForSessions current_batch;
                size_t current_batch_bytes_size = 0;

                bool has_read_request = false;
                bool has_reconfig_request = false;

                /// If new request is not read request or reconfig request we must process it through quorum.
                /// Otherwise we will process it locally.
                if (request.request->getOpNum() == Coordination::OpNum::Reconfig)
                    has_reconfig_request = true;
                else if (coordination_settings->quorum_reads || !request.request->isReadRequest())
                {
                    current_batch_bytes_size += request.request->bytesSize();
                    current_batch.emplace_back(request);

                    const auto try_get_request = [&]
                    {
                        /// Trying to get batch requests as fast as possible
                        if (requests_queue->tryPop(request))
                        {
                            CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequets);
                            /// Don't append read request into batch, we have to process them separately
                            if (!coordination_settings->quorum_reads && request.request->isReadRequest())
                            {
                                const auto & last_request = current_batch.back();
                                std::lock_guard lock(read_request_queue_mutex);
                                read_request_queue[last_request.session_id][last_request.request->xid].push_back(request);
                            }
                            else if (request.request->getOpNum() == Coordination::OpNum::Reconfig)
                            {
                                has_reconfig_request = true;
                                return false;
                            }
                            else
                            {
                                current_batch_bytes_size += request.request->bytesSize();
                                current_batch.emplace_back(request);
                            }

                            return true;
                        }

                        return false;
                    };

                    /// TODO: Deprecate max_requests_quick_batch_size and use only max_requests_batch_size and max_requests_batch_bytes_size
                    size_t max_quick_batch_size = coordination_settings->max_requests_quick_batch_size;
                    while (!shutdown_called && !has_read_request &&
                        !has_reconfig_request &&
                        current_batch.size() < max_quick_batch_size && current_batch_bytes_size < max_batch_bytes_size &&
                        try_get_request())
                        ;

                    const auto prev_result_done = [&]
                    {
                        /// has_result == false && get_result_code == OK means that our request still not processed.
                        /// Sometimes NuRaft set errorcode without setting result, so we check both here.
                        return !prev_result || prev_result->has_result() || prev_result->get_result_code() != nuraft::cmd_result_code::OK;
                    };

                    /// Waiting until previous append will be successful, or batch is big enough
                    while (!shutdown_called && !has_read_request &&
                        !has_reconfig_request && !prev_result_done() &&
                        current_batch.size() <= max_batch_size
                        && current_batch_bytes_size < max_batch_bytes_size)
                    {
                        try_get_request();
                    }
                }
                else
                    has_read_request = true;

                if (shutdown_called)
                    break;

                /// Forcefully process all previous pending requests
                if (prev_result)
                    forceWaitAndProcessResult(prev_result, prev_batch);

                /// Process collected write requests batch
                if (!current_batch.empty())
                {
                    LOG_TRACE(log, "Processing requests batch, size: {}, bytes: {}", current_batch.size(), current_batch_bytes_size);

                    auto result = server->putRequestBatch(current_batch);

                    if (result)
                    {
                        /// If we will execute read or reconfig next, we have to process result now
                        if (has_read_request || has_reconfig_request)
                            forceWaitAndProcessResult(result, current_batch);
                    }
                    else
                    {
                        addErrorResponses(current_batch, Coordination::Error::ZCONNECTIONLOSS);
                        current_batch.clear();
                        current_batch_bytes_size = 0;
                    }

                    prev_batch = std::move(current_batch);
                    prev_result = result;
                }

                if (has_reconfig_request)
                    server->getKeeperStateMachine()->reconfigure(request);

                /// Read request always goes after write batch (last request)
                if (has_read_request)
                {
                    if (server->isLeaderAlive())
                        server->putLocalReadRequest(request);
                    else
                        addErrorResponses({request}, Coordination::Error::ZCONNECTIONLOSS);
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void KeeperDispatcher::responseThread()
{
    setThreadName("KeeperRspT");
    while (!shutdown_called)
    {
        KeeperStorage::ResponseForSession response_for_session;

        uint64_t max_wait = configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds();

        if (responses_queue.tryPop(response_for_session, max_wait))
        {
            if (shutdown_called)
                break;

            try
            {
                 setResponse(response_for_session.session_id, response_for_session.response);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}

void KeeperDispatcher::snapshotThread()
{
    setThreadName("KeeperSnpT");
    while (!shutdown_called)
    {
        CreateSnapshotTask task;
        if (!snapshots_queue.pop(task))
            break;

        if (shutdown_called)
            break;

        try
        {
            auto snapshot_file_info = task.create_snapshot(std::move(task.snapshot));

            if (snapshot_file_info.path.empty())
                continue;

            if (isLeader())
                snapshot_s3.uploadSnapshot(snapshot_file_info);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void KeeperDispatcher::setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response)
{
    std::lock_guard lock(session_to_response_callback_mutex);

    /// Special new session response.
    if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::SessionID)
    {
        const Coordination::ZooKeeperSessionIDResponse & session_id_resp = dynamic_cast<const Coordination::ZooKeeperSessionIDResponse &>(*response);

        /// Nobody waits for this session id
        if (session_id_resp.server_id != server->getServerID() || !new_session_id_response_callback.contains(session_id_resp.internal_id))
            return;

        auto callback = new_session_id_response_callback[session_id_resp.internal_id];
        callback(response);
        new_session_id_response_callback.erase(session_id_resp.internal_id);
    }
    else /// Normal response, just write to client
    {
        auto session_response_callback = session_to_response_callback.find(session_id);

        /// Session was disconnected, just skip this response
        if (session_response_callback == session_to_response_callback.end())
            return;

        session_response_callback->second(response);

        /// Session closed, no more writes
        if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::Close)
        {
            session_to_response_callback.erase(session_response_callback);
            CurrentMetrics::sub(CurrentMetrics::KeeperAliveConnections);
        }
    }
}

bool KeeperDispatcher::putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id)
{
    {
        /// If session was already disconnected than we will ignore requests
        std::lock_guard lock(session_to_response_callback_mutex);
        if (!session_to_response_callback.contains(session_id))
            return false;
    }

    KeeperStorage::RequestForSession request_info;
    request_info.request = request;
    using namespace std::chrono;
    request_info.time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    request_info.session_id = session_id;

    if (shutdown_called)
        return false;

    /// Put close requests without timeouts
    if (request->getOpNum() == Coordination::OpNum::Close)
    {
        if (!requests_queue->push(std::move(request_info)))
            throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push request to queue");
    }
    else if (!requests_queue->tryPush(std::move(request_info), configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds()))
    {
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Cannot push request to queue within operation timeout");
    }
    CurrentMetrics::add(CurrentMetrics::KeeperOutstandingRequets);
    return true;
}

void KeeperDispatcher::initialize(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper, bool start_async, const MultiVersion<Macros>::Version & macros)
{
    LOG_DEBUG(log, "Initializing storage dispatcher");

    configuration_and_settings = KeeperConfigurationAndSettings::loadFromConfig(config, standalone_keeper);
    requests_queue = std::make_unique<RequestsQueue>(configuration_and_settings->coordination_settings->max_request_queue_size);

    request_thread = ThreadFromGlobalPool([this] { requestThread(); });
    responses_thread = ThreadFromGlobalPool([this] { responseThread(); });
    snapshot_thread = ThreadFromGlobalPool([this] { snapshotThread(); });

    snapshot_s3.startup(config, macros);

    keeper_context = std::make_shared<KeeperContext>(standalone_keeper);
    keeper_context->initialize(config, this);

    server = std::make_unique<KeeperServer>(
        configuration_and_settings,
        config,
        responses_queue,
        snapshots_queue,
        keeper_context,
        snapshot_s3,
        [this](const KeeperStorage::RequestForSession & request_for_session)
        {
            /// check if we have queue of read requests depending on this request to be committed
            std::lock_guard lock(read_request_queue_mutex);
            if (auto it = read_request_queue.find(request_for_session.session_id); it != read_request_queue.end())
            {
                auto & xid_to_request_queue = it->second;

                if (auto request_queue_it = xid_to_request_queue.find(request_for_session.request->xid);
                    request_queue_it != xid_to_request_queue.end())
                {
                    for (const auto & read_request : request_queue_it->second)
                    {
                        if (server->isLeaderAlive())
                            server->putLocalReadRequest(read_request);
                        else
                            addErrorResponses({read_request}, Coordination::Error::ZCONNECTIONLOSS);
                    }

                    xid_to_request_queue.erase(request_queue_it);
                }
            }
        });

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

    /// Start it after keeper server start
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
            if (shutdown_called.exchange(true))
                return;

            LOG_DEBUG(log, "Shutting down storage dispatcher");

            if (session_cleaner_thread.joinable())
                session_cleaner_thread.join();

            if (requests_queue)
            {
                requests_queue->finish();

                if (request_thread.joinable())
                    request_thread.join();
            }

            responses_queue.finish();
            if (responses_thread.joinable())
                responses_thread.join();

            snapshots_queue.finish();
            if (snapshot_thread.joinable())
                snapshot_thread.join();

            cluster_update_queue.finish();
            if (update_configuration_thread.joinable())
                update_configuration_thread.join();
        }

        KeeperStorage::RequestForSession request_for_session;

        /// Set session expired for all pending requests
        while (requests_queue && requests_queue->tryPop(request_for_session))
        {
            CurrentMetrics::sub(CurrentMetrics::KeeperOutstandingRequets);
            auto response = request_for_session.request->makeResponse();
            response->error = Coordination::Error::ZSESSIONEXPIRED;
            setResponse(request_for_session.session_id, response);
        }

        KeeperStorage::RequestsForSessions close_requests;
        {
            /// Clear all registered sessions
            std::lock_guard lock(session_to_response_callback_mutex);

            if (server && hasLeader())
            {
                close_requests.reserve(session_to_response_callback.size());
                // send to leader CLOSE requests for active sessions
                for (const auto & [session, response] : session_to_response_callback)
                {
                    auto request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
                    request->xid = Coordination::CLOSE_XID;
                    using namespace std::chrono;
                    KeeperStorage::RequestForSession request_info
                    {
                        .session_id = session,
                        .time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count(),
                        .request = std::move(request),
                        .digest = std::nullopt
                    };

                    close_requests.push_back(std::move(request_info));
                }
            }

            session_to_response_callback.clear();
        }

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

                auto session_shutdown_timeout = configuration_and_settings->coordination_settings->session_shutdown_timeout.totalMilliseconds();
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

void KeeperDispatcher::registerSession(int64_t session_id, ZooKeeperResponseCallback callback)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    if (!session_to_response_callback.try_emplace(session_id, callback).second)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in dispatcher", session_id);
    CurrentMetrics::add(CurrentMetrics::KeeperAliveConnections);
}

void KeeperDispatcher::sessionCleanerTask()
{
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
                    using namespace std::chrono;
                    KeeperStorage::RequestForSession request_info
                    {
                        .session_id = dead_session,
                        .time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count(),
                        .request = std::move(request),
                        .digest = std::nullopt
                    };
                    if (!requests_queue->push(std::move(request_info)))
                        LOG_INFO(log, "Cannot push close request to queue while cleaning outdated sessions");
                    CurrentMetrics::add(CurrentMetrics::KeeperOutstandingRequets);

                    /// Remove session from registered sessions
                    finishSession(dead_session);
                    LOG_INFO(log, "Dead session close request pushed");
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        auto time_to_sleep = configuration_and_settings->coordination_settings->dead_session_check_period_ms.totalMilliseconds();
        std::this_thread::sleep_for(std::chrono::milliseconds(time_to_sleep));
    }
}

void KeeperDispatcher::finishSession(int64_t session_id)
{
    /// shutdown() method will cleanup sessions if needed
    if (shutdown_called)
        return;

    {
        std::lock_guard lock(session_to_response_callback_mutex);
        auto session_it = session_to_response_callback.find(session_id);
        if (session_it != session_to_response_callback.end())
        {
            session_to_response_callback.erase(session_it);
            CurrentMetrics::sub(CurrentMetrics::KeeperAliveConnections);
        }
    }
    {
        std::lock_guard lock(read_request_queue_mutex);
        read_request_queue.erase(session_id);
    }
}

void KeeperDispatcher::addErrorResponses(const KeeperStorage::RequestsForSessions & requests_for_sessions, Coordination::Error error)
{
    for (const auto & request_for_session : requests_for_sessions)
    {
        KeeperStorage::ResponsesForSessions responses;
        auto response = request_for_session.request->makeResponse();
        response->xid = request_for_session.request->xid;
        response->zxid = 0;
        response->error = error;
        if (!responses_queue.push(DB::KeeperStorage::ResponseForSession{request_for_session.session_id, response}))
            throw Exception(ErrorCodes::SYSTEM_ERROR,
                "Could not push error response xid {} zxid {} error message {} to responses queue",
                response->xid,
                response->zxid,
                error);
    }
}

void KeeperDispatcher::forceWaitAndProcessResult(RaftAppendResult & result, KeeperStorage::RequestsForSessions & requests_for_sessions)
{
    if (!result->has_result())
        result->get();

    /// If we get some errors, than send them to clients
    if (!result->get_accepted() || result->get_result_code() == nuraft::cmd_result_code::TIMEOUT)
        addErrorResponses(requests_for_sessions, Coordination::Error::ZOPERATIONTIMEOUT);
    else if (result->get_result_code() != nuraft::cmd_result_code::OK)
        addErrorResponses(requests_for_sessions, Coordination::Error::ZCONNECTIONLOSS);

    result = nullptr;
    requests_for_sessions.clear();
}

int64_t KeeperDispatcher::getSessionID(int64_t session_timeout_ms)
{
    /// New session id allocation is a special request, because we cannot process it in normal
    /// way: get request -> put to raft -> set response for registered callback.
    KeeperStorage::RequestForSession request_info;
    std::shared_ptr<Coordination::ZooKeeperSessionIDRequest> request = std::make_shared<Coordination::ZooKeeperSessionIDRequest>();
    /// Internal session id. It's a temporary number which is unique for each client on this server
    /// but can be same on different servers.
    request->internal_id = internal_session_id_counter.fetch_add(1);
    request->session_timeout_ms = session_timeout_ms;
    request->server_id = server->getServerID();

    request_info.request = request;
    using namespace std::chrono;
    request_info.time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    request_info.session_id = -1;

    auto promise = std::make_shared<std::promise<int64_t>>();
    auto future = promise->get_future();

    {
        std::lock_guard lock(session_to_response_callback_mutex);
        new_session_id_response_callback[request->internal_id] = [promise, internal_id = request->internal_id] (const Coordination::ZooKeeperResponsePtr & response)
        {
            if (response->getOpNum() != Coordination::OpNum::SessionID)
                promise->set_exception(std::make_exception_ptr(Exception(ErrorCodes::LOGICAL_ERROR,
                            "Incorrect response of type {} instead of SessionID response", response->getOpNum())));

            auto session_id_response = dynamic_cast<const Coordination::ZooKeeperSessionIDResponse &>(*response);
            if (session_id_response.internal_id != internal_id)
            {
                promise->set_exception(std::make_exception_ptr(Exception(ErrorCodes::LOGICAL_ERROR,
                            "Incorrect response with internal id {} instead of {}", session_id_response.internal_id, internal_id)));
            }

            if (response->error != Coordination::Error::ZOK)
                promise->set_exception(std::make_exception_ptr(zkutil::KeeperException::fromMessage(response->error, "SessionID request failed with error")));

            promise->set_value(session_id_response.session_id);
        };
    }

    /// Push new session request to queue
    if (!requests_queue->tryPush(std::move(request_info), session_timeout_ms))
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Cannot push session id request to queue within session timeout");
    CurrentMetrics::add(CurrentMetrics::KeeperOutstandingRequets);

    if (future.wait_for(std::chrono::milliseconds(session_timeout_ms)) != std::future_status::ready)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Cannot receive session id within session timeout");

    /// Forcefully wait for request execution because we cannot process any other
    /// requests for this client until it get new session id.
    return future.get();
}

void KeeperDispatcher::clusterUpdateWithReconfigDisabledThread()
{
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
    while (!shutdown_called)
    {
        ClusterUpdateAction action;
        if (!cluster_update_queue.pop(action))
            return;

        if (server->applyConfigUpdate(action))
            LOG_DEBUG(log, "Processing config update {}: accepted", action);
        else // TODO (myrrc) sleep a random amount? sleep less?
        {
            (void)cluster_update_queue.pushFront(action);
            LOG_DEBUG(log, "Processing config update {}: declined, backoff", action);
            std::this_thread::sleep_for(50ms);
        }
    }
}

void KeeperDispatcher::pushClusterUpdates(ClusterUpdateActions && actions)
{
    if (shutdown_called) return;
    for (auto && action : actions)
    {
        if (!cluster_update_queue.push(std::move(action)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot push configuration update");
        LOG_DEBUG(log, "Processing config update {}: pushed", action);
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
}

void KeeperDispatcher::updateKeeperStatLatency(uint64_t process_time_ms)
{
    keeper_stats.updateLatency(process_time_ms);
}

static uint64_t getTotalSize(const DiskPtr & disk, const std::string & path = "")
{
    checkStackSize();

    uint64_t size = 0;
    for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
    {
        if (disk->isFile(it->path()))
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
    result.outstanding_requests_count = requests_queue->size();
    {
        std::lock_guard lock(session_to_response_callback_mutex);
        result.alive_connections_count = session_to_response_callback.size();
    }
    return result;
}

void KeeperDispatcher::cleanResources()
{
#if USE_JEMALLOC
    LOG_TRACE(&Poco::Logger::get("KeeperDispatcher"), "Purging unused memory");
    Stopwatch watch;
    mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", nullptr, nullptr, nullptr, 0);
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurge);
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurgeTimeMicroseconds, watch.elapsedMicroseconds());
#endif
}

}
