#include <Coordination/KeeperStorageDispatcher.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <future>
#include <chrono>

namespace DB
{

namespace ErrorCodes
{

    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
}

KeeperStorageDispatcher::KeeperStorageDispatcher()
    : coordination_settings(std::make_shared<CoordinationSettings>())
    , log(&Poco::Logger::get("KeeperDispatcher"))
{
}


void KeeperStorageDispatcher::requestThread()
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

        UInt64 max_wait = UInt64(coordination_settings->operation_timeout_ms.totalMilliseconds());
        uint64_t max_batch_size = coordination_settings->max_requests_batch_size;

        /// The code below do a very simple thing: batch all write (quorum) requests into vector until
        /// previous write batch is not finished or max_batch size achieved. The main complexity goes from
        /// the ability to process read requests without quorum (from local state). So when we are collecting
        /// requests into a batch we must check that the new request is not read request. Otherwise we have to
        /// process all already accumulated write requests, wait them synchronously and only after that process
        /// read request. So reads are some kind of "separator" for writes.
        try
        {
            if (requests_queue->tryPop(request, max_wait))
            {
                if (shutdown_called)
                    break;

                KeeperStorage::RequestsForSessions current_batch;

                bool has_read_request = false;

                /// If new request is not read request or we must to process it through quorum.
                /// Otherwise we will process it locally.
                if (coordination_settings->quorum_reads || !request.request->isReadRequest())
                {
                    current_batch.emplace_back(request);

                    /// Waiting until previous append will be successful, or batch is big enough
                    /// has_result == false && get_result_code == OK means that our request still not processed.
                    /// Sometimes NuRaft set errorcode without setting result, so we check both here.
                    while (prev_result && (!prev_result->has_result() && prev_result->get_result_code() == nuraft::cmd_result_code::OK) && current_batch.size() <= max_batch_size)
                    {
                        /// Trying to get batch requests as fast as possible
                        if (requests_queue->tryPop(request, 1))
                        {
                            /// Don't append read request into batch, we have to process them separately
                            if (!coordination_settings->quorum_reads && request.request->isReadRequest())
                            {
                                has_read_request = true;
                                break;
                            }
                            else
                            {

                                current_batch.emplace_back(request);
                            }
                        }

                        if (shutdown_called)
                            break;
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
                    auto result = server->putRequestBatch(current_batch);

                    if (result)
                    {
                        if (has_read_request) /// If we will execute read request next, than we have to process result now
                            forceWaitAndProcessResult(result, current_batch);
                    }
                    else
                    {
                        addErrorResponses(current_batch, Coordination::Error::ZCONNECTIONLOSS);
                        current_batch.clear();
                    }

                    prev_batch = current_batch;
                    prev_result = result;
                }

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

void KeeperStorageDispatcher::responseThread()
{
    setThreadName("KeeperRspT");
    while (!shutdown_called)
    {
        KeeperStorage::ResponseForSession response_for_session;

        UInt64 max_wait = UInt64(coordination_settings->operation_timeout_ms.totalMilliseconds());

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

void KeeperStorageDispatcher::snapshotThread()
{
    setThreadName("KeeperSnpT");
    while (!shutdown_called)
    {
        CreateSnapshotTask task;
        snapshots_queue.pop(task);

        if (shutdown_called)
            break;

        try
        {
            task.create_snapshot(std::move(task.snapshot));
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void KeeperStorageDispatcher::setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::SessionID)
    {
        const Coordination::ZooKeeperSessionIDResponse & session_id_resp = dynamic_cast<const Coordination::ZooKeeperSessionIDResponse &>(*response);

        /// Nobody waits for this session id
        if (session_id_resp.server_id != server->getServerID() || !new_session_id_response_callback.count(session_id_resp.internal_id))
            return;

        auto callback = new_session_id_response_callback[session_id_resp.internal_id];
        callback(response);
        new_session_id_response_callback.erase(session_id_resp.internal_id);
    }
    else
    {
        auto session_writer = session_to_response_callback.find(session_id);
        if (session_writer == session_to_response_callback.end())
            return;

        session_writer->second(response);

        /// Session closed, no more writes
        if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::Close)
        {
            session_to_response_callback.erase(session_writer);
        }
    }
}

bool KeeperStorageDispatcher::putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id)
{
    {
        std::lock_guard lock(session_to_response_callback_mutex);
        if (session_to_response_callback.count(session_id) == 0)
            return false;
    }

    KeeperStorage::RequestForSession request_info;
    request_info.request = request;
    request_info.session_id = session_id;

    std::lock_guard lock(push_request_mutex);

    if (shutdown_called)
        return false;

    /// Put close requests without timeouts
    if (request->getOpNum() == Coordination::OpNum::Close)
        requests_queue->push(std::move(request_info));
    else if (!requests_queue->tryPush(std::move(request_info), coordination_settings->operation_timeout_ms.totalMilliseconds()))
        throw Exception("Cannot push request to queue within operation timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    return true;
}

void KeeperStorageDispatcher::initialize(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper)
{
    LOG_DEBUG(log, "Initializing storage dispatcher");
    int myid = config.getInt("keeper_server.server_id");

    coordination_settings->loadFromConfig("keeper_server.coordination_settings", config);
    requests_queue = std::make_unique<RequestsQueue>(coordination_settings->max_requests_batch_size);

    request_thread = ThreadFromGlobalPool([this] { requestThread(); });
    responses_thread = ThreadFromGlobalPool([this] { responseThread(); });
    snapshot_thread = ThreadFromGlobalPool([this] { snapshotThread(); });

    server = std::make_unique<KeeperServer>(
        myid, coordination_settings, config, responses_queue, snapshots_queue, standalone_keeper);
    try
    {
        LOG_DEBUG(log, "Waiting server to initialize");
        server->startup();
        LOG_DEBUG(log, "Server initialized, waiting for quorum");

        server->waitInit();
        LOG_DEBUG(log, "Quorum initialized");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }


    session_cleaner_thread = ThreadFromGlobalPool([this] { sessionCleanerTask(); });

    LOG_DEBUG(log, "Dispatcher initialized");
}

void KeeperStorageDispatcher::shutdown()
{
    try
    {
        {
            std::lock_guard lock(push_request_mutex);

            if (shutdown_called)
                return;

            LOG_DEBUG(log, "Shutting down storage dispatcher");
            shutdown_called = true;

            if (session_cleaner_thread.joinable())
                session_cleaner_thread.join();

            if (requests_queue)
            {
                requests_queue->push({});
                if (request_thread.joinable())
                    request_thread.join();
            }

            responses_queue.push({});
            if (responses_thread.joinable())
                responses_thread.join();

            snapshots_queue.push({});
            if (snapshot_thread.joinable())
                snapshot_thread.join();
        }

        if (server)
            server->shutdown();

        KeeperStorage::RequestForSession request_for_session;

        /// Set session expired for all pending requests
        while (requests_queue && requests_queue->tryPop(request_for_session))
        {
            if (request_for_session.request)
            {
                auto response = request_for_session.request->makeResponse();
                response->error = Coordination::Error::ZSESSIONEXPIRED;
                setResponse(request_for_session.session_id, response);
            }
            else
            {
                break;
            }
        }

        std::lock_guard lock(session_to_response_callback_mutex);
        session_to_response_callback.clear();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    LOG_DEBUG(log, "Dispatcher shut down");
}

KeeperStorageDispatcher::~KeeperStorageDispatcher()
{
    shutdown();
}

void KeeperStorageDispatcher::registerSession(int64_t session_id, ZooKeeperResponseCallback callback)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    if (!session_to_response_callback.try_emplace(session_id, callback).second)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in dispatcher", session_id);
}

void KeeperStorageDispatcher::sessionCleanerTask()
{
    while (true)
    {
        if (shutdown_called)
            return;

        try
        {
            if (isLeader())
            {
                auto dead_sessions = server->getDeadSessions();
                for (int64_t dead_session : dead_sessions)
                {
                    LOG_INFO(log, "Found dead session {}, will try to close it", dead_session);
                    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
                    request->xid = Coordination::CLOSE_XID;
                    KeeperStorage::RequestForSession request_info;
                    request_info.request = request;
                    request_info.session_id = dead_session;
                    {
                        std::lock_guard lock(push_request_mutex);
                        requests_queue->push(std::move(request_info));
                    }
                    finishSession(dead_session);
                    LOG_INFO(log, "Dead session close request pushed");
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(coordination_settings->dead_session_check_period_ms.totalMilliseconds()));
    }
}

void KeeperStorageDispatcher::finishSession(int64_t session_id)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    auto session_it = session_to_response_callback.find(session_id);
    if (session_it != session_to_response_callback.end())
        session_to_response_callback.erase(session_it);
}

void KeeperStorageDispatcher::addErrorResponses(const KeeperStorage::RequestsForSessions & requests_for_sessions, Coordination::Error error)
{
    for (const auto & [session_id, request] : requests_for_sessions)
    {
        KeeperStorage::ResponsesForSessions responses;
        auto response = request->makeResponse();
        response->xid = request->xid;
        response->zxid = 0;
        response->error = error;
        responses_queue.push(DB::KeeperStorage::ResponseForSession{session_id, response});
    }
}

void KeeperStorageDispatcher::forceWaitAndProcessResult(RaftAppendResult & result, KeeperStorage::RequestsForSessions & requests_for_sessions)
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

int64_t KeeperStorageDispatcher::getSessionID(int64_t session_timeout_ms)
{
    KeeperStorage::RequestForSession request_info;
    std::shared_ptr<Coordination::ZooKeeperSessionIDRequest> request = std::make_shared<Coordination::ZooKeeperSessionIDRequest>();
    request->internal_id = internal_session_id_counter.fetch_add(1);
    request->session_timeout_ms = session_timeout_ms;
    request->server_id = server->getServerID();

    request_info.request = request;
    request_info.session_id = -1;

    auto promise = std::make_shared<std::promise<int64_t>>();
    auto future = promise->get_future();
    {
        std::lock_guard lock(session_to_response_callback_mutex);
        new_session_id_response_callback[request->internal_id] = [promise, internal_id = request->internal_id] (const Coordination::ZooKeeperResponsePtr & response)
        {
            if (response->getOpNum() != Coordination::OpNum::SessionID)
                promise->set_exception(std::make_exception_ptr(Exception(ErrorCodes::LOGICAL_ERROR,
                            "Incorrect response of type {} instead of SessionID response", Coordination::toString(response->getOpNum()))));

            auto session_id_response = dynamic_cast<const Coordination::ZooKeeperSessionIDResponse &>(*response);
            if (session_id_response.internal_id != internal_id)
            {
                promise->set_exception(std::make_exception_ptr(Exception(ErrorCodes::LOGICAL_ERROR,
                            "Incorrect response with internal id {} instead of {}", session_id_response.internal_id, internal_id)));
            }

            if (response->error != Coordination::Error::ZOK)
                promise->set_exception(std::make_exception_ptr(zkutil::KeeperException("SessionID request failed with error", response->error)));

            promise->set_value(session_id_response.session_id);
        };
    }

    {
        std::lock_guard lock(push_request_mutex);
        if (!requests_queue->tryPush(std::move(request_info), session_timeout_ms))
            throw Exception("Cannot push session id request to queue within session timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    }

    if (future.wait_for(std::chrono::milliseconds(session_timeout_ms)) != std::future_status::ready)
        throw Exception("Cannot receive session id within session timeout", ErrorCodes::TIMEOUT_EXCEEDED);

    return future.get();
}

}
