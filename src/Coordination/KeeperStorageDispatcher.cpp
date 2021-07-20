#include <Coordination/KeeperStorageDispatcher.h>
#include <Common/setThreadName.h>

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
    while (!shutdown_called)
    {
        KeeperStorage::RequestForSession request;

        UInt64 max_wait = UInt64(coordination_settings->operation_timeout_ms.totalMilliseconds());

        if (requests_queue.tryPop(request, max_wait))
        {
            if (shutdown_called)
                break;

            try
            {
                server->putRequest(request);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
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
    auto session_writer = session_to_response_callback.find(session_id);
    if (session_writer == session_to_response_callback.end())
        return;

    session_writer->second(response);
    /// Session closed, no more writes
    if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::Close)
        session_to_response_callback.erase(session_writer);
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
    /// Put close requests without timeouts
    if (request->getOpNum() == Coordination::OpNum::Close)
        requests_queue.push(std::move(request_info));
    else if (!requests_queue.tryPush(std::move(request_info), coordination_settings->operation_timeout_ms.totalMilliseconds()))
        throw Exception("Cannot push request to queue within operation timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    return true;
}

void KeeperStorageDispatcher::initialize(const Poco::Util::AbstractConfiguration & config)
{
    LOG_DEBUG(log, "Initializing storage dispatcher");
    int myid = config.getInt("keeper_server.server_id");

    coordination_settings->loadFromConfig("keeper_server.coordination_settings", config);

    request_thread = ThreadFromGlobalPool([this] { requestThread(); });
    responses_thread = ThreadFromGlobalPool([this] { responseThread(); });
    snapshot_thread = ThreadFromGlobalPool([this] { snapshotThread(); });

    server = std::make_unique<KeeperServer>(myid, coordination_settings, config, responses_queue, snapshots_queue);
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

            /// FIXME not the best way to notify
            requests_queue.push({});
            if (request_thread.joinable())
                request_thread.join();

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
        while (requests_queue.tryPop(request_for_session))
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
                        requests_queue.push(std::move(request_info));
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

}
