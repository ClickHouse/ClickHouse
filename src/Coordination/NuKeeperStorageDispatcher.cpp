#include <Coordination/NuKeeperStorageDispatcher.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{

    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
}

NuKeeperStorageDispatcher::NuKeeperStorageDispatcher()
    : log(&Poco::Logger::get("NuKeeperDispatcher"))
{
}

void NuKeeperStorageDispatcher::processingThread()
{
    setThreadName("NuKeeperSProc");
    while (!shutdown_called)
    {
        NuKeeperStorage::RequestForSession request;

        UInt64 max_wait = UInt64(operation_timeout.totalMilliseconds());

        if (requests_queue.tryPop(request, max_wait))
        {
            if (shutdown_called)
                break;

            try
            {
                auto responses = server->putRequest(request);
                for (const auto & response_for_session : responses)
                    setResponse(response_for_session.session_id, response_for_session.response);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}

void NuKeeperStorageDispatcher::setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response)
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

bool NuKeeperStorageDispatcher::putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id)
{
    {
        std::lock_guard lock(session_to_response_callback_mutex);
        if (session_to_response_callback.count(session_id) == 0)
            return false;
    }

    NuKeeperStorage::RequestForSession request_info;
    request_info.request = request;
    request_info.session_id = session_id;

    std::lock_guard lock(push_request_mutex);
    /// Put close requests without timeouts
    if (request->getOpNum() == Coordination::OpNum::Close)
        requests_queue.push(std::move(request_info));
    else if (!requests_queue.tryPush(std::move(request_info), operation_timeout.totalMilliseconds()))
        throw Exception("Cannot push request to queue within operation timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    return true;
}

namespace
{
    bool shouldBuildQuorum(int32_t myid, int32_t my_priority, bool my_can_become_leader, const std::vector<std::tuple<int, std::string, int, bool, int32_t>> & server_configs)
    {
        if (!my_can_become_leader)
            return false;

        int32_t minid = myid;
        bool has_equal_priority = false;
        for (const auto & [id, hostname, port, can_become_leader, priority] : server_configs)
        {
            if (my_priority < priority)
                return false;
            else if (my_priority == priority)
                has_equal_priority = true;
            minid = std::min(minid, id);
        }

        if (has_equal_priority)
            return minid == myid;
        else
            return true;
    }
}

void NuKeeperStorageDispatcher::initialize(const Poco::Util::AbstractConfiguration & config)
{
    LOG_DEBUG(log, "Initializing storage dispatcher");
    int myid = config.getInt("test_keeper_server.server_id");
    std::string myhostname;
    int myport;
    int32_t my_priority = 1;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys("test_keeper_server.raft_configuration", keys);
    bool my_can_become_leader = true;

    std::vector<std::tuple<int, std::string, int, bool, int32_t>> server_configs;
    std::vector<int32_t> ids;
    for (const auto & server_key : keys)
    {
        int server_id = config.getInt("test_keeper_server.raft_configuration." + server_key + ".id");
        std::string hostname = config.getString("test_keeper_server.raft_configuration." + server_key + ".hostname");
        int port = config.getInt("test_keeper_server.raft_configuration." + server_key + ".port");
        bool can_become_leader = config.getBool("test_keeper_server.raft_configuration." + server_key + ".can_become_leader", true);
        int32_t priority = config.getInt("test_keeper_server.raft_configuration." + server_key + ".priority", 1);
        if (server_id == myid)
        {
            myhostname = hostname;
            myport = port;
            my_can_become_leader = can_become_leader;
            my_priority = priority;
        }
        else
        {
            server_configs.emplace_back(server_id, hostname, port, can_become_leader, priority);
        }
        ids.push_back(server_id);
    }

    server = std::make_unique<NuKeeperServer>(myid, myhostname, myport);
    try
    {
        server->startup();
        if (shouldBuildQuorum(myid, my_priority, my_can_become_leader, server_configs))
        {
            for (const auto & [id, hostname, port, can_become_leader, priority] : server_configs)
            {
                LOG_DEBUG(log, "Adding server with id {} ({}:{})", id, hostname, port);
                do
                {
                    server->addServer(id, hostname + ":" + std::to_string(port), can_become_leader, priority);
                }
                while (!server->waitForServer(id));

                LOG_DEBUG(log, "Server with id {} ({}:{}) added to cluster", id, hostname, port);
            }
        }
        else
        {
            LOG_DEBUG(log, "Waiting for {} servers to build cluster", ids.size());
            server->waitForServers(ids);
            server->waitForCatchUp();
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    processing_thread = ThreadFromGlobalPool([this] { processingThread(); });
    session_cleaner_thread = ThreadFromGlobalPool([this] { sessionCleanerTask(); });

    LOG_DEBUG(log, "Dispatcher initialized");
}

void NuKeeperStorageDispatcher::shutdown()
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

            if (processing_thread.joinable())
                processing_thread.join();
        }

        if (server)
            server->shutdown();

        NuKeeperStorage::RequestForSession request_for_session;
        while (requests_queue.tryPop(request_for_session))
        {
            auto response = request_for_session.request->makeResponse();
            response->error = Coordination::Error::ZSESSIONEXPIRED;
            setResponse(request_for_session.session_id, response);
        }
        session_to_response_callback.clear();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    LOG_DEBUG(log, "Dispatcher shut down");
}

NuKeeperStorageDispatcher::~NuKeeperStorageDispatcher()
{
    shutdown();
}

void NuKeeperStorageDispatcher::registerSession(int64_t session_id, ZooKeeperResponseCallback callback)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    if (!session_to_response_callback.try_emplace(session_id, callback).second)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in dispatcher", session_id);
}

void NuKeeperStorageDispatcher::sessionCleanerTask()
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
                    putRequest(request, dead_session);
                    {
                        std::lock_guard lock(session_to_response_callback_mutex);
                        auto session_it = session_to_response_callback.find(dead_session);
                        if (session_it != session_to_response_callback.end())
                            session_to_response_callback.erase(session_it);
                    }
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        /*FIXME*/
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

void NuKeeperStorageDispatcher::finishSession(int64_t session_id)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    auto session_it = session_to_response_callback.find(session_id);
    if (session_it != session_to_response_callback.end())
        session_to_response_callback.erase(session_it);
}

}
