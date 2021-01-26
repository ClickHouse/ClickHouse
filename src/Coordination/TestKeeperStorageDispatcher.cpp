#include <Coordination/TestKeeperStorageDispatcher.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{

    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
}

void TestKeeperStorageDispatcher::processingThread()
{
    setThreadName("TestKeeperSProc");
    while (!shutdown)
    {
        TestKeeperStorage::RequestForSession request;

        UInt64 max_wait = UInt64(operation_timeout.totalMilliseconds());

        if (requests_queue.tryPop(request, max_wait))
        {
            if (shutdown)
                break;
            try
            {
                auto responses = server->putRequests({request});
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

void TestKeeperStorageDispatcher::setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response)
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

void TestKeeperStorageDispatcher::finalize()
{
    {
        std::lock_guard lock(push_request_mutex);

        if (shutdown)
            return;

        shutdown = true;

        if (processing_thread.joinable())
            processing_thread.join();
    }

    if (server)
    {
        TestKeeperStorage::RequestsForSessions expired_requests;
        TestKeeperStorage::RequestForSession request;
        while (requests_queue.tryPop(request))
            expired_requests.push_back(TestKeeperStorage::RequestForSession{request});

        auto expired_responses = server->shutdown(expired_requests);

        for (const auto & response_for_session : expired_responses)
            setResponse(response_for_session.session_id, response_for_session.response);
    }
}

bool TestKeeperStorageDispatcher::putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id)
{

    {
        std::lock_guard lock(session_to_response_callback_mutex);
        if (session_to_response_callback.count(session_id) == 0)
            return false;
    }

    TestKeeperStorage::RequestForSession request_info;
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


void TestKeeperStorageDispatcher::initialize(const Poco::Util::AbstractConfiguration & config)
{
    int myid = config.getInt("test_keeper_server.server_id");
    std::string myhostname;
    int myport;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys("test_keeper_server.raft_configuration", keys);
    bool my_can_become_leader = true;

    std::vector<std::tuple<int, std::string, int, bool>> server_configs;
    for (const auto & server_key : keys)
    {
        int server_id = config.getInt("test_keeper_server.raft_configuration." + server_key + ".id");
        std::string hostname = config.getString("test_keeper_server.raft_configuration." + server_key + ".hostname");
        int port = config.getInt("test_keeper_server.raft_configuration." + server_key + ".port");
        bool can_become_leader = config.getBool("test_keeper_server.raft_configuration." + server_key + ".can_become_leader", true);
        if (server_id == myid)
        {
            myhostname = hostname;
            myport = port;
            my_can_become_leader = can_become_leader;
        }
        else
        {
            server_configs.emplace_back(server_id, hostname, port, can_become_leader);
        }
    }

    server = std::make_unique<NuKeeperServer>(myid, myhostname, myport, my_can_become_leader);
    server->startup();
    if (my_can_become_leader)
    {
        for (const auto & [id, hostname, port, can_become_leader] : server_configs)
            server->addServer(id, hostname + ":" + std::to_string(port), can_become_leader);
    }

    processing_thread = ThreadFromGlobalPool([this] { processingThread(); });

}

TestKeeperStorageDispatcher::~TestKeeperStorageDispatcher()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void TestKeeperStorageDispatcher::registerSession(int64_t session_id, ZooKeeperResponseCallback callback)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    if (!session_to_response_callback.try_emplace(session_id, callback).second)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in dispatcher", session_id);
}

void TestKeeperStorageDispatcher::finishSession(int64_t session_id)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    auto session_it = session_to_response_callback.find(session_id);
    if (session_it != session_to_response_callback.end())
        session_to_response_callback.erase(session_it);
}

}
