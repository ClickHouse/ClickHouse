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
    try
    {
        while (!shutdown)
        {
            TestKeeperStorage::RequestForSession request;

            UInt64 max_wait = UInt64(operation_timeout.totalMilliseconds());

            if (requests_queue.tryPop(request, max_wait))
            {
                if (shutdown)
                    break;

                auto responses = server.putRequests({request});
                for (const auto & response_for_session : responses)
                    setResponse(response_for_session.session_id, response_for_session.response);
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        finalize();
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

    //TestKeeperStorage::RequestsForSessions expired_requests;
    //TestKeeperStorage::RequestForSession request;
    //while (requests_queue.tryPop(request))
    //    expired_requests.push_back(TestKeeperStorage::RequestForSession{request});

    //auto expired_responses = storage.finalize(expired_requests);

    //for (const auto & response_for_session : expired_responses)
    //    setResponse(response_for_session.session_id, response_for_session.response);
    /// TODO FIXME
    server.shutdown();
}

void TestKeeperStorageDispatcher::putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id)
{

    {
        std::lock_guard lock(session_to_response_callback_mutex);
        if (session_to_response_callback.count(session_id) == 0)
            throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknown session id {}", session_id);
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
}

TestKeeperStorageDispatcher::TestKeeperStorageDispatcher()
    : server(1, "localhost", 44444)
{
    server.startup();
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
