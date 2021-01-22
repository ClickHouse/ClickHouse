#include <Common/ZooKeeper/TestKeeperStorageDispatcher.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{

    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
}

}
namespace zkutil
{

void TestKeeperStorageDispatcher::processingThread()
{
    setThreadName("TestKeeperSProc");
    try
    {
        while (!shutdown)
        {
            RequestInfo info;

            UInt64 max_wait = UInt64(operation_timeout.totalMilliseconds());

            if (requests_queue.tryPop(info, max_wait))
            {
                if (shutdown)
                    break;

                auto responses = storage.processRequest(info.request, info.session_id);
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

    RequestInfo info;
    TestKeeperStorage::RequestsForSessions expired_requests;
    while (requests_queue.tryPop(info))
        expired_requests.push_back(TestKeeperStorage::RequestForSession{info.session_id, info.request});

    auto expired_responses = storage.finalize(expired_requests);

    for (const auto & response_for_session : expired_responses)
        setResponse(response_for_session.session_id, response_for_session.response);
}

void TestKeeperStorageDispatcher::putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id)
{

    {
        std::lock_guard lock(session_to_response_callback_mutex);
        if (session_to_response_callback.count(session_id) == 0)
            throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknown session id {}", session_id);
    }

    RequestInfo request_info;
    request_info.time = clock::now();
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
{
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
