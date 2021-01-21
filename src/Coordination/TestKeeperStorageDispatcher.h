#pragma once

#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Coordination/TestKeeperStorage.h>
#include <functional>

namespace DB
{

using ZooKeeperResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr & response)>;

class TestKeeperStorageDispatcher
{
private:
    Poco::Timespan operation_timeout{0, Coordination::DEFAULT_OPERATION_TIMEOUT_MS * 1000};

    using clock = std::chrono::steady_clock;

    struct RequestInfo
    {
        Coordination::ZooKeeperRequestPtr request;
        clock::time_point time;
        int64_t session_id;
    };

    std::mutex push_request_mutex;

    using RequestsQueue = ConcurrentBoundedQueue<RequestInfo>;
    RequestsQueue requests_queue{1};
    std::atomic<bool> shutdown{false};
    using SessionToResponseCallback = std::unordered_map<int64_t, ZooKeeperResponseCallback>;

    std::mutex session_to_response_callback_mutex;
    SessionToResponseCallback session_to_response_callback;

    ThreadFromGlobalPool processing_thread;

    TestKeeperStorage storage;
    std::mutex session_id_mutex;

private:
    void processingThread();
    void finalize();
    void setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response);

public:
    TestKeeperStorageDispatcher();
    ~TestKeeperStorageDispatcher();

    void putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id);

    int64_t getSessionID()
    {
        std::lock_guard lock(session_id_mutex);
        return storage.getSessionID();
    }

    void registerSession(int64_t session_id, ZooKeeperResponseCallback callback);
    /// Call if we don't need any responses for this session no more (session was expired)
    void finishSession(int64_t session_id);
};

}
