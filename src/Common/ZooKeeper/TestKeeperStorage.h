#pragma once

#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

namespace zkutil
{

using namespace DB;

class TestKeeperStorage
{

public:
    struct TestKeeperRequest;
    using TestKeeperRequestPtr = std::shared_ptr<TestKeeperRequest>;

    std::atomic<int64_t> session_id_counter{0};

    struct Node
    {
        String data;
        Coordination::ACLs acls;
        bool is_ephemeral = false;
        bool is_sequental = false;
        Coordination::Stat stat{};
        int32_t seq_num = 0;
    };

    using Container = std::map<std::string, Node>;

    using WatchCallbacks = std::vector<Coordination::WatchCallback>;
    using Watches = std::map<String /* path, relative of root_path */, WatchCallbacks>;

    Container container;

    String root_path;

    std::atomic<int64_t> zxid{0};

    Watches watches;
    Watches list_watches;   /// Watches for 'list' request (watches on children).

    using clock = std::chrono::steady_clock;

    struct RequestInfo
    {
        TestKeeperRequestPtr request;
        Coordination::ResponseCallback callback;
        Coordination::WatchCallback watch;
        clock::time_point time;
    };
    using RequestsQueue = ConcurrentBoundedQueue<RequestInfo>;
    RequestsQueue requests_queue{1};

    void pushRequest(RequestInfo && request);

    void finalize();

    ThreadFromGlobalPool processing_thread;

    void processingThread();

    void writeResponse(const Coordination::ZooKeeperResponsePtr & response);

public:
    void putRequest(const Coordination::ZooKeeperRequestPtr & request, std::shared_ptr<WriteBuffer> response_out);

    int64_t getSessionID()
    {
        return session_id_counter.fetch_add(1);
    }
    int64_t getZXID()
    {
        return zxid.fetch_add(1);
    }
};
   
}
