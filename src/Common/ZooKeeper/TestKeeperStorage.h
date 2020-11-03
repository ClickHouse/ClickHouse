#pragma once

#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <future>

namespace zkutil
{

using namespace DB;
struct TestKeeperStorageRequest;
using TestKeeperStorageRequestPtr = std::shared_ptr<TestKeeperStorageRequest>;

class TestKeeperStorage
{

public:

    Poco::Timespan operation_timeout{10000};
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
    std::atomic<bool> shutdown{false};

    Watches watches;
    Watches list_watches;   /// Watches for 'list' request (watches on children).

    using clock = std::chrono::steady_clock;

    struct RequestInfo
    {
        TestKeeperStorageRequestPtr request;
        std::function<void(const Coordination::ZooKeeperResponsePtr &)> response_callback;
        clock::time_point time;
    };
    std::mutex push_request_mutex;
    using RequestsQueue = ConcurrentBoundedQueue<RequestInfo>;
    RequestsQueue requests_queue{1};

    void finalize();

    ThreadFromGlobalPool processing_thread;

    void processingThread();

public:
    using AsyncResponse = std::future<Coordination::ZooKeeperResponsePtr>;
    TestKeeperStorage();
    ~TestKeeperStorage();
    AsyncResponse putRequest(const Coordination::ZooKeeperRequestPtr & request);

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
