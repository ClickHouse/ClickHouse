#pragma once

#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <future>
#include <unordered_map>
#include <unordered_set>

namespace zkutil
{

using namespace DB;
struct TestKeeperStorageRequest;
using TestKeeperStorageRequestPtr = std::shared_ptr<TestKeeperStorageRequest>;
using ResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr &)>;

class TestKeeperStorage
{

public:

    Poco::Timespan operation_timeout{0, Coordination::DEFAULT_OPERATION_TIMEOUT_MS * 1000};
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

    struct Watcher
    {
        int64_t session_id;
        ResponseCallback watch_callback;
    };

    using Container = std::map<std::string, Node>;
    using Ephemerals = std::unordered_map<int64_t, std::unordered_set<String>>;
    using SessionAndWatcher = std::unordered_map<int64_t, std::unordered_set<String>>;

    using WatchCallbacks = std::vector<Watcher>;
    using Watches = std::map<String /* path, relative of root_path */, WatchCallbacks>;

    Container container;
    Ephemerals ephemerals;
    SessionAndWatcher sessions_and_watchers;

    std::atomic<int64_t> zxid{0};
    std::atomic<bool> shutdown{false};

    Watches watches;
    Watches list_watches;   /// Watches for 'list' request (watches on children).

    using clock = std::chrono::steady_clock;

    struct RequestInfo
    {
        TestKeeperStorageRequestPtr request;
        ResponseCallback response_callback;
        ResponseCallback watch_callback;
        clock::time_point time;
        int64_t session_id;
    };

    std::mutex push_request_mutex;
    using RequestsQueue = ConcurrentBoundedQueue<RequestInfo>;
    RequestsQueue requests_queue{1};

    void finalize();

    ThreadFromGlobalPool processing_thread;

    void processingThread();
    void clearDeadWatches(int64_t session_id);

public:
    using AsyncResponse = std::future<Coordination::ZooKeeperResponsePtr>;
    TestKeeperStorage();
    ~TestKeeperStorage();
    struct ResponsePair
    {
        AsyncResponse response;
        std::optional<AsyncResponse> watch_response;
    };
    void putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, ResponseCallback callback);
    void putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, ResponseCallback callback, ResponseCallback watch_callback);

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
