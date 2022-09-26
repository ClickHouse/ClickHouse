#pragma once

#include <mutex>
#include <map>
#include <atomic>
#include <thread>
#include <chrono>

#include <Poco/Timespan.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>


namespace Coordination
{

struct TestKeeperRequest;
using TestKeeperRequestPtr = std::shared_ptr<TestKeeperRequest>;


/** Looks like ZooKeeper but stores all data in memory of server process.
  * All data is not shared between different servers and is lost after server restart.
  *
  * The only purpose is to more simple testing for interaction with ZooKeeper within a single server.
  * This still makes sense, because multiple replicas of a single table can be created on a single server,
  *  and it is used to test replication logic.
  *
  * Does not support ACLs. Does not support NULL node values.
  *
  * NOTE: You can add various failure modes for better testing.
  */
class TestKeeper final : public IKeeper
{
public:
    TestKeeper(const String & root_path_, Poco::Timespan operation_timeout_);
    ~TestKeeper() override;

    bool isExpired() const override { return expired; }
    int64_t getSessionID() const override { return 0; }


    void create(
            const String & path,
            const String & data,
            bool is_ephemeral,
            bool is_sequential,
            const ACLs & acls,
            CreateCallback callback) override;

    void remove(
            const String & path,
            int32_t version,
            RemoveCallback callback) override;

    void exists(
            const String & path,
            ExistsCallback callback,
            WatchCallback watch) override;

    void get(
            const String & path,
            GetCallback callback,
            WatchCallback watch) override;

    void set(
            const String & path,
            const String & data,
            int32_t version,
            SetCallback callback) override;

    void list(
            const String & path,
            ListCallback callback,
            WatchCallback watch) override;

    void check(
            const String & path,
            int32_t version,
            CheckCallback callback) override;

    void multi(
            const Requests & requests,
            MultiCallback callback) override;

    void finalize(const String & reason) override;

    struct Node
    {
        String data;
        ACLs acls;
        bool is_ephemeral = false;
        bool is_sequental = false;
        Stat stat{};
        int32_t seq_num = 0;
    };

    using Container = std::map<std::string, Node>;

    using WatchCallbacks = std::vector<WatchCallback>;
    using Watches = std::map<String /* path, relative of root_path */, WatchCallbacks>;

private:
    using clock = std::chrono::steady_clock;

    struct RequestInfo
    {
        TestKeeperRequestPtr request;
        ResponseCallback callback;
        WatchCallback watch;
        clock::time_point time;
    };

    Container container;

    String root_path;
    ACLs default_acls;

    Poco::Timespan operation_timeout;

    std::mutex push_request_mutex;
    std::atomic<bool> expired{false};

    int64_t zxid = 0;

    Watches watches;
    Watches list_watches;   /// Watches for 'list' request (watches on children).

    using RequestsQueue = ConcurrentBoundedQueue<RequestInfo>;
    RequestsQueue requests_queue{1};

    void pushRequest(RequestInfo && request);


    ThreadFromGlobalPool processing_thread;

    void processingThread();
};

}

