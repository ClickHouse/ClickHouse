#pragma once

#include <mutex>
#include <map>
#include <atomic>
#include <thread>
#include <chrono>

#include <Poco/Timespan.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Coordination/KeeperFeatureFlags.h>


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
    explicit TestKeeper(const zkutil::ZooKeeperArgs & args_);
    ~TestKeeper() override;

    bool isExpired() const override { return expired; }
    std::optional<int8_t> getConnectedNodeIdx() const override { return 0; }
    String getConnectedHostPort() const override { return "TestKeeper:0000"; }
    int64_t getConnectionXid() const override { return 0; }
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

    void removeRecursive(
        const String & path,
        uint32_t remove_nodes_limit,
        RemoveRecursiveCallback callback) override;

    void exists(
            const String & path,
            ExistsCallback callback,
            WatchCallbackPtr watch) override;

    void get(
            const String & path,
            GetCallback callback,
            WatchCallbackPtr watch) override;

    void set(
            const String & path,
            const String & data,
            int32_t version,
            SetCallback callback) override;

    void list(
            const String & path,
            ListRequestType list_request_type,
            ListCallback callback,
            WatchCallbackPtr watch) override;

    void check(
            const String & path,
            int32_t version,
            CheckCallback callback) override;

    void sync(
            const String & path,
            SyncCallback callback) override;

    void reconfig(
        std::string_view joining,
        std::string_view leaving,
        std::string_view new_members,
        int32_t version,
        ReconfigCallback callback) final;

    void multi(
            const Requests & requests,
            MultiCallback callback) override;

    void multi(
            std::span<const RequestPtr> requests,
            MultiCallback callback) override;

    void finalize(const String & reason) override;

    bool isFeatureEnabled(DB::KeeperFeatureFlag) const override
    {
        return false;
    }

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

    using WatchCallbacks = std::unordered_set<WatchCallbackPtr>;
    using Watches = std::map<String /* path, relative of root_path */, WatchCallbacks>;

private:
    using clock = std::chrono::steady_clock;

    struct RequestInfo
    {
        TestKeeperRequestPtr request;
        ResponseCallback callback;
        WatchCallbackPtr watch;
        clock::time_point time;
    };

    Container container;

    zkutil::ZooKeeperArgs args;

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
