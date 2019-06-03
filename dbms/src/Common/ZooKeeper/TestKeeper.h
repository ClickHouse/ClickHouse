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

namespace Coordination {
String parentPath(const String& path);

std::vector<String> children(const String& path);

using WatchCallbacks = std::vector<WatchCallback>;
using Watches = std::map<String /* path, relative of root_path */, WatchCallbacks>;

extern Watches watches;
extern Watches list_watches;
extern std::mutex watches_mutex;

struct TestKeeperRequest;

using clock = std::chrono::steady_clock;

struct RequestInfo {
    std::shared_ptr<TestKeeperRequest> request;
    ResponseCallback callback;
    WatchCallback watch;
    clock::time_point time;
};

struct TestNode
{
    String data;
    ACLs acls;
    bool is_ephemeral;
    bool is_sequental;
    Stat stat;
    int seq_num;
};

extern std::map<std::string, TestNode> architecture;;
extern long int global_zxid;

struct TestKeeperRequest;


class TestKeeper : public IKeeper {


public:
    TestKeeper(
            const String & root_path,
            Poco::Timespan operation_timeout);

    using OpNum = int32_t;

    ~TestKeeper() override;

    bool isExpired() const override { return expired; }

    int64_t getSessionID() const override { return session_id; }


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

//        void printArchitecture() override;

private:
    String root_path;
    ACLs default_acls;

    Poco::Timespan operation_timeout;

    std::mutex push_request_mutex;

    std::atomic<bool> expired{false};

    int64_t session_id = 0;



    void createWatchCallBack(const String& path);

    using RequestsQueue = ConcurrentBoundedQueue<RequestInfo>;

    RequestsQueue requests_queue{1};

    void pushRequest(RequestInfo &&request);

    void finalize();

    ThreadFromGlobalPool processing_thread;

    void processingThread();
};

struct TestKeeperResponse;
using TestKeeperResponsePtr = std::shared_ptr<TestKeeperResponse>;

struct TestKeeperRequest : virtual Request
{
    long int zxid;

    TestKeeperRequest() = default;
    TestKeeperRequest(const TestKeeperRequest &) = default;
    virtual ~TestKeeperRequest() = default;

    virtual TestKeeper::OpNum getOpNum() const = 0;

    virtual TestKeeperResponsePtr createResponse() const = 0;

    virtual TestKeeperResponsePtr makeResponse() const = 0;
};

}