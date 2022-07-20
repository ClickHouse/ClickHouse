#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SocketAddress.h>

#include <map>
#include <mutex>
#include <chrono>
#include <vector>
#include <memory>
#include <thread>
#include <atomic>
#include <cstdint>
#include <optional>
#include <functional>


/** ZooKeeper C++ library, a replacement for libzookeeper.
  *
  * Motivation.
  *
  * libzookeeper has many bugs:
  * - segfaults: for example, if zookeeper connection was interrupted while reading result of multi response;
  * - memory corruption: for example, as a result of double free inside libzookeeper;
  * - no timeouts for synchronous operations: they may stuck forever under simple Jepsen-like tests;
  * - logical errors: for example, chroot prefix is not removed from the results of multi responses.
  * - data races;
  *
  * The code of libzookeeper is over complicated:
  * - memory ownership is unclear and bugs are very difficult to track and fix.
  * - extremely creepy code for implementation of "chroot" feature.
  *
  * As of 2018, there are no active maintainers of libzookeeper:
  * - bugs in JIRA are fixed only occasionally with ad-hoc patches by library users.
  *
  * libzookeeper is a classical example of bad code written in C.
  *
  * In Go, Python and Rust programming languages,
  *  there are separate libraries for ZooKeeper, not based on libzookeeper.
  * Motivation is almost the same. Example:
  * https://github.com/python-zk/kazoo/blob/master/docs/implementation.rst
  *
  * About "session restore" feature.
  *
  * libzookeeper has the feature of session restore. Client receives session id and session token from the server,
  *  and when connection is lost, it can quickly reconnect to any server with the same session id and token,
  *  to continue with existing session.
  * libzookeeper performs this reconnection automatically.
  *
  * This feature is proven to be harmful.
  * For example, it makes very difficult to correctly remove ephemeral nodes.
  * This may lead to weird bugs in application code.
  * For example, our developers have found that type of bugs in Curator Java library.
  *
  * On the other side, session restore feature has no advantages,
  *  because every application should be able to establish new session and reinitialize internal state,
  *  when the session is lost and cannot be restored.
  *
  * This library never restores the session. In case of any error, the session is considered as expired
  *  and you should create a new instance of ZooKeeper object and reinitialize the application state.
  *
  * This library is not intended to be CPU efficient. Hundreds of thousands operations per second is usually enough.
  */


namespace CurrentMetrics
{
    extern const Metric ZooKeeperSession;
}

namespace DB
{
    class ZooKeeperLog;
}

namespace Coordination
{

using namespace DB;

/** Usage scenario: look at the documentation for IKeeper class.
  */
class ZooKeeper final : public IKeeper
{
public:
    struct Node
    {
        Poco::Net::SocketAddress address;
        bool secure;
    };

    using Nodes = std::vector<Node>;

    /** Connection to nodes is performed in order. If you want, shuffle them manually.
      * Operation timeout couldn't be greater than session timeout.
      * Operation timeout applies independently for network read, network write, waiting for events and synchronization.
      */
    ZooKeeper(
        const Nodes & nodes,
        const String & root_path,
        const String & auth_scheme,
        const String & auth_data,
        Poco::Timespan session_timeout_,
        Poco::Timespan connection_timeout,
        Poco::Timespan operation_timeout_,
        std::shared_ptr<ZooKeeperLog> zk_log_);

    ~ZooKeeper() override;


    /// If expired, you can only destroy the object. All other methods will throw exception.
    bool isExpired() const override { return requests_queue.isFinished(); }

    /// Useful to check owner of ephemeral node.
    int64_t getSessionID() const override { return session_id; }

    void executeGenericRequest(
        const ZooKeeperRequestPtr & request,
        ResponseCallback callback);

    /// See the documentation about semantics of these methods in IKeeper class.

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
        ListRequestType list_request_type,
        ListCallback callback,
        WatchCallback watch) override;

    void check(
        const String & path,
        int32_t version,
        CheckCallback callback) override;

    void sync(
         const String & path,
         SyncCallback callback) override;

    void multi(
        const Requests & requests,
        MultiCallback callback) override;

    /// Without forcefully invalidating (finalizing) ZooKeeper session before
    /// establishing a new one, there was a possibility that server is using
    /// two ZooKeeper sessions simultaneously in different parts of code.
    /// This is strong antipattern and we always prevented it.

    /// ZooKeeper is linearizeable for writes, but not linearizeable for
    /// reads, it only maintains "sequential consistency": in every session
    /// you observe all events in order but possibly with some delay. If you
    /// perform write in one session, then notify different part of code and
    /// it will do read in another session, that read may not see the
    /// already performed write.

    void finalize(const String & reason)  override { finalize(false, false, reason); }

    void setZooKeeperLog(std::shared_ptr<DB::ZooKeeperLog> zk_log_);

private:
    String root_path;
    ACLs default_acls;

    Poco::Timespan session_timeout;
    Poco::Timespan operation_timeout;

    Poco::Net::StreamSocket socket;
    /// To avoid excessive getpeername(2) calls.
    Poco::Net::SocketAddress socket_address;
    std::optional<ReadBufferFromPocoSocket> in;
    std::optional<WriteBufferFromPocoSocket> out;

    int64_t session_id = 0;

    std::atomic<XID> next_xid {1};
    /// Mark session finalization start. Used to avoid simultaneous
    /// finalization from different threads. One-shot flag.
    std::atomic_flag finalization_started;

    using clock = std::chrono::steady_clock;

    struct RequestInfo
    {
        ZooKeeperRequestPtr request;
        ResponseCallback callback;
        WatchCallback watch;
        clock::time_point time;
    };

    using RequestsQueue = ConcurrentBoundedQueue<RequestInfo>;

    RequestsQueue requests_queue{1024};
    void pushRequest(RequestInfo && info);

    using Operations = std::map<XID, RequestInfo>;

    Operations operations TSA_GUARDED_BY(operations_mutex);
    std::mutex operations_mutex;

    using WatchCallbacks = std::vector<WatchCallback>;
    using Watches = std::map<String /* path, relative of root_path */, WatchCallbacks>;

    Watches watches TSA_GUARDED_BY(watches_mutex);
    std::mutex watches_mutex;

    ThreadFromGlobalPool send_thread;
    ThreadFromGlobalPool receive_thread;

    Poco::Logger * log;

    void connect(
        const Nodes & node,
        Poco::Timespan connection_timeout);

    void sendHandshake();
    void receiveHandshake();

    void sendAuth(const String & scheme, const String & data);

    void receiveEvent();

    void sendThread();
    void receiveThread();

    void close();

    /// Call all remaining callbacks and watches, passing errors to them.
    void finalize(bool error_send, bool error_receive, const String & reason);

    template <typename T>
    void write(const T &);

    template <typename T>
    void read(T &);

    void logOperationIfNeeded(const ZooKeeperRequestPtr & request, const ZooKeeperResponsePtr & response = nullptr, bool finalize = false);

    CurrentMetrics::Increment active_session_metric_increment{CurrentMetrics::ZooKeeperSession};
    std::shared_ptr<ZooKeeperLog> zk_log;
};

}
