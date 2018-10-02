#pragma once

#include <Core/Types.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/CurrentMetrics.h>
#include <Common/ZooKeeper/IKeeper.h>

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
  * - bugs in JIRA are fixed only occasionaly with ad-hoc patches by library users.
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


namespace Coordination
{

using namespace DB;

struct ZooKeeperRequest;



/** Usage scenario: look at the documentation for IKeeper class.
  */
class ZooKeeper : public IKeeper
{
public:
    using Addresses = std::vector<Poco::Net::SocketAddress>;

    using XID = int32_t;
    using OpNum = int32_t;

    /** Connection to addresses is performed in order. If you want, shuffle them manually.
      * Operation timeout couldn't be greater than session timeout.
      * Operation timeout applies independently for network read, network write, waiting for events and synchronization.
      */
    ZooKeeper(
        const Addresses & addresses,
        const String & root_path,
        const String & auth_scheme,
        const String & auth_data,
        Poco::Timespan session_timeout,
        Poco::Timespan connection_timeout,
        Poco::Timespan operation_timeout);

    ~ZooKeeper() override;


    /// If expired, you can only destroy the object. All other methods will throw exception.
    bool isExpired() const override { return expired; }

    /// Useful to check owner of ephemeral node.
    int64_t getSessionID() const override { return session_id; }


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
        ListCallback callback,
        WatchCallback watch) override;

    void check(
        const String & path,
        int32_t version,
        CheckCallback callback) override;

    void multi(
        const Requests & requests,
        MultiCallback callback) override;

private:
    String root_path;
    ACLs default_acls;

    Poco::Timespan session_timeout;
    Poco::Timespan operation_timeout;

    Poco::Net::StreamSocket socket;
    std::optional<ReadBufferFromPocoSocket> in;
    std::optional<WriteBufferFromPocoSocket> out;

    int64_t session_id = 0;

    std::atomic<XID> next_xid {1};
    std::atomic<bool> expired {false};
    std::mutex push_request_mutex;

    using clock = std::chrono::steady_clock;

    struct RequestInfo
    {
        std::shared_ptr<ZooKeeperRequest> request;
        ResponseCallback callback;
        WatchCallback watch;
        clock::time_point time;
    };

    using RequestsQueue = ConcurrentBoundedQueue<RequestInfo>;

    RequestsQueue requests_queue{1};
    void pushRequest(RequestInfo && request);

    using Operations = std::map<XID, RequestInfo>;

    Operations operations;
    std::mutex operations_mutex;

    using WatchCallbacks = std::vector<WatchCallback>;
    using Watches = std::map<String /* path, relative of root_path */, WatchCallbacks>;

    Watches watches;
    std::mutex watches_mutex;

    std::thread send_thread;
    std::thread receive_thread;

    void connect(
        const Addresses & addresses,
        Poco::Timespan connection_timeout);

    void sendHandshake();
    void receiveHandshake();

    void sendAuth(const String & scheme, const String & data);

    void receiveEvent();

    void sendThread();
    void receiveThread();

    void close();

    /// Call all remaining callbacks and watches, passing errors to them.
    void finalize(bool error_send, bool error_receive);

    template <typename T>
    void write(const T &);

    template <typename T>
    void read(T &);

    CurrentMetrics::Increment active_session_metric_increment{CurrentMetrics::ZooKeeperSession};
};

}
