#pragma once

#include <Core/Types.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/CurrentMetrics.h>

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


namespace ZooKeeperImpl
{

using namespace DB;


class Exception : public DB::Exception
{
private:
    /// Delegate constructor, used to minimize repetition; last parameter used for overload resolution.
    Exception(const std::string & msg, const int32_t code, int);

public:
    explicit Exception(const int32_t code);
    Exception(const std::string & msg, const int32_t code);
    Exception(const int32_t code, const std::string & path);
    Exception(const Exception & exc);

    const char * name() const throw() override { return "ZooKeeperImpl::Exception"; }
    const char * className() const throw() override { return "ZooKeeperImpl::Exception"; }
    Exception * clone() const override { return new Exception(*this); }

    const int32_t code;
};


/** Usage scenario:
  * - create an object and issue commands;
  * - you provide callbacks for your commands; callbacks are invoked in internal thread and must be cheap:
  *   for example, just signal a condvar / fulfull a promise.
  * - you also may provide callbacks for watches; they are also invoked in internal thread and must be cheap.
  * - whenever you receive exception with ZSESSIONEXPIRED code or method isExpired returns true,
  *   the ZooKeeper instance is no longer usable - you may only destroy it and probably create another.
  * - whenever session is expired or ZooKeeper instance is destroying, all callbacks are notified with special event.
  * - data for callbacks must be alive when ZooKeeper instance is alive.
  */
class ZooKeeper
{
public:
    using Addresses = std::vector<Poco::Net::SocketAddress>;

    struct ACL
    {
        static constexpr int32_t Read = 1;
        static constexpr int32_t Write = 2;
        static constexpr int32_t Create = 4;
        static constexpr int32_t Delete = 8;
        static constexpr int32_t Admin = 16;
        static constexpr int32_t All = 0x1F;

        int32_t permissions;
        String scheme;
        String id;

        void write(WriteBuffer & out) const;
    };
    using ACLs = std::vector<ACL>;

    struct Stat
    {
        int64_t czxid;
        int64_t mzxid;
        int64_t ctime;
        int64_t mtime;
        int32_t version;
        int32_t cversion;
        int32_t aversion;
        int64_t ephemeralOwner;
        int32_t dataLength;
        int32_t numChildren;
        int64_t pzxid;

        void read(ReadBuffer & in);
    };

    using XID = int32_t;
    using OpNum = int32_t;


    struct Response
    {
        int32_t error = 0;
        virtual ~Response() {}
        virtual void readImpl(ReadBuffer &) = 0;

        virtual void removeRootPath(const String & /* root_path */) {}
    };

    using ResponsePtr = std::shared_ptr<Response>;
    using Responses = std::vector<ResponsePtr>;
    using ResponseCallback = std::function<void(const Response &)>;

    struct Request
    {
        XID xid = 0;
        bool has_watch = false;

        virtual ~Request() {}
        virtual OpNum getOpNum() const = 0;

        /// Writes length, xid, op_num, then the rest.
        void write(WriteBuffer & out) const;
        virtual void writeImpl(WriteBuffer &) const = 0;

        virtual ResponsePtr makeResponse() const = 0;

        virtual void addRootPath(const String & /* root_path */) {}
        virtual String getPath() const = 0;
    };

    using RequestPtr = std::shared_ptr<Request>;
    using Requests = std::vector<RequestPtr>;

    struct HeartbeatRequest final : Request
    {
        OpNum getOpNum() const override { return 11; }
        void writeImpl(WriteBuffer &) const override {}
        ResponsePtr makeResponse() const override;
        String getPath() const override { return {}; }
    };

    struct HeartbeatResponse final : Response
    {
        void readImpl(ReadBuffer &) override {}
    };

    struct WatchResponse final : Response
    {
        int32_t type = 0;
        int32_t state = 0;
        String path;

        void readImpl(ReadBuffer &) override;
        void removeRootPath(const String & root_path) override;
    };

    using WatchCallback = std::function<void(const WatchResponse &)>;

    struct AuthRequest final : Request
    {
        int32_t type = 0;   /// ignored by the server
        String scheme;
        String data;

        OpNum getOpNum() const override { return 100; }
        void writeImpl(WriteBuffer &) const override;
        ResponsePtr makeResponse() const override;
        String getPath() const override { return {}; }
    };

    struct AuthResponse final : Response
    {
        void readImpl(ReadBuffer &) override {}
    };

    struct CloseRequest final : Request
    {
        OpNum getOpNum() const override { return -11; }
        void writeImpl(WriteBuffer &) const override {}
        ResponsePtr makeResponse() const override;
        String getPath() const override { return {}; }
    };

    struct CloseResponse final : Response
    {
        void readImpl(ReadBuffer &) override;
    };

    struct CreateRequest final : Request
    {
        String path;
        String data;
        bool is_ephemeral = false;
        bool is_sequential = false;
        ACLs acls;

        OpNum getOpNum() const override { return 1; }
        void writeImpl(WriteBuffer &) const override;
        ResponsePtr makeResponse() const override;
        void addRootPath(const String & root_path) override;
        String getPath() const override { return path; }
    };

    struct CreateResponse final : Response
    {
        String path_created;

        void readImpl(ReadBuffer &) override;
        void removeRootPath(const String & root_path) override;
    };

    struct RemoveRequest final : Request
    {
        String path;
        int32_t version = -1;

        OpNum getOpNum() const override { return 2; }
        void writeImpl(WriteBuffer &) const override;
        ResponsePtr makeResponse() const override;
        void addRootPath(const String & root_path) override;
        String getPath() const override { return path; }
    };

    struct RemoveResponse final : Response
    {
        void readImpl(ReadBuffer &) override {}
    };

    struct ExistsRequest final : Request
    {
        String path;

        OpNum getOpNum() const override { return 3; }
        void writeImpl(WriteBuffer &) const override;
        ResponsePtr makeResponse() const override;
        void addRootPath(const String & root_path) override;
        String getPath() const override { return path; }
    };

    struct ExistsResponse final : Response
    {
        Stat stat;

        void readImpl(ReadBuffer &) override;
    };

    struct GetRequest final : Request
    {
        String path;

        OpNum getOpNum() const override { return 4; }
        void writeImpl(WriteBuffer &) const override;
        ResponsePtr makeResponse() const override;
        void addRootPath(const String & root_path) override;
        String getPath() const override { return path; }
    };

    struct GetResponse final : Response
    {
        String data;
        Stat stat;

        void readImpl(ReadBuffer &) override;
    };

    struct SetRequest final : Request
    {
        String path;
        String data;
        int32_t version = -1;

        OpNum getOpNum() const override { return 5; }
        void writeImpl(WriteBuffer &) const override;
        ResponsePtr makeResponse() const override;
        void addRootPath(const String & root_path) override;
        String getPath() const override { return path; }
    };

    struct SetResponse final : Response
    {
        Stat stat;

        void readImpl(ReadBuffer &) override;
    };

    struct ListRequest final : Request
    {
        String path;

        OpNum getOpNum() const override { return 12; }
        void writeImpl(WriteBuffer &) const override;
        ResponsePtr makeResponse() const override;
        void addRootPath(const String & root_path) override;
        String getPath() const override { return path; }
    };

    struct ListResponse final : Response
    {
        std::vector<String> names;
        Stat stat;

        void readImpl(ReadBuffer &) override;
    };

    struct CheckRequest final : Request
    {
        String path;
        int32_t version = -1;

        OpNum getOpNum() const override { return 13; }
        void writeImpl(WriteBuffer &) const override;
        ResponsePtr makeResponse() const override;
        void addRootPath(const String & root_path) override;
        String getPath() const override { return path; }
    };

    struct CheckResponse final : Response
    {
        void readImpl(ReadBuffer &) override {}
    };

    struct MultiRequest final : Request
    {
        Requests requests;

        OpNum getOpNum() const override { return 14; }
        void writeImpl(WriteBuffer &) const override;
        ResponsePtr makeResponse() const override;
        void addRootPath(const String & root_path) override;
        String getPath() const override { return {}; }
    };

    struct MultiResponse final : Response
    {
        Responses responses;

        MultiResponse(const Requests & requests);

        void readImpl(ReadBuffer &) override;
        void removeRootPath(const String & root_path) override;
    };

    /// This response may be received only as an element of responses in MultiResponse.
    struct ErrorResponse final : Response
    {
        void readImpl(ReadBuffer &) override;
    };


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

    ~ZooKeeper();


    /// If expired, you can only destroy the object. All other methods will throw exception.
    bool isExpired() const { return expired; }

    /// Useful to check owner of ephemeral node.
    int64_t getSessionID() const { return session_id; }


    using CreateCallback = std::function<void(const CreateResponse &)>;
    using RemoveCallback = std::function<void(const RemoveResponse &)>;
    using ExistsCallback = std::function<void(const ExistsResponse &)>;
    using GetCallback = std::function<void(const GetResponse &)>;
    using SetCallback = std::function<void(const SetResponse &)>;
    using ListCallback = std::function<void(const ListResponse &)>;
    using CheckCallback = std::function<void(const CheckResponse &)>;
    using MultiCallback = std::function<void(const MultiResponse &)>;

    /// If the method will throw an exception, callbacks won't be called.
    ///
    /// After the method is executed successfully, you must wait for callbacks
    ///  (don't destroy callback data before it will be called).
    ///
    /// All callbacks are executed sequentially (the execution of callbacks is serialized).
    ///
    /// If an exception is thrown inside the callback, the session will expire,
    ///  and all other callbacks will be called with "Session expired" error.

    void create(
        const String & path,
        const String & data,
        bool is_ephemeral,
        bool is_sequential,
        const ACLs & acls,
        CreateCallback callback);

    void remove(
        const String & path,
        int32_t version,
        RemoveCallback callback);

    void exists(
        const String & path,
        ExistsCallback callback,
        WatchCallback watch);

    void get(
        const String & path,
        GetCallback callback,
        WatchCallback watch);

    void set(
        const String & path,
        const String & data,
        int32_t version,
        SetCallback callback);

    void list(
        const String & path,
        ListCallback callback,
        WatchCallback watch);

    void check(
        const String & path,
        int32_t version,
        CheckCallback callback);

    void multi(
        const Requests & requests,
        MultiCallback callback);


    enum Error
    {
        ZOK = 0,

        /** System and server-side errors.
          * This is never thrown by the server, it shouldn't be used other than
          * to indicate a range. Specifically error codes greater than this
          * value, but lesser than ZAPIERROR, are system errors.
          */
        ZSYSTEMERROR = -1,

        ZRUNTIMEINCONSISTENCY = -2, /// A runtime inconsistency was found
        ZDATAINCONSISTENCY = -3,    /// A data inconsistency was found
        ZCONNECTIONLOSS = -4,       /// Connection to the server has been lost
        ZMARSHALLINGERROR = -5,     /// Error while marshalling or unmarshalling data
        ZUNIMPLEMENTED = -6,        /// Operation is unimplemented
        ZOPERATIONTIMEOUT = -7,     /// Operation timeout
        ZBADARGUMENTS = -8,         /// Invalid arguments
        ZINVALIDSTATE = -9,         /// Invliad zhandle state

        /** API errors.
          * This is never thrown by the server, it shouldn't be used other than
          * to indicate a range. Specifically error codes greater than this
          * value are API errors.
          */
        ZAPIERROR = -100,

        ZNONODE = -101,                     /// Node does not exist
        ZNOAUTH = -102,                     /// Not authenticated
        ZBADVERSION = -103,                 /// Version conflict
        ZNOCHILDRENFOREPHEMERALS = -108,    /// Ephemeral nodes may not have children
        ZNODEEXISTS = -110,                 /// The node already exists
        ZNOTEMPTY = -111,                   /// The node has children
        ZSESSIONEXPIRED = -112,             /// The session has been expired by the server
        ZINVALIDCALLBACK = -113,            /// Invalid callback specified
        ZINVALIDACL = -114,                 /// Invalid ACL specified
        ZAUTHFAILED = -115,                 /// Client authentication failed
        ZCLOSING = -116,                    /// ZooKeeper is closing
        ZNOTHING = -117,                    /// (not error) no server responses to process
        ZSESSIONMOVED = -118                /// Session moved to another server, so operation is ignored
    };

    static const char * errorMessage(int32_t code);

    /// For watches.
    enum State
    {
        EXPIRED_SESSION = -112,
        AUTH_FAILED = -113,
        CONNECTING = 1,
        ASSOCIATING = 2,
        CONNECTED = 3,
        NOTCONNECTED = 999
    };

    enum Event
    {
        CREATED = 1,
        DELETED = 2,
        CHANGED = 3,
        CHILD = 4,
        SESSION = -1,
        NOTWATCHING = -2
    };

private:
    String root_path;
    ACLs default_acls;

    Poco::Timespan session_timeout;
    Poco::Timespan operation_timeout;

    Poco::Net::StreamSocket socket;
    std::optional<ReadBufferFromPocoSocket> in;
    std::optional<WriteBufferFromPocoSocket> out;

    int64_t session_id = 0;

    std::atomic<XID> xid {1};
    std::atomic<bool> expired {false};
    std::mutex push_request_mutex;

    using clock = std::chrono::steady_clock;

    struct RequestInfo
    {
        RequestPtr request;
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

};
