#pragma once

#include <Core/Types.h>
#include <Common/ConcurrentBoundedQueue.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SocketAddress.h>

#include <map>
#include <vector>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <optional>


namespace ZooKeeperImpl
{

using namespace DB;

/** Usage scenario:
  * - create an object and issue commands;
  * - you provide callbacks for your commands; callbacks are invoked in internal thread and must be cheap:
  *   for example, just signal a condvar / fulfull a promise.
  * - you also may provide callbacks for watches; they are also invoked in internal thread and must be cheap.
  * - whenever you receive SessionExpired exception of method isValid returns false,
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
        int32_t permissions;
        String scheme;
        String id;

        void write(WriteBuffer & out) const;
    };
    using ACLs = std::vector<ACL>;

    using WatchCallback = std::function<void()>;

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
    };

    using ResponsePtr = std::shared_ptr<Response>;
    using ResponseCallback = std::function<void(const Response &)>;

    struct Request
    {
        XID xid;

        virtual ~Request() {};
        virtual OpNum getOpNum() const = 0;

        /// Writes length, xid, op_num, then the rest.
        void write(WriteBuffer & out) const;
        virtual void writeImpl(WriteBuffer &) const = 0;

        virtual ResponsePtr makeResponse() const = 0;
    };

    using RequestPtr = std::shared_ptr<Request>;
    using Requests = std::vector<RequestPtr>;

    struct HeartbeatRequest final : Request
    {
        OpNum getOpNum() const override { return 11; }
        void writeImpl(WriteBuffer &) const override {}
        ResponsePtr makeResponse() const override;
    };

    struct HeartbeatResponse final : Response
    {
        void readImpl(ReadBuffer &) override {}
    };

    struct CloseRequest final : Request
    {
        OpNum getOpNum() const override { return -11; }
        void writeImpl(WriteBuffer &) const override {}
        ResponsePtr makeResponse() const override;
    };

    struct CreateRequest final : Request
    {
        String path;
        String data;
        bool is_ephemeral;
        bool is_sequential;
        ACLs acls;

        OpNum getOpNum() const override { return 1; }
        void writeImpl(WriteBuffer &) const override;
        ResponsePtr makeResponse() const override;
    };

    struct CreateResponse final : Response
    {
        String path_created;

        void readImpl(ReadBuffer &) override;
    };

    using CreateCallback = std::function<void(const CreateResponse &)>;

    struct RemoveRequest final : Request
    {
        String path;

        OpNum getOpNum() const override { return 2; }
        void writeImpl(WriteBuffer &) const override;
        ResponsePtr makeResponse() const override;
    };

    struct RemoveResponse final : Response
    {
        void readImpl(ReadBuffer &) override {}
    };

    using RemoveCallback = std::function<void(const RemoveResponse &)>;

    struct ExistsRequest final : Request
    {
        String path;

        OpNum getOpNum() const override { return 3; }
        void writeImpl(WriteBuffer &) const override;
        ResponsePtr makeResponse() const override;
    };

    struct ExistsResponse final : Response
    {
        Stat stat;

        void readImpl(ReadBuffer &) override;
    };

    using ExistsCallback = std::function<void(const ExistsResponse &)>;

    struct GetRequest final : Request
    {
        String path;

        OpNum getOpNum() const override { return 4; }
        void writeImpl(WriteBuffer &) const override;
        ResponsePtr makeResponse() const override;
    };

    struct GetResponse final : Response
    {
        String data;
        Stat stat;

        void readImpl(ReadBuffer &) override;
    };

    using GetCallback = std::function<void(const GetResponse &)>;

    /// Connection to addresses is performed in order. If you want, shuffle them manually.
    ZooKeeper(
        const Addresses & addresses,
        const String & root_path,
        const String & auth_scheme,
        const String & auth_data,
        Poco::Timespan session_timeout,
        Poco::Timespan connection_timeout);

    ~ZooKeeper();

    /// If not valid, you can only destroy the object. All other methods will throw exception.
    bool isValid() const { return !expired; }

    void create(
        const String & path,
        const String & data,
        bool is_ephemeral,
        bool is_sequential,
        const ACLs & acls,
        CreateCallback callback);

    void remove(
        const String & path);

    void exists(
        const String & path);

    void get(
        const String & path);

    void set(
        const String & path,
        const String & data);

    void list(
        const String & path);

    void check(
        const String & path);

    void multi();

    void close();

private:
    String root_path;
    Poco::Timespan session_timeout;

    Poco::Net::StreamSocket socket;
    std::optional<ReadBufferFromPocoSocket> in;
    std::optional<WriteBufferFromPocoSocket> out;

    struct RequestInfo
    {
        RequestPtr request;
        ResponseCallback callback;
    };

    using RequestsQueue = ConcurrentBoundedQueue<RequestInfo>;

    RequestsQueue requests{1};

    using Operations = std::map<XID, RequestInfo>;

    Operations operations;
    std::mutex operations_mutex;

    std::thread send_thread;
    std::thread receive_thread;

    std::atomic<bool> stop {false};
    std::atomic<bool> expired {false};

    String addRootPath(const String &);
    String removeRootPath(const String &);

    void connect(
        const Addresses & addresses,
        Poco::Timespan connection_timeout);

    void sendHandshake();
    void receiveHandshake();

    void sendAuth(XID xid, const String & auth_scheme, const String & auth_data);

    void receiveEvent();

    void sendThread();
    void receiveThread();

    template <typename T>
    void write(const T &);

    template <typename T>
    void read(T &);
};

};
