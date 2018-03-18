#pragma once

#include <Common/Types.h>
#include <Common/ConcurrentBoundedQueue.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SocketAddress.h>

#include <map>
#include <vector>
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

        void write(WriteBuffer & out);
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

    struct Request
    {
        virtual ~Request() {};
        virtual int32_t op_num() const = 0;
        virtual void write(WriteBuffer & out) const = 0;
    };

    using RequestPtr = std::unique_ptr<Request>;
    using Requests = std::vector<RequestPtr>;

    struct Response
    {
        virtual ~Response() {}
        virtual void read(ReadBuffer & in) = 0;
    };

    using ResponsePtr = std::unique_ptr<Response>;
    using ResponseCallback = std::function<void(const Response &)>;

    struct HeartbeatRequest : Request
    {
        void write(WriteBuffer & out) const override;
    };

    struct HeartbeatResponse : Response
    {
        void read(ReadBuffer & in) override;
    };

    struct CreateRequest : Request
    {
        String path;
        String data;
        bool is_ephemeral;
        bool is_sequential;
        ACLs acls;

        void write(WriteBuffer & out) const override;
    };

    struct CreateResponse : Response
    {
        String path_created;

        void read(ReadBuffer & in) override;
    };

    using CreateCallback = std::function<void(const CreateResponse &)>;

    struct RemoveRequest : Request
    {
        String path;

        void write(WriteBuffer & out) const override;
    };

    struct RemoveResponse : Response
    {
        void read(ReadBuffer & in) override {}
    };

    struct ExistsRequest : Request
    {
        static constexpr int32_t op_num = 3;

        String path;

        void write(WriteBuffer & out) const override;
    };

    struct ExistsResponse : Response
    {
        Stat stat;

        void read(ReadBuffer & in) override;
    };

    struct GetRequest : Request
    {
        static constexpr int32_t op_num = 4;

        String path;

        void write(WriteBuffer & out) const override;
    };

    struct GetResponse : Response
    {
        String data;
        Stat stat;

        void read(ReadBuffer & in) override;
    };

    using RemoveCallback = std::function<void(const RemoveResponse &)>;

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

    ConcurrentBoundedQueue<RequestPtr> requests(1);

    using XID = uint32_t;
    using ResponseCallbacks = std::map<XID, ResponseCallback>;

    ResponseCallbacks response_callbacks;
    std::mutex response_callbacks_mutex;

    std::thread send_thread;
    std::thread receive_thread;

    std::atomic<bool> stop {false};
    std::atomic<bool> expired {false};

    void connect();
    void sendHandshake();
    void receiveHandshake();
    void sendAuth(XID xid, const String & auth_scheme, const String & auth_data);
    void sendRequest(XID xid, const Request & request);
    void receiveEvent();

    void sendThread(ReadBuffer & in);
    void receiveThread(ReadBuffer & in);

    template <typename T>
    void write(const T &);

    template <typename T>
    void read(T &);
};

};
