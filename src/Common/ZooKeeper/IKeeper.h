#pragma once

#include <base/types.h>
#include <Common/Exception.h>
#include <Coordination/KeeperFeatureFlags.h>
#include <Poco/Net/SocketAddress.h>

#include <vector>
#include <memory>
#include <cstdint>
#include <span>
#include <functional>

/** Generic interface for ZooKeeper-like services.
  * Possible examples are:
  * - ZooKeeper client itself;
  * - fake ZooKeeper client for testing;
  * - ZooKeeper emulation layer on top of Etcd, FoundationDB, whatever.
  */

namespace DB
{
namespace ErrorCodes
{
    extern const int KEEPER_EXCEPTION;
}
}

namespace Coordination
{

using namespace DB;


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

    bool operator<(const ACL & other) const
    {
        return std::tuple(permissions, scheme, id)
            < std::tuple(other.permissions, other.scheme, other.id);
    }
};

using ACLs = std::vector<ACL>;

struct Stat
{
    int64_t czxid{0};
    int64_t mzxid{0};
    int64_t ctime{0};
    int64_t mtime{0};
    int32_t version{0};
    int32_t cversion{0};
    int32_t aversion{0};
    int64_t ephemeralOwner{0}; /// NOLINT
    int32_t dataLength{0}; /// NOLINT
    int32_t numChildren{0}; /// NOLINT
    int64_t pzxid{0};

    bool operator==(const Stat &) const = default;
};

enum class Error : int32_t
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
    ZINVALIDSTATE = -9,         /// Invalid zhandle state

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
    ZSESSIONMOVED = -118,               /// Session moved to another server, so operation is ignored
    ZNOTREADONLY = -119,                /// State-changing request is passed to read-only server
};

/// Network errors and similar. You should reinitialize ZooKeeper session in case of these errors
bool isHardwareError(Error code);

/// Valid errors sent from the server about database state (like "no node"). Logical and authentication errors (like "bad arguments") are not here.
bool isUserError(Error code);

const char * errorMessage(Error code);

struct Request;
using RequestPtr = std::shared_ptr<Request>;
using Requests = std::vector<RequestPtr>;

struct Request
{
    Request() = default;
    Request(const Request &) = default;
    Request & operator=(const Request &) = default;
    virtual ~Request() = default;
    virtual String getPath() const = 0;
    virtual void addRootPath(const String & /* root_path */) {}
    virtual size_t bytesSize() const { return 0; }
};

struct Response;
using ResponsePtr = std::shared_ptr<Response>;
using Responses = std::vector<ResponsePtr>;
using ResponseCallback = std::function<void(const Response &)>;

struct Response
{
    Error error = Error::ZOK;
    int64_t zxid = 0;

    Response() = default;
    Response(const Response &) = default;
    Response & operator=(const Response &) = default;
    virtual ~Response() = default;
    virtual void removeRootPath(const String & /* root_path */) {}
    virtual size_t bytesSize() const { return 0; }
};

struct WatchResponse : virtual Response
{
    int32_t type = 0;
    int32_t state = 0;
    String path;

    void removeRootPath(const String & root_path) override;

    size_t bytesSize() const override { return path.size() + sizeof(type) + sizeof(state); }
};

using WatchCallback = std::function<void(const WatchResponse &)>;
/// Passing watch callback as a shared_ptr allows to
///  - avoid copying of the callback
///  - registering the same callback only once per path
using WatchCallbackPtr = std::shared_ptr<WatchCallback>;

struct SetACLRequest : virtual Request
{
    String path;
    ACLs acls;
    int32_t version = -1;

    void addRootPath(const String & root_path) override;
    String getPath() const override { return path; }
    size_t bytesSize() const override { return path.size() + sizeof(version) + acls.size() * sizeof(ACL); }
};

struct SetACLResponse : virtual Response
{
    Stat stat;

    size_t bytesSize() const override { return sizeof(Stat); }
};

struct GetACLRequest : virtual Request
{
    String path;

    void addRootPath(const String & root_path) override;
    String getPath() const override { return path; }
    size_t bytesSize() const override { return path.size(); }
};

struct GetACLResponse : virtual Response
{
    ACLs acl;
    Stat stat;
    size_t bytesSize() const override { return sizeof(Stat) + acl.size() * sizeof(ACL); }
};

struct CreateRequest : virtual Request
{
    String path;
    String data;
    bool is_ephemeral = false;
    bool is_sequential = false;
    ACLs acls;

    /// should it succeed if node already exists
    bool not_exists = false;

    void addRootPath(const String & root_path) override;
    String getPath() const override { return path; }

    size_t bytesSize() const override { return path.size() + data.size()
            + sizeof(is_ephemeral) + sizeof(is_sequential) + acls.size() * sizeof(ACL); }
};

struct CreateResponse : virtual Response
{
    String path_created;

    void removeRootPath(const String & root_path) override;

    size_t bytesSize() const override { return path_created.size(); }
};

struct RemoveRequest : virtual Request
{
    String path;
    int32_t version = -1;

    void addRootPath(const String & root_path) override;
    String getPath() const override { return path; }

    size_t bytesSize() const override { return path.size() + sizeof(version); }
};

struct RemoveResponse : virtual Response
{
};

struct RemoveRecursiveRequest : virtual Request
{
    String path;

    /// strict limit for number of deleted nodes
    uint32_t remove_nodes_limit = 1;

    void addRootPath(const String & root_path) override;
    String getPath() const override { return path; }

    size_t bytesSize() const override { return path.size() + sizeof(remove_nodes_limit); }
};

struct RemoveRecursiveResponse : virtual Response
{
};

struct ExistsRequest : virtual Request
{
    String path;

    void addRootPath(const String & root_path) override;
    String getPath() const override { return path; }

    size_t bytesSize() const override { return path.size(); }
};

struct ExistsResponse : virtual Response
{
    Stat stat;

    size_t bytesSize() const override { return sizeof(Stat); }
};

struct GetRequest : virtual Request
{
    String path;

    void addRootPath(const String & root_path) override;
    String getPath() const override { return path; }

    size_t bytesSize() const override { return path.size(); }
};

struct GetResponse : virtual Response
{
    String data;
    Stat stat;

    size_t bytesSize() const override { return data.size() + sizeof(stat); }
};

struct SetRequest : virtual Request
{
    String path;
    String data;
    int32_t version = -1;

    void addRootPath(const String & root_path) override;
    String getPath() const override { return path; }

    size_t bytesSize() const override { return path.size() + data.size() + sizeof(version); }
};

struct SetResponse : virtual Response
{
    Stat stat;

    size_t bytesSize() const override { return sizeof(stat); }
};

enum class ListRequestType : uint8_t
{
    ALL,
    PERSISTENT_ONLY,
    EPHEMERAL_ONLY
};

struct ListRequest : virtual Request
{
    String path;

    void addRootPath(const String & root_path) override;
    String getPath() const override { return path; }

    size_t bytesSize() const override { return path.size(); }
};

struct ListResponse : virtual Response
{
    std::vector<String> names;
    Stat stat;

    size_t bytesSize() const override
    {
        size_t size = sizeof(stat);
        for (const auto & name : names)
            size += name.size();
        return size;
    }
};

struct CheckRequest : virtual Request
{
    String path;
    int32_t version = -1;

    /// should it check if a node DOES NOT exist
    bool not_exists = false;

    void addRootPath(const String & root_path) override;
    String getPath() const override { return path; }

    size_t bytesSize() const override { return path.size() + sizeof(version); }
};

struct CheckResponse : virtual Response
{
};

struct SyncRequest : virtual Request
{
    String path;

    void addRootPath(const String & root_path) override;
    String getPath() const override { return path; }

    size_t bytesSize() const override { return path.size(); }
};

struct SyncResponse : virtual Response
{
    String path;

    size_t bytesSize() const override { return path.size(); }
};

struct ReconfigRequest : virtual Request
{
    String joining;
    String leaving;
    String new_members;
    int32_t version;

    String getPath() const final { return keeper_config_path; }

    size_t bytesSize() const final
    {
        return joining.size() + leaving.size() + new_members.size() + sizeof(version);
    }
};

struct ReconfigResponse : virtual Response
{
    String value;
    Stat stat;

    size_t bytesSize() const override { return value.size() + sizeof(stat); }
};

template <typename T>
struct MultiRequest : virtual Request
{
    std::vector<T> requests;

    void addRootPath(const String & root_path) override
    {
        for (auto & request : requests)
            request->addRootPath(root_path);
    }

    String getPath() const override { return {}; }

    size_t bytesSize() const override
    {
        size_t size = 0;
        for (const auto & request : requests)
            size += request->bytesSize();
        return size;
    }
};

struct MultiResponse : virtual Response
{
    Responses responses;

    void removeRootPath(const String & root_path) override;

    size_t bytesSize() const override
    {
        size_t size = 0;
        for (const auto & response : responses)
            size += response->bytesSize();
        return size;
    }
};

/// This response may be received only as an element of responses in MultiResponse.
struct ErrorResponse : virtual Response
{
};


using CreateCallback = std::function<void(const CreateResponse &)>;
using RemoveCallback = std::function<void(const RemoveResponse &)>;
using RemoveRecursiveCallback = std::function<void(const RemoveRecursiveResponse &)>;
using ExistsCallback = std::function<void(const ExistsResponse &)>;
using GetCallback = std::function<void(const GetResponse &)>;
using SetCallback = std::function<void(const SetResponse &)>;
using ListCallback = std::function<void(const ListResponse &)>;
using CheckCallback = std::function<void(const CheckResponse &)>;
using SyncCallback = std::function<void(const SyncResponse &)>;
using ReconfigCallback = std::function<void(const ReconfigResponse &)>;
using MultiCallback = std::function<void(const MultiResponse &)>;

/// For watches.
enum State
{
    EXPIRED_SESSION = -112,
    AUTH_FAILED = -113,
    CONNECTING = 1,
    ASSOCIATING = 2,
    CONNECTED = 3,
    READONLY = 5,
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


class Exception : public DB::Exception
{
private:
    /// Delegate constructor, used to minimize repetition; last parameter used for overload resolution.
    Exception(const std::string & msg, Error code_, int); /// NOLINT
    Exception(PreformattedMessage && msg, Error code_);

    /// Message must be a compile-time constant
    template <typename T>
    requires std::is_convertible_v<T, String>
    Exception(T && message, Error code_) : DB::Exception(std::forward<T>(message), DB::ErrorCodes::KEEPER_EXCEPTION, /* remote_= */ false), code(code_)
    {
        incrementErrorMetrics(code);
    }

    static void incrementErrorMetrics(Error code_);

public:
    explicit Exception(Error code_); /// NOLINT
    Exception(const Exception & exc);

    template <typename... Args>
    Exception(Error code_, FormatStringHelper<Args...> fmt, Args &&... args)
        : DB::Exception(DB::ErrorCodes::KEEPER_EXCEPTION, std::move(fmt), std::forward<Args>(args)...)
        , code(code_)
    {
        incrementErrorMetrics(code);
    }

    static Exception createDeprecated(const std::string & msg, Error code_)
    {
        return Exception(msg, code_, 0);
    }

    static Exception fromPath(Error code_, const std::string & path)
    {
        return Exception(code_, "Coordination error: {}, path {}", errorMessage(code_), path);
    }

    /// Message must be a compile-time constant
    template <typename T>
    requires std::is_convertible_v<T, String>
    static Exception fromMessage(Error code_, T && message)
    {
        return Exception(std::forward<T>(message), code_);
    }

    const char * name() const noexcept override { return "Coordination::Exception"; }
    const char * className() const noexcept override { return "Coordination::Exception"; }
    Exception * clone() const override { return new Exception(*this); }

    const Error code;
};

class SimpleFaultInjection
{
public:
    SimpleFaultInjection(Float64 probability_before, Float64 probability_after_, const String & description_);
    ~SimpleFaultInjection() noexcept(false);

private:
    Float64 probability_after = 0;
    String description;
    int exceptions_level = 0;
};


/** Usage scenario:
  * - create an object and issue commands;
  * - you provide callbacks for your commands; callbacks are invoked in internal thread and must be cheap:
  *   for example, just signal a condvar / fulfill a promise.
  * - you also may provide callbacks for watches; they are also invoked in internal thread and must be cheap.
  * - whenever you receive exception with ZSESSIONEXPIRED code or method isExpired returns true,
  *   the ZooKeeper instance is no longer usable - you may only destroy it and probably create another.
  * - whenever session is expired or ZooKeeper instance is destroying, all callbacks are notified with special event.
  * - data for callbacks must be alive when ZooKeeper instance is alive, so try to avoid capturing references in callbacks, it's error-prone.
  */
class IKeeper
{
public:
    virtual ~IKeeper() = default;

    /// If expired, you can only destroy the object. All other methods will throw exception.
    virtual bool isExpired() const = 0;

    /// Get the current connected node idx.
    virtual std::optional<int8_t> getConnectedNodeIdx() const = 0;

    /// Get the current connected host and port.
    virtual String getConnectedHostPort() const = 0;

    /// Get the xid of current connection.
    virtual int32_t getConnectionXid() const = 0;

    /// Useful to check owner of ephemeral node.
    virtual int64_t getSessionID() const = 0;

    virtual String tryGetAvailabilityZone() { return ""; }

    /// If the method will throw an exception, callbacks won't be called.
    ///
    /// After the method is executed successfully, you must wait for callbacks
    ///  (don't destroy callback data before it will be called).
    /// TODO: The above line is the description of an error-prone interface. It's better
    ///  to replace callbacks with std::future results, so the caller shouldn't think about
    ///  lifetime of the callback data.
    ///
    /// All callbacks are executed sequentially (the execution of callbacks is serialized).
    ///
    /// If an exception is thrown inside the callback, the session will expire,
    ///  and all other callbacks will be called with "Session expired" error.

    virtual void create(
        const String & path,
        const String & data,
        bool is_ephemeral,
        bool is_sequential,
        const ACLs & acls,
        CreateCallback callback) = 0;

    virtual void remove(
        const String & path,
        int32_t version,
        RemoveCallback callback) = 0;

    virtual void removeRecursive(
        const String & path,
        uint32_t remove_nodes_limit,
        RemoveRecursiveCallback callback) = 0;

    virtual void exists(
        const String & path,
        ExistsCallback callback,
        WatchCallbackPtr watch) = 0;

    virtual void get(
        const String & path,
        GetCallback callback,
        WatchCallbackPtr watch) = 0;

    virtual void set(
        const String & path,
        const String & data,
        int32_t version,
        SetCallback callback) = 0;

    virtual void list(
        const String & path,
        ListRequestType list_request_type,
        ListCallback callback,
        WatchCallbackPtr watch) = 0;

    virtual void check(
        const String & path,
        int32_t version,
        CheckCallback callback) = 0;

    virtual void sync(
        const String & path,
        SyncCallback callback) = 0;

    virtual void reconfig(
        std::string_view joining,
        std::string_view leaving,
        std::string_view new_members,
        int32_t version,
        ReconfigCallback callback) = 0;

    virtual void multi(
        std::span<const RequestPtr> requests,
        MultiCallback callback) = 0;

    virtual void multi(
        const Requests & requests,
        MultiCallback callback) = 0;

    virtual bool isFeatureEnabled(DB::KeeperFeatureFlag feature_flag) const = 0;

    virtual const DB::KeeperFeatureFlags * getKeeperFeatureFlags() const { return nullptr; }

    /// Expire session and finish all pending requests
    virtual void finalize(const String & reason) = 0;
};

}

template <> struct fmt::formatter<Coordination::Error> : fmt::formatter<std::string_view>
{
    constexpr auto format(Coordination::Error code, auto & ctx) const
    {
        return formatter<string_view>::format(Coordination::errorMessage(code), ctx);
    }
};
