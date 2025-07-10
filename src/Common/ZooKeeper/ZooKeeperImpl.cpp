#include <Common/ZooKeeper/ZooKeeperConstants.h>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <base/getThreadId.h>
#include <base/sleep.h>
#include <Common/CurrentThread.h>
#include <Common/EventNotifier.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Histogram.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>

#include <Poco/Net/NetException.h>
#include <Poco/Net/DNS.h>

#include <Coordination/KeeperConstants.h>
#include "config.h"

#if USE_SSL
#    include <Poco/Net/SecureStreamSocket.h>
#endif

#include <array>


namespace ProfileEvents
{
    extern const Event ZooKeeperInit;
    extern const Event ZooKeeperTransactions;
    extern const Event ZooKeeperCreate;
    extern const Event ZooKeeperRemove;
    extern const Event ZooKeeperExists;
    extern const Event ZooKeeperMulti;
    extern const Event ZooKeeperMultiRead;
    extern const Event ZooKeeperMultiWrite;
    extern const Event ZooKeeperReconfig;
    extern const Event ZooKeeperGet;
    extern const Event ZooKeeperSet;
    extern const Event ZooKeeperList;
    extern const Event ZooKeeperCheck;
    extern const Event ZooKeeperSync;
    extern const Event ZooKeeperClose;
    extern const Event ZooKeeperWaitMicroseconds;
    extern const Event ZooKeeperBytesSent;
    extern const Event ZooKeeperBytesReceived;
    extern const Event ZooKeeperWatchResponse;
}

namespace CurrentMetrics
{
    extern const Metric ZooKeeperRequest;
    extern const Metric ZooKeeperWatch;
}

namespace Metrics::ResponseTime
{
    using namespace DB;

    Histogram::MetricFamily & mf = Histogram::Factory::instance().registerMetric(
        "keeper_response_time_ms",
        "The response time of Keeper, in milliseconds",
        {1, 2, 5, 10, 25, 50, 75, 100, 125, 150, 200, 250, 300, 500, 1000, 2000},
        {"operation"}
    );

    Histogram::Metric & create = mf.withLabels({"create"});
    Histogram::Metric & remove = mf.withLabels({"remove"});
    Histogram::Metric & removeRecursive = mf.withLabels({"remove_recursive"});
    Histogram::Metric & exists = mf.withLabels({"exists"});
    Histogram::Metric & get = mf.withLabels({"get"});
    Histogram::Metric & set = mf.withLabels({"set"});
    Histogram::Metric & list = mf.withLabels({"list"});
    Histogram::Metric & check = mf.withLabels({"check"});
    Histogram::Metric & sync = mf.withLabels({"sync"});
    Histogram::Metric & reconfig = mf.withLabels({"reconfig"});
    Histogram::Metric & multi = mf.withLabels({"multi"});

    template <typename Response>
    void instrument(std::function<void(const Response &)> & callback, Histogram::Metric & histogram)
    {
        callback = [&histogram, callback, timer = Stopwatch()](const Response & response)
        {
            const Int64 response_time = timer.elapsedMilliseconds();
            histogram.observe(response_time);
            return callback(response);
        };
    }
}


/** ZooKeeper wire protocol.

Debugging example:
strace -t -f -e trace=network -s1000 -x ./clickhouse-zookeeper-cli localhost:2181

All numbers are in network byte order (big endian). Sizes are 32 bit. Numbers are signed.

zxid - incremental transaction number at server side.
xid - unique request number at client side.

Client connects to one of the specified hosts.
Client sends:

int32_t sizeof_connect_req;   \x00\x00\x00\x2c (44 bytes)

struct connect_req
{
    int32_t protocolVersion;  \x00\x00\x00\x00 (Currently zero)
    int64_t lastZxidSeen;     \x00\x00\x00\x00\x00\x00\x00\x00 (Zero at first connect)
    int32_t timeOut;          \x00\x00\x75\x30 (Session timeout in milliseconds: 30000)
    int64_t sessionId;        \x00\x00\x00\x00\x00\x00\x00\x00 (Zero at first connect)
    int32_t passwd_len;       \x00\x00\x00\x10 (16)
    char passwd[16];          \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 (Zero at first connect)
};

Server replies:

struct prime_struct
{
    int32_t len;              \x00\x00\x00\x24 (36 bytes)
    int32_t protocolVersion;  \x00\x00\x00\x00
    int32_t timeOut;          \x00\x00\x75\x30
    int64_t sessionId;        \x01\x62\x2c\x3d\x82\x43\x00\x27
    int32_t passwd_len;       \x00\x00\x00\x10
    char passwd[16];          \x3b\x8c\xe0\xd4\x1f\x34\xbc\x88\x9c\xa7\x68\x69\x78\x64\x98\xe9
};

Client remembers session id and session password.


Client may send authentication request (optional).


Each one third of timeout, client sends heartbeat:

int32_t length_of_heartbeat_request \x00\x00\x00\x08 (8)
int32_t ping_xid                    \xff\xff\xff\xfe (-2, constant)
int32_t ping_op                     \x00\x00\x00\x0b ZOO_PING_OP 11

Server replies:

int32_t length_of_heartbeat_response \x00\x00\x00\x10
int32_t ping_xid                     \xff\xff\xff\xfe
int64 zxid                           \x00\x00\x00\x00\x00\x01\x87\x98 (incremental server generated number)
int32_t err                          \x00\x00\x00\x00


Client sends requests. For example, create persistent node '/hello' with value 'world'.

int32_t request_length \x00\x00\x00\x3a
int32_t xid            \x5a\xad\x72\x3f      Arbitrary number. Used for identification of requests/responses.
                                         libzookeeper uses unix timestamp for first xid and then autoincrement to that value.
int32_t op_num         \x00\x00\x00\x01      ZOO_CREATE_OP 1
int32_t path_length    \x00\x00\x00\x06
path                   \x2f\x68\x65\x6c\x6c\x6f  /hello
int32_t data_length    \x00\x00\x00\x05
data                   \x77\x6f\x72\x6c\x64      world
ACLs:
    int32_t num_acls   \x00\x00\x00\x01
    ACL:
        int32_t permissions \x00\x00\x00\x1f
        string scheme   \x00\x00\x00\x05
                        \x77\x6f\x72\x6c\x64      world
        string id       \x00\x00\x00\x06
                        \x61\x6e\x79\x6f\x6e\x65  anyone
int32_t flags           \x00\x00\x00\x00

Server replies:

int32_t response_length \x00\x00\x00\x1a
int32_t xid             \x5a\xad\x72\x3f
int64 zxid              \x00\x00\x00\x00\x00\x01\x87\x99
int32_t err             \x00\x00\x00\x00
string path_created     \x00\x00\x00\x06
                        \x2f\x68\x65\x6c\x6c\x6f  /hello - may differ to original path in case of sequential nodes.


Client may place a watch in their request.

For example, client sends "exists" request with watch:

request length \x00\x00\x00\x12
xid            \x5a\xae\xb2\x0d
op_num         \x00\x00\x00\x03
path           \x00\x00\x00\x05
               \x2f\x74\x65\x73\x74     /test
bool watch     \x01

Server will send response as usual.
And later, server may send special watch event.

struct WatcherEvent
{
    int32_t type;
    int32_t state;
    char * path;
};

response length    \x00\x00\x00\x21
special watch xid  \xff\xff\xff\xff
special watch zxid \xff\xff\xff\xff\xff\xff\xff\xff
err                \x00\x00\x00\x00
type               \x00\x00\x00\x02     DELETED_EVENT_DEF 2
state              \x00\x00\x00\x03     CONNECTED_STATE_DEF 3
path               \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74  /test


Example of multi request:

request length     \x00\x00\x00\x82 130
xid                \x5a\xae\xd6\x16
op_num             \x00\x00\x00\x0e 14

for every command:

    int32_t type;  \x00\x00\x00\x01 create
    bool done;     \x00 false
    int32_t err;   \xff\xff\xff\xff -1

    path           \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74 /test
    data           \x00\x00\x00\x06
                   \x6d\x75\x6c\x74\x69\x31 multi1
    acl            \x00\x00\x00\x01
                   \x00\x00\x00\x1f
                   \x00\x00\x00\x05
                   \x77\x6f\x72\x6c\x64     world
                   \x00\x00\x00\x06
                   \x61\x6e\x79\x6f\x6e\x65 anyone
    flags          \x00\x00\x00\x00

    int32_t type;  \x00\x00\x00\x05 set
    bool done      \x00 false
    int32_t err;   \xff\xff\xff\xff -1

    path           \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74
    data           \x00\x00\x00\x06
                   \x6d\x75\x6c\x74\x69\x32 multi2
    version        \xff\xff\xff\xff

    int32_t type   \x00\x00\x00\x02 remove
    bool done      \x00
    int32_t err    \xff\xff\xff\xff -1

    path           \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74
    version        \xff\xff\xff\xff

after commands:

    int32_t type   \xff\xff\xff\xff -1
    bool done      \x01 true
    int32_t err    \xff\xff\xff\xff

Example of multi response:

response length    \x00\x00\x00\x81 129
xid                \x5a\xae\xd6\x16
zxid               \x00\x00\x00\x00\x00\x01\x87\xe1
err                \x00\x00\x00\x00

in a loop:

    type           \x00\x00\x00\x01 create
    done           \x00
    err            \x00\x00\x00\x00

    path_created   \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74

    type           \x00\x00\x00\x05 set
    done           \x00
    err            \x00\x00\x00\x00

    stat           \x00\x00\x00\x00\x00\x01\x87\xe1
                   \x00\x00\x00\x00\x00\x01\x87\xe1
                   \x00\x00\x01\x62\x3a\xf4\x35\x0c
                   \x00\x00\x01\x62\x3a\xf4\x35\x0c
                   \x00\x00\x00\x01
                   \x00\x00\x00\x00
                   \x00\x00\x00\x00
                   \x00\x00\x00\x00\x00\x00\x00\x00
                   \x00\x00\x00\x06
                   \x00\x00\x00\x00
                   \x00\x00\x00\x00\x00\x01\x87\xe1

    type           \x00\x00\x00\x02 remove
    done           \x00
    err            \x00\x00\x00\x00

after:

    type           \xff\xff\xff\xff
    done           \x01
    err            \xff\xff\xff\xff

  */


namespace Coordination
{

using namespace DB;

template <typename T>
void ZooKeeper::write(const T & x)
{
    Coordination::write(x, getWriteBuffer());
}

template <typename T>
void ZooKeeper::read(T & x)
{
    Coordination::read(x, getReadBuffer());
}

WriteBuffer & ZooKeeper::getWriteBuffer()
{
    if (compressed_out)
        return *compressed_out;
    return *out;
}

void ZooKeeper::flushWriteBuffer()
{
    if (compressed_out)
         compressed_out->next();
    out->next();
}

void ZooKeeper::cancelWriteBuffer() noexcept
{
    if (compressed_out)
         compressed_out->cancel();
    if (out)
        out->cancel();
}

ReadBuffer & ZooKeeper::getReadBuffer()
{
    if (compressed_in)
        return *compressed_in;
    return *in;
}

static void removeRootPath(String & path, const String & chroot)
{
    if (chroot.empty())
        return;

    if (path.size() <= chroot.size())
        throw Exception::fromMessage(Error::ZDATAINCONSISTENCY, "Received path is not longer than chroot");

    path = path.substr(chroot.size());
}

ZooKeeper::~ZooKeeper()
{
    try
    {
        finalize(false, false, "Destructor called");

        send_thread.join();
        receive_thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}


ZooKeeper::ZooKeeper(
    const zkutil::ShuffleHosts & nodes,
    const zkutil::ZooKeeperArgs & args_,
    std::shared_ptr<ZooKeeperLog> zk_log_)
    : path_acls(args_.path_acls), args(args_)
{
    log = getLogger("ZooKeeperClient");
    std::atomic_store(&zk_log, std::move(zk_log_));

    if (!args.chroot.empty())
    {
        if (args.chroot.back() == '/')
            args.chroot.pop_back();
    }

    if (args.auth_scheme.empty())
    {
        ACL acl;
        acl.permissions = ACL::All;
        acl.scheme = "world";
        acl.id = "anyone";
        default_acls.emplace_back(std::move(acl));
    }
    else
    {
        ACL acl;
        acl.permissions = ACL::All;
        acl.scheme = "auth";
        acl.id = "";
        default_acls.emplace_back(std::move(acl));
    }

    if (args.enable_fault_injections_during_startup)
        setupFaultDistributions();

    try
    {
        use_compression = args.use_compression;
        if (args.use_xid_64)
        {
            use_xid_64 = true;
            close_xid = CLOSE_XID_64;
        }
        connect(nodes, args.connection_timeout_ms * 1000);
    }
    catch (...)
    {
        /// If we get exception & compression is enabled, then its possible that keeper does not support compression,
        /// try without compression
        if (use_compression)
        {
            use_compression = false;
            connect(nodes, args.connection_timeout_ms * 1000);
        }
        else
            throw;
    }

    if (!args.auth_scheme.empty())
        sendAuth(args.auth_scheme, args.identity);

    try
    {
        send_thread = ThreadFromGlobalPool([this] { sendThread(); });
        receive_thread = ThreadFromGlobalPool([this] { receiveThread(); });

        initFeatureFlags();
        keeper_feature_flags.logFlags(log);

        ProfileEvents::increment(ProfileEvents::ZooKeeperInit);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to connect to ZooKeeper");

        try
        {
            requests_queue.finish();
            socket.shutdown();
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }

        send_thread.join();
        receive_thread.join();

        throw;
    }
}


void ZooKeeper::connect(
    const zkutil::ShuffleHosts & nodes,
    Poco::Timespan connection_timeout)
{
    if (nodes.empty())
        throw Exception::fromMessage(Error::ZBADARGUMENTS, "No nodes passed to ZooKeeper constructor");

    /// We always have at least one attempt to connect.
    size_t num_tries = args.num_connection_retries + 1;

    bool connected = false;
    bool dns_error = false;

    size_t resolved_count = 0;
    for (const auto & node : nodes)
    {
        try
        {
            const Poco::Net::SocketAddress host_socket_addr{node.host};
            LOG_TRACE(log, "Adding ZooKeeper host {} ({}), az: {}, priority: {}", node.host, host_socket_addr.toString(), node.az_info, node.priority.value);
            node.address = host_socket_addr;
            ++resolved_count;
        }
        catch (const Poco::Net::HostNotFoundException & e)
        {
            /// Most likely it's misconfiguration and wrong hostname was specified
            LOG_ERROR(log, "Cannot use ZooKeeper host {}, reason: {}", node.host, e.displayText());
        }
        catch (const Poco::Net::DNSException & e)
        {
            /// Most likely DNS is not available now
            dns_error = true;
            LOG_ERROR(log, "Cannot use ZooKeeper host {} due to DNS error: {}", node.host, e.displayText());
        }
    }

    if (resolved_count == 0)
    {
        /// For DNS errors we throw exception with ZCONNECTIONLOSS code, so it will be considered as hardware error, not user error
        if (dns_error)
            throw zkutil::KeeperException::fromMessage(
                Coordination::Error::ZCONNECTIONLOSS, "Cannot resolve any of provided ZooKeeper hosts due to DNS error");
        throw zkutil::KeeperException::fromMessage(Coordination::Error::ZCONNECTIONLOSS, "Cannot use any of provided ZooKeeper nodes");
    }

    WriteBufferFromOwnString fail_reasons;
    for (size_t try_no = 0; try_no < num_tries; ++try_no)
    {
        for (const auto & node : nodes)
        {
            try
            {
                if (!node.address)
                    continue;

                /// Reset the state of previous attempt.
                if (node.secure)
                {
#if USE_SSL
                    socket = Poco::Net::SecureStreamSocket();
#else
                    throw Poco::Exception(
                        "Communication with ZooKeeper over SSL is disabled because poco library was built without NetSSL support.");
#endif
                }
                else
                {
                    socket = Poco::Net::StreamSocket();
                }

                socket.connect(*node.address, connection_timeout);
                socket_address = socket.peerAddress();

                socket.setReceiveTimeout(args.operation_timeout_ms * 1000);
                socket.setSendTimeout(args.operation_timeout_ms * 1000);
                socket.setNoDelay(true);

                in.emplace(socket);
                out.emplace(socket);
                compressed_in.reset();
                compressed_out.reset();

                try
                {
                    sendHandshake();
                }
                catch (DB::Exception & e)
                {
                    e.addMessage("while sending handshake to ZooKeeper");
                    throw;
                }

                try
                {
                    receiveHandshake();
                }
                catch (DB::Exception & e)
                {
                    e.addMessage("while receiving handshake from ZooKeeper");
                    throw;
                }

                connected = true;
                if (use_compression)
                {
                    compressed_in.emplace(*in);
                    compressed_out.emplace(*out, CompressionCodecFactory::instance().get("LZ4", {}));
                }

                original_index.store(node.original_index);
                break;
            }
            catch (...)
            {
                fail_reasons << "\n" << getCurrentExceptionMessage(false) << ", " << node.address->toString();
                cancelWriteBuffer();
            }
        }

        if (connected)
            break;
    }

    if (!connected)
    {
        WriteBufferFromOwnString message;
        bool first = true;
        for (const auto & node : nodes)
        {
            if (!node.address)
                continue;

            if (first)
                first = false;
            else
                message << ", ";

            if (node.secure)
                message << "secure://";

            message << node.address->toString();
        }

        message << fail_reasons.str() << "\n";
        throw Exception(Error::ZCONNECTIONLOSS, "All connection tries failed while connecting to ZooKeeper. nodes: {}", message.str());
    }

    LOG_INFO(log, "Connected to ZooKeeper at {} with session_id {}{}", socket.peerAddress().toString(), session_id, fail_reasons.str());
}


void ZooKeeper::sendHandshake()
{
    int32_t handshake_length = 45;
    int64_t last_zxid_seen = 0;
    int32_t timeout = args.session_timeout_ms;
    int64_t previous_session_id = 0;    /// We don't support session restore. So previous session_id is always zero.
    std::string password = args.password;
    password.resize(Coordination::PASSWORD_LENGTH, '\0');
    bool read_only = true;

    write(handshake_length);
    if (use_xid_64)
    {
        write(ZOOKEEPER_PROTOCOL_VERSION_WITH_XID_64);
        write(use_compression);
    }
    else if (use_compression)
    {
        write(ZOOKEEPER_PROTOCOL_VERSION_WITH_COMPRESSION);
    }
    else
    {
        write(ZOOKEEPER_PROTOCOL_VERSION);
    }
    write(last_zxid_seen);
    write(timeout);
    write(previous_session_id);
    write(password);
    write(read_only);
    flushWriteBuffer();
}

void ZooKeeper::receiveHandshake()
{
    int32_t handshake_length;
    int32_t protocol_version_read;
    int32_t timeout;
    std::array<char, PASSWORD_LENGTH> passwd;
    bool read_only;

    read(handshake_length);
    if (handshake_length != SERVER_HANDSHAKE_LENGTH && handshake_length != SERVER_HANDSHAKE_LENGTH_WITH_READONLY)
        throw Exception(Error::ZMARSHALLINGERROR, "Unexpected handshake length received: {}", handshake_length);

    read(protocol_version_read);

    /// Special way to tell a client that server is not ready to serve it.
    /// It's better for faster failover than just connection drop.
    /// Implemented in clickhouse-keeper.
    if (protocol_version_read == KEEPER_PROTOCOL_VERSION_CONNECTION_REJECT)
        throw Exception::fromMessage(Error::ZCONNECTIONLOSS,
                                     "Keeper server rejected the connection during the handshake. "
                                     "Possibly it's overloaded, doesn't see leader or stale");

    if (use_xid_64)
    {
        if (protocol_version_read < ZOOKEEPER_PROTOCOL_VERSION_WITH_XID_64)
            throw Exception(Error::ZMARSHALLINGERROR, "Unexpected protocol version with 64bit XID: {}", protocol_version_read);
    }
    else if (use_compression)
    {
        if (protocol_version_read != ZOOKEEPER_PROTOCOL_VERSION_WITH_COMPRESSION)
            throw Exception(Error::ZMARSHALLINGERROR, "Unexpected protocol version with compression: {}", protocol_version_read);
    }
    else if (protocol_version_read != ZOOKEEPER_PROTOCOL_VERSION)
        throw Exception(Error::ZMARSHALLINGERROR, "Unexpected protocol version: {}", protocol_version_read);

    read(timeout);
    if (timeout != args.session_timeout_ms)
        /// Use timeout from server.
        args.session_timeout_ms = timeout;

    read(session_id);
    read(passwd);
    if (handshake_length == SERVER_HANDSHAKE_LENGTH_WITH_READONLY)
        read(read_only);
}


void ZooKeeper::sendAuth(const String & scheme, const String & data)
{
    ZooKeeperAuthRequest request;
    request.scheme = scheme;
    request.data = data;
    request.xid = AUTH_XID;
    request.write(getWriteBuffer(), use_xid_64);
    flushWriteBuffer();

    int32_t length;
    XID read_xid;
    int64_t zxid;
    Error err;

    read(length);
    size_t count_before_event = in->count();
    if (use_xid_64)
    {
        read(read_xid);
    }
    else
    {
        int32_t xid_32{0};
        read(xid_32);
        read_xid = xid_32;
    }

    read(zxid);
    read(err);

    if (read_xid != AUTH_XID)
        throw Exception(Error::ZMARSHALLINGERROR, "Unexpected event received in reply to auth request: {}", read_xid);

    if (!use_compression)
    {
        int32_t actual_length = static_cast<int32_t>(in->count() - count_before_event);
        if (length != actual_length)
        throw Exception(Error::ZMARSHALLINGERROR, "Response length doesn't match. Expected: {}, actual: {}", length, actual_length);

    }

    if (err != Error::ZOK)
        throw Exception(Error::ZMARSHALLINGERROR, "Error received in reply to auth request. Code: {}. Message: {}",
                        static_cast<int32_t>(err), err);
}

void ZooKeeper::sendThread()
{
    setThreadName("ZooKeeperSend");

    auto prev_heartbeat_time = clock::now();

    try
    {
        while (!requests_queue.isFinished())
        {
            auto prev_bytes_sent = out->count();

            auto now = clock::now();
            auto next_heartbeat_time = prev_heartbeat_time + std::chrono::milliseconds(args.session_timeout_ms / 3);

            maybeInjectSendSleep();

            if (next_heartbeat_time > now)
            {
                /// Wait for the next request in queue. No more than operation timeout. No more than until next heartbeat time.
                UInt64 max_wait = std::min(
                    static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(next_heartbeat_time - now).count()),
                    static_cast<UInt64>(args.operation_timeout_ms));

                RequestInfo info;
                if (requests_queue.tryPop(info, max_wait))
                {
                    /// After we popped element from the queue, we must register callbacks (even in the case when expired == true right now),
                    ///  because they must not be lost (callbacks must be called because the user will wait for them).

                    if (info.request->xid != close_xid)
                    {
                        CurrentMetrics::add(CurrentMetrics::ZooKeeperRequest);
                        std::lock_guard lock(operations_mutex);
                        operations[info.request->xid] = info;
                    }

                    if (info.watch)
                    {
                        info.request->has_watch = true;
                    }

                    if (requests_queue.isFinished())
                    {
                        break;
                    }

                    if (info.request->add_root_path)
                        info.request->addRootPath(args.chroot);

                    info.request->probably_sent = true;
                    info.request->write(getWriteBuffer(), use_xid_64);
                    flushWriteBuffer();

                    logOperationIfNeeded(info.request);

                    /// We sent close request, exit
                    if (info.request->xid == close_xid)
                        break;
                }
            }
            else
            {
                /// Send heartbeat.
                prev_heartbeat_time = clock::now();

                ZooKeeperHeartbeatRequest request;
                request.xid = PING_XID;
                request.write(getWriteBuffer(), use_xid_64);
                flushWriteBuffer();
            }

            ProfileEvents::increment(ProfileEvents::ZooKeeperBytesSent, out->count() - prev_bytes_sent);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
        finalize(true, false, "Exception in sendThread");
    }
}


void ZooKeeper::receiveThread()
{
    setThreadName("ZooKeeperRecv");

    try
    {
        Int64 waited_us = 0;
        while (!requests_queue.isFinished())
        {
            maybeInjectRecvSleep();
            auto prev_bytes_received = in->count();

            clock::time_point now = clock::now();
            UInt64 max_wait_us = args.operation_timeout_ms * 1000;
            std::optional<RequestInfo> earliest_operation;

            {
                std::lock_guard lock(operations_mutex);
                if (!operations.empty())
                {
                    /// Operations are ordered by xid (and consequently, by time).
                    earliest_operation = operations.begin()->second;
                    auto earliest_operation_deadline = earliest_operation->time + std::chrono::microseconds(args.operation_timeout_ms * 1000);
                    if (now > earliest_operation_deadline)
                        throw Exception(Error::ZOPERATIONTIMEOUT, "Operation timeout (deadline of {} ms already expired) for path: {}",
                                        args.operation_timeout_ms, earliest_operation->request->getPath());
                    max_wait_us = std::chrono::duration_cast<std::chrono::microseconds>(earliest_operation_deadline - now).count();
                }
            }

            if (in->poll(max_wait_us))
            {
                if (finalization_started.test())
                    break;

                receiveEvent();
                waited_us = 0;
            }
            else
            {
                if (earliest_operation)
                {
                    throw Exception(Error::ZOPERATIONTIMEOUT, "Operation timeout (no response in {} ms) for request {} for path: {}",
                        args.operation_timeout_ms, earliest_operation->request->getOpNum(), earliest_operation->request->getPath());
                }
                waited_us += max_wait_us;
                if (waited_us >= args.session_timeout_ms * 1000)
                    throw Exception(Error::ZOPERATIONTIMEOUT, "Nothing is received in session timeout of {} ms", args.session_timeout_ms);

            }

            ProfileEvents::increment(ProfileEvents::ZooKeeperBytesReceived, in->count() - prev_bytes_received);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
        finalize(false, true, "Exception in receiveThread");
    }
}


void ZooKeeper::receiveEvent()
{
    int32_t length;
    XID xid;
    int64_t zxid;
    Error err;

    read(length);
    size_t count_before_event = in->count();
    if (use_xid_64)
    {
        read(xid);
    }
    else
    {
        int32_t xid_32{0};
        read(xid_32);
        xid = xid_32;
    }
    read(zxid);
    read(err);

    RequestInfo request_info;
    ZooKeeperResponsePtr response;
    UInt64 elapsed_microseconds = 0;

    maybeInjectRecvFault();

    if (xid == PING_XID)
    {
        if (err != Error::ZOK)
            throw Exception(Error::ZRUNTIMEINCONSISTENCY, "Received error in heartbeat response: {}", err);

        response = std::make_shared<ZooKeeperHeartbeatResponse>();
    }
    else if (xid == WATCH_XID)
    {
        ProfileEvents::increment(ProfileEvents::ZooKeeperWatchResponse);
        response = std::make_shared<ZooKeeperWatchResponse>();

        request_info.callback = [this](const Response & response_)
        {
            const WatchResponse & watch_response = dynamic_cast<const WatchResponse &>(response_);

            std::lock_guard lock(watches_mutex);

            auto it = watches.find(watch_response.path);
            if (it == watches.end())
            {
                /// This is Ok.
                /// Because watches are identified by path.
                /// And there may exist many watches for single path.
                /// And watch is added to the list of watches on client side
                ///  slightly before than it is registered by the server.
                /// And that's why new watch may be already fired by old event,
                ///  but then the server will actually register new watch
                ///  and will send event again later.
            }
            else
            {
                for (const auto & callback : it->second)
                    if (callback)
                        (*callback)(watch_response);   /// NOTE We may process callbacks not under mutex.

                CurrentMetrics::sub(CurrentMetrics::ZooKeeperWatch, it->second.size());
                watches.erase(it);
            }
        };
    }
    else
    {
        {
            std::lock_guard lock(operations_mutex);

            auto it = operations.find(xid);
            if (it == operations.end())
                throw Exception(Error::ZRUNTIMEINCONSISTENCY, "Received response for unknown xid {}", xid);

            /// After this point, we must invoke callback, that we've grabbed from 'operations'.
            /// Invariant: all callbacks are invoked either in case of success or in case of error.
            /// (all callbacks in 'operations' are guaranteed to be invoked)

            request_info = std::move(it->second);
            operations.erase(it);
            CurrentMetrics::sub(CurrentMetrics::ZooKeeperRequest);
        }

        elapsed_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - request_info.time).count();
        ProfileEvents::increment(ProfileEvents::ZooKeeperWaitMicroseconds, elapsed_microseconds);
    }

    try
    {
        if (!response)
            response = request_info.request->makeResponse();

        response->xid = xid;
        response->zxid = zxid;

        if (err != Error::ZOK)
        {
            response->error = err;
        }
        else
        {
            response->readImpl(getReadBuffer());

            if (!request_info.request || request_info.request->add_root_path)
                response->removeRootPath(args.chroot);
        }
        /// Instead of setting the watch in sendEvent, set it in receiveEvent because need to check the response.
        /// The watch shouldn't be set if the node does not exist and it will never exist like sequential ephemeral nodes.
        /// By using getData() instead of exists(), a watch won't be set if the node doesn't exist.
        if (request_info.watch)
        {
            bool add_watch = false;
            /// 3 indicates the ZooKeeperExistsRequest.
            // For exists, we set the watch on both node exist and nonexist case.
            // For other case like getData, we only set the watch when node exists.
            if (request_info.request->getOpNum() == OpNum::Exists)
                add_watch = (response->error == Error::ZOK || response->error == Error::ZNONODE);
            else
                add_watch = response->error == Error::ZOK;

            if (add_watch)
            {

                /// The key of wathces should exclude the args.chroot
                String req_path = request_info.request->getPath();
                removeRootPath(req_path, args.chroot);
                std::lock_guard lock(watches_mutex);
                auto & callbacks = watches[req_path];
                if (request_info.watch && *request_info.watch)
                {
                    if (callbacks.insert(request_info.watch).second)
                        CurrentMetrics::add(CurrentMetrics::ZooKeeperWatch);
                }
            }
        }

        if (!use_compression)
        {
            int32_t actual_length = static_cast<int32_t>(in->count() - count_before_event);

            if (length != actual_length)
                throw Exception(Error::ZMARSHALLINGERROR, "Response length doesn't match. Expected: {}, actual: {}",
                                length, actual_length);
        }

        logOperationIfNeeded(request_info.request, response, /* finalize= */ false, elapsed_microseconds);
    }
    catch (...)
    {
        tryLogCurrentException(log);

        /// Unrecoverable. Don't leave incorrect state in memory.
        if (!response)
            std::terminate();

        /// In case we cannot read the response, we should indicate it as the error of that type
        ///  when the user cannot assume whether the request was processed or not.
        response->error = Error::ZCONNECTIONLOSS;

        try
        {
            if (request_info.callback)
                request_info.callback(*response);

            logOperationIfNeeded(request_info.request, response, /* finalize= */ false, elapsed_microseconds);
        }
        catch (...)
        {
            /// Throw initial exception, not exception from callback.
            tryLogCurrentException(log);
        }

        throw;
    }

    /// Exception in callback will propagate to receiveThread and will lead to session expiration. This is Ok.

    if (request_info.callback)
        request_info.callback(*response);

    /// Finalize current session if we receive a hardware error from ZooKeeper
    if (err != Error::ZOK && isHardwareError(err))
        finalize(/*error_send*/ false, /*error_receive*/ true, fmt::format("Hardware error: {}", err));
}


void ZooKeeper::finalize(bool error_send, bool error_receive, const String & reason)
{
    /// If some thread (send/receive) already finalizing session don't try to do it
    bool already_started = finalization_started.test_and_set();

    if (already_started)
        return;

    LOG_INFO(log, "Finalizing session {}. finalization_started: {}, queue_finished: {}, reason: '{}'",
             session_id, already_started, requests_queue.isFinished(), reason);

    auto expire_session_if_not_expired = [&]
    {
        /// No new requests will appear in queue after finish()
        bool was_already_finished = requests_queue.finish();
        if (!was_already_finished)
        {
            active_session_metric_increment.destroy();
            /// Notify all subscribers (ReplicatedMergeTree tables) about expired session
            EventNotifier::instance().notify(Error::ZSESSIONEXPIRED);
        }
    };

    try
    {
        if (!error_send)
        {
            /// Send close event. This also signals sending thread to stop.
            try
            {
                close();
            }
            catch (...)
            {
                /// This happens for example, when "Cannot push request to queue within operation timeout".
                /// Just mark session expired in case of error on close request, otherwise sendThread may not stop.
                expire_session_if_not_expired();
                tryLogCurrentException(log);
            }

            /// Send thread will exit after sending close request or on expired flag
            send_thread.join();
        }

        /// Set expired flag after we sent close event
        expire_session_if_not_expired();

        cancelWriteBuffer();

        try
        {
            /// This will also wakeup the receiving thread.
            socket.shutdown();
        }
        catch (...)
        {
            /// We must continue to execute all callbacks, because the user is waiting for them.
            tryLogCurrentException(log);
        }

        if (!error_receive)
            receive_thread.join();

        {
            std::lock_guard lock(operations_mutex);

            for (auto & op : operations)
            {
                RequestInfo & request_info = op.second;
                ZooKeeperResponsePtr response = request_info.request->makeResponse();

                response->error = request_info.request->probably_sent
                    ? Error::ZCONNECTIONLOSS
                    : Error::ZSESSIONEXPIRED;
                response->xid = request_info.request->xid;
                UInt64 elapsed_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - request_info.time).count();

                if (request_info.callback)
                {
                    try
                    {
                        request_info.callback(*response);
                        logOperationIfNeeded(request_info.request, response, true, elapsed_microseconds);
                    }
                    catch (...)
                    {
                        /// We must continue to all other callbacks, because the user is waiting for them.
                        tryLogCurrentException(log);
                    }
                }
            }

            CurrentMetrics::sub(CurrentMetrics::ZooKeeperRequest, operations.size());
            operations.clear();
        }

        {
            std::lock_guard lock(watches_mutex);

            Int64 watch_callback_count = 0;
            for (auto & path_watches : watches)
            {
                WatchResponse response;
                response.type = SESSION;
                response.state = EXPIRED_SESSION;
                response.error = Error::ZSESSIONEXPIRED;

                for (const auto & callback : path_watches.second)
                {
                    watch_callback_count += 1;
                    if (callback)
                    {
                        try
                        {
                            (*callback)(response);
                        }
                        catch (...)
                        {
                            tryLogCurrentException(log);
                        }
                    }
                }
            }

            CurrentMetrics::sub(CurrentMetrics::ZooKeeperWatch, watch_callback_count);
            watches.clear();
        }

        /// Drain queue
        RequestInfo info;
        while (requests_queue.tryPop(info))
        {
            if (info.callback)
            {
                ZooKeeperResponsePtr response = info.request->makeResponse();
                if (response)
                {
                    response->error = Error::ZSESSIONEXPIRED;
                    response->xid = info.request->xid;
                    try
                    {
                        info.callback(*response);
                        UInt64 elapsed_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - info.time).count();
                        logOperationIfNeeded(info.request, response, true, elapsed_microseconds);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(log);
                    }
                }
            }
            if (info.watch)
            {
                WatchResponse response;
                response.type = SESSION;
                response.state = EXPIRED_SESSION;
                response.error = Error::ZSESSIONEXPIRED;
                try
                {
                    (*info.watch)(response);
                }
                catch (...)
                {
                    tryLogCurrentException(log);
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}


void ZooKeeper::pushRequest(RequestInfo && info)
{
    try
    {
        info.time = clock::now();
        auto maybe_zk_log = std::atomic_load(&zk_log);
        if (maybe_zk_log)
        {
            info.request->thread_id = getThreadId();
            info.request->query_id = String(CurrentThread::getQueryId());
        }

        if (!info.request->xid)
        {
            info.request->xid = next_xid.fetch_add(1);
            if (!use_xid_64)
                info.request->xid = static_cast<int32_t>(info.request->xid);

            if (info.request->xid == close_xid)
                throw Exception::fromMessage(Error::ZSESSIONEXPIRED, "xid equal to close_xid");
            if (info.request->xid < 0)
                throw Exception::fromMessage(Error::ZSESSIONEXPIRED, "XID overflow");

            if (auto * multi_request = dynamic_cast<ZooKeeperMultiRequest *>(info.request.get()))
            {
                for (auto & request : multi_request->requests)
                    dynamic_cast<ZooKeeperRequest &>(*request).xid = multi_request->xid;
            }
        }

        maybeInjectSendFault();

        if (!requests_queue.tryPush(std::move(info), args.operation_timeout_ms))
        {
            if (requests_queue.isFinished())
                throw Exception::fromMessage(Error::ZSESSIONEXPIRED, "Session expired");

            throw Exception(Error::ZOPERATIONTIMEOUT, "Cannot push request to queue within operation timeout of {} ms", args.operation_timeout_ms);
        }
    }
    catch (...)
    {
        finalize(false, false, getCurrentExceptionMessage(false, false, false));
        throw;
    }

    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
}

bool ZooKeeper::isFeatureEnabled(KeeperFeatureFlag feature_flag) const
{
    return keeper_feature_flags.isEnabled(feature_flag);
}

std::optional<String> ZooKeeper::tryGetSystemZnode(const std::string & path, const std::string & description)
{
    auto promise = std::make_shared<std::promise<Coordination::GetResponse>>();
    auto future = promise->get_future();

    ZooKeeperGetRequest request;
    request.path = path;
    request.add_root_path = false;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperGetRequest>(std::move(request));
    request_info.callback = [promise](const Response & response) { promise->set_value(dynamic_cast<const GetResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperGet);

    if (future.wait_for(std::chrono::milliseconds(args.operation_timeout_ms)) != std::future_status::ready)
        throw Exception(Error::ZOPERATIONTIMEOUT, "Failed to get {}: timeout", description);

    auto response = future.get();

    if (response.error == Coordination::Error::ZNONODE)
    {
        LOG_TRACE(log, "Failed to get {}", description);
        return std::nullopt;
    }
    if (response.error != Coordination::Error::ZOK)
    {
        throw Exception(response.error, "Failed to get {}", description);
    }

    return std::move(response.data);
}

void ZooKeeper::initFeatureFlags()
{
    if (auto feature_flags = tryGetSystemZnode(keeper_api_feature_flags_path, "feature flags"); feature_flags.has_value())
    {
        keeper_feature_flags.setFeatureFlags(std::move(*feature_flags));
        return;
    }

    auto keeper_api_version_string = tryGetSystemZnode(keeper_api_version_path, "API version");

    DB::KeeperApiVersion keeper_api_version{DB::KeeperApiVersion::ZOOKEEPER_COMPATIBLE};

    if (!keeper_api_version_string.has_value())
    {
        LOG_TRACE(log, "API version not found, assuming {}", keeper_api_version);
        return;
    }

    DB::ReadBufferFromOwnString buf(*keeper_api_version_string);
    uint8_t keeper_version{0};
    DB::readIntText(keeper_version, buf);
    keeper_api_version = static_cast<DB::KeeperApiVersion>(keeper_version);
    LOG_TRACE(log, "Detected server's API version: {}", keeper_api_version);
    keeper_feature_flags.fromApiVersion(keeper_api_version);
}

String ZooKeeper::tryGetAvailabilityZone()
{
    auto res = tryGetSystemZnode(keeper_availability_zone_path, "availability zone");
    if (res)
    {
        LOG_TRACE(log, "Availability zone for ZooKeeper at {}: {}", getConnectedHostPort(), *res);
        return *res;
    }
    return "";
}


void ZooKeeper::executeGenericRequest(
    const ZooKeeperRequestPtr & request,
    ResponseCallback callback,
    WatchCallbackPtr watch)
{
    RequestInfo request_info;
    request_info.request = request;
    request_info.callback = callback;
    request_info.watch = watch;

    pushRequest(std::move(request_info));
}

void ZooKeeper::create(
    const String & path,
    const String & data,
    bool is_ephemeral,
    bool is_sequential,
    const ACLs & acls,
    CreateCallback callback)
{
    ZooKeeperCreateRequest request;
    request.path = path;
    request.data = data;
    request.is_ephemeral = is_ephemeral;
    request.is_sequential = is_sequential;

    ACLs final_acls = acls.empty() ? default_acls : acls;

    // Append path-specific ACLs if configured for this path
    auto path_acls_it = path_acls.find(path);
    if (path_acls_it != path_acls.end())
    {
        final_acls.push_back(path_acls_it->second);
    }

    request.acls = std::move(final_acls);

    Metrics::ResponseTime::instrument(callback, Metrics::ResponseTime::create);

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperCreateRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const CreateResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperCreate);
}

void ZooKeeper::remove(
    const String & path,
    int32_t version,
    RemoveCallback callback)
{
    ZooKeeperRemoveRequest request;
    request.path = path;
    request.version = version;

    Metrics::ResponseTime::instrument(callback, Metrics::ResponseTime::remove);

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperRemoveRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const RemoveResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperRemove);
}

void ZooKeeper::removeRecursive(
    const String &path,
    uint32_t remove_nodes_limit,
    RemoveRecursiveCallback callback)
{
    if (!isFeatureEnabled(KeeperFeatureFlag::REMOVE_RECURSIVE))
        throw Exception::fromMessage(Error::ZBADARGUMENTS, "RemoveRecursive request type cannot be used because it's not supported by the server");

    ZooKeeperRemoveRecursiveRequest request;
    request.path = path;
    request.remove_nodes_limit = remove_nodes_limit;

    Metrics::ResponseTime::instrument(callback, Metrics::ResponseTime::removeRecursive);

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperRemoveRecursiveRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const RemoveRecursiveResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperRemove);
}

void ZooKeeper::exists(
    const String & path,
    ExistsCallback callback,
    WatchCallbackPtr watch)
{
    ZooKeeperExistsRequest request;
    request.path = path;

    Metrics::ResponseTime::instrument(callback, Metrics::ResponseTime::exists);

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperExistsRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ExistsResponse &>(response)); };
    request_info.watch = watch;

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperExists);
}


void ZooKeeper::get(
    const String & path,
    GetCallback callback,
    WatchCallbackPtr watch)
{
    ZooKeeperGetRequest request;
    request.path = path;

    Metrics::ResponseTime::instrument(callback, Metrics::ResponseTime::get);

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperGetRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const GetResponse &>(response)); };
    request_info.watch = watch;

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperGet);
}


void ZooKeeper::set(
    const String & path,
    const String & data,
    int32_t version,
    SetCallback callback)
{
    ZooKeeperSetRequest request;
    request.path = path;
    request.data = data;
    request.version = version;

    Metrics::ResponseTime::instrument(callback, Metrics::ResponseTime::set);

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperSetRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const SetResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperSet);
}


void ZooKeeper::list(
    const String & path,
    ListRequestType list_request_type,
    ListCallback callback,
    WatchCallbackPtr watch)
{
    std::shared_ptr<ZooKeeperListRequest> request{nullptr};
    if (!isFeatureEnabled(KeeperFeatureFlag::FILTERED_LIST))
    {
        if (list_request_type != ListRequestType::ALL)
            throw Exception::fromMessage(Error::ZBADARGUMENTS, "Filtered list request type cannot be used because it's not supported by the server");

        request = std::make_shared<ZooKeeperListRequest>();
    }
    else
    {
        auto filtered_list_request = std::make_shared<ZooKeeperFilteredListRequest>();
        filtered_list_request->list_request_type = list_request_type;
        request = std::move(filtered_list_request);
    }

    request->path = path;

    Metrics::ResponseTime::instrument(callback, Metrics::ResponseTime::list);

    RequestInfo request_info;
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ListResponse &>(response)); };
    if (watch)
        request_info.watch = std::move(watch);
    request_info.request = std::move(request);

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperList);
}


void ZooKeeper::check(
    const String & path,
    int32_t version,
    CheckCallback callback)
{
    ZooKeeperCheckRequest request;
    request.path = path;
    request.version = version;

    Metrics::ResponseTime::instrument(callback, Metrics::ResponseTime::check);

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperCheckRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const CheckResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperCheck);
}

void ZooKeeper::sync(
     const String & path,
     SyncCallback callback)
{
    ZooKeeperSyncRequest request;
    request.path = path;

    Metrics::ResponseTime::instrument(callback, Metrics::ResponseTime::sync);

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperSyncRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const SyncResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperSync);
}

void ZooKeeper::reconfig(
    std::string_view joining,
    std::string_view leaving,
    std::string_view new_members,
    int32_t version,
    ReconfigCallback callback)
{
    ZooKeeperReconfigRequest request;
    request.joining = joining;
    request.leaving = leaving;
    request.new_members = new_members;
    request.version = version;

    Metrics::ResponseTime::instrument(callback, Metrics::ResponseTime::reconfig);

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperReconfigRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ReconfigResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperReconfig);
}

void ZooKeeper::multi(
    const Requests & requests,
    MultiCallback callback)
{
    multi(std::span(requests), std::move(callback));
}

void ZooKeeper::multi(
    std::span<const RequestPtr> requests,
    MultiCallback callback)
{
    // If path_acls is not empty, iterate through requests and apply path-specific ACLs to create requests
    if (!path_acls.empty())
    {
        for (const auto & generic_request : requests)
        {
            if (auto * create_request = dynamic_cast<CreateRequest *>(generic_request.get()))
            {
                // Check if there's a path-specific ACL for this path
                auto path_acls_it = path_acls.find(create_request->path);
                if (path_acls_it != path_acls.end())
                {
                    // If ACLs are empty, use default_acls first
                    if (create_request->acls.empty())
                        create_request->acls = default_acls;

                    // Append the path-specific ACL
                    create_request->acls.push_back(path_acls_it->second);
                }
            }
        }
    }

    ZooKeeperMultiRequest request(requests, default_acls);

    if (request.getOpNum() == OpNum::MultiRead && !isFeatureEnabled(KeeperFeatureFlag::MULTI_READ))
            throw Exception::fromMessage(Error::ZBADARGUMENTS, "MultiRead request type cannot be used because it's not supported by the server");

    Metrics::ResponseTime::instrument(callback, Metrics::ResponseTime::multi);

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperMultiRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const MultiResponse &>(response)); };

    bool is_read_request = request_info.request->isReadRequest();
    pushRequest(std::move(request_info));

    ProfileEvents::increment(ProfileEvents::ZooKeeperMulti);
    if (is_read_request)
        ProfileEvents::increment(ProfileEvents::ZooKeeperMultiRead);
    else
        ProfileEvents::increment(ProfileEvents::ZooKeeperMultiWrite);
}


void ZooKeeper::close()
{
    ZooKeeperCloseRequest request;
    request.xid = close_xid;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperCloseRequest>(std::move(request));

    if (!requests_queue.tryPush(std::move(request_info), args.operation_timeout_ms))
        throw Exception(Error::ZOPERATIONTIMEOUT, "Cannot push close request to queue within operation timeout of {} ms", args.operation_timeout_ms);

    ProfileEvents::increment(ProfileEvents::ZooKeeperClose);
}


std::optional<int8_t> ZooKeeper::getConnectedNodeIdx() const
{
    int8_t res = original_index.load();
    if (res == -1)
        return std::nullopt;
    return res;
}

String ZooKeeper::getConnectedHostPort() const
{
    auto idx = getConnectedNodeIdx();
    if (idx)
        return args.hosts[*idx];
    return "";
}

int64_t ZooKeeper::getConnectionXid() const
{
    return next_xid.load();
}


void ZooKeeper::setZooKeeperLog(std::shared_ptr<DB::ZooKeeperLog> zk_log_)
{
    /// logOperationIfNeeded(...) uses zk_log and can be called from different threads, so we have to use atomic shared_ptr
    std::atomic_store(&zk_log, std::move(zk_log_));
}

#ifdef ZOOKEEPER_LOG
void ZooKeeper::logOperationIfNeeded(const ZooKeeperRequestPtr & request, const ZooKeeperResponsePtr & response, bool finalize, UInt64 elapsed_microseconds)
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return;

    ZooKeeperLogElement::Type log_type = ZooKeeperLogElement::UNKNOWN;
    Decimal64 event_time = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch()
                               ).count();
    LogElements elems;
    if (request)
    {
        request->createLogElements(elems);
        log_type = ZooKeeperLogElement::REQUEST;
    }
    else
    {
        assert(response);
        assert(response->xid == PING_XID || response->xid == WATCH_XID);
        elems.emplace_back();
    }

    if (response)
    {
        response->fillLogElements(elems, 0);
        log_type = ZooKeeperLogElement::RESPONSE;
    }

    if (finalize)
        log_type = ZooKeeperLogElement::FINALIZE;

    for (auto & elem : elems)
    {
        elem.type = log_type;
        elem.event_time = event_time;
        elem.address = socket_address;
        elem.session_id = session_id;
        elem.duration_microseconds = elapsed_microseconds;
        if (request)
        {
            elem.thread_id = request->thread_id;
            elem.query_id = request->query_id;
        }
        maybe_zk_log->add(std::move(elem));
    }
}
#else
void ZooKeeper::logOperationIfNeeded(const ZooKeeperRequestPtr &, const ZooKeeperResponsePtr &, bool, UInt64)
{}
#endif


void ZooKeeper::setServerCompletelyStarted()
{
    if (!args.enable_fault_injections_during_startup)
        setupFaultDistributions();
}

void ZooKeeper::setupFaultDistributions()
{
    /// It makes sense (especially, for async requests) to inject a fault in two places:
    /// pushRequest (before request is sent) and receiveEvent (after request was executed).
    if (0 < args.send_fault_probability && args.send_fault_probability <= 1)
    {
        LOG_INFO(log, "ZK send fault: {}%", args.send_fault_probability * 100);
        send_inject_fault.emplace(args.send_fault_probability);
    }
    if (0 < args.recv_fault_probability && args.recv_fault_probability <= 1)
    {
        LOG_INFO(log, "ZK recv fault: {}%", args.recv_fault_probability * 100);
        recv_inject_fault.emplace(args.recv_fault_probability);
    }
    if (0 < args.send_sleep_probability && args.send_sleep_probability <= 1)
    {
        LOG_INFO(log, "ZK send sleep: {}% -> {}ms", args.send_sleep_probability * 100, args.send_sleep_ms);
        send_inject_sleep.emplace(args.send_sleep_probability);
    }
    if (0 < args.recv_sleep_probability && args.recv_sleep_probability <= 1)
    {
        LOG_INFO(log, "ZK recv sleep: {}% -> {}ms", args.recv_sleep_probability * 100, args.recv_sleep_ms);
        recv_inject_sleep.emplace(args.recv_sleep_probability);
    }
    inject_setup.test_and_set();
}

void ZooKeeper::maybeInjectSendFault()
{
    if (unlikely(inject_setup.test() && send_inject_fault && send_inject_fault.value()(thread_local_rng)))
        throw Exception::fromMessage(Error::ZSESSIONEXPIRED, "Session expired (fault injected on send)");
}

void ZooKeeper::maybeInjectRecvFault()
{
    if (unlikely(inject_setup.test() && recv_inject_fault && recv_inject_fault.value()(thread_local_rng)))
        throw Exception::fromMessage(Error::ZSESSIONEXPIRED, "Session expired (fault injected on recv)");
}

void ZooKeeper::maybeInjectSendSleep()
{
    if (unlikely(inject_setup.test() && send_inject_sleep && send_inject_sleep.value()(thread_local_rng)))
        sleepForMilliseconds(args.send_sleep_ms);
}

void ZooKeeper::maybeInjectRecvSleep()
{
    if (unlikely(inject_setup.test() && recv_inject_sleep && recv_inject_sleep.value()(thread_local_rng)))
        sleepForMilliseconds(args.recv_sleep_ms);
}
}
