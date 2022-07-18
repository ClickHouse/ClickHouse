#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/logger_useful.h>
#include <base/getThreadId.h>

#include <Common/config.h>

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
    Coordination::write(x, *out);
}

template <typename T>
void ZooKeeper::read(T & x)
{
    Coordination::read(x, *in);
}

static void removeRootPath(String & path, const String & root_path)
{
    if (root_path.empty())
        return;

    if (path.size() <= root_path.size())
        throw Exception("Received path is not longer than root_path", Error::ZDATAINCONSISTENCY);

    path = path.substr(root_path.size());
}

ZooKeeper::~ZooKeeper()
{
    try
    {
        finalize(false, false, "Destructor called");

        if (send_thread.joinable())
            send_thread.join();

        if (receive_thread.joinable())
            receive_thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}


ZooKeeper::ZooKeeper(
    const Nodes & nodes,
    const String & root_path_,
    const String & auth_scheme,
    const String & auth_data,
    Poco::Timespan session_timeout_,
    Poco::Timespan connection_timeout,
    Poco::Timespan operation_timeout_,
    std::shared_ptr<ZooKeeperLog> zk_log_)
    : root_path(root_path_),
    session_timeout(session_timeout_),
    operation_timeout(std::min(operation_timeout_, session_timeout_))
{
    log = &Poco::Logger::get("ZooKeeperClient");
    std::atomic_store(&zk_log, std::move(zk_log_));

    if (!root_path.empty())
    {
        if (root_path.back() == '/')
            root_path.pop_back();
    }

    if (auth_scheme.empty())
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

    connect(nodes, connection_timeout);

    if (!initApiVersion())
    {
        // We failed to get the version, let's reconnect in case
        // the connection became faulty
        socket.close();
        connect(nodes, connection_timeout);
    }

    if (!auth_scheme.empty())
        sendAuth(auth_scheme, auth_data);

    send_thread = ThreadFromGlobalPool([this] { sendThread(); });
    receive_thread = ThreadFromGlobalPool([this] { receiveThread(); });

    ProfileEvents::increment(ProfileEvents::ZooKeeperInit);
}

void ZooKeeper::connect(
    const Nodes & nodes,
    Poco::Timespan connection_timeout)
{
    if (nodes.empty())
        throw Exception("No nodes passed to ZooKeeper constructor", Error::ZBADARGUMENTS);

    static constexpr size_t num_tries = 3;
    bool connected = false;

    WriteBufferFromOwnString fail_reasons;
    for (size_t try_no = 0; try_no < num_tries; ++try_no)
    {
        for (const auto & node : nodes)
        {
            try
            {
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

                socket.connect(node.address, connection_timeout);
                socket_address = socket.peerAddress();

                socket.setReceiveTimeout(operation_timeout);
                socket.setSendTimeout(operation_timeout);
                socket.setNoDelay(true);

                in.emplace(socket);
                out.emplace(socket);

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
                break;
            }
            catch (...)
            {
                fail_reasons << "\n" << getCurrentExceptionMessage(false) << ", " << node.address.toString();
            }
        }

        if (connected)
            break;
    }

    if (!connected)
    {
        WriteBufferFromOwnString message;
        message << "All connection tries failed while connecting to ZooKeeper. nodes: ";
        bool first = true;
        for (const auto & node : nodes)
        {
            if (first)
                first = false;
            else
                message << ", ";

            if (node.secure)
                message << "secure://";

            message << node.address.toString();
        }

        message << fail_reasons.str() << "\n";
        throw Exception(message.str(), Error::ZCONNECTIONLOSS);
    }
    else
    {
        LOG_TEST(log, "Connected to ZooKeeper at {} with session_id {}{}", socket.peerAddress().toString(), session_id, fail_reasons.str());
    }
}


void ZooKeeper::sendHandshake()
{
    int32_t handshake_length = 44;
    int64_t last_zxid_seen = 0;
    int32_t timeout = session_timeout.totalMilliseconds();
    int64_t previous_session_id = 0;    /// We don't support session restore. So previous session_id is always zero.
    constexpr int32_t passwd_len = 16;
    std::array<char, passwd_len> passwd {};

    write(handshake_length);
    write(ZOOKEEPER_PROTOCOL_VERSION);
    write(last_zxid_seen);
    write(timeout);
    write(previous_session_id);
    write(passwd);

    out->next();
}


void ZooKeeper::receiveHandshake()
{
    int32_t handshake_length;
    int32_t protocol_version_read;
    int32_t timeout;
    std::array<char, PASSWORD_LENGTH> passwd;

    read(handshake_length);
    if (handshake_length != SERVER_HANDSHAKE_LENGTH)
        throw Exception("Unexpected handshake length received: " + DB::toString(handshake_length), Error::ZMARSHALLINGERROR);

    read(protocol_version_read);
    if (protocol_version_read != ZOOKEEPER_PROTOCOL_VERSION)
    {
        /// Special way to tell a client that server is not ready to serve it.
        /// It's better for faster failover than just connection drop.
        /// Implemented in clickhouse-keeper.
        if (protocol_version_read == KEEPER_PROTOCOL_VERSION_CONNECTION_REJECT)
            throw Exception("Keeper server rejected the connection during the handshake. Possibly it's overloaded, doesn't see leader or stale", Error::ZCONNECTIONLOSS);
        else
            throw Exception("Unexpected protocol version: " + DB::toString(protocol_version_read), Error::ZMARSHALLINGERROR);
    }

    read(timeout);
    if (timeout != session_timeout.totalMilliseconds())
        /// Use timeout from server.
        session_timeout = timeout * Poco::Timespan::MILLISECONDS;

    read(session_id);
    read(passwd);
}


void ZooKeeper::sendAuth(const String & scheme, const String & data)
{
    ZooKeeperAuthRequest request;
    request.scheme = scheme;
    request.data = data;
    request.xid = AUTH_XID;
    request.write(*out);

    int32_t length;
    XID read_xid;
    int64_t zxid;
    Error err;

    read(length);
    size_t count_before_event = in->count();
    read(read_xid);
    read(zxid);
    read(err);

    if (read_xid != AUTH_XID)
        throw Exception("Unexpected event received in reply to auth request: " + DB::toString(read_xid),
            Error::ZMARSHALLINGERROR);

    int32_t actual_length = in->count() - count_before_event;
    if (length != actual_length)
        throw Exception("Response length doesn't match. Expected: " + DB::toString(length) + ", actual: " + DB::toString(actual_length),
            Error::ZMARSHALLINGERROR);

    if (err != Error::ZOK)
        throw Exception("Error received in reply to auth request. Code: " + DB::toString(static_cast<int32_t>(err)) + ". Message: " + String(errorMessage(err)),
            Error::ZMARSHALLINGERROR);
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
            auto next_heartbeat_time = prev_heartbeat_time + std::chrono::milliseconds(session_timeout.totalMilliseconds() / 3);

            if (next_heartbeat_time > now)
            {
                /// Wait for the next request in queue. No more than operation timeout. No more than until next heartbeat time.
                UInt64 max_wait = std::min(
                    static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(next_heartbeat_time - now).count()),
                    static_cast<UInt64>(operation_timeout.totalMilliseconds()));

                RequestInfo info;
                if (requests_queue.tryPop(info, max_wait))
                {
                    /// After we popped element from the queue, we must register callbacks (even in the case when expired == true right now),
                    ///  because they must not be lost (callbacks must be called because the user will wait for them).

                    if (info.request->xid != CLOSE_XID)
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

                    info.request->addRootPath(root_path);

                    info.request->probably_sent = true;
                    info.request->write(*out);

                    logOperationIfNeeded(info.request);

                    /// We sent close request, exit
                    if (info.request->xid == CLOSE_XID)
                        break;
                }
            }
            else
            {
                /// Send heartbeat.
                prev_heartbeat_time = clock::now();

                ZooKeeperHeartbeatRequest request;
                request.xid = PING_XID;
                request.write(*out);
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
        Int64 waited = 0;
        while (!requests_queue.isFinished())
        {
            auto prev_bytes_received = in->count();

            clock::time_point now = clock::now();
            UInt64 max_wait = operation_timeout.totalMicroseconds();
            std::optional<RequestInfo> earliest_operation;

            {
                std::lock_guard lock(operations_mutex);
                if (!operations.empty())
                {
                    /// Operations are ordered by xid (and consequently, by time).
                    earliest_operation = operations.begin()->second;
                    auto earliest_operation_deadline = earliest_operation->time + std::chrono::microseconds(operation_timeout.totalMicroseconds());
                    if (now > earliest_operation_deadline)
                        throw Exception("Operation timeout (deadline already expired) for path: " + earliest_operation->request->getPath(), Error::ZOPERATIONTIMEOUT);
                    max_wait = std::chrono::duration_cast<std::chrono::microseconds>(earliest_operation_deadline - now).count();
                }
            }

            if (in->poll(max_wait))
            {
                if (requests_queue.isFinished())
                    break;

                receiveEvent();
                waited = 0;
            }
            else
            {
                if (earliest_operation)
                {
                    throw Exception("Operation timeout (no response) for request " + toString(earliest_operation->request->getOpNum()) + " for path: " + earliest_operation->request->getPath(), Error::ZOPERATIONTIMEOUT);
                }
                waited += max_wait;
                if (waited >= session_timeout.totalMicroseconds())
                    throw Exception("Nothing is received in session timeout", Error::ZOPERATIONTIMEOUT);

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
    read(xid);
    read(zxid);
    read(err);

    RequestInfo request_info;
    ZooKeeperResponsePtr response;

    if (xid == PING_XID)
    {
        if (err != Error::ZOK)
            throw Exception("Received error in heartbeat response: " + String(errorMessage(err)), Error::ZRUNTIMEINCONSISTENCY);

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
                for (auto & callback : it->second)
                    if (callback)
                        callback(watch_response);   /// NOTE We may process callbacks not under mutex.

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
                throw Exception("Received response for unknown xid " + DB::toString(xid), Error::ZRUNTIMEINCONSISTENCY);

            /// After this point, we must invoke callback, that we've grabbed from 'operations'.
            /// Invariant: all callbacks are invoked either in case of success or in case of error.
            /// (all callbacks in 'operations' are guaranteed to be invoked)

            request_info = std::move(it->second);
            operations.erase(it);
            CurrentMetrics::sub(CurrentMetrics::ZooKeeperRequest);
        }

        auto elapsed_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - request_info.time).count();
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
            response->readImpl(*in);
            response->removeRootPath(root_path);
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
                CurrentMetrics::add(CurrentMetrics::ZooKeeperWatch);

                /// The key of wathces should exclude the root_path
                String req_path = request_info.request->getPath();
                removeRootPath(req_path, root_path);
                std::lock_guard lock(watches_mutex);
                watches[req_path].emplace_back(std::move(request_info.watch));
            }
        }

        int32_t actual_length = in->count() - count_before_event;
        if (length != actual_length)
            throw Exception("Response length doesn't match. Expected: " + DB::toString(length) + ", actual: " + DB::toString(actual_length), Error::ZMARSHALLINGERROR);

        logOperationIfNeeded(request_info.request, response);   //-V614
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

            logOperationIfNeeded(request_info.request, response);
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
}


void ZooKeeper::finalize(bool error_send, bool error_receive, const String & reason)
{
    /// If some thread (send/receive) already finalizing session don't try to do it
    bool already_started = finalization_started.test_and_set();

    LOG_TEST(log, "Finalizing session {}: finalization_started={}, queue_finished={}, reason={}",
             session_id, already_started, requests_queue.isFinished(), reason);

    if (already_started)
        return;

    auto expire_session_if_not_expired = [&]
    {
        /// No new requests will appear in queue after finish()
        bool was_already_finished = requests_queue.finish();
        if (!was_already_finished)
            active_session_metric_increment.destroy();
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
            if (send_thread.joinable())
                send_thread.join();
        }

        /// Set expired flag after we sent close event
        expire_session_if_not_expired();

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

        if (!error_receive && receive_thread.joinable())
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

                if (request_info.callback)
                {
                    try
                    {
                        request_info.callback(*response);
                        logOperationIfNeeded(request_info.request, response, true);
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

                for (auto & callback : path_watches.second)
                {
                    watch_callback_count += 1;
                    if (callback)
                    {
                        try
                        {
                            callback(response);
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
                        logOperationIfNeeded(info.request, response, true);
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
                    info.watch(response);
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
        if (zk_log)
        {
            info.request->thread_id = getThreadId();
            info.request->query_id = String(CurrentThread::getQueryId());
        }

        if (!info.request->xid)
        {
            info.request->xid = next_xid.fetch_add(1);
            if (info.request->xid == CLOSE_XID)
                throw Exception("xid equal to close_xid", Error::ZSESSIONEXPIRED);
            if (info.request->xid < 0)
                throw Exception("XID overflow", Error::ZSESSIONEXPIRED);

            if (auto * multi_request = dynamic_cast<ZooKeeperMultiRequest *>(info.request.get()))
            {
                for (auto & request : multi_request->requests)
                    dynamic_cast<ZooKeeperRequest &>(*request).xid = multi_request->xid;
            }
        }

        if (!requests_queue.tryPush(std::move(info), operation_timeout.totalMilliseconds()))
        {
            if (requests_queue.isFinished())
                throw Exception("Session expired", Error::ZSESSIONEXPIRED);

            throw Exception("Cannot push request to queue within operation timeout", Error::ZOPERATIONTIMEOUT);
        }
    }
    catch (...)
    {
        finalize(false, false, getCurrentExceptionMessage(false, false, false));
        throw;
    }

    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
}

Coordination::KeeperApiVersion ZooKeeper::getApiVersion()
{
    return keeper_api_version;
}

bool ZooKeeper::initApiVersion()
{
    try
    {
        ZooKeeperApiVersionRequest request;
        request.write(*out);

        if (!in->poll(operation_timeout.totalMilliseconds()))
        {
            LOG_ERROR(&Poco::Logger::get("ZooKeeper"), "Failed to get version: timeout");
            return false;
        }

        ZooKeeperApiVersionResponse response;

        int32_t length;
        XID xid;
        int64_t zxid;
        Error err;
        read(length);
        read(xid);
        read(zxid);
        read(err);

        response.readImpl(*in);

        keeper_api_version = static_cast<KeeperApiVersion>(response.api_version);
        return true;
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(&Poco::Logger::get("ZooKeeper"), "Failed to get version: {}", e.message());
        return false;
    }
}


void ZooKeeper::executeGenericRequest(
    const ZooKeeperRequestPtr & request,
    ResponseCallback callback)
{
    RequestInfo request_info;
    request_info.request = request;
    request_info.callback = callback;

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
    request.acls = acls.empty() ? default_acls : acls;

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

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperRemoveRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const RemoveResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperRemove);
}


void ZooKeeper::exists(
    const String & path,
    ExistsCallback callback,
    WatchCallback watch)
{
    ZooKeeperExistsRequest request;
    request.path = path;

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
    WatchCallback watch)
{
    ZooKeeperGetRequest request;
    request.path = path;

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
    WatchCallback watch)
{
    std::shared_ptr<ZooKeeperListRequest> request{nullptr};
    if (keeper_api_version < Coordination::KeeperApiVersion::V1)
    {
        if (list_request_type != ListRequestType::ALL)
            throw Exception("Filtered list request type cannot be used because it's not supported by the server", Error::ZBADARGUMENTS);

        request = std::make_shared<ZooKeeperListRequest>();
    }
    else
    {
        auto filtered_list_request = std::make_shared<ZooKeeperFilteredListRequest>();
        filtered_list_request->list_request_type = list_request_type;
        request = std::move(filtered_list_request);
    }

    request->path = path;

    RequestInfo request_info;
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ListResponse &>(response)); };
    request_info.watch = watch;
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

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperSyncRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const SyncResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperSync);
}


void ZooKeeper::multi(
    const Requests & requests,
    MultiCallback callback)
{
    ZooKeeperMultiRequest request(requests, default_acls);

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperMultiRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const MultiResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperMulti);
}


void ZooKeeper::close()
{
    ZooKeeperCloseRequest request;
    request.xid = CLOSE_XID;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperCloseRequest>(std::move(request));

    if (!requests_queue.tryPush(std::move(request_info), operation_timeout.totalMilliseconds()))
        throw Exception("Cannot push close request to queue within operation timeout", Error::ZOPERATIONTIMEOUT);

    ProfileEvents::increment(ProfileEvents::ZooKeeperClose);
}


void ZooKeeper::setZooKeeperLog(std::shared_ptr<DB::ZooKeeperLog> zk_log_)
{
    /// logOperationIfNeeded(...) uses zk_log and can be called from different threads, so we have to use atomic shared_ptr
    std::atomic_store(&zk_log, std::move(zk_log_));
}

#ifdef ZOOKEEPER_LOG
void ZooKeeper::logOperationIfNeeded(const ZooKeeperRequestPtr & request, const ZooKeeperResponsePtr & response, bool finalize)
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
        if (request)
        {
            elem.thread_id = request->thread_id;
            elem.query_id = request->query_id;
        }
        maybe_zk_log->add(elem);
    }
}
#else
void ZooKeeper::logOperationIfNeeded(const ZooKeeperRequestPtr &, const ZooKeeperResponsePtr &, bool)
{}
#endif

}
