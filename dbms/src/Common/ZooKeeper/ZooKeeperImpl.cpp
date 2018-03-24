#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>

#include <Poco/Exception.h>
#include <Poco/Net/NetException.h>

#include <array>

//#include <iostream>


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
int32_t xid            \x5a\xad\x72\x3f      Arbitary number. Used for identification of requests/responses.
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


namespace ZooKeeperImpl
{

using namespace DB;


/// Assuming we are at little endian.

void write(int64_t x, WriteBuffer & out)
{
    x = __builtin_bswap64(x);
    writeBinary(x, out);
}

void write(int32_t x, WriteBuffer & out)
{
    x = __builtin_bswap32(x);
    writeBinary(x, out);
}

void write(bool x, WriteBuffer & out)
{
    writeBinary(x, out);
}

void write(const String & s, WriteBuffer & out)
{
    write(int32_t(s.size()), out);
    out.write(s.data(), s.size());
}

template <size_t N> void write(std::array<char, N> s, WriteBuffer & out)
{
    write(int32_t(N), out);
    out.write(s.data(), N);
}

template <typename T> void write(const std::vector<T> & arr, WriteBuffer & out)
{
    write(int32_t(arr.size()), out);
    for (const auto & elem : arr)
        write(elem, out);
}

void write(const ZooKeeper::ACL & acl, WriteBuffer & out)
{
    acl.write(out);
}

void ZooKeeper::ACL::write(WriteBuffer & out) const
{
    ZooKeeperImpl::write(permissions, out);
    ZooKeeperImpl::write(scheme, out);
    ZooKeeperImpl::write(id, out);
}


void read(int64_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap64(x);
}

void read(int32_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap32(x);
}

void read(bool & x, ReadBuffer & in)
{
    readBinary(x, in);
}

void read(String & s, ReadBuffer & in)
{
    static constexpr int32_t max_string_size = 1 << 20;
    int32_t size = 0;
    read(size, in);
    if (size < 0)
        throw Exception("Negative size");   /// TODO Actually it means that zookeeper node have NULL value. Maybe better to treat it like empty string.
    if (size > max_string_size)
        throw Exception("Too large string size");   /// TODO error code
    s.resize(size);
    in.read(&s[0], size);
}

template <size_t N> void read(std::array<char, N> & s, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size != N)
        throw Exception("Unexpected array size");   /// TODO error code
    in.read(&s[0], N);
}

void read(ZooKeeper::Stat & stat, ReadBuffer & in)
{
    stat.read(in);
}

void ZooKeeper::Stat::read(ReadBuffer & in)
{
    ZooKeeperImpl::read(czxid, in);
    ZooKeeperImpl::read(mzxid, in);
    ZooKeeperImpl::read(ctime, in);
    ZooKeeperImpl::read(mtime, in);
    ZooKeeperImpl::read(version, in);
    ZooKeeperImpl::read(cversion, in);
    ZooKeeperImpl::read(aversion, in);
    ZooKeeperImpl::read(ephemeralOwner, in);
    ZooKeeperImpl::read(dataLength, in);
    ZooKeeperImpl::read(numChildren, in);
    ZooKeeperImpl::read(pzxid, in);
}

template <typename T> void read(std::vector<T> & arr, ReadBuffer & in)
{
    static constexpr int32_t max_array_size = 1 << 20;
    int32_t size = 0;
    read(size, in);
    if (size < 0)
        throw Exception("Negative size");
    if (size > max_array_size)
        throw Exception("Too large array size");   /// TODO error code
    arr.resize(size);
    for (auto & elem : arr)
        read(elem, in);
}


template <typename T>
void ZooKeeper::write(const T & x)
{
    ZooKeeperImpl::write(x, *out);
}

template <typename T>
void ZooKeeper::read(T & x)
{
    ZooKeeperImpl::read(x, *in);
}


static constexpr int32_t protocol_version = 0;

static constexpr ZooKeeper::XID watch_xid = -1;
static constexpr ZooKeeper::XID ping_xid = -2;
static constexpr ZooKeeper::XID auth_xid = -4;

static constexpr ZooKeeper::XID close_xid = 0x7FFFFFFF;


const char * ZooKeeper::errorMessage(int32_t code)
{
    switch (code)
    {
        case ZOK:                       return "Ok";
        case ZSYSTEMERROR:              return "System error";
        case ZRUNTIMEINCONSISTENCY:     return "Run time inconsistency";
        case ZDATAINCONSISTENCY:        return "Data inconsistency";
        case ZCONNECTIONLOSS:           return "Connection loss";
        case ZMARSHALLINGERROR:         return "Marshalling error";
        case ZUNIMPLEMENTED:            return "Unimplemented";
        case ZOPERATIONTIMEOUT:         return "Operation timeout";
        case ZBADARGUMENTS:             return "Bad arguments";
        case ZINVALIDSTATE:             return "Invalid zhandle state";
        case ZAPIERROR:                 return "API error";
        case ZNONODE:                   return "No node";
        case ZNOAUTH:                   return "Not authenticated";
        case ZBADVERSION:               return "Bad version";
        case ZNOCHILDRENFOREPHEMERALS:  return "No children for ephemerals";
        case ZNODEEXISTS:               return "Node exists";
        case ZNOTEMPTY:                 return "Not empty";
        case ZSESSIONEXPIRED:           return "Session expired";
        case ZINVALIDCALLBACK:          return "Invalid callback";
        case ZINVALIDACL:               return "Invalid ACL";
        case ZAUTHFAILED:               return "Authentication failed";
        case ZCLOSING:                  return "ZooKeeper is closing";
        case ZNOTHING:                  return "(not error) no server responses to process";
        case ZSESSIONMOVED:             return "Session moved to another server, so operation is ignored";
    }
    if (code > 0)
        return strerror(code);

    return "unknown error";
}


ZooKeeper::~ZooKeeper()
{
    try
    {
        finalize(false, false);

        if (send_thread.joinable())
            send_thread.join();

        if (receive_thread.joinable())
            receive_thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


ZooKeeper::ZooKeeper(
    const Addresses & addresses,
    const String & root_path_,
    const String & auth_scheme,
    const String & auth_data,
    Poco::Timespan session_timeout,
    Poco::Timespan connection_timeout,
    Poco::Timespan operation_timeout)
    : root_path(root_path_),
    session_timeout(session_timeout),
    operation_timeout(operation_timeout)
{
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

    connect(addresses, connection_timeout);

    sendHandshake();
    receiveHandshake();

    if (!auth_scheme.empty())
        sendAuth(auth_scheme, auth_data);

    send_thread = std::thread([this] { sendThread(); });
    receive_thread = std::thread([this] { receiveThread(); });
}


void ZooKeeper::connect(
    const Addresses & addresses,
    Poco::Timespan connection_timeout)
{
    static constexpr size_t num_tries = 3;
    bool connected = false;

    for (size_t try_no = 0; try_no < num_tries; ++try_no)
    {
        for (const auto & address : addresses)
        {
            try
            {
                socket.connect(address, connection_timeout);
                connected = true;
                break;
            }
            catch (const Poco::Net::NetException & e)
            {
                /// TODO log exception
            }
            catch (const Poco::TimeoutException & e)
            {
            }
        }

        if (connected)
            break;
    }

    if (!connected)
        throw Exception("All connection tries failed"); /// TODO more info; error code

    socket.setReceiveTimeout(operation_timeout);
    socket.setSendTimeout(operation_timeout);
    socket.setNoDelay(true);

    in.emplace(socket);
    out.emplace(socket);
}


void ZooKeeper::sendHandshake()
{
    int32_t handshake_length = 44;
    int64_t last_zxid_seen = 0;
    int32_t timeout = session_timeout.totalMilliseconds();
    int64_t session_id = 0;
    constexpr int32_t passwd_len = 16;
    std::array<char, passwd_len> passwd {};

    write(handshake_length);
    write(protocol_version);
    write(last_zxid_seen);
    write(timeout);
    write(session_id);
    write(passwd);

    out->next();
}


void ZooKeeper::receiveHandshake()
{
    int32_t handshake_length;
    int32_t protocol_version_read;
    int32_t timeout;
    constexpr int32_t passwd_len = 16;
    std::array<char, passwd_len> passwd;

    read(handshake_length);
    if (handshake_length != 36)
        throw Exception("Unexpected handshake length received: " + toString(handshake_length));

    read(protocol_version_read);
    if (protocol_version_read != protocol_version)
        throw Exception("Unexpected protocol version: " + toString(protocol_version_read));

    read(timeout);
    if (timeout != session_timeout.totalMilliseconds())
        throw Exception("Received different session timeout from server: " + toString(timeout));

    read(session_id);
    read(passwd);
}


void ZooKeeper::sendAuth(const String & scheme, const String & data)
{
    AuthRequest request;
    request.scheme = scheme;
    request.data = data;
    request.xid = auth_xid;
    request.write(*out);

    int32_t length;
    XID xid;
    int64_t zxid;
    int32_t err;

    read(length);
    size_t count_before_event = in->count();
    read(xid);
    read(zxid);
    read(err);

    if (xid != auth_xid)
        throw Exception("Unexpected event recievent in reply to auth request: " + toString(xid));

    int32_t actual_length = in->count() - count_before_event;
    if (length != actual_length)
        throw Exception("Response length doesn't match. Expected: " + toString(length) + ", actual: " + toString(actual_length));

    if (err)
        throw Exception("Error received in reply to auth request. Code: " + toString(err) + ". Message: " + String(errorMessage(err)));
}


void ZooKeeper::sendThread()
{
    auto prev_heartbeat_time = clock::now();

    try
    {
        while (!expired)
        {
            auto now = clock::now();
            auto next_heartbeat_time = prev_heartbeat_time + std::chrono::milliseconds(session_timeout.totalMilliseconds() / 3);

            if (next_heartbeat_time > now)
            {
                /// Wait for the next request in queue. No more than operation timeout. No more than until next heartbeat time.
                UInt64 max_wait = std::min(
                    std::chrono::duration_cast<std::chrono::milliseconds>(next_heartbeat_time - now).count(),
                    operation_timeout.totalMilliseconds());

                RequestPtr request;
                if (requests.tryPop(request, max_wait))
                {
                    request->write(*out);

                    if (request->xid == close_xid)
                        break;
                }
            }
            else
            {
                /// Send heartbeat.
                prev_heartbeat_time = clock::now();

                HeartbeatRequest request;
                request.xid = ping_xid;
                request.write(*out);
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        finalize(true, false);
    }

    /// Drain queue
    RequestPtr request;
    while (requests.tryPop(request))
        ;
}


void ZooKeeper::receiveThread()
{
    try
    {
        Int64 waited = 0;
        while (!expired)
        {
            clock::time_point now = clock::now();
            UInt64 max_wait = operation_timeout.totalMicroseconds();
            bool has_operations = false;

            {
                std::lock_guard lock(operations_mutex);
                if (!operations.empty())
                {
                    /// Operations are ordered by xid (and consequently, by time).
                    has_operations = true;
                    auto earliest_operation_deadline = operations.begin()->second.time + std::chrono::microseconds(operation_timeout.totalMicroseconds());
                    if (now > earliest_operation_deadline)
                        throw Exception("Operation timeout");
                    max_wait = std::chrono::duration_cast<std::chrono::microseconds>(earliest_operation_deadline - now).count();
                }
            }

            if (in->poll(max_wait))
            {
                if (expired)
                    break;

                receiveEvent();
                waited = 0;
            }
            else
            {
                if (has_operations)
                    throw Exception("Operation timeout");
                waited += max_wait;
                if (waited > session_timeout.totalMicroseconds())
                    throw Exception("Nothing is received in session timeout");

            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        finalize(false, true);
    }
}


void ZooKeeper::Request::write(WriteBuffer & out) const
{
    /// Excessive copy to calculate length.
    WriteBufferFromOwnString buf;
    ZooKeeperImpl::write(xid, buf);
    ZooKeeperImpl::write(getOpNum(), buf);
    writeImpl(buf);
    ZooKeeperImpl::write(buf.str(), out);
    out.next();
}


ZooKeeper::ResponsePtr ZooKeeper::HeartbeatRequest::makeResponse() const { return std::make_shared<HeartbeatResponse>(); }
ZooKeeper::ResponsePtr ZooKeeper::AuthRequest::makeResponse() const { return std::make_shared<AuthResponse>(); }
ZooKeeper::ResponsePtr ZooKeeper::CreateRequest::makeResponse() const { return std::make_shared<CreateResponse>(); }
ZooKeeper::ResponsePtr ZooKeeper::RemoveRequest::makeResponse() const { return std::make_shared<RemoveResponse>(); }
ZooKeeper::ResponsePtr ZooKeeper::ExistsRequest::makeResponse() const { return std::make_shared<ExistsResponse>(); }
ZooKeeper::ResponsePtr ZooKeeper::GetRequest::makeResponse() const { return std::make_shared<GetResponse>(); }
ZooKeeper::ResponsePtr ZooKeeper::SetRequest::makeResponse() const { return std::make_shared<SetResponse>(); }
ZooKeeper::ResponsePtr ZooKeeper::ListRequest::makeResponse() const { return std::make_shared<ListResponse>(); }
ZooKeeper::ResponsePtr ZooKeeper::CheckRequest::makeResponse() const { return std::make_shared<CheckResponse>(); }
ZooKeeper::ResponsePtr ZooKeeper::MultiRequest::makeResponse() const { return std::make_shared<MultiResponse>(requests); }
ZooKeeper::ResponsePtr ZooKeeper::CloseRequest::makeResponse() const { return std::make_shared<CloseResponse>(); }


void addRootPath(String & path, const String & root_path)
{
    if (path.empty())
        throw Exception("Path cannot be empty");

    if (path[0] != '/')
        throw Exception("Path must begin with /");

    if (root_path.empty())
        return;

    path = root_path + path;
}

void removeRootPath(String & path, const String & root_path)
{
    if (root_path.empty())
        return;

    if (path.size() <= root_path.size())
        throw Exception("Received path is not longer than root_path");

    path = path.substr(root_path.size());
}


void ZooKeeper::CreateRequest::addRootPath(const String & root_path) { ZooKeeperImpl::addRootPath(path, root_path); }
void ZooKeeper::RemoveRequest::addRootPath(const String & root_path) { ZooKeeperImpl::addRootPath(path, root_path); }
void ZooKeeper::ExistsRequest::addRootPath(const String & root_path) { ZooKeeperImpl::addRootPath(path, root_path); }
void ZooKeeper::GetRequest::addRootPath(const String & root_path) { ZooKeeperImpl::addRootPath(path, root_path); }
void ZooKeeper::SetRequest::addRootPath(const String & root_path) { ZooKeeperImpl::addRootPath(path, root_path); }
void ZooKeeper::ListRequest::addRootPath(const String & root_path) { ZooKeeperImpl::addRootPath(path, root_path); }
void ZooKeeper::CheckRequest::addRootPath(const String & root_path) { ZooKeeperImpl::addRootPath(path, root_path); }

void ZooKeeper::MultiRequest::addRootPath(const String & root_path)
{
    for (auto & request : requests)
        request->addRootPath(root_path);
}

void ZooKeeper::CreateResponse::removeRootPath(const String & root_path) { ZooKeeperImpl::removeRootPath(path_created, root_path); }
void ZooKeeper::WatchResponse::removeRootPath(const String & root_path) { ZooKeeperImpl::removeRootPath(path, root_path); }

void ZooKeeper::MultiResponse::removeRootPath(const String & root_path)
{
    for (auto & response : responses)
        response->removeRootPath(root_path);
}


void ZooKeeper::receiveEvent()
{
    int32_t length;
    XID xid;
    int64_t zxid;
    int32_t err;

    read(length);
    size_t count_before_event = in->count();
    read(xid);
    read(zxid);
    read(err);

    RequestInfo request_info;
    ResponsePtr response;

    if (xid == ping_xid)
    {
        if (err)
            throw Exception("Received error in heartbeat response: " + String(errorMessage(err)));

        response = std::make_shared<HeartbeatResponse>();

//        std::cerr << "Received heartbeat\n";
    }
    else if (xid == watch_xid)
    {
        response = std::make_shared<WatchResponse>();

        request_info.callback = [this](const Response & response)
        {
            const WatchResponse & watch_response = static_cast<const WatchResponse &>(response);

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

                watches.erase(it);
            }
        };

//        std::cerr << "Received watch\n";
    }
    else
    {
        {
            std::lock_guard lock(operations_mutex);

            auto it = operations.find(xid);
            if (it == operations.end())
                throw Exception("Received response for unknown xid");

            request_info = std::move(it->second);
            operations.erase(it);
        }

//        std::cerr << "Received response: " << request_info.request->getOpNum() << "\n";

        response = request_info.request->makeResponse();
    }

    if (err)
        response->error = err;
    else
    {
        response->readImpl(*in);
        response->removeRootPath(root_path);
    }

    int32_t actual_length = in->count() - count_before_event;
    if (length != actual_length)
        throw Exception("Response length doesn't match. Expected: " + toString(length) + ", actual: " + toString(actual_length));

    if (request_info.callback)
        request_info.callback(*response);
}


void ZooKeeper::finalize(bool error_send, bool error_receive)
{
    bool expired_prev = false;
    if (expired.compare_exchange_strong(expired_prev, true))
    {
        try
        {
            if (!error_send)
            {
                /// Send close event. This also signals sending thread to wakeup and then stop.
                close();
                send_thread.join();
            }

            /// This will also wakeup receiving thread.
            socket.shutdown();

            if (!error_receive)
                receive_thread.join();

            {
                std::lock_guard lock(operations_mutex);

                for (auto & op : operations)
                {
                    RequestInfo & request_info = op.second;
                    ResponsePtr response = request_info.request->makeResponse();
                    response->error = ZCONNECTIONLOSS;
                    if (request_info.callback)
                        request_info.callback(*response);
                }

                operations.clear();
            }

            {
                std::lock_guard lock(watches_mutex);

                for (auto & path_watches : watches)
                {
                    WatchResponse response;
                    response.type = SESSION;
                    response.state = EXPIRED_SESSION;
                    response.error = ZCONNECTIONLOSS;

                    for (auto & callback : path_watches.second)
                        if (callback)
                            callback(response);
                }

                watches.clear();
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


void ZooKeeper::AuthRequest::writeImpl(WriteBuffer & out) const
{
    ZooKeeperImpl::write(type, out);
    ZooKeeperImpl::write(scheme, out);
    ZooKeeperImpl::write(data, out);
}

void ZooKeeper::CreateRequest::writeImpl(WriteBuffer & out) const
{
    ZooKeeperImpl::write(path, out);
    ZooKeeperImpl::write(data, out);
    ZooKeeperImpl::write(acls, out);

    int32_t flags = 0;

    if (is_ephemeral)
        flags |= 1;
    if (is_sequential)
        flags |= 2;

    ZooKeeperImpl::write(flags, out);
}

void ZooKeeper::RemoveRequest::writeImpl(WriteBuffer & out) const
{
    ZooKeeperImpl::write(path, out);
    ZooKeeperImpl::write(version, out);
}

void ZooKeeper::ExistsRequest::writeImpl(WriteBuffer & out) const
{
    ZooKeeperImpl::write(path, out);
    ZooKeeperImpl::write(has_watch, out);
}

void ZooKeeper::GetRequest::writeImpl(WriteBuffer & out) const
{
    ZooKeeperImpl::write(path, out);
    ZooKeeperImpl::write(has_watch, out);
}

void ZooKeeper::SetRequest::writeImpl(WriteBuffer & out) const
{
    ZooKeeperImpl::write(path, out);
    ZooKeeperImpl::write(data, out);
    ZooKeeperImpl::write(version, out);
}

void ZooKeeper::ListRequest::writeImpl(WriteBuffer & out) const
{
    ZooKeeperImpl::write(path, out);
    ZooKeeperImpl::write(has_watch, out);
}

void ZooKeeper::CheckRequest::writeImpl(WriteBuffer & out) const
{
    ZooKeeperImpl::write(path, out);
    ZooKeeperImpl::write(version, out);
}

void ZooKeeper::MultiRequest::writeImpl(WriteBuffer & out) const
{
    for (const auto & request : requests)
    {
        bool done = false;
        int32_t error = -1;

        ZooKeeperImpl::write(request->getOpNum(), out);
        ZooKeeperImpl::write(done, out);
        ZooKeeperImpl::write(error, out);

        request->writeImpl(out);
    }

    OpNum op_num = -1;
    bool done = true;
    int32_t error = -1;

    ZooKeeperImpl::write(op_num, out);
    ZooKeeperImpl::write(done, out);
    ZooKeeperImpl::write(error, out);
}


void ZooKeeper::WatchResponse::readImpl(ReadBuffer & in)
{
    ZooKeeperImpl::read(type, in);
    ZooKeeperImpl::read(state, in);
    ZooKeeperImpl::read(path, in);
}

void ZooKeeper::CreateResponse::readImpl(ReadBuffer & in)
{
    ZooKeeperImpl::read(path_created, in);
}

void ZooKeeper::ExistsResponse::readImpl(ReadBuffer & in)
{
    ZooKeeperImpl::read(stat, in);
}

void ZooKeeper::GetResponse::readImpl(ReadBuffer & in)
{
    ZooKeeperImpl::read(data, in);
    ZooKeeperImpl::read(stat, in);
}

void ZooKeeper::SetResponse::readImpl(ReadBuffer & in)
{
    ZooKeeperImpl::read(stat, in);
}

void ZooKeeper::ListResponse::readImpl(ReadBuffer & in)
{
    ZooKeeperImpl::read(names, in);
    ZooKeeperImpl::read(stat, in);
}

void ZooKeeper::ErrorResponse::readImpl(ReadBuffer & in)
{
    int32_t read_error;
    ZooKeeperImpl::read(read_error, in);

    if (read_error != error)
        throw Exception("Error code in ErrorResponse (" + toString(read_error) + ") doesn't match error code in header (" + toString(error) + ")");
}

void ZooKeeper::CloseResponse::readImpl(ReadBuffer &)
{
    throw Exception("Received response for close request");
}

ZooKeeper::MultiResponse::MultiResponse(const Requests & requests)
{
    responses.reserve(requests.size());

    for (const auto & request : requests)
        responses.emplace_back(request->makeResponse());
}

void ZooKeeper::MultiResponse::readImpl(ReadBuffer & in)
{
    for (auto & response : responses)
    {
        OpNum op_num;
        bool done;
        int32_t op_error;

        ZooKeeperImpl::read(op_num, in);
        ZooKeeperImpl::read(done, in);
        ZooKeeperImpl::read(op_error, in);

//        std::cerr << "Received result for multi: " << op_num << "\n";

        if (done)
            throw Exception("Not enough results received for multi transaction");

        /// op_num == -1 is special for multi transaction.
        /// For unknown reason, error code is duplicated in header and in response body.

        if (op_num == -1)
            response = std::make_shared<ErrorResponse>();

        if (op_error)
        {
            response->error = op_error;

            /// Set error for whole transaction.
            /// If some operations fail, ZK send global error as zero and then send details about each operation.
            /// It will set error code for first failed operation and it will set special "runtime inconsistency" code for other operations.
            if (!error && op_error != ZRUNTIMEINCONSISTENCY)
                error = op_error;
        }

        if (!op_error || op_num == -1)
            response->readImpl(in);
    }

    /// Footer.
    {
        OpNum op_num;
        bool done;
        int32_t error;

        ZooKeeperImpl::read(op_num, in);
        ZooKeeperImpl::read(done, in);
        ZooKeeperImpl::read(error, in);

        if (!done)
            throw Exception("Too many results received for multi transaction");
        if (op_num != -1)
            throw Exception("Unexpected op_num received at the end of results for multi transaction");
        if (error != -1)
            throw Exception("Unexpected error value received at the end of results for multi transaction");
    }
}


void ZooKeeper::pushRequest(RequestInfo && info)
{
    /// If the request is close request, we push it even after session is expired - because it will signal sending thread to stop.
    if (expired && info.request->xid != close_xid)
        throw Exception("Session expired");

    info.request->addRootPath(root_path);

    info.time = clock::now();

    if (!info.request->xid)
    {
        info.request->xid = xid.fetch_add(1);
        if (info.request->xid < 0)
            throw Exception("XID overflow");
    }

    {
        std::lock_guard lock(operations_mutex);
        operations[info.request->xid] = info;
    }

    if (info.watch)
    {
        info.request->has_watch = true;
        std::lock_guard lock(watches_mutex);
        watches[info.request->getPath()].emplace_back(std::move(info.watch));
    }

    if (!requests.tryPush(info.request, session_timeout.totalMilliseconds()))
        throw Exception("Cannot push request to queue within session timeout");
}


void ZooKeeper::create(
    const String & path,
    const String & data,
    bool is_ephemeral,
    bool is_sequential,
    const ACLs & acls,
    CreateCallback callback)
{
    CreateRequest request;
    request.path = path;
    request.data = data;
    request.is_ephemeral = is_ephemeral;
    request.is_sequential = is_sequential;
    request.acls = acls.empty() ? default_acls : acls;

    RequestInfo request_info;
    request_info.request = std::make_shared<CreateRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(typeid_cast<const CreateResponse &>(response)); };

    pushRequest(std::move(request_info));
}


void ZooKeeper::remove(
    const String & path,
    int32_t version,
    RemoveCallback callback)
{
    RemoveRequest request;
    request.path = path;
    request.version = version;

    RequestInfo request_info;
    request_info.request = std::make_shared<RemoveRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(typeid_cast<const RemoveResponse &>(response)); };

    pushRequest(std::move(request_info));
}


void ZooKeeper::exists(
    const String & path,
    ExistsCallback callback,
    WatchCallback watch)
{
    ExistsRequest request;
    request.path = path;

    RequestInfo request_info;
    request_info.request = std::make_shared<ExistsRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(typeid_cast<const ExistsResponse &>(response)); };
    request_info.watch = watch;

    pushRequest(std::move(request_info));
}


void ZooKeeper::get(
    const String & path,
    GetCallback callback,
    WatchCallback watch)
{
    GetRequest request;
    request.path = path;

    RequestInfo request_info;
    request_info.request = std::make_shared<GetRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(typeid_cast<const GetResponse &>(response)); };
    request_info.watch = watch;

    pushRequest(std::move(request_info));
}


void ZooKeeper::set(
    const String & path,
    const String & data,
    int32_t version,
    SetCallback callback)
{
    SetRequest request;
    request.path = path;
    request.data = data;
    request.version = version;

    RequestInfo request_info;
    request_info.request = std::make_shared<SetRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(typeid_cast<const SetResponse &>(response)); };

    pushRequest(std::move(request_info));
}


void ZooKeeper::list(
    const String & path,
    ListCallback callback,
    WatchCallback watch)
{
    ListRequest request;
    request.path = path;

    RequestInfo request_info;
    request_info.request = std::make_shared<ListRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(typeid_cast<const ListResponse &>(response)); };
    request_info.watch = watch;

    pushRequest(std::move(request_info));
}


void ZooKeeper::check(
    const String & path,
    int32_t version,
    CheckCallback callback)
{
    CheckRequest request;
    request.path = path;
    request.version = version;

    RequestInfo request_info;
    request_info.request = std::make_shared<CheckRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(typeid_cast<const CheckResponse &>(response)); };

    pushRequest(std::move(request_info));
}


void ZooKeeper::multi(
    const Requests & requests,
    MultiCallback callback)
{
    MultiRequest request;
    request.requests = requests;

    for (auto & elem : request.requests)
        if (CreateRequest * create = typeid_cast<CreateRequest *>(elem.get()))
            if (create->acls.empty())
                create->acls = default_acls;

    RequestInfo request_info;
    request_info.request = std::make_shared<MultiRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(typeid_cast<const MultiResponse &>(response)); };

    pushRequest(std::move(request_info));
}


void ZooKeeper::close()
{
    CloseRequest request;
    request.xid = close_xid;

    RequestInfo request_info;
    request_info.request = std::make_shared<CloseRequest>(std::move(request));

    pushRequest(std::move(request_info));
}


}
