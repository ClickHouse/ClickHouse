#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>

#include <Poco/Exception.h>
#include <Poco/Net/NetException.h>

#include <chrono>
#include <array>

#include <iostream>


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
int32_t type           \x00\x00\x00\x01      ZOO_CREATE_OP 1
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

void write(const String & s, WriteBuffer & out)
{
    write(int32_t(s.size()), out);
    out.write(s.data(), s.size());
}

template <size_t N> void write(std::array<char, N> s, WriteBuffer & out)
{
    std::cerr << __PRETTY_FUNCTION__ << "\n";
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

void read(String & s, ReadBuffer & in)
{
    static constexpr int32_t max_string_size = 1 << 20;
    int32_t size = 0;
    read(size, in);
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


template <typename T>
void ZooKeeper::write(const T & x)
{
    std::cerr << __PRETTY_FUNCTION__ << "\n";
    ZooKeeperImpl::write(x, *out);
}

template <typename T>
void ZooKeeper::read(T & x)
{
    ZooKeeperImpl::read(x, *in);
}


static constexpr int32_t protocol_version = 0;

//static constexpr ZooKeeper::XID watch_xid = -1;
static constexpr ZooKeeper::XID ping_xid = -2;
//static constexpr ZooKeeper::XID auth_xid = -4;

static constexpr ZooKeeper::XID close_xid = -3;


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
    stop = true;

    if (send_thread.joinable())
        send_thread.join();

    if (receive_thread.joinable())
        receive_thread.join();

    if (!expired)
        close();
}


ZooKeeper::ZooKeeper(
    const Addresses & addresses,
    const String & root_path_,
    const String & auth_scheme,
    const String & auth_data,
    Poco::Timespan session_timeout,
    Poco::Timespan connection_timeout)
    : root_path(root_path_),
    session_timeout(session_timeout)
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
        sendAuth(-1, auth_scheme, auth_data);

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

    socket.setReceiveTimeout(session_timeout);
    socket.setSendTimeout(session_timeout);
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
    int64_t session_id;
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


/*void ZooKeeper::sendAuth(XID xid, const String & auth_scheme, const String & auth_data)
{
    // TODO
}*/


void ZooKeeper::close()
{
    CloseRequest request;
    request.xid = close_xid;
    request.write(*out);
    expired = true;
}


void ZooKeeper::sendThread()
{
    XID xid = 0;    /// TODO deal with xid overflow
    auto prev_heartbeat_time = std::chrono::steady_clock::now();

    try
    {
        while (!stop)
        {
            ++xid;

            auto now =  std::chrono::steady_clock::now();
            auto next_heartbeat_time = prev_heartbeat_time + std::chrono::milliseconds(session_timeout.totalMilliseconds() / 3);
            UInt64 max_wait = 0;
            if (next_heartbeat_time > now)
                max_wait = std::chrono::duration_cast<std::chrono::milliseconds>(next_heartbeat_time - now).count();

            RequestInfo request_info;
            if (requests.tryPop(request_info, max_wait))
            {
                request_info.request->xid = xid;

                {
                    std::lock_guard lock(operations_mutex);
                    operations[xid] = request_info;
                }

                request_info.request->write(*out);
            }
            else
            {
                prev_heartbeat_time = std::chrono::steady_clock::now();

                HeartbeatRequest request;
                request.xid = ping_xid;
                request.write(*out);
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        expired = true;
        stop = true;
    }

    /// TODO drain queue
}


void ZooKeeper::receiveThread()
{
    try
    {
        while (!stop)
        {
            if (!in->poll(session_timeout.totalMicroseconds()))
                throw Exception("Nothing is received in session timeout");

            receiveEvent();
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        expired = true;
        stop = true;
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
ZooKeeper::ResponsePtr ZooKeeper::CreateRequest::makeResponse() const { return std::make_shared<CreateResponse>(); }
//ZooKeeper::ResponsePtr ZooKeeper::RemoveRequest::makeResponse() const { return std::make_shared<RemoveResponse>(); }
//ZooKeeper::ResponsePtr ZooKeeper::ExistsRequest::makeResponse() const { return std::make_shared<ExistsResponse>(); }
//ZooKeeper::ResponsePtr ZooKeeper::GetRequest::makeResponse() const { return std::make_shared<GetResponse>(); }
ZooKeeper::ResponsePtr ZooKeeper::CloseRequest::makeResponse() const { throw Exception("Received response for close request"); }


void ZooKeeper::receiveEvent()
{
    int32_t length;
    int32_t xid;
    int64_t zxid;
    int32_t err;

    read(length);
    size_t count_before_event = in->count();
    read(xid);
    read(zxid);
    read(err);

    if (xid == ping_xid)
    {
        /// TODO process err
        return;
    }

    RequestInfo request_info;

    {
        std::lock_guard lock(operations_mutex);

        auto it = operations.find(xid);
        if (it == operations.end())
            throw Exception("Received response for unknown xid");

        request_info = std::move(it->second);
        operations.erase(it);
    }

    ResponsePtr response = request_info.request->makeResponse();

    if (err)
        response->error = err;
    else
        response->readImpl(*in);

    int32_t actual_length = in->count() - count_before_event;
    if (length != actual_length)
        throw Exception("Response length doesn't match");

    if (request_info.callback)
        request_info.callback(*response);
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


void ZooKeeper::CreateResponse::readImpl(ReadBuffer & in)
{
    ZooKeeperImpl::read(path_created, in);
}


String ZooKeeper::addRootPath(const String & path)
{
    if (path.empty())
        throw Exception("Path cannot be empty");

    if (path[0] != '/')
        throw Exception("Path must begin with /");

    if (root_path.empty())
        return path;

    return root_path + path;
}

String ZooKeeper::removeRootPath(const String & path)
{
    if (root_path.empty())
        return path;

    if (path.size() <= root_path.size())
        throw Exception("Received path is not longer than root_path");

    return path.substr(root_path.size());
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
    request.path = addRootPath(path);
    request.data = data;
    request.is_ephemeral = is_ephemeral;
    request.is_sequential = is_sequential;
    request.acls = acls.empty() ? default_acls : acls;

    RequestInfo request_info;
    request_info.request = std::make_shared<CreateRequest>(std::move(request));
    request_info.callback = [callback, this](const Response & response)
    {
        auto concrete_response = typeid_cast<const CreateResponse &>(response);
        concrete_response.path_created = removeRootPath(concrete_response.path_created);
        callback(concrete_response);
    };

    if (!requests.tryPush(request_info, session_timeout.totalMilliseconds()))
        throw Exception("Cannot push request to queue within session timeout");
}


}
