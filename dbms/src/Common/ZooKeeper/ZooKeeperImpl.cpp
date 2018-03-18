#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/Exception.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuferFromString.h>

#include <Poco/Exception.h>
#include <Poco/Net/NetException.h>

#include <chrono>


/** ZooKeeper wire protocol.

Debugging example:
strace -f -e trace=network -s1000 -x ./clickhouse-zookeeper-cli localhost:2181

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
    write(int32_t(s.size()));
    out.write(s.data(), s.size());
}

template <size_t N> void write(char s[N], WriteBuffer & out)
{
    write(int32_t(N));
    out.write(s, N);
}

template <typename T> void write(const std::vector<T> & arr, WriteBuffer & out)
{
    write(int32_t(arr.size()));
    for (const auto & elem : arr)
        write(elem);
}

void write(const ZooKeeper::ACL & acl, WriteBuffer & out)
{
    acl.write(out);
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
    read(size);
    if (size > max_string_size)
        throw Exception("Too large string size");   /// TODO error code
    s.resize(size);
    in.read(&s[0], size);
}

template <size_t N> void read(char(&arr)[N], ReadBuffer & in)
{
    int32_t size = 0;
    read(size);
    if (size != N)
        throw Exception("Unexpected array size");   /// TODO error code
    in.read(arr, N);
}

void read(ZooKeeper::Stat & stat, ReadBuffer & in)
{
    stat.read(in);
}


template <typename T>
void ZooKeeper::write(const T & x)
{
    write(x, out);
}

template <typename T>
void ZooKeeper::read(T & x)
{
    read(x, in);
}



ZooKeeper::~ZooKeeper()
{
    stop = true;

    if (send_thread.joinable())
        send_thread.join();

    if (receive_thread.joinable())
        receive_thread.join();
}


ZooKeeper::ZooKeeper(
    const Addresses & addresses,
    const String & root_path,
    const String & auth_scheme,
    const String & auth_data,
    Poco::Timespan session_timeout,
    Poco::Timespan connection_timeout)
    : root_path(root_path),
    session_timeout(session_timeout)
{
    connect();

    sendHandshake();
    receiveHandshake();

    if (!auth_scheme.empty())
        sendAuth(-1, auth_scheme, auth_data);

    send_thread = std::thread([this] { sendThread(); });
    receive_thread = std::thread([this] { receiveThread(); });
}


void ZooKeeper::connect(
    const Addresses & addresses)
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

    socket->setReceiveTimeout(session_timeout);
    socket->setSendTimeout(session_timeout);
    socket->setNoDelay(true);

    in.emplace(socket);
    out.emplace(socket);
}


void ZooKeeper::sendHandshake()
{
    int32_t handshake_length = 44;
    int32_t protocol_version = 0;
    int64_t last_zxid_seen = 0;
    int32_t timeout = session_timeout.totalMilliseconds();
    int64_t session_id = 0;
    constexpr int32_t passwd_len = 16;
    char passwd[passwd_len] {};

    write(handshake_length);
    write(protocol_version);
    write(last_zxid_seen);
    write(timeout);
    write(session_id);
    write(passwd);

    out.next();
}


void ZooKeeper::receiveHandshake()
{
    int32_t handshake_length;
    int32_t protocol_version;
    int32_t timeout;
    int64_t session_id;
    constexpr int32_t passwd_len = 16;
    char passwd[passwd_len] {};

    read(handshake_length);
    if (handshake_length != 36)
        throw Exception("Unexpected handshake length received: " + toString(handshake_length));

    read(protocol_version);
    if (protocol_version != 0)
        throw Exception("Unexpected protocol version: " + toString(protocol_version));

    read(timeout);
    if (timeout != session_timeout.totalMilliseconds())
        throw Exception("Received different session timeout from server: " + toString(timeout));

    read(session_id);
    read(passwd);
}


void ZooKeeper::sendAuth(XID xid, const String & auth_scheme, const String & auth_data)
{
    // TODO
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
            auto next_heartbeat_time = prev_heartbeat_time + std::chrono::duration<std::chrono::milliseconds>(session_timeout.totalMilliseconds());
            auto max_wait = next_heartbeat_time > now
                ? std::chrono::duration_cast<std::chrono::milliseconds>(next_heartbeat_time - now)
                : 0;

            RequestPtr request;
            if (requests.tryPop(request, max_wait))
            {
                sendRequest(xid, *request);
            }
            else
            {
                prev_heartbeat_time = std::chrono::steady_clock::now();
                sendRequest(xid, HeartbeatRequest());
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

}


void ZooKeeper::sendRequest(XID xid, const Request & request)
{
    /// Excessive copy to calculate length.
    WriteBufferFromOwnString buf;
    write(xid, buf);
    request.write(buf);
    write(buf.str());
}


void ZooKeeper::HeartbeatRequest::write(WriteBuffer & out) const
{
    int32_t op_num = 11;
    write(op_num);
}


}
