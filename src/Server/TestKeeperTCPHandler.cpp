#include <Server/TestKeeperTCPHandler.h>
#include <Core/Types.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <Poco/Net/NetException.h>
#include <Common/CurrentThread.h>
#include <Common/Stopwatch.h>
#include <Common/NetException.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>
#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;

}

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-function"
#endif

/// ZooKeeper has 1 MB node size and serialization limit by default,
/// but it can be raised up, so we have a slightly larger limit on our side.
#define MAX_STRING_OR_ARRAY_SIZE (1 << 28)  /// 256 MiB

/// Assuming we are at little endian.

static void write(int64_t x, WriteBuffer & out)
{
    x = __builtin_bswap64(x);
    writeBinary(x, out);
}

static void write(int32_t x, WriteBuffer & out)
{
    x = __builtin_bswap32(x);
    writeBinary(x, out);
}

static void write(bool x, WriteBuffer & out)
{
    writeBinary(x, out);
}

static void write(const String & s, WriteBuffer & out)
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

static void write(const Coordination::ACL & acl, WriteBuffer & out)
{
    write(acl.permissions, out);
    write(acl.scheme, out);
    write(acl.id, out);
}

static void write(const Coordination::Stat & stat, WriteBuffer & out)
{
    write(stat.czxid, out);
    write(stat.mzxid, out);
    write(stat.ctime, out);
    write(stat.mtime, out);
    write(stat.version, out);
    write(stat.cversion, out);
    write(stat.aversion, out);
    write(stat.ephemeralOwner, out);
    write(stat.dataLength, out);
    write(stat.numChildren, out);
    write(stat.pzxid, out);
}

static void write(const Coordination::Error & x, WriteBuffer & out)
{
    write(static_cast<int32_t>(x), out);
}

static void read(int64_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap64(x);
}

static void read(int32_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap32(x);
}

static void read(Coordination::Error & x, ReadBuffer & in)
{
    int32_t code;
    read(code, in);
    x = Coordination::Error(code);
}

static void read(bool & x, ReadBuffer & in)
{
    readBinary(x, in);
}

static void read(String & s, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);

    if (size == -1)
    {
        /// It means that zookeeper node has NULL value. We will treat it like empty string.
        s.clear();
        return;
    }

    if (size < 0)
        throw Exception("Negative size while reading string from ZooKeeper", ErrorCodes::LOGICAL_ERROR);

    if (size > MAX_STRING_OR_ARRAY_SIZE)
        throw Exception("Too large string size while reading from ZooKeeper", ErrorCodes::LOGICAL_ERROR);

    s.resize(size);
    in.read(s.data(), size);
}

template <size_t N> void read(std::array<char, N> & s, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size != N)
        throw Exception("Unexpected array size while reading from ZooKeeper", ErrorCodes::LOGICAL_ERROR);
    in.read(s.data(), N);
}

static void read(Coordination::Stat & stat, ReadBuffer & in)
{
    read(stat.czxid, in);
    read(stat.mzxid, in);
    read(stat.ctime, in);
    read(stat.mtime, in);
    read(stat.version, in);
    read(stat.cversion, in);
    read(stat.aversion, in);
    read(stat.ephemeralOwner, in);
    read(stat.dataLength, in);
    read(stat.numChildren, in);
    read(stat.pzxid, in);
}

template <typename T> void read(std::vector<T> & arr, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size < 0)
        throw Exception("Negative size while reading array from ZooKeeper", ErrorCodes::LOGICAL_ERROR);
    if (size > MAX_STRING_OR_ARRAY_SIZE)
        throw Exception("Too large array size while reading from ZooKeeper", ErrorCodes::LOGICAL_ERROR);
    arr.resize(size);
    for (auto & elem : arr)
        read(elem, in);
}

static void read(Coordination::ACL & acl, ReadBuffer & in)
{
    read(acl.permissions, in);
    read(acl.scheme, in);
    read(acl.id, in);
}

#ifdef __clang__
#pragma clang diagnostic pop
#endif

void TestKeeperTCPHandler::sendHandshake()
{
    static constexpr int32_t handshake_length = 36;
    static constexpr int32_t protocol_version = 0;
    static constexpr int32_t DEFAULT_SESSION_TIMEOUT = 30000;

    write(handshake_length, *out);
    write(protocol_version, *out);
    write(DEFAULT_SESSION_TIMEOUT, *out);
    write(test_keeper_storage->getSessionID(), *out);
    constexpr int32_t passwd_len = 16;
    std::array<char, passwd_len> passwd{};
    write(passwd, *out);
    out->next();
}

void TestKeeperTCPHandler::run()
{
    runImpl();
}

void TestKeeperTCPHandler::receiveHandshake()
{
    int32_t handshake_length;
    int32_t protocol_version;
    int64_t last_zxid_seen;
    int32_t timeout;
    int64_t previous_session_id = 0;    /// We don't support session restore. So previous session_id is always zero.
    constexpr int32_t passwd_len = 16;
    std::array<char, passwd_len> passwd {};

    read(handshake_length, *in);
    if (handshake_length != 44)
        throw Exception("Unexpected handshake length received: " + toString(handshake_length), ErrorCodes::LOGICAL_ERROR);

    read(protocol_version, *in);

    if (protocol_version != 0)
        throw Exception("Unexpected protocol version: " + toString(protocol_version), ErrorCodes::LOGICAL_ERROR);

    read(last_zxid_seen, *in);

    if (last_zxid_seen != 0)
        throw Exception("Non zero last_zxid_seen is not supported", ErrorCodes::LOGICAL_ERROR);

    read(timeout, *in);
    read(previous_session_id, *in);

    if (previous_session_id != 0)
        throw Exception("Non zero previous session id is not supported", ErrorCodes::LOGICAL_ERROR);

    read(passwd, *in);
}


void TestKeeperTCPHandler::runImpl()
{
    setThreadName("TstKprHandler");
    ThreadStatus thread_status;
    auto global_receive_timeout = global_context.getSettingsRef().receive_timeout;
    auto global_send_timeout = global_context.getSettingsRef().send_timeout;

    socket().setReceiveTimeout(global_receive_timeout);
    socket().setSendTimeout(global_send_timeout);
    socket().setNoDelay(true);

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());

    if (in->eof())
    {
        LOG_WARNING(log, "Client has not sent any data.");
        return;
    }

    try
    {
        receiveHandshake();
    }
    catch (const Exception & e) /// Typical for an incorrect username, password, or address.
    {
        LOG_DEBUG(log, "Cannot receive handshake {}", e.displayText());
    }

    sendHandshake();

    while (true)
    {
        UInt64 max_wait = operation_timeout.totalMicroseconds();
        using namespace std::chrono_literals;
        if (!responses.empty() && responses.front().wait_for(100ms) == std::future_status::ready)
        {
            auto response = responses.front().get();
            response->write(*out);
            responses.pop();
        }

        if (in->poll(max_wait))
        {
            receiveHeartbeatRequest();
        }
    }
}


bool TestKeeperTCPHandler::receiveHeartbeatRequest()
{
    LOG_DEBUG(log, "Receiving heartbeat event");
    int32_t length;
    read(length, *in);
    int32_t total_count = in->count();
    LOG_DEBUG(log, "RECEIVED LENGTH {}", length);
    int32_t xid;
    LOG_DEBUG(log, "READING XID");
    read(xid, *in);

    LOG_DEBUG(log, "Received xid {}", xid);

    Coordination::ZooKeeperRequestPtr request;
    if (xid == -2)
    {
        int32_t opnum;
        read(opnum, *in);
        LOG_DEBUG(log, "RRECEIVED OP NUM {}", opnum);
        request = std::make_shared<Coordination::ZooKeeperHeartbeatRequest>();
        request->xid = xid;
        request->readImpl(*in);
        int32_t readed = in->count() - total_count;
        if (readed != length)
            LOG_DEBUG(log, "EXPECTED TO READ {}, BUT GOT {}", length, readed);
    }
    else
    {
        int32_t opnum;
        read(opnum, *in);
        LOG_DEBUG(log, "RRECEIVED OP NUM {}", opnum);
        if (opnum == 1)
            request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
        else if (opnum == 4)
            request = std::make_shared<Coordination::ZooKeeperGetRequest>();
        request->readImpl(*in);
        request->xid = xid;
        int32_t readed = in->count() - total_count;
        if (readed != length)
            LOG_DEBUG(log, "EXPECTED TO READ {}, BUT GOT {}", length, readed);
        LOG_DEBUG(log, "REQUEST PUTTED TO STORAGE");
    }

    responses.push(test_keeper_storage->putRequest(request));

    LOG_DEBUG(log, "Event received");
    return false;
}


void TestKeeperTCPHandler::sendHeartbeatResponse()
{
    LOG_DEBUG(log, "Sending heartbeat event");
    int32_t length = sizeof(int32_t) + sizeof(int64_t) + sizeof(Coordination::Error);
    write(length, *out);
    int64_t zxid = test_keeper_storage->getZXID();
    int32_t xid = -2;
    write(xid, *out);
    write(zxid, *out);
    write(Coordination::Error::ZOK, *out);
    auto response = std::make_shared<Coordination::ZooKeeperHeartbeatResponse>();
    response->writeImpl(*out);
    out->next();
}

}
