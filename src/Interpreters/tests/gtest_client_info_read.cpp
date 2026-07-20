#include <Core/ProtocolDefines.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ClientInfo.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/AutoPtr.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/StreamChannel.h>
#include <gtest/gtest.h>

#include <sstream>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

/// Serialize the prefix of a ClientInfo wire payload up to and including the initial_address string,
/// using the given query_kind and address bytes verbatim. ClientInfo::read consumes: query_kind
/// (1 byte), initial_user (string), initial_query_id (string), initial_address (string).
/// For a SECONDARY_QUERY a malformed initial_address is rejected at this point, so the truncated
/// remainder is never read; that is what the reject tests rely on.
/// Callers must feed the result to ReadBufferFromOwnString: ReadBufferFromString is non-owning, so
/// reading from a temporary returned here would dangle.
String makeClientInfoPrefix(ClientInfo::QueryKind query_kind, const String & address_string)
{
    WriteBufferFromOwnString buf;
    writeBinary(static_cast<UInt8>(query_kind), buf);
    writeBinary(String("default"), buf);   /// initial_user
    writeBinary(String("query-id"), buf);  /// initial_query_id
    writeBinary(address_string, buf);       /// initial_address
    buf.finalize();
    return buf.str();
}

/// Serialize a COMPLETE, well-formed ClientInfo wire payload at DBMS_TCP_PROTOCOL_VERSION with the
/// given query_kind and a verbatim initial_address string (interface = TCP). Unlike the prefix
/// helper this emits every field ClientInfo::read expects, so read() runs to completion even when it
/// does not reject the address. Used to prove that an INITIAL_QUERY accepts a non-IP initial_address
/// leniently (the server overwrites it later) instead of throwing.
String makeFullClientInfoWire(ClientInfo::QueryKind query_kind, const String & address_string)
{
    WriteBufferFromOwnString buf;
    writeBinary(static_cast<UInt8>(query_kind), buf);
    writeBinary(String("default"), buf);                       /// initial_user
    writeBinary(String("query-id"), buf);                      /// initial_query_id
    writeBinary(address_string, buf);                          /// initial_address (verbatim)
    writeBinary(static_cast<Int64>(0), buf);                   /// initial_query_start_time (>= 54449)
    writeBinary(static_cast<UInt8>(ClientInfo::Interface::TCP), buf); /// interface
    /// interface == TCP block
    writeBinary(String("os-user"), buf);                       /// os_user
    writeBinary(String("client-host"), buf);                   /// client_hostname
    writeBinary(String("ClickHouse client"), buf);             /// client_name
    writeVarUInt(static_cast<UInt64>(1), buf);                 /// client_version_major
    writeVarUInt(static_cast<UInt64>(1), buf);                 /// client_version_minor
    writeVarUInt(static_cast<UInt64>(DBMS_TCP_PROTOCOL_VERSION), buf); /// client_tcp_protocol_version
    writeBinary(String(""), buf);                              /// quota_key (>= 54060)
    writeVarUInt(static_cast<UInt64>(0), buf);                 /// distributed_depth (>= 54448)
    writeVarUInt(static_cast<UInt64>(DBMS_TCP_PROTOCOL_VERSION), buf); /// client_version_patch (TCP, >= 54401)
    writeBinary(static_cast<UInt8>(0), buf);                   /// have OpenTelemetry trace id = no (>= 54442)
    writeVarUInt(static_cast<UInt64>(0), buf);                 /// collaborate_with_initiator (>= 54453)
    writeVarUInt(static_cast<UInt64>(0), buf);                 /// obsolete_count_participating_replicas
    writeVarUInt(static_cast<UInt64>(0), buf);                 /// number_of_current_replica
    writeVarUInt(static_cast<UInt64>(0), buf);                 /// script_query_number (>= 54475)
    writeVarUInt(static_cast<UInt64>(0), buf);                 /// script_line_number
    writeBinary(static_cast<UInt8>(0), buf);                   /// have_jwt = no (>= 54476)
    writeBinary(String(""), buf);                              /// client_agent (>= 54485)
    writeBinary(false, buf);                                   /// is_internal (>= 54486)
    buf.finalize();
    return buf.str();
}

class LoggerStateGuard final
{
public:
    explicit LoggerStateGuard(const LoggerPtr & logger_)
        : logger(logger_)
        , channel(logger_->getChannel(), true)
        , level(logger_->getLevel())
    {
    }

    ~LoggerStateGuard()
    {
        logger->setChannel(channel.get());
        logger->setLevel(level);
    }

private:
    LoggerPtr logger;
    Poco::AutoPtr<Poco::Channel> channel;
    int level;
};

}

/// A non-numeric port arriving over the wire must NOT reach Poco's getservbyname()
/// (which is trapped to SIGILL in debug/sanitizer builds). For a SECONDARY_QUERY (where the wire
/// initial_address is consumed verbatim) it must be a catchable error instead.
TEST(ClientInfoRead, MalformedAddressNonNumericPortThrows)
{
    for (const String & bad : {"host:http", "127.0.0.1:notaport", "example.com:80x", "[::1]:abc"})
    {
        ClientInfo info;
        ReadBufferFromOwnString in(makeClientInfoPrefix(ClientInfo::QueryKind::SECONDARY_QUERY, bad));
        try
        {
            info.read(in, DBMS_TCP_PROTOCOL_VERSION);
            FAIL() << "Expected an exception for address: " << bad;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << "address: " << bad;
        }
    }
}

TEST(ClientInfoRead, MalformedAddressMissingPortThrows)
{
    for (const String & bad : {"hostonly", "127.0.0.1:", "[::1]"})
    {
        ClientInfo info;
        ReadBufferFromOwnString in(makeClientInfoPrefix(ClientInfo::QueryKind::SECONDARY_QUERY, bad));
        EXPECT_THROW(info.read(in, DBMS_TCP_PROTOCOL_VERSION), Exception) << "address: " << bad;
    }
}

TEST(ClientInfoRead, PortOutOfRangeThrows)
{
    ClientInfo info;
    ReadBufferFromOwnString in(makeClientInfoPrefix(ClientInfo::QueryKind::SECONDARY_QUERY, "127.0.0.1:70000"));
    try
    {
        info.read(in, DBMS_TCP_PROTOCOL_VERSION);
        FAIL() << "Expected an exception for out-of-range port";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
}

/// A numeric port is not enough: a non-IP host (e.g. "host:9000" or ":9000") must NOT reach
/// Poco's DNS::hostByName() / gethostbyname() family (also trapped to SIGILL). ClientInfo::write
/// only ever emits an IP literal, so for a SECONDARY_QUERY a non-IP host is corrupted input and
/// must be a catchable error.
TEST(ClientInfoRead, NonIpHostThrows)
{
    for (const String & bad : {"host:9000", ":9000", "example.com:80", "localhost:9000", "[notipv6]:9000"})
    {
        ClientInfo info;
        ReadBufferFromOwnString in(makeClientInfoPrefix(ClientInfo::QueryKind::SECONDARY_QUERY, bad));
        try
        {
            info.read(in, DBMS_TCP_PROTOCOL_VERSION);
            FAIL() << "Expected an exception for address: " << bad;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << "address: " << bad;
        }
    }
}

/// A leading-'/' value makes Poco build a UNIX_LOCAL SocketAddress whose host()/port() throw later.
/// ClientInfo::write never emits this form, and every consumer of a SECONDARY_QUERY initial_address
/// calls host()/port(), so it must be rejected here as INCORRECT_DATA rather than parsed into a
/// UNIX_LOCAL address.
TEST(ClientInfoRead, UnixLocalPathThrows)
{
    for (const String & bad : {"/tmp/ch.sock", "/", "/var/run/clickhouse-server/clickhouse-server.sock"})
    {
        ClientInfo info;
        ReadBufferFromOwnString in(makeClientInfoPrefix(ClientInfo::QueryKind::SECONDARY_QUERY, bad));
        try
        {
            info.read(in, DBMS_TCP_PROTOCOL_VERSION);
            FAIL() << "Expected an exception for address: " << bad;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << "address: " << bad;
        }
    }
}

/// For an INITIAL_QUERY the server overwrites initial_address with the real peer address
/// (Session::makeQueryContextImpl), so the wire value is discarded. To preserve compatibility with
/// the pre-validation native protocol (which documented a generic host:port), a non-IP / malformed
/// initial_address from an initiating client must be accepted leniently - never resolved, never
/// throwing - and fall back to a default endpoint. The SIGILL guard still holds: the parser never
/// calls a resolver, so the forms below cannot reach getservbyname()/DNS.
TEST(ClientInfoRead, InitialQueryAcceptsNonIpAddressLeniently)
{
    for (const String & value : {"localhost:9000", "host:http", ":9000", "/tmp/ch.sock", "garbage"})
    {
        ClientInfo info;
        ReadBufferFromOwnString in(makeFullClientInfoWire(ClientInfo::QueryKind::INITIAL_QUERY, value));
        ASSERT_NO_THROW(info.read(in, DBMS_TCP_PROTOCOL_VERSION)) << "address: " << value;
        /// Not a valid "ip:port", so it falls back to the default endpoint instead of being resolved.
        EXPECT_EQ(info.initial_address->toString(), Poco::Net::SocketAddress{}.toString()) << "address: " << value;
    }
}

/// A well-formed IP literal plus numeric port (the only form ClientInfo::write ever produces) must
/// still parse for any query kind: a full write -> read round-trip preserves the initial address.
TEST(ClientInfoRead, ValidNumericAddressRoundTrips)
{
    for (const String & good : {"127.0.0.1:9000", "[::1]:9000", "8.8.8.8:0", "0.0.0.0:65535", "[2001:db8::1]:443"})
    {
        ClientInfo out;
        out.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        out.initial_user = "default";
        out.initial_query_id = "query-id";
        out.initial_address = std::make_shared<Poco::Net::SocketAddress>(good);
        out.interface = ClientInfo::Interface::TCP;

        WriteBufferFromOwnString buf;
        out.write(buf, DBMS_TCP_PROTOCOL_VERSION);
        buf.finalize();

        ClientInfo in_info;
        ReadBufferFromString in(buf.str());
        ASSERT_NO_THROW(in_info.read(in, DBMS_TCP_PROTOCOL_VERSION)) << "address: " << good;
        EXPECT_EQ(in_info.initial_address->toString(), good);
    }
}

/// The same valid round-trip must hold for a SECONDARY_QUERY, where the wire initial_address is
/// kept and consumed verbatim (system.query_log, interserver authenticate).
TEST(ClientInfoRead, ValidNumericAddressRoundTripsSecondaryQuery)
{
    for (const String & good : {"10.0.0.1:9000", "[2001:db8::1]:9440"})
    {
        ClientInfo info;
        ReadBufferFromOwnString in(makeFullClientInfoWire(ClientInfo::QueryKind::SECONDARY_QUERY, good));
        ASSERT_NO_THROW(info.read(in, DBMS_TCP_PROTOCOL_VERSION)) << "address: " << good;
        EXPECT_EQ(info.initial_address->toString(), good);
    }
}

TEST(ClientInfoForwardedFor, ParsesNumericAddresses)
{
    const auto check_address = [](const String & value, const String & expected_host, UInt16 expected_port)
    {
        ClientInfo info;
        info.forwarded_for = value;

        const auto address = info.getLastForwardedFor();
        ASSERT_TRUE(address) << "address: " << value;
        EXPECT_EQ(address->host().toString(), expected_host) << "address: " << value;
        EXPECT_EQ(address->port(), expected_port) << "address: " << value;
    };

    check_address("123.124.125.126", "123.124.125.126", 0);
    check_address("123.124.125.126:80", "123.124.125.126", 80);
    check_address("2001:db8::1", "2001:db8::1", 0);
    check_address("[2001:db8::1]:443", "2001:db8::1", 443);
    check_address("not-used.example," + String(2, ' ') + "203.0.113.7:65535" + String(2, ' '), "203.0.113.7", 65535);
}

TEST(ClientInfoForwardedFor, RejectsNonNumericAndMalformedAddresses)
{
    for (const String & bad : {
        "localhost",
        "localhost:80",
        "attacker.example:443",
        "127.0.0.1:http",
        "[not-an-ip]:80",
        "[2001:db8::1]",
        "127.0.0.1:65536",
        "127.0.0.1:"})
    {
        ClientInfo info;
        info.forwarded_for = bad;

        std::optional<Poco::Net::SocketAddress> address;
        EXPECT_NO_THROW(address = info.getLastForwardedFor()) << "address: " << bad;
        EXPECT_FALSE(address) << "address: " << bad;
    }

    ClientInfo trailing_whitespace;
    trailing_whitespace.forwarded_for = "127.0.0.1," + String(3, ' ');
    EXPECT_FALSE(trailing_whitespace.getLastForwardedFor());

    ClientInfo empty;
    EXPECT_FALSE(empty.getLastForwardedFor());

    ClientInfo hostname;
    hostname.forwarded_for = "localhost";
    EXPECT_EQ(hostname.getLastForwardedForHost(), "");
}

TEST(ClientInfoForwardedFor, LogsRejectedAddressAtWarning)
{
    std::ostringstream log_output; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    auto stream_channel = Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(log_output));
    auto log = getLogger("ClientInfo");
    LoggerStateGuard logger_state_guard(log);
    log->setChannel(stream_channel.get());
    log->setLevel("warning");

    ClientInfo info;
    info.forwarded_for = "attacker.example";
    EXPECT_FALSE(info.getLastForwardedFor());

    EXPECT_NE(
        log_output.str().find("Invalid address in `X-Forwarded-For` HTTP header: 'attacker.example'"),
        std::string::npos);
}
