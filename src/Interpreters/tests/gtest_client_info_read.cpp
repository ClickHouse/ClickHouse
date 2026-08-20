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
#include <fmt/format.h>
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
String makeFullClientInfoWire(
    ClientInfo::QueryKind query_kind,
    const String & address_string,
    UInt64 client_version_major = 1,
    UInt64 client_version_minor = 1,
    UInt64 client_version_patch = DBMS_TCP_PROTOCOL_VERSION,
    UInt64 client_tcp_protocol_version = DBMS_TCP_PROTOCOL_VERSION,
    bool is_time_series_target_read = false,
    const std::vector<QualifiedTableName> & time_series_target_tables = {})
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
    writeVarUInt(client_version_major, buf);                   /// client_version_major
    writeVarUInt(client_version_minor, buf);                   /// client_version_minor
    writeVarUInt(client_tcp_protocol_version, buf);            /// client_tcp_protocol_version
    writeBinary(String(""), buf);                              /// quota_key (>= 54060)
    writeVarUInt(static_cast<UInt64>(0), buf);                 /// distributed_depth (>= 54448)
    writeVarUInt(client_version_patch, buf);                   /// client_version_patch (TCP, >= 54401)
    writeBinary(static_cast<UInt8>(0), buf);                   /// have OpenTelemetry trace id = no (>= 54442)
    writeVarUInt(static_cast<UInt64>(0), buf);                 /// collaborate_with_initiator (>= 54453)
    writeVarUInt(static_cast<UInt64>(0), buf);                 /// obsolete_count_participating_replicas
    writeVarUInt(static_cast<UInt64>(0), buf);                 /// number_of_current_replica
    writeVarUInt(static_cast<UInt64>(0), buf);                 /// script_query_number (>= 54475)
    writeVarUInt(static_cast<UInt64>(0), buf);                 /// script_line_number
    writeBinary(static_cast<UInt8>(0), buf);                   /// have_jwt = no (>= 54476)
    writeBinary(String(""), buf);                              /// client_agent (>= 54485)
    writeBinary(false, buf);                                   /// is_internal (>= 54486)
    writeBinary(static_cast<UInt8>(0), buf);                   /// have_current_roles = no (>= 54488)
    writeBinary(is_time_series_target_read, buf);              /// is_time_series_target_read (>= 54493)
    writeBinary(false, buf);                                   /// ignore_quota (>= 54494)
    writeVarUInt(time_series_target_tables.size(), buf);       /// TimeSeries target scope (>= 54495)
    for (const auto & table : time_series_target_tables)
    {
        writeBinary(table.database, buf);
        writeBinary(table.table, buf);
    }
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
        out.initial_address = Poco::Net::SocketAddress(good);
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

/// Numeric IPv4 and IPv6 addresses, with or without numeric ports, must be accepted. For a
/// comma-separated `X-Forwarded-For` chain, the last proxy-appended element is used after trimming.
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

/// Hostnames and malformed endpoints must be rejected without resolver calls. Empty input and an empty
/// last chain element are also rejected, and `getLastForwardedForHost` returns an empty string.
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

    /// Successful and rejected results are reused while `forwarded_for` is unchanged,
    /// including by copies sharing the cache, so repeated access logs an invalid value once.
TEST(ClientInfoForwardedFor, LogsEachRejectedAddressOnlyOnce)
{
    std::ostringstream log_output; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    auto stream_channel = Poco::AutoPtr<Poco::StreamChannel>(new Poco::StreamChannel(log_output));
    auto log = getLogger("ClientInfo");
    LoggerStateGuard logger_state_guard(log);
    log->setChannel(stream_channel.get());
    log->setLevel("debug");

    ClientInfo info;
    info.forwarded_for = "attacker.example";
    EXPECT_FALSE(info.getLastForwardedFor());
    EXPECT_EQ(info.getLastForwardedForHost(), "");
    EXPECT_FALSE(info.getLastForwardedFor());

    /// Copies initially share the cached result. Changing `forwarded_for` on a copy causes its next lookup
    /// to install a new cache entry without affecting the entry used by the original object.
    ClientInfo copied_info = info;
    EXPECT_FALSE(copied_info.getLastForwardedFor());
    copied_info.forwarded_for = "second-attacker.example";
    EXPECT_FALSE(copied_info.getLastForwardedFor());
    EXPECT_EQ(copied_info.getLastForwardedForHost(), "");
    EXPECT_FALSE(info.getLastForwardedFor());

    const String log_text = log_output.str();
    for (const String & rejected : {"attacker.example", "second-attacker.example"})
    {
        const String message = fmt::format("Invalid address in `X-Forwarded-For` HTTP header: '{}'", rejected);
        const auto first_position = log_text.find(message);
        ASSERT_NE(first_position, std::string::npos);
        EXPECT_EQ(log_text.find(message, first_position + message.size()), std::string::npos);
    }
}

/// `current_roles` is tri-state: nullopt = not sent (remote keeps defaults), empty = SET ROLE NONE (remote
/// drops defaults), non-empty = active roles. Empty must round-trip as empty, not collapse to nullopt.
TEST(ClientInfoRead, CurrentRolesRoundTripsTriState)
{
    auto round_trip = [](const std::optional<std::vector<String>> & roles)
    {
        ClientInfo out;
        out.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        out.initial_user = "default";
        out.initial_query_id = "query-id";
        out.initial_address = std::make_optional<Poco::Net::SocketAddress>("127.0.0.1:9000");
        out.interface = ClientInfo::Interface::TCP;
        out.current_roles = roles;

        WriteBufferFromOwnString buf;
        out.write(buf, DBMS_TCP_PROTOCOL_VERSION);
        buf.finalize();

        ClientInfo in_info;
        ReadBufferFromString in(buf.str());
        in_info.read(in, DBMS_TCP_PROTOCOL_VERSION);
        return in_info.current_roles;
    };

    /// Not sent stays not sent.
    EXPECT_FALSE(round_trip(std::nullopt).has_value());

    /// SET ROLE NONE: empty-but-present survives as an empty list, not nullopt.
    auto none = round_trip(std::vector<String>{});
    ASSERT_TRUE(none.has_value());
    EXPECT_TRUE(none->empty());

    /// Active roles round-trip verbatim.
    auto some = round_trip(std::vector<String>{"role_a", "role_b"});
    ASSERT_TRUE(some.has_value());
    EXPECT_EQ(*some, (std::vector<String>{"role_a", "role_b"}));
}

TEST(ClientInfoRead, TimeSeriesTargetReadMarkerRoundTrips)
{
    ClientInfo out;
    out.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    out.initial_user = "default";
    out.initial_query_id = "query-id";
    out.initial_address = std::make_optional<Poco::Net::SocketAddress>("127.0.0.1:9000");
    out.interface = ClientInfo::Interface::TCP;
    out.is_time_series_target_read = true;
    out.ignore_quota = true;
    out.time_series_target_tables = {
        QualifiedTableName{"default", "metrics"},
        QualifiedTableName{"remote_db", "remote_metrics"},
    };

    WriteBufferFromOwnString buf;
    out.write(buf, DBMS_TCP_PROTOCOL_VERSION);
    buf.finalize();

    ClientInfo in_info;
    ReadBufferFromString in(buf.str());
    in_info.read(in, DBMS_TCP_PROTOCOL_VERSION);
    EXPECT_TRUE(in_info.is_time_series_target_read);
    EXPECT_TRUE(in_info.ignore_quota);
    EXPECT_EQ(in_info.time_series_target_tables, out.time_series_target_tables);

    ClientInfo no_marker = out;
    no_marker.is_time_series_target_read = false;
    WriteBufferFromOwnString no_marker_buf;
    no_marker.write(no_marker_buf, DBMS_TCP_PROTOCOL_VERSION);
    no_marker_buf.finalize();

    ClientInfo no_marker_info;
    ReadBufferFromString no_marker_in(no_marker_buf.str());
    no_marker_info.read(no_marker_in, DBMS_TCP_PROTOCOL_VERSION);
    EXPECT_FALSE(no_marker_info.is_time_series_target_read);
    EXPECT_TRUE(no_marker_info.time_series_target_tables.empty());

    WriteBufferFromOwnString old_buf;
    out.write(old_buf, DBMS_MIN_PROTOCOL_VERSION_WITH_TIME_SERIES_TARGET_READ - 1);
    old_buf.finalize();

    ClientInfo old_info;
    ReadBufferFromString old_in(old_buf.str());
    old_info.read(old_in, DBMS_MIN_PROTOCOL_VERSION_WITH_TIME_SERIES_TARGET_READ - 1);
    EXPECT_FALSE(old_info.is_time_series_target_read);

    WriteBufferFromOwnString old_scope_buf;
    out.write(old_scope_buf, DBMS_MIN_PROTOCOL_VERSION_WITH_TIME_SERIES_TARGET_SCOPE - 1);
    old_scope_buf.finalize();

    ClientInfo old_scope_info;
    ReadBufferFromString old_scope_in(old_scope_buf.str());
    old_scope_info.read(old_scope_in, DBMS_MIN_PROTOCOL_VERSION_WITH_TIME_SERIES_TARGET_SCOPE - 1);
    EXPECT_FALSE(old_scope_info.is_time_series_target_read);
    EXPECT_TRUE(old_scope_info.time_series_target_tables.empty());

    WriteBufferFromOwnString old_ignore_quota_buf;
    out.write(old_ignore_quota_buf, DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_EXECUTION_FLAGS - 1);
    old_ignore_quota_buf.finalize();

    ClientInfo old_ignore_quota_info;
    ReadBufferFromString old_ignore_quota_in(old_ignore_quota_buf.str());
    old_ignore_quota_info.read(old_ignore_quota_in, DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_EXECUTION_FLAGS - 1);
    EXPECT_FALSE(old_ignore_quota_info.is_time_series_target_read);
    EXPECT_FALSE(old_ignore_quota_info.ignore_quota);
    EXPECT_TRUE(old_ignore_quota_info.time_series_target_tables.empty());
}

TEST(ClientInfoRead, TimeSeriesTargetScopeRejectsEmptyNames)
{
    for (const auto & target : {QualifiedTableName{"", "metrics"}, QualifiedTableName{"default", ""}})
    {
        ClientInfo out;
        out.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        out.initial_user = "default";
        out.initial_query_id = "query-id";
        out.initial_address = std::make_optional<Poco::Net::SocketAddress>("127.0.0.1:9000");
        out.interface = ClientInfo::Interface::TCP;
        out.is_time_series_target_read = true;
        out.time_series_target_tables = {target};

        WriteBufferFromOwnString buf;
        EXPECT_THROW(out.write(buf, DBMS_TCP_PROTOCOL_VERSION), Exception);
    }
}

TEST(ClientInfoRead, TimeSeriesTargetScopeReadRejectsEmptyNames)
{
    for (const auto & target : {QualifiedTableName{"", "metrics"}, QualifiedTableName{"default", ""}})
    {
        const auto wire = makeFullClientInfoWire(
            ClientInfo::QueryKind::SECONDARY_QUERY,
            "127.0.0.1:9000",
            1,
            1,
            DBMS_TCP_PROTOCOL_VERSION,
            DBMS_TCP_PROTOCOL_VERSION,
            true,
            {target});

        ClientInfo info;
        ReadBufferFromOwnString in(wire);
        EXPECT_THROW(info.read(in, DBMS_TCP_PROTOCOL_VERSION), Exception);
    }
}

TEST(ClientInfoRead, RegularTcpInitialQueryClearsServerGeneratedFields)
{
    ClientInfo info;
    info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    info.is_internal = true;
    info.ignore_quota = true;
    info.is_time_series_target_read = true;
    info.time_series_target_tables = {QualifiedTableName{"default", "metrics"}};

    info.sanitizeServerGeneratedFields(false, DBMS_TCP_PROTOCOL_VERSION);

    EXPECT_FALSE(info.is_internal);
    EXPECT_FALSE(info.ignore_quota);
    EXPECT_FALSE(info.is_time_series_target_read);
    EXPECT_TRUE(info.time_series_target_tables.empty());

    const auto flags = info.getTrustedQueryFlags(false, DBMS_TCP_PROTOCOL_VERSION);
    EXPECT_FALSE(flags.internal);
    EXPECT_FALSE(flags.ignore_quota);
}

TEST(ClientInfoRead, RegularTcpSecondaryQueryClearsServerGeneratedFields)
{
    ClientInfo info;
    info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    info.is_internal = true;
    info.ignore_quota = true;
    info.is_time_series_target_read = true;
    info.time_series_target_tables = {QualifiedTableName{"default", "metrics"}};

    info.sanitizeServerGeneratedFields(false, DBMS_TCP_PROTOCOL_VERSION);

    EXPECT_FALSE(info.is_internal);
    EXPECT_FALSE(info.ignore_quota);
    EXPECT_FALSE(info.is_time_series_target_read);
    EXPECT_TRUE(info.time_series_target_tables.empty());

    const auto flags = info.getTrustedQueryFlags(false, DBMS_TCP_PROTOCOL_VERSION);
    EXPECT_FALSE(flags.internal);
    EXPECT_FALSE(flags.ignore_quota);
}

TEST(ClientInfoRead, RegularTcpSecondaryCapabilitiesDoNotCrossInterserverHop)
{
    ClientInfo info;
    info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    info.is_internal = true;
    info.ignore_quota = true;
    info.is_time_series_target_read = true;
    info.time_series_target_tables = {QualifiedTableName{"default", "metrics"}};

    info.sanitizeServerGeneratedFields(false, DBMS_TCP_PROTOCOL_VERSION);
    info.setTrustedQueryFlags(info.getTrustedQueryFlags(false, DBMS_TCP_PROTOCOL_VERSION));

    const auto forwarded = info.getClientInfoForInterserverForwarding();
    EXPECT_FALSE(forwarded.is_internal);
    EXPECT_FALSE(forwarded.ignore_quota);
    EXPECT_FALSE(forwarded.is_time_series_target_read);
    EXPECT_TRUE(forwarded.time_series_target_tables.empty());

    const auto downstream_flags = forwarded.getTrustedQueryFlags(
        true, DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_EXECUTION_FLAGS);
    EXPECT_FALSE(downstream_flags.internal);
    EXPECT_FALSE(downstream_flags.ignore_quota);
}

TEST(ClientInfoRead, InitialQueryCannotCarryTimeSeriesTargetScope)
{
    ClientInfo info;
    info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    info.is_time_series_target_read = true;
    info.time_series_target_tables = {QualifiedTableName{"default", "metrics"}};

    info.sanitizeServerGeneratedFields(true, DBMS_TCP_PROTOCOL_VERSION);

    EXPECT_FALSE(info.is_time_series_target_read);
    EXPECT_TRUE(info.time_series_target_tables.empty());
}

TEST(ClientInfoRead, ReadResetsTrustedForwardingFlags)
{
    ClientInfo info;
    info.setTrustedQueryFlags(QueryFlags{.internal = true, .ignore_quota = true});

    ReadBufferFromOwnString in(makeFullClientInfoWire(ClientInfo::QueryKind::INITIAL_QUERY, "127.0.0.1:9000"));
    ASSERT_NO_THROW(info.read(in, DBMS_TCP_PROTOCOL_VERSION));
    EXPECT_FALSE(info.getTrustedQueryFlagsForForwarding().internal);
    EXPECT_FALSE(info.getTrustedQueryFlagsForForwarding().ignore_quota);

    info.setTrustedQueryFlags(QueryFlags{.internal = true, .ignore_quota = true});
    WriteBufferFromOwnString no_query_payload;
    writeBinary(static_cast<UInt8>(ClientInfo::QueryKind::NO_QUERY), no_query_payload);
    no_query_payload.finalize();
    ReadBufferFromOwnString no_query_in(no_query_payload.str());
    ASSERT_NO_THROW(info.read(no_query_in, DBMS_TCP_PROTOCOL_VERSION));
    EXPECT_FALSE(info.getTrustedQueryFlagsForForwarding().internal);
    EXPECT_FALSE(info.getTrustedQueryFlagsForForwarding().ignore_quota);
}

TEST(ClientInfoRead, OlderInterserverInternalFlagIsNotTrusted)
{
    constexpr auto old_protocol_revision = DBMS_MIN_PROTOCOL_VERSION_WITH_TIME_SERIES_TARGET_READ - 1; /// 54492

    ClientInfo out;
    out.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    out.initial_user = "default";
    out.initial_query_id = "query-id";
    out.initial_address = std::make_optional<Poco::Net::SocketAddress>("127.0.0.1:9000");
    out.interface = ClientInfo::Interface::TCP;
    out.is_internal = true;

    WriteBufferFromOwnString buf;
    out.write(buf, old_protocol_revision);
    buf.finalize();

    ClientInfo received;
    ReadBufferFromString in(buf.str());
    ASSERT_NO_THROW(received.read(in, old_protocol_revision));
    ASSERT_TRUE(received.is_internal);

    received.sanitizeServerGeneratedFields(true, old_protocol_revision);
    EXPECT_FALSE(received.is_internal);

    received.is_internal = true;
    received.sanitizeServerGeneratedFields(false, old_protocol_revision);
    EXPECT_FALSE(received.is_internal);

    received.is_internal = true;
    received.sanitizeServerGeneratedFields(true, DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_EXECUTION_FLAGS);
    EXPECT_TRUE(received.is_internal);
}

TEST(ClientInfoRead, TrustedQueryFlagsRequireInterserverAuthentication)
{
    ClientInfo regular_initial;
    regular_initial.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    regular_initial.is_internal = true;
    regular_initial.ignore_quota = true;

    const auto regular_initial_flags = regular_initial.getTrustedQueryFlags(false, DBMS_TCP_PROTOCOL_VERSION);
    EXPECT_FALSE(regular_initial_flags.internal);
    EXPECT_FALSE(regular_initial_flags.ignore_quota);

    ClientInfo regular_secondary;
    regular_secondary.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    regular_secondary.is_internal = true;
    regular_secondary.ignore_quota = true;

    const auto regular_secondary_flags = regular_secondary.getTrustedQueryFlags(false, DBMS_TCP_PROTOCOL_VERSION);
    EXPECT_FALSE(regular_secondary_flags.internal);
    EXPECT_FALSE(regular_secondary_flags.ignore_quota);

    ClientInfo info;
    info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    info.is_internal = true;
    info.ignore_quota = true;
    const auto old_interserver_flags = info.getTrustedQueryFlags(
        true, DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_EXECUTION_FLAGS - 1);
    EXPECT_FALSE(old_interserver_flags.internal);
    EXPECT_FALSE(old_interserver_flags.ignore_quota);

    const auto authenticated_interserver_flags = info.getTrustedQueryFlags(
        true, DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_EXECUTION_FLAGS);
    EXPECT_TRUE(authenticated_interserver_flags.internal);
    EXPECT_TRUE(authenticated_interserver_flags.ignore_quota);
}

TEST(ClientInfoRead, InterserverForwardingUsesTrustedQueryFlags)
{
    ClientInfo info;
    info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    info.is_internal = true;
    info.ignore_quota = true;

    auto forwarded = info.getClientInfoForInterserverForwarding();
    EXPECT_FALSE(forwarded.is_internal);
    EXPECT_FALSE(forwarded.ignore_quota);
    EXPECT_FALSE(forwarded.getTrustedQueryFlags(true, DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_EXECUTION_FLAGS).internal);
    EXPECT_FALSE(forwarded.getTrustedQueryFlags(true, DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_EXECUTION_FLAGS).ignore_quota);

    info.setTrustedQueryFlags(QueryFlags{.internal = true, .ignore_quota = true});
    forwarded = info.getClientInfoForInterserverForwarding();
    EXPECT_TRUE(forwarded.is_internal);
    EXPECT_TRUE(forwarded.ignore_quota);
    EXPECT_TRUE(forwarded.getTrustedQueryFlags(true, DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_EXECUTION_FLAGS).internal);
    EXPECT_TRUE(forwarded.getTrustedQueryFlags(true, DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_EXECUTION_FLAGS).ignore_quota);

    info.setTrustedQueryFlags(QueryFlags{});
    forwarded = info.getClientInfoForInterserverForwarding();
    EXPECT_FALSE(forwarded.is_internal);
    EXPECT_FALSE(forwarded.ignore_quota);
}

TEST(ClientInfoRead, ForwardedQueryFlagsKeepTrustedProvenance)
{
    ClientInfo untrusted;
    untrusted.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    untrusted.is_internal = true;
    untrusted.ignore_quota = true;
    untrusted.sanitizeServerGeneratedFields(false, DBMS_TCP_PROTOCOL_VERSION);

    const auto local_flags = untrusted.getTrustedQueryFlags(false, DBMS_TCP_PROTOCOL_VERSION);
    EXPECT_FALSE(local_flags.internal);
    EXPECT_FALSE(local_flags.ignore_quota);

    /// This is the state executeQuery() records for the current query before fan-out.
    untrusted.setTrustedQueryFlags(local_flags);
    const auto forwarded_untrusted = untrusted.getClientInfoForInterserverForwarding();
    const auto downstream_untrusted_flags = forwarded_untrusted.getTrustedQueryFlags(
        true, DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_EXECUTION_FLAGS);
    EXPECT_FALSE(downstream_untrusted_flags.internal);
    EXPECT_FALSE(downstream_untrusted_flags.ignore_quota);

    ClientInfo server_internal;
    server_internal.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    server_internal.setTrustedQueryFlags(QueryFlags{.internal = true, .ignore_quota = true});
    const auto forwarded_internal = server_internal.getClientInfoForInterserverForwarding();
    const auto downstream_internal_flags = forwarded_internal.getTrustedQueryFlags(
        true, DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_EXECUTION_FLAGS);
    EXPECT_TRUE(downstream_internal_flags.internal);
    EXPECT_TRUE(downstream_internal_flags.ignore_quota);
}

/// An older peer can forward a server-initiated query whose context was never filled with a
/// version, so `client_version_*` arrives as 0.0.0 over the wire and `read` overwrites the seed
/// taken from the session. `setClientVersionFromConnectionIfUnknown` must then restore the peer's
/// version from the connection hello, so that version-gated compatibility logic and the
/// zero-version check in `RemoteQueryExecutor` on the next hop do not misfire during a rolling
/// upgrade. This mirrors the exact `TCPHandler::receiveQuery` sequence: seed, read, normalize.
TEST(ClientInfoVersionFromConnection, ZeroWireVersionFilledFromConnectionHello)
{
    ClientInfo info;
    info.connection_client_version_major = 26;
    info.connection_client_version_minor = 7;
    info.connection_client_version_patch = 1;
    info.connection_tcp_protocol_version = DBMS_TCP_PROTOCOL_VERSION;

    ReadBufferFromOwnString in(makeFullClientInfoWire(ClientInfo::QueryKind::SECONDARY_QUERY, "127.0.0.1:9000", 0, 0, 0, 0));
    ASSERT_NO_THROW(info.read(in, DBMS_TCP_PROTOCOL_VERSION));
    ASSERT_EQ(info.client_version_major, 0u);

    info.setClientVersionFromConnectionIfUnknown();

    EXPECT_EQ(info.client_version_major, 26u);
    EXPECT_EQ(info.client_version_minor, 7u);
    EXPECT_EQ(info.client_version_patch, 1u);
    EXPECT_EQ(info.client_tcp_protocol_version, DBMS_TCP_PROTOCOL_VERSION);
}

/// A known (non-zero) wire version is authoritative: it identifies the initial client, which may
/// differ from the immediate peer, and must never be replaced by the connection hello version.
TEST(ClientInfoVersionFromConnection, KnownWireVersionIsKept)
{
    ClientInfo info;
    info.connection_client_version_major = 26;
    info.connection_client_version_minor = 7;
    info.connection_client_version_patch = 1;
    info.connection_tcp_protocol_version = DBMS_TCP_PROTOCOL_VERSION;

    ReadBufferFromOwnString in(makeFullClientInfoWire(ClientInfo::QueryKind::SECONDARY_QUERY, "127.0.0.1:9000", 25, 3, 2, 54467));
    ASSERT_NO_THROW(info.read(in, DBMS_TCP_PROTOCOL_VERSION));

    info.setClientVersionFromConnectionIfUnknown();

    EXPECT_EQ(info.client_version_major, 25u);
    EXPECT_EQ(info.client_version_minor, 3u);
    EXPECT_EQ(info.client_version_patch, 2u);
    EXPECT_EQ(info.client_tcp_protocol_version, 54467u);
}

/// Without a connection hello version (e.g. a synthesized local context, not a TCP connection)
/// there is nothing trustworthy to fill from, so the zero version must stay zero - the sender-side
/// check in `RemoteQueryExecutor` is what reports such a context as a logical error.
TEST(ClientInfoVersionFromConnection, NoConnectionVersionIsNoop)
{
    ClientInfo info;
    info.setClientVersionFromConnectionIfUnknown();

    EXPECT_EQ(info.client_version_major, 0u);
    EXPECT_EQ(info.client_version_minor, 0u);
    EXPECT_EQ(info.client_version_patch, 0u);
    EXPECT_EQ(info.client_tcp_protocol_version, 0u);
}
