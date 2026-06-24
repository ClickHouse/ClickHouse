#include <Core/ProtocolDefines.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ClientInfo.h>
#include <Common/Exception.h>
#include <Poco/Net/SocketAddress.h>
#include <gtest/gtest.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

/// Serialize the prefix of a ClientInfo wire payload up to and including the initial_address string,
/// using the given address bytes verbatim. ClientInfo::read consumes: query_kind (1 byte),
/// initial_user (string), initial_query_id (string), initial_address (string).
String makeClientInfoPrefix(const String & address_string)
{
    WriteBufferFromOwnString buf;
    writeBinary(static_cast<UInt8>(ClientInfo::QueryKind::INITIAL_QUERY), buf);
    writeBinary(String("default"), buf);   /// initial_user
    writeBinary(String("query-id"), buf);  /// initial_query_id
    writeBinary(address_string, buf);       /// initial_address
    buf.finalize();
    return buf.str();
}

}

/// A non-numeric port arriving over the wire must NOT reach Poco's getservbyname()
/// (which is trapped to SIGILL in debug/sanitizer builds). It must be a catchable error instead.
TEST(ClientInfoRead, MalformedAddressNonNumericPortThrows)
{
    for (const String & bad : {"host:http", "127.0.0.1:notaport", "example.com:80x", "[::1]:abc"})
    {
        ClientInfo info;
        ReadBufferFromString in(makeClientInfoPrefix(bad));
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
        ReadBufferFromString in(makeClientInfoPrefix(bad));
        EXPECT_THROW(info.read(in, DBMS_TCP_PROTOCOL_VERSION), Exception) << "address: " << bad;
    }
}

TEST(ClientInfoRead, PortOutOfRangeThrows)
{
    ClientInfo info;
    ReadBufferFromString in(makeClientInfoPrefix("127.0.0.1:70000"));
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
/// only ever emits an IP literal, so a non-IP host is corrupted input and must be a catchable error.
TEST(ClientInfoRead, NonIpHostThrows)
{
    for (const String & bad : {"host:9000", ":9000", "example.com:80", "localhost:9000", "[notipv6]:9000"})
    {
        ClientInfo info;
        ReadBufferFromString in(makeClientInfoPrefix(bad));
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

/// A well-formed IP literal plus numeric port (the only form ClientInfo::write ever produces) must
/// still parse: a full write -> read round-trip preserves the initial address.
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
