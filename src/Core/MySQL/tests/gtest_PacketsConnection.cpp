#include <gtest/gtest.h>

#include <base/types.h>
#include <Core/MySQL/PacketsConnection.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

using namespace DB;
using namespace DB::MySQLProtocol;
using namespace DB::MySQLProtocol::Generic;
using namespace DB::MySQLProtocol::ConnectionPhase;

namespace
{

/// Exposes the protected payload (de)serialization so we can round-trip a single packet without the framing layer.
struct TestableHandshakeResponse : public HandshakeResponse
{
    using HandshakeResponse::HandshakeResponse;
    using HandshakeResponse::readPayloadImpl;
    using HandshakeResponse::writePayloadImpl;
};

void checkSecureConnectionRoundTrip(size_t auth_response_size)
{
    const String auth_response(auth_response_size, 'x');

    TestableHandshakeResponse written(
        CLIENT_SECURE_CONNECTION, /* max_packet_size */ 0xFFFFFF, /* character_set */ 33,
        /* username */ "default", /* database */ "", auth_response, /* auth_plugin_name */ "");

    WriteBufferFromOwnString out;
    written.writePayloadImpl(out);

    TestableHandshakeResponse read;
    ReadBufferFromString in(out.str());
    read.readPayloadImpl(in);

    ASSERT_EQ(read.username, "default");
    ASSERT_EQ(read.auth_response.size(), auth_response_size);
    ASSERT_EQ(read.auth_response, auth_response);
}

}

/// The CLIENT_SECURE_CONNECTION path encodes the auth_response length as a single unsigned byte.
/// A length >= 128 used to be read into a signed `char` and sign-extended to a multi-gigabyte value,
/// so the response could not round-trip at all. Check the whole [0, 255] range, with emphasis on the boundary.
TEST(MySQLHandshakeResponse, SecureConnectionAuthResponseLength)
{
    checkSecureConnectionRoundTrip(0);
    checkSecureConnectionRoundTrip(20); /// usual native-password response
    checkSecureConnectionRoundTrip(127);
    checkSecureConnectionRoundTrip(128); /// high bit set: regressed before the fix
    checkSecureConnectionRoundTrip(200);
    checkSecureConnectionRoundTrip(255); /// maximum value of the length byte
}

/// Reading a raw packet whose length byte has the high bit set must consume exactly that many bytes,
/// not sign-extend the length into a request to allocate ~4 GB.
TEST(MySQLHandshakeResponse, SecureConnectionLengthByteIsUnsigned)
{
    const UInt8 len = 0xFF;
    const String auth_response(len, 'a');

    WriteBufferFromOwnString out;
    /// capability_flags
    writeBinaryLittleEndian(static_cast<UInt32>(CLIENT_SECURE_CONNECTION), out);
    /// max_packet_size
    writeBinaryLittleEndian(static_cast<UInt32>(0), out);
    /// character_set
    writeChar(0, out);
    /// 23 reserved bytes
    writeChar(0, 23, out);
    /// username (NUL-terminated)
    writeChar(0, out);
    /// auth_response: one length byte followed by the data
    writeChar(static_cast<char>(len), out);
    writeString(auth_response, out);

    TestableHandshakeResponse read;
    ReadBufferFromString in(out.str());
    read.readPayloadImpl(in);

    ASSERT_EQ(read.auth_response.size(), static_cast<size_t>(len));
    ASSERT_EQ(read.auth_response, auth_response);
    ASSERT_TRUE(in.eof());
}
