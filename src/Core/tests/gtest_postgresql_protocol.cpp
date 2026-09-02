#include <gtest/gtest.h>

#include <Core/PostgreSQLProtocol.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <string>

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
}

using namespace DB;
namespace Messaging = DB::PostgreSQLProtocol::Messaging;

namespace
{

void putUInt8(std::string & s, UInt8 v)
{
    s.push_back(static_cast<char>(v));
}

void putInt16(std::string & s, Int16 v)
{
    s.push_back(static_cast<char>((v >> 8) & 0xFF));
    s.push_back(static_cast<char>(v & 0xFF));
}

void putInt32(std::string & s, Int32 v)
{
    for (int i = 3; i >= 0; --i)
        s.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

/// Run `body` over the bytes and report whether it threw UNKNOWN_PACKET_FROM_CLIENT.
template <typename F>
bool throwsUnknownPacket(const std::string & bytes, F && body)
{
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    try
    {
        body(in);
        return false;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
        return e.code() == ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT;
    }
}

class FailingReadBuffer : public ReadBuffer
{
public:
    FailingReadBuffer() : ReadBuffer(nullptr, 0) {}

private:
    bool nextImpl() override
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Synthetic read failure");
    }
};

}

TEST(PostgreSQLProtocol, DropMessageRejectsLengthBelowFour)
{
    /// The message-length field includes its own four bytes, so anything below 4 underflows `size - 4`.
    for (Int32 size = 0; size < 4; ++size)
    {
        std::string bytes;
        putInt32(bytes, size);
        WriteBufferFromOwnString out;
        EXPECT_TRUE(throwsUnknownPacket(bytes, [&](ReadBuffer & in)
        {
            Messaging::MessageTransport mt(&in, &out);
            mt.dropMessage();
        })) << "size = " << size;
    }

    /// A well-formed length skips the declared number of trailing bytes.
    std::string bytes;
    putInt32(bytes, 8);
    bytes += "abcd";
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    WriteBufferFromOwnString out;
    Messaging::MessageTransport mt(&in, &out);
    EXPECT_NO_THROW(mt.dropMessage());
}

TEST(PostgreSQLProtocol, StartupMessageRejectsParametersExceedingDeclaredPayload)
{
    std::string bytes("user\0default\0", 13);
    bytes.push_back('\0');

    {
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::StartupMessage msg(static_cast<Int32>(bytes.size()));
        EXPECT_NO_THROW(msg.deserialize(in));
        EXPECT_EQ(msg.user, "default");
    }

    ReadBufferFromMemory in(bytes.data(), bytes.size());
    try
    {
        Messaging::StartupMessage msg(static_cast<Int32>(bytes.size() - 1));
        msg.deserialize(in);
        FAIL() << "Expected malformed startup message to be rejected";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
    }
    EXPECT_EQ(in.count(), bytes.size() - 1);
}

TEST(PostgreSQLProtocol, StartupMessageDoesNotReadPastDeclaredPayload)
{
    const std::string bytes("user\0default\0\0subsequent data", 29);

    for (Int32 payload_size : {2, 7})
    {
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        try
        {
            Messaging::StartupMessage msg(payload_size);
            msg.deserialize(in);
            FAIL() << "Expected truncated startup message to be rejected";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
        }
        EXPECT_EQ(in.count(), payload_size);
    }
}

TEST(PostgreSQLProtocol, StartupMessageValidatesPayloadAndTerminator)
{
    /// Total startup message lengths from 0 through 8 leave no room for the required final zero.
    for (Int32 payload_size = -8; payload_size <= 0; ++payload_size)
    {
        EXPECT_TRUE(throwsUnknownPacket("", [&](ReadBuffer & in)
        {
            Messaging::StartupMessage msg(payload_size);
            msg.deserialize(in);
        })) << "payload_size = " << payload_size;
    }

    {
        const std::string bytes(1, '\0');
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::StartupMessage msg(static_cast<Int32>(bytes.size()));
        EXPECT_NO_THROW(msg.deserialize(in));
        EXPECT_EQ(in.count(), bytes.size());
    }

    EXPECT_TRUE(throwsUnknownPacket("x", [&](ReadBuffer & in)
    {
        Messaging::StartupMessage msg(1);
        msg.deserialize(in);
    }));
}

TEST(PostgreSQLProtocol, StartupMessagePreservesUnrelatedReadErrors)
{
    FailingReadBuffer in;
    Messaging::StartupMessage msg(1);
    try
    {
        msg.deserialize(in);
        FAIL() << "Expected synthetic read failure";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
    }
}

TEST(PostgreSQLProtocol, SASLResponseRejectsLengthBelowFour)
{
    for (Int32 size = 0; size < 4; ++size)
    {
        std::string bytes;
        putUInt8(bytes, 'p');
        putInt32(bytes, size);
        EXPECT_TRUE(throwsUnknownPacket(bytes, [](ReadBuffer & in)
        {
            Messaging::SASLResponse msg;
            msg.deserialize(in);
        })) << "size = " << size;
    }

    /// size == 4 means an empty SASL payload.
    std::string bytes;
    putUInt8(bytes, 'p');
    putInt32(bytes, 4);
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    Messaging::SASLResponse msg;
    EXPECT_NO_THROW(msg.deserialize(in));
    EXPECT_TRUE(msg.sasl_mechanism.empty());
}

TEST(PostgreSQLProtocol, SASLInitialResponseHandlesMechanismLength)
{
    auto build = [](Int32 size_sasl_mechanism, const std::string & data)
    {
        std::string bytes;
        putUInt8(bytes, 'p');
        putInt32(bytes, 0); /// the outer size field is not used for bounds here
        bytes += "SCRAM-SHA-256";
        bytes.push_back('\0');
        putInt32(bytes, size_sasl_mechanism);
        bytes += data;
        return bytes;
    };

    /// Below -1 is malformed.
    EXPECT_TRUE(throwsUnknownPacket(build(-2, ""), [](ReadBuffer & in)
    {
        Messaging::SASLInitialResponse msg;
        msg.deserialize(in);
    }));

    /// -1 is the protocol sentinel for "no initial response".
    {
        std::string bytes = build(-1, "");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::SASLInitialResponse msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        EXPECT_TRUE(msg.sasl_mechanism.empty());
    }

    /// A non-negative length reads exactly that many bytes.
    {
        std::string bytes = build(3, "abc");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::SASLInitialResponse msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        EXPECT_EQ(msg.sasl_mechanism, "abc");
    }
}

TEST(PostgreSQLProtocol, BindHandlesParameterLength)
{
    auto build = [](Int32 sz_param, const std::string & data)
    {
        std::string bytes;
        putInt32(bytes, 0); /// the outer size field is not used for bounds here
        bytes.push_back('\0'); /// empty portal name
        bytes.push_back('\0'); /// empty statement name
        putInt16(bytes, 0); /// no parameter format codes
        putInt16(bytes, 1); /// one parameter
        putInt32(bytes, sz_param);
        bytes += data;
        putInt16(bytes, 0); /// no result format codes
        return bytes;
    };

    /// Below -1 is malformed.
    EXPECT_TRUE(throwsUnknownPacket(build(-2, ""), [](ReadBuffer & in)
    {
        Messaging::BindQuery msg;
        msg.deserialize(in);
    }));

    /// -1 is the protocol sentinel for a NULL parameter; no value bytes follow.
    {
        std::string bytes = build(-1, "");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::BindQuery msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        ASSERT_EQ(msg.parameters.size(), 1u);
        EXPECT_EQ(msg.parameters[0], "NULL");
    }

    /// A non-negative length reads exactly that many bytes.
    {
        std::string bytes = build(2, "hi");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        Messaging::BindQuery msg;
        EXPECT_NO_THROW(msg.deserialize(in));
        ASSERT_EQ(msg.parameters.size(), 1u);
        EXPECT_EQ(msg.parameters[0], "hi");
    }
}

TEST(PostgreSQLProtocol, CopyDataRejectsLengthBelowFour)
{
    for (Int32 size = 0; size < 4; ++size)
    {
        std::string bytes;
        putInt32(bytes, size);
        EXPECT_TRUE(throwsUnknownPacket(bytes, [](ReadBuffer & in)
        {
            Messaging::CopyInData msg;
            msg.deserialize(in);
        })) << "size = " << size;
    }

    /// A well-formed length carries `size - 4` payload bytes.
    std::string bytes;
    putInt32(bytes, 6);
    bytes += "ab";
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    Messaging::CopyInData msg;
    EXPECT_NO_THROW(msg.deserialize(in));
    EXPECT_EQ(msg.query, "ab");
}
