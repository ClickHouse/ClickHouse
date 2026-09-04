#include <gtest/gtest.h>

#include <Core/PostgreSQLProtocol.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <limits>
#include <string>

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
}

using namespace DB;
namespace Messaging = DB::PostgreSQLProtocol::Messaging;

namespace
{

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

/// A frontend message frame: the four-byte length field, which counts itself, followed by the payload.
/// The message type byte is read separately by `receiveMessageType`, so it is not part of the frame.
std::string frame(const std::string & payload)
{
    std::string bytes;
    putInt32(bytes, static_cast<Int32>(payload.size() + sizeof(Int32)));
    bytes += payload;
    return bytes;
}

/// Run `body` over the bytes and report whether it threw `UNKNOWN_PACKET_FROM_CLIENT`.
template <typename F>
bool throwsUnknownPacket(const std::string & bytes, F && body)
{
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    WriteBufferFromOwnString out;
    Messaging::MessageTransport mt(&in, &out);
    try
    {
        body(mt);
        return false;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
        return e.code() == ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT;
    }
}

}

TEST(PostgreSQLProtocol, DropMessageRejectsLengthBelowFour)
{
    /// The message-length field includes its own four bytes, so anything below 4 underflows `size - 4`.
    for (Int32 size = 0; size < 4; ++size)
    {
        std::string bytes;
        putInt32(bytes, size);
        EXPECT_TRUE(throwsUnknownPacket(bytes, [](Messaging::MessageTransport & mt)
        {
            mt.dropMessage();
        })) << "size = " << size;
    }

    /// A well-formed length skips the declared number of trailing bytes.
    std::string bytes = frame("abcd");
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    WriteBufferFromOwnString out;
    Messaging::MessageTransport mt(&in, &out);
    EXPECT_NO_THROW(mt.dropMessage());
}

TEST(PostgreSQLProtocol, ReceiveRejectsLengthBelowFour)
{
    for (Int32 size = 0; size < 4; ++size)
    {
        std::string bytes;
        putInt32(bytes, size);
        EXPECT_TRUE(throwsUnknownPacket(bytes, [](Messaging::MessageTransport & mt)
        {
            mt.receive<Messaging::SyncQuery>();
        })) << "size = " << size;
    }

    /// A `Sync` carries no payload, so its frame is exactly the length field.
    std::string bytes = frame("");
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    WriteBufferFromOwnString out;
    Messaging::MessageTransport mt(&in, &out);
    EXPECT_NO_THROW(mt.receive<Messaging::SyncQuery>());
}

TEST(PostgreSQLProtocol, SASLResponseReadsWholeFrame)
{
    /// The payload of a `SASLResponse` is the rest of its frame, with no terminator.
    {
        std::string bytes = frame("");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        WriteBufferFromOwnString out;
        Messaging::MessageTransport mt(&in, &out);
        std::unique_ptr<Messaging::SASLResponse> msg;
        ASSERT_NO_THROW(msg = mt.receive<Messaging::SASLResponse>());
        EXPECT_TRUE(msg->sasl_mechanism.empty());
    }

    {
        std::string bytes = frame("c=biws,r=nonce,p=proof");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        WriteBufferFromOwnString out;
        Messaging::MessageTransport mt(&in, &out);
        std::unique_ptr<Messaging::SASLResponse> msg;
        ASSERT_NO_THROW(msg = mt.receive<Messaging::SASLResponse>());
        EXPECT_EQ(msg->sasl_mechanism, "c=biws,r=nonce,p=proof");
    }
}

TEST(PostgreSQLProtocol, SASLInitialResponseHandlesMechanismLength)
{
    auto build = [](Int32 size_sasl_mechanism, const std::string & data)
    {
        std::string payload = "SCRAM-SHA-256";
        payload.push_back('\0');
        putInt32(payload, size_sasl_mechanism);
        payload += data;
        return frame(payload);
    };

    /// Below -1 is malformed.
    EXPECT_TRUE(throwsUnknownPacket(build(-2, ""), [](Messaging::MessageTransport & mt)
    {
        mt.receive<Messaging::SASLInitialResponse>();
    }));

    /// -1 is the protocol sentinel for "no initial response".
    {
        std::string bytes = build(-1, "");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        WriteBufferFromOwnString out;
        Messaging::MessageTransport mt(&in, &out);
        std::unique_ptr<Messaging::SASLInitialResponse> msg;
        ASSERT_NO_THROW(msg = mt.receive<Messaging::SASLInitialResponse>());
        EXPECT_TRUE(msg->sasl_mechanism.empty());
    }

    /// A non-negative length reads exactly that many bytes.
    {
        std::string bytes = build(3, "abc");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        WriteBufferFromOwnString out;
        Messaging::MessageTransport mt(&in, &out);
        std::unique_ptr<Messaging::SASLInitialResponse> msg;
        ASSERT_NO_THROW(msg = mt.receive<Messaging::SASLInitialResponse>());
        EXPECT_EQ(msg->sasl_mechanism, "abc");
    }

    /// A field declaring more bytes than the frame carries is rejected on the message boundary
    /// instead of being allocated up front.
    EXPECT_TRUE(throwsUnknownPacket(build(1000000, "abc"), [](Messaging::MessageTransport & mt)
    {
        mt.receive<Messaging::SASLInitialResponse>();
    }));
}

TEST(PostgreSQLProtocol, BindHandlesParameterLength)
{
    auto build = [](Int32 sz_param, const std::string & data)
    {
        std::string payload;
        payload.push_back('\0'); /// empty portal name
        payload.push_back('\0'); /// empty statement name
        putInt16(payload, 0); /// no parameter format codes
        putInt16(payload, 1); /// one parameter
        putInt32(payload, sz_param);
        payload += data;
        putInt16(payload, 0); /// no result format codes
        return frame(payload);
    };

    /// Below -1 is malformed.
    EXPECT_TRUE(throwsUnknownPacket(build(-2, ""), [](Messaging::MessageTransport & mt)
    {
        mt.receive<Messaging::BindQuery>();
    }));

    /// -1 is the protocol sentinel for a NULL parameter; no value bytes follow.
    {
        std::string bytes = build(-1, "");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        WriteBufferFromOwnString out;
        Messaging::MessageTransport mt(&in, &out);
        std::unique_ptr<Messaging::BindQuery> msg;
        ASSERT_NO_THROW(msg = mt.receive<Messaging::BindQuery>());
        ASSERT_EQ(msg->parameters.size(), 1u);
        EXPECT_EQ(msg->parameters[0], "NULL");
    }

    /// A non-negative length reads exactly that many bytes.
    {
        std::string bytes = build(2, "hi");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        WriteBufferFromOwnString out;
        Messaging::MessageTransport mt(&in, &out);
        std::unique_ptr<Messaging::BindQuery> msg;
        ASSERT_NO_THROW(msg = mt.receive<Messaging::BindQuery>());
        ASSERT_EQ(msg->parameters.size(), 1u);
        EXPECT_EQ(msg->parameters[0], "hi");
    }

    /// A parameter declaring more bytes than the frame carries is rejected.
    EXPECT_TRUE(throwsUnknownPacket(build(1000000, "hi"), [](Messaging::MessageTransport & mt)
    {
        mt.receive<Messaging::BindQuery>();
    }));
}

TEST(PostgreSQLProtocol, CopyDataReadsWholeFrame)
{
    for (Int32 size = 0; size < 4; ++size)
    {
        std::string bytes;
        putInt32(bytes, size);
        EXPECT_TRUE(throwsUnknownPacket(bytes, [](Messaging::MessageTransport & mt)
        {
            mt.receive<Messaging::CopyInData>();
        })) << "size = " << size;
    }

    /// A well-formed frame carries `size - 4` payload bytes.
    std::string bytes = frame("ab");
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    WriteBufferFromOwnString out;
    Messaging::MessageTransport mt(&in, &out);
    std::unique_ptr<Messaging::CopyInData> msg;
    ASSERT_NO_THROW(msg = mt.receive<Messaging::CopyInData>());
    EXPECT_EQ(msg->query, "ab");
}

TEST(PostgreSQLProtocol, ReceiveRejectsTrailingBytesAndRealigns)
{
    /// A `Query` frame that ends early and smuggles a whole second `Query` in its tail: the declared
    /// length is a frame boundary, so the tail is rejected instead of being read as the next message.
    std::string smuggling_payload = "SELECT 1";
    smuggling_payload.push_back('\0');
    smuggling_payload += "SELECT 2";
    smuggling_payload.push_back('\0');

    std::string good_payload = "SELECT 3";
    good_payload.push_back('\0');

    std::string bytes = frame(smuggling_payload) + frame(good_payload);
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    WriteBufferFromOwnString out;
    Messaging::MessageTransport mt(&in, &out);

    EXPECT_THROW(mt.receive<Messaging::Query>(), Exception);

    /// The rejected frame was skipped in full, so the next message is read from its start.
    std::unique_ptr<Messaging::Query> next;
    ASSERT_NO_THROW(next = mt.receive<Messaging::Query>());
    EXPECT_EQ(next->query, "SELECT 3");
}

TEST(PostgreSQLProtocol, ReceiveRealignsAfterFailedParsing)
{
    /// The first frame fails in the middle of parsing; the rest of it must still be skipped.
    std::string bad_payload = "SCRAM-SHA-256";
    bad_payload.push_back('\0');
    putInt32(bad_payload, -2); /// malformed mechanism length
    bad_payload += "trailing garbage";

    std::string good_payload = "SELECT 4";
    good_payload.push_back('\0');

    std::string bytes = frame(bad_payload) + frame(good_payload);
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    WriteBufferFromOwnString out;
    Messaging::MessageTransport mt(&in, &out);

    EXPECT_THROW(mt.receive<Messaging::SASLInitialResponse>(), Exception);

    std::unique_ptr<Messaging::Query> next;
    ASSERT_NO_THROW(next = mt.receive<Messaging::Query>());
    EXPECT_EQ(next->query, "SELECT 4");
}

TEST(PostgreSQLProtocol, ReceiveRejectsFrameShorterThanDeclared)
{
    /// The declared length is a frame boundary in both directions: a frame that ends before it -
    /// the client declared more bytes than it sent and then closed the write side - must be
    /// rejected, not parsed from the bytes that did arrive. This holds whether the parser stops on
    /// its own terminator (`Query`, `PasswordMessage`) or reads to the end of the frame
    /// (`SASLResponse`).
    auto truncated = [](const std::string & payload)
    {
        std::string bytes;
        /// A thousand bytes of the declared frame are never sent.
        putInt32(bytes, static_cast<Int32>(payload.size() + sizeof(Int32) + 1000));
        bytes += payload;
        return bytes;
    };

    std::string query_payload = "SELECT 1";
    query_payload.push_back('\0');

    std::string password_payload = "x";
    password_payload.push_back('\0');

    {
        std::string bytes = truncated(query_payload);
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        WriteBufferFromOwnString out;
        Messaging::MessageTransport mt(&in, &out);
        EXPECT_THROW(mt.receive<Messaging::Query>(), Exception);
    }

    {
        std::string bytes = truncated(password_payload);
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        WriteBufferFromOwnString out;
        Messaging::MessageTransport mt(&in, &out);
        EXPECT_THROW(mt.receive<Messaging::PasswordMessage>(), Exception);
    }

    {
        std::string bytes = truncated("c=biws,r=nonce,p=proof");
        ReadBufferFromMemory in(bytes.data(), bytes.size());
        WriteBufferFromOwnString out;
        Messaging::MessageTransport mt(&in, &out);
        EXPECT_THROW(mt.receive<Messaging::SASLResponse>(), Exception);
    }
}

TEST(PostgreSQLProtocol, CommandCompletePreservesUInt64RowCount)
{
    WriteBufferFromOwnString out;
    Messaging::CommandComplete response(Messaging::CommandComplete::SELECT, std::numeric_limits<UInt64>::max());
    response.serialize(out);
    out.finalize();

    std::string expected;
    expected.push_back('C');
    putInt32(expected, 32);
    expected += "SELECT 18446744073709551615";
    expected.push_back('\0');

    EXPECT_EQ(out.str(), expected);
}
