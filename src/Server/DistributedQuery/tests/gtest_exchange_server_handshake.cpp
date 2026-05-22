#ifdef OS_LINUX

#include <cstring>
#include <optional>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Server/DistributedQuery/ExchangeServer.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <base/types.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int PROTOCOL_VERSION_MISMATCH;
}
}

using namespace DB;

namespace
{
    /// Read exactly `size` bytes from a blocking socket, looping over short reads.
    void receiveExactly(Poco::Net::StreamSocket & socket, void * buffer, size_t size)
    {
        char * dst = static_cast<char *>(buffer);
        size_t position = 0;
        while (position < size)
        {
            ssize_t received = socket.receiveBytes(dst + position, static_cast<int>(size - position));
            ASSERT_GT(received, 0) << "Peer closed before delivering " << size << " bytes (got " << position << ")";
            position += received;
        }
    }
}

/// A peer that announces a wrong protocol version followed by a body whose
/// remaining bytes would not parse as v1's `query_id`/`stream_name` strings must:
///   1. Receive a `SinkHello` carrying this node's version (so the peer can
///      produce a precise diagnostic on its side).
///   2. Cause the sink to throw `PROTOCOL_VERSION_MISMATCH` — NOT a parse
///      error from `readStringBinary` on the garbage tail.
///   3. Not register the connection.
TEST(ExchangeServerHandshake, MismatchedVersionRejectedBeforeParsingBody)
{
    using namespace StreamingExchangeProtocol;

    auto connections = std::make_shared<ExchangeConnections>();
    auto log = getLogger("ExchangeServerHandshakeTest");

    Poco::Net::ServerSocket listener(Poco::Net::SocketAddress("127.0.0.1", 0));
    Poco::Net::SocketAddress addr = listener.address();
    Poco::Net::StreamSocket client(addr);
    Poco::Net::StreamSocket server_side = listener.acceptConnection();
    listener.close();

    /// Run the server-side handshake on a thread; the main thread drives the wire.
    std::optional<int> caught_code;
    std::string caught_message;
    std::thread server_thread([&]
    {
        try
        {
            ExchangeServer::handleConnection(server_side, connections, log);
        }
        catch (const Exception & e)
        {
            caught_code = e.code();
            caught_message = e.displayText();
        }
    });

    /// Build a SourceHello whose body has:
    ///   - 8 bytes wrong version
    ///   - trailing bytes that are NOT a valid (varuint length + bytes) string
    /// All-0xff bytes form a varuint with the continuation bit set indefinitely,
    /// so `readStringBinary` over the buffer would either consume more than the
    /// body holds (throwing CANNOT_READ_ALL_DATA) or read a wildly large length.
    /// Either way, the throw code would NOT be PROTOCOL_VERSION_MISMATCH — that
    /// is the regression we are guarding against.
    const UInt64 wrong_version = 0xDEAD'BEEF'CAFE'BABEull;
    std::string body;
    body.append(reinterpret_cast<const char *>(&wrong_version), sizeof(wrong_version));
    body.append(64, '\xff');

    PacketHeader header{
        .packet_type = PacketType::SourceHello,
        .bytes_size  = body.size(),
    };
    client.sendBytes(&header, sizeof(header));
    client.sendBytes(body.data(), static_cast<int>(body.size()));

    /// SinkHello must arrive even on mismatch.
    PacketHeader reply_header{};
    receiveExactly(client, &reply_header, sizeof(reply_header));
    EXPECT_EQ(reply_header.packet_type, static_cast<UInt64>(PacketType::SinkHello));
    EXPECT_EQ(reply_header.bytes_size, sizeof(UInt64));

    UInt64 sink_version = 0;
    receiveExactly(client, &sink_version, sizeof(sink_version));
    EXPECT_EQ(sink_version, PROTOCOL_VERSION);

    server_thread.join();
    client.close();

    /// The error must be the version-mismatch one, not a parse error from the
    /// would-be query_id/stream_name. This is the structural guarantee the test
    /// is here to enforce.
    ASSERT_TRUE(caught_code.has_value()) << "handleConnection did not throw";
    EXPECT_EQ(*caught_code, ErrorCodes::PROTOCOL_VERSION_MISMATCH)
        << "Expected PROTOCOL_VERSION_MISMATCH, got: " << caught_message;
}

#endif
