#include <gtest/gtest.h>

#include <IO/ReadBufferFromPocoSocketChunked.h>
#include <IO/ReadHelpers.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <string>
#include <vector>

namespace
{

void appendChunkWord(std::string & wire, UInt32 value)
{
    for (size_t i = 0; i < sizeof(value); ++i)
        wire.push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
}

/// Sends `wire` in one write, so that the reader receives several chunks in one socket read,
/// and checks that `count` and `chunkFramingBytes` add up to the exact bytes on the wire.
void checkCount(const std::string & wire, const std::vector<size_t> & packet_sizes, const std::vector<size_t> & packet_framing)
{
    Poco::Net::ServerSocket listener(Poco::Net::SocketAddress("127.0.0.1", 0));
    Poco::Net::StreamSocket client;
    client.connect(listener.address());
    Poco::Net::StreamSocket server = listener.acceptConnection();

    ASSERT_EQ(client.sendBytes(wire.data(), static_cast<int>(wire.size())), static_cast<int>(wire.size()));
    client.shutdownSend();

    DB::ReadBufferFromPocoSocketChunked in(server);
    in.enableChunked();

    size_t payload = 0;
    size_t framing = 0;
    for (size_t i = 0; i < packet_sizes.size(); ++i)
    {
        const size_t packet_start = in.count() + in.chunkFramingBytes();
        std::string packet(packet_sizes[i], '\0');
        in.readStrict(packet.data(), packet.size());

        payload += packet_sizes[i];
        framing += packet_framing[i];
        EXPECT_EQ(in.count(), payload) << "packet " << i;
        EXPECT_EQ(in.chunkFramingBytes(), framing) << "packet " << i;
        EXPECT_EQ(in.count() + in.chunkFramingBytes() - packet_start, packet_sizes[i] + packet_framing[i]) << "packet " << i;
    }

    EXPECT_EQ(in.count() + in.chunkFramingBytes(), wire.size());
}

}

TEST(ReadBufferFromPocoSocketChunked, CountManyChunksInOneRead)
{
    constexpr size_t packets = 100;
    constexpr UInt32 packet_size = 100;

    std::string wire;
    for (size_t i = 0; i < packets; ++i)
    {
        appendChunkWord(wire, packet_size);
        wire.append(packet_size, static_cast<char>('a' + i % 26));
        appendChunkWord(wire, 0);
    }
    ASSERT_EQ(wire.size(), 10800);

    checkCount(wire, std::vector<size_t>(packets, packet_size), std::vector<size_t>(packets, 8));
}

TEST(ReadBufferFromPocoSocketChunked, CountContinuationChunks)
{
    constexpr size_t packets = 50;
    constexpr UInt32 part_size = 50;

    /// Each packet is sent in two parts: the size of the second part comes in the middle of the packet.
    std::string wire;
    for (size_t i = 0; i < packets; ++i)
    {
        appendChunkWord(wire, part_size);
        wire.append(part_size, 'x');
        appendChunkWord(wire, part_size);
        wire.append(part_size, 'y');
        appendChunkWord(wire, 0);
    }

    checkCount(wire, std::vector<size_t>(packets, 2 * part_size), std::vector<size_t>(packets, 12));
}
