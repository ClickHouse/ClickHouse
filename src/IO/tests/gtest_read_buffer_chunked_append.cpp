#include <gtest/gtest.h>

#include <IO/ReadBufferFromPocoSocketChunked.h>
#include <base/unaligned.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>

#include <pcg_random.hpp>

#include <string>
#include <thread>

using namespace DB;

namespace
{

void appendFrame(std::string & raw, UInt32 value)
{
    char frame[sizeof(value)];
    unalignedStoreLittleEndian<UInt32>(frame, value);
    raw.append(frame, sizeof(frame));
}

/// The data split into chunks (some of them into several parts) as the chunked protocol sends it. The first chunk is
/// not preceded by an end marker.
std::string makeChunkedStream(const std::string & data, pcg64 & rng)
{
    std::string raw;
    size_t offset = 0;
    while (offset < data.size())
    {
        size_t chunk_size = std::min<size_t>(1 + rng() % 40, data.size() - offset);
        size_t chunk_end = offset + chunk_size;
        while (offset < chunk_end)
        {
            size_t part_size = std::min<size_t>(1 + rng() % 15, chunk_end - offset);
            appendFrame(raw, static_cast<UInt32>(part_size));
            raw.append(data, offset, part_size);
            offset += part_size;
        }
        appendFrame(raw, 0);
    }
    return raw;
}

}

/// `appendWithoutWaiting` removes the framing of the chunks that have arrived (in arbitrary fragments) and never waits,
/// and the normal reading continues correctly after it.
TEST(ReadBufferFromPocoSocketChunked, AppendWithoutWaiting)
{
    Poco::Net::ServerSocket server(Poco::Net::SocketAddress("127.0.0.1", 0));
    for (UInt64 seed = 0; seed < 300; ++seed)
    {
        pcg64 rng(seed);
        std::string data(1 + rng() % 600, '\0');
        for (auto & c : data)
            c = static_cast<char>(rng());
        const std::string raw = makeChunkedStream(data, rng);

        Poco::Net::StreamSocket client;
        client.connect(server.address());
        Poco::Net::StreamSocket accepted = server.acceptConnection();
        client.setNoDelay(true);

        ReadBufferFromPocoSocketChunked in(accepted, 16 + rng() % 256);
        in.enableChunked();

        size_t sent = 0;
        size_t consumed = 0;
        while (sent < raw.size())
        {
            const size_t fragment = std::min<size_t>(1 + rng() % 12, raw.size() - sent);
            client.sendBytes(raw.data() + sent, static_cast<int>(fragment));
            sent += fragment;
            while (accepted.available() == 0)
                std::this_thread::yield();

            for (size_t i = 0, iterations = rng() % 4; i < iterations; ++i)
            {
                const size_t before = in.available();
                ASSERT_TRUE(in.appendWithoutWaiting(1 + rng() % 20, accepted.available() > 0));
                ASSERT_GE(in.available(), before);
                ASSERT_LE(consumed + in.available(), data.size());
                ASSERT_EQ(std::string(in.position(), in.available()), data.substr(consumed, in.available())) << "seed " << seed;

                const size_t take = in.available() ? rng() % (in.available() + 1) : 0;
                in.position() += take;
                consumed += take;
            }
        }

        /// Everything is sent, so the normal reading does not wait.
        std::string rest(data.size() - consumed, '\0');
        in.readStrict(rest.data(), rest.size());
        ASSERT_EQ(rest, data.substr(consumed)) << "seed " << seed;
        ASSERT_EQ(in.available(), 0);
        ASSERT_FALSE(in.hasBufferedData()) << "seed " << seed;
    }
}
