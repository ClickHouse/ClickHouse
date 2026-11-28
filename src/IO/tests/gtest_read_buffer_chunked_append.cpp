#include <gtest/gtest.h>

#include <IO/ReadBufferFromPocoSocketChunked.h>
#include <base/unaligned.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>

#include <pcg_random.hpp>

#include <atomic>
#include <exception>
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
/// and the normal reading continues correctly after it, also while the rest of the stream is still arriving in fragments
/// (for example, an end marker of a chunk can arrive byte by byte).
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

        /// Send a part of the stream before the normal reading starts and the rest while it is in progress.
        const size_t sent_before_reading = rng() % (raw.size() + 1);
        size_t sent = 0;
        size_t consumed = 0;
        while (sent < sent_before_reading)
        {
            const size_t fragment = std::min<size_t>(1 + rng() % 12, sent_before_reading - sent);
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

        /// Send the rest in small fragments, each one after the previous one is received, so that the normal reading
        /// gets the frame headers and the end markers of the chunks in several reads.
        const UInt64 sender_seed = rng();
        std::atomic<bool> stop_sending = false;
        std::thread sender([&raw, &client, &accepted, &stop_sending, sent, sender_seed]
        {
            pcg64 sender_rng(sender_seed);
            for (size_t offset = sent; offset < raw.size() && !stop_sending;)
            {
                const size_t fragment = std::min<size_t>(1 + sender_rng() % 3, raw.size() - offset);
                client.sendBytes(raw.data() + offset, static_cast<int>(fragment));
                offset += fragment;
                while (accepted.available() > 0 && !stop_sending)
                    std::this_thread::yield();
            }
        });

        std::string rest(data.size() - consumed, '\0');
        std::exception_ptr read_exception;
        try
        {
            in.readStrict(rest.data(), rest.size());
        }
        catch (...)
        {
            read_exception = std::current_exception();
        }
        stop_sending = true;
        sender.join();
        ASSERT_FALSE(read_exception) << "seed " << seed;
        ASSERT_EQ(rest, data.substr(consumed)) << "seed " << seed;
        ASSERT_EQ(in.available(), 0);
        ASSERT_FALSE(in.hasBufferedData()) << "seed " << seed;
    }
}
