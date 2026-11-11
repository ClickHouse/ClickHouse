#include <gtest/gtest.h>
#include <config.h>

#if USE_AMQPCPP

#include <Storages/UVLoop.h>
#include <Storages/RabbitMQ/RabbitMQConnection.h>
#include <Storages/RabbitMQ/RabbitMQConsumer.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>

#include <atomic>
#include <future>
#include <thread>

using namespace DB;

namespace
{

/// Without the fix 50000 rounds lost a wake-up in 11 of 12 runs.
constexpr UInt64 rounds = 500000;
constexpr UInt64 timeout_ms = 10000;

struct ConsumerFixture
{
    UVLoop loop;
    LoggerPtr log = getLogger("RabbitMQConsumerTest");
    RabbitMQHandler handler{loop.getLoop(), log};
    std::vector<String> queues{"q"};
};

}

TEST(RabbitMQConsumer, DeliveryWakesWaitingReader)
{
    ConsumerFixture f;
    RabbitMQConsumer consumer(f.handler, f.queues, 1, "test", f.log, 100);

    std::atomic<UInt64> armed = 0;
    std::atomic<bool> done = false;
    /// The helper spins instead of yielding, so that its notify lands as close as possible to the reader's wait.
    std::thread helper([&]
    {
        for (UInt64 i = 1; i <= rounds && !done; ++i)
        {
            while (armed < i && !done) {}
            if (!done)
                consumer.pushMessage(
                    {.message = "x", .message_id = "", .timestamp = 0, .redelivered = false, .delivery_tag = i, .channel_id = "c"});
        }
    });

    UInt64 lost = 0;
    for (UInt64 i = 1; i <= rounds && !lost; ++i)
    {
        armed = i;
        Stopwatch watch;
        consumer.waitForMessages(timeout_ms);
        if (watch.elapsedMilliseconds() >= timeout_ms / 2 || !consumer.consume())
            lost = i;
    }
    done = true;
    helper.join();
    EXPECT_EQ(lost, 0) << "delivery " << lost << " did not wake the reader";
}

TEST(RabbitMQConsumer, StopWakesWaitingReader)
{
    ConsumerFixture f;

    /// A stopped consumer never waits again, so every round uses a fresh one.
    std::atomic<RabbitMQConsumer *> target = nullptr;
    std::atomic<UInt64> armed = 0;
    std::atomic<UInt64> stopped = 0;
    std::atomic<bool> done = false;
    std::thread helper([&]
    {
        for (UInt64 i = 1; i <= rounds && !done; ++i)
        {
            while (armed < i && !done) {}
            if (done)
                break;
            target.load()->stop();
            stopped = i;
        }
    });

    UInt64 lost = 0;
    for (UInt64 i = 1; i <= rounds && !lost; ++i)
    {
        RabbitMQConsumer consumer(f.handler, f.queues, 1, "test", f.log, 100);
        target = &consumer;
        armed = i;
        Stopwatch watch;
        consumer.waitForMessages(timeout_ms);
        if (watch.elapsedMilliseconds() >= timeout_ms / 2)
            lost = i;
        /// The consumer is destroyed at the end of the round, so `stop` must have returned.
        while (stopped < i)
            std::this_thread::yield();
    }
    done = true;
    helper.join();
    EXPECT_EQ(lost, 0) << "stop " << lost << " did not wake the reader";
}

/// A delivery of the old channel waiting for queue space on the loop thread must be dropped when the channel is replaced.
TEST(RabbitMQConsumer, ReplacementDropsDeliveryInFlightOnOldChannel)
{
    /// Accepts the connection but never answers the handshake: enough to create channels. connect() gives up after
    /// its retries.
    Poco::Net::ServerSocket broker(Poco::Net::SocketAddress("127.0.0.1", 0));
    LoggerPtr log = getLogger("RabbitMQConsumerTest");
    RabbitMQConfiguration configuration;
    configuration.host = "127.0.0.1";
    configuration.port = broker.address().port();
    RabbitMQConnection connection(configuration, log);
    connection.connect();
    RabbitMQHandler & handler = connection.getHandler();
    std::vector<String> queues{"q"};
    RabbitMQConsumer consumer(handler, queues, 1, "test", log, /* queue_size = */ 1);
    SCOPE_EXIT({
        consumer.stop();
        handler.stopLoop();
    });
    consumer.updateChannel(connection);
    constexpr UInt64 old_generation = 1; /// The first channel's.

    auto old_delivery = [](UInt64 tag)
    {
        return RabbitMQConsumer::MessageData{
            .message = "x", .message_id = "", .timestamp = 0, .redelivered = false, .delivery_tag = tag, .channel_id = "old"};
    };
    ASSERT_TRUE(consumer.receive(old_generation, old_delivery(1)));
    handler.startLoop();

    /// The queue is full, so this delivery parks the loop thread.
    std::atomic<bool> queued = true;
    std::thread delivery([&] { handler.run([&] { queued = consumer.receive(old_generation, old_delivery(2)); }); });
    /// Not observable from outside: give the delivery time to park. If it is late, it sees the new generation and the
    /// test passes without exercising the wake-up.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto replaced = std::async(std::launch::async, [&] { consumer.updateChannel(connection); });
    const bool finished = replaced.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
    EXPECT_TRUE(finished) << "updateChannel waits for the loop thread, which waits for space in the queue";
    if (!finished)
        consumer.stop(); /// Releases the delivery, so that the threads can be joined.
    replaced.get();
    delivery.join();

    EXPECT_FALSE(queued) << "a delivery of the replaced channel was queued";
    EXPECT_FALSE(consumer.hasPendingMessages()) << "a delivery of the replaced channel stayed queued";
}

namespace
{

constexpr UInt32 method(UInt16 class_id, UInt16 method_id) { return UInt32{class_id} << 16 | method_id; }

/// Answers the AMQP 0-9-1 methods that wait for a reply, enough to open a connection and a consuming channel and to
/// close both gracefully. Serves one connection after the other.
class FakeBroker
{
public:
    FakeBroker() : thread([this] { serve(); }) {}

    ~FakeBroker()
    {
        stopped = true;
        thread.join();
    }

    UInt16 port() const { return server.address().port(); }

private:
    using Bytes = std::vector<UInt8>;

    void serve()
    {
        while (!stopped)
        {
            if (!server.poll(poll_timeout, Poco::Net::Socket::SELECT_READ))
                continue;
            auto socket = server.acceptConnection();
            try
            {
                session(socket);
            }
            catch (const Poco::Exception &) // NOLINT(bugprone-empty-catch) Ok: the client closed the connection (reset, broken pipe).
            {
            }
        }
    }

    void session(Poco::Net::StreamSocket & socket)
    {
        read(socket, 8); /// Protocol header.
        /// connection.start: version 0-9, no server properties, mechanism PLAIN, locale en_US.
        send(socket, 0, 10, 10, {0, 9, 0, 0, 0, 0, 0, 0, 0, 5, 'P', 'L', 'A', 'I', 'N', 0, 0, 0, 5, 'e', 'n', '_', 'U', 'S'});
        while (!stopped)
        {
            if (!socket.poll(poll_timeout, Poco::Net::Socket::SELECT_READ))
                continue;
            const Bytes header = read(socket, 7);
            const Bytes payload = read(socket, bigEndian(&header[3], 4) + 1);
            if (header[0] != 1) /// Not a method frame.
                continue;
            const auto channel = static_cast<UInt16>(bigEndian(&header[1], 2));
            switch (bigEndian(payload.data(), 4)) /// Class and method id.
            {
                case method(10, 11): send(socket, 0, 10, 30, {0x07, 0xff, 0, 2, 0, 0, 0, 0}); break; /// start-ok -> tune
                case method(10, 40): send(socket, 0, 10, 41, {0}); break; /// open
                case method(10, 50): send(socket, 0, 10, 51, {}); break; /// close
                case method(20, 10): send(socket, channel, 20, 11, {0, 0, 0, 0}); break; /// channel.open
                case method(20, 40): send(socket, channel, 20, 41, {}); break; /// channel.close
                case method(60, 10): send(socket, channel, 60, 11, {}); break; /// basic.qos
                case method(60, 20): send(socket, channel, 60, 21, {1, 't'}); break; /// basic.consume
                default: break;
            }
        }
    }

    static UInt32 bigEndian(const UInt8 * bytes, size_t size)
    {
        UInt32 value = 0;
        for (size_t i = 0; i < size; ++i)
            value = value << 8 | bytes[i];
        return value;
    }

    static void put(Bytes & bytes, UInt32 value, size_t size)
    {
        for (size_t i = size; i-- > 0;)
            bytes.push_back(static_cast<UInt8>(value >> (8 * i)));
    }

    static Bytes read(Poco::Net::StreamSocket & socket, size_t size)
    {
        Bytes bytes(size);
        for (size_t done = 0; done < size;)
        {
            const int received = socket.receiveBytes(bytes.data() + done, static_cast<int>(size - done));
            if (received <= 0)
                throw Poco::Net::ConnectionResetException();
            done += received;
        }
        return bytes;
    }

    static void send(Poco::Net::StreamSocket & socket, UInt16 channel, UInt16 class_id, UInt16 method_id, const Bytes & arguments)
    {
        Bytes frame{1};
        put(frame, channel, 2);
        put(frame, static_cast<UInt32>(arguments.size() + 4), 4);
        put(frame, class_id, 2);
        put(frame, method_id, 2);
        frame.insert(frame.end(), arguments.begin(), arguments.end());
        frame.push_back(0xce);
        socket.sendBytes(frame.data(), static_cast<int>(frame.size()));
    }

    const Poco::Timespan poll_timeout{0, 10000};
    Poco::Net::ServerSocket server{Poco::Net::SocketAddress("127.0.0.1", 0)};
    std::atomic<bool> stopped = false;
    std::thread thread;
};

}

/// After the loop thread died, reconnecting must fail the consumers' channels, or they would keep them.
TEST(RabbitMQConsumer, ReconnectAfterLoopThreadDiedFailsChannels)
{
    FakeBroker broker;
    LoggerPtr log = getLogger("RabbitMQConsumerTest");
    RabbitMQConfiguration configuration;
    configuration.host = "127.0.0.1";
    configuration.port = broker.port();
    /// Declared before the connection: its close callback may run in ~RabbitMQHandler.
    uv_timer_t thrower;
    RabbitMQConnection connection(configuration, log);
    ASSERT_TRUE(connection.connect());
    RabbitMQHandler & handler = connection.getHandler();
    std::vector<String> queues{"q"};
    RabbitMQConsumer consumer(handler, queues, 1, "test", log, 100);
    consumer.updateChannel(connection);
    handler.startLoop();
    /// A failed ASSERT returns: stop the loop thread before the consumer and the connection are destroyed.
    SCOPE_EXIT({ handler.stopLoop(); });
    for (size_t i = 0; i < 1000 && consumer.getChannelID().empty(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_FALSE(consumer.getChannelID().empty()) << "the channel did not open";

    handler.run([&]
    {
        uv_timer_init(handler.getLoop(), &thrower);
        uv_timer_start(&thrower, [](uv_timer_t *) { throw std::runtime_error("boom"); }, /* timeout_ms = */ 1, /* repeat = */ 0);
    });
    for (size_t i = 0; i < 1000 && handler.loopRunning(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    handler.run([&] { uv_close(reinterpret_cast<uv_handle_t *>(&thrower), nullptr); });
    ASSERT_FALSE(handler.loopRunning());
    ASSERT_FALSE(connection.isConnected());
    ASSERT_FALSE(consumer.needChannelUpdate());

    EXPECT_TRUE(connection.reconnect());
    EXPECT_TRUE(consumer.needChannelUpdate()) << "the consumer keeps a channel of the replaced connection";
}

#endif
