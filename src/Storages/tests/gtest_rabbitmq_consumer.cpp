#include <gtest/gtest.h>
#include <config.h>

#if USE_AMQPCPP

#include <Storages/UVLoop.h>
#include <Storages/RabbitMQ/RabbitMQConsumer.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <atomic>
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

#endif
