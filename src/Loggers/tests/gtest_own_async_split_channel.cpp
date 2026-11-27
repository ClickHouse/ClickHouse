#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include <Poco/NullChannel.h>

#include <Common/ProfileEvents.h>
#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnPatternFormatter.h>
#include <Loggers/OwnSplitChannel.h>

namespace ProfileEvents
{
extern const Event AsyncLoggingFileLogTotalMessages;
extern const Event AsyncLoggingFileLogDroppedMessages;
}

using namespace DB;

/// Keeper starts the AsynchronousMetrics thread, which calls `getAsynchronousMetrics`, before it rebuilds
/// the loggers, so the metrics can be read while `addChannel` is still registering channels.
/// `addChannel` used to append to `channels` and then to `queues` without a lock, and a reader running
/// between the two appends indexed `queues` out of bounds.
///
/// The queue is large on purpose: constructing it (it allocates and initializes every slot) happens between
/// the two appends, so a big queue keeps the window open long enough for the reader to hit it every time.
TEST(OwnAsyncSplitChannel, GetAsynchronousMetricsWhileAddingChannels)
{
    constexpr size_t queue_size = 1 << 21;
    constexpr size_t channels_to_add = 8;

    OwnAsyncSplitChannel split(queue_size);

    std::atomic<bool> stop = false;
    std::atomic<size_t> reads = 0;
    std::thread reader(
        [&]
        {
            while (!stop.load(std::memory_order_relaxed))
            {
                auto metrics = split.getAsynchronousMetrics();
                /// Never more entries than registered channels, and each entry has a name.
                EXPECT_LE(metrics.size(), channels_to_add);
                for (const auto & [name, size] : metrics)
                    EXPECT_FALSE(name.empty());
                reads.fetch_add(1, std::memory_order_relaxed);
            }
        });

    for (size_t i = 0; i < channels_to_add; ++i)
    {
        Poco::AutoPtr<OwnPatternFormatter> formatter(new OwnPatternFormatter);
        auto channel = std::make_shared<OwnFormattingChannel>(formatter, new Poco::NullChannel);
        split.addChannel(
            channel,
            "Channel" + std::to_string(i),
            Poco::Message::PRIO_TRACE,
            ProfileEvents::AsyncLoggingFileLogTotalMessages,
            ProfileEvents::AsyncLoggingFileLogDroppedMessages);
    }

    stop = true;
    reader.join();

    EXPECT_GT(reads.load(), 0u);
    EXPECT_EQ(split.getAsynchronousMetrics().size(), channels_to_add);
}
