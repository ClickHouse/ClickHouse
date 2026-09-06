#include <Common/DoubleBufferedProducer.h>

#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <optional>
#include <stdexcept>
#include <thread>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
/// A convenient thread name for tests; the group is null, so ThreadGroupSwitcher is a no-op.
constexpr ThreadName kName = ThreadName::SEND_TO_SHELL_CMD;
}

/// The producer fills a two-slot payload array and the consumer reads it back. Because the buffer a
/// producer writes is not reused until the consumer releases it, and the coordinator's mutex
/// establishes happens-before between the fill and the read, the consumer must observe exactly the
/// value the producer wrote into that slot, in order. Deliberately uses non-atomic payload storage
/// so that ThreadSanitizer would flag a missing synchronization.
TEST(DoubleBufferedProducer, ProducesAllItemsInOrderWithVisibility)
{
    constexpr size_t N = 5000;
    std::array<size_t, 2> payload{{0, 0}};
    size_t next_value = 0;

    DoubleBufferedProducer producer;
    producer.start(nullptr, kName, [&](size_t index) -> std::optional<size_t>
    {
        if (next_value >= N)
            return std::nullopt;
        payload[index] = next_value;   /// write into the buffer the coordinator handed us
        return next_value++;           /// carry the value as the reported "size"
    });

    size_t expected = 0;
    while (auto item = producer.next())
    {
        EXPECT_EQ(item->size, expected);
        EXPECT_EQ(payload[item->index], expected);  /// visibility across threads
        producer.release(item->index);
        ++expected;
    }
    EXPECT_EQ(expected, N);
}

/// At most one buffer is "ready but not taken" while the consumer holds the other, so a freshly
/// taken item must use the other buffer than the one still held. This checks the double-buffering
/// invariant directly.
TEST(DoubleBufferedProducer, AlternatesWhileConsumerHoldsABuffer)
{
    constexpr size_t N = 2000;
    size_t next_value = 0;

    DoubleBufferedProducer producer;
    producer.start(nullptr, kName, [&](size_t) -> std::optional<size_t>
    {
        if (next_value >= N)
            return std::nullopt;
        return next_value++;
    });

    std::optional<size_t> held_index;
    size_t count = 0;
    while (auto item = producer.next())
    {
        if (held_index.has_value())
        {
            EXPECT_NE(item->index, *held_index);  /// cannot reuse the buffer we still hold
            producer.release(*held_index);        /// release the previous one now
        }
        held_index = item->index;
        ++count;
    }
    if (held_index.has_value())
        producer.release(*held_index);
    EXPECT_EQ(count, N);
}

/// An exception thrown by the producer callback is surfaced from next() on the consumer thread,
/// after the items produced before the failure have been delivered.
TEST(DoubleBufferedProducer, PropagatesProducerException)
{
    constexpr size_t fail_at = 3;
    size_t value = 0;

    DoubleBufferedProducer producer;
    producer.start(nullptr, kName, [&](size_t) -> std::optional<size_t>
    {
        if (value == fail_at)
            throw std::runtime_error("boom");
        return value++;
    });

    size_t delivered = 0;
    bool threw = false;
    try
    {
        while (auto item = producer.next())
        {
            producer.release(item->index);
            ++delivered;
        }
    }
    catch (const std::runtime_error & e)
    {
        threw = true;
        EXPECT_STREQ(e.what(), "boom");
    }

    EXPECT_TRUE(threw);
    EXPECT_EQ(delivered, fail_at);
}

/// The producer callback that returns std::nullopt immediately yields no items.
TEST(DoubleBufferedProducer, HandlesEmptyInput)
{
    DoubleBufferedProducer producer;
    producer.start(nullptr, kName, [](size_t) -> std::optional<size_t> { return std::nullopt; });
    EXPECT_FALSE(producer.next().has_value());
}

/// Destroying the coordinator while the producer still has work to do must stop and join cleanly,
/// without hanging, even if the consumer took only a few items.
TEST(DoubleBufferedProducer, StopsCleanlyWhenConsumerStopsEarly)
{
    std::atomic<size_t> produced{0};
    {
        DoubleBufferedProducer producer;
        producer.start(nullptr, kName, [&](size_t) -> std::optional<size_t>
        {
            /// A large but finite amount of work; the consumer will stop early.
            size_t v = produced.fetch_add(1);
            if (v >= 100000)
                return std::nullopt;
            return v;
        });

        for (size_t i = 0; i < 5; ++i)
        {
            auto item = producer.next();
            ASSERT_TRUE(item.has_value());
            producer.release(item->index);
        }
        /// producer goes out of scope here -> stop() must join without deadlock
    }
    SUCCEED();
}

/// Regression test for a deadlock: a consumer blocked in next() must be woken by stop(). The
/// consumer here takes both buffers without releasing them, so the producer runs out of free
/// buffers and goes idle while `ready` is empty; the next() call then blocks with no ready item and
/// `finished` still false. stop() must unblock it (return std::nullopt) instead of hanging forever.
TEST(DoubleBufferedProducer, StopWakesBlockedConsumer)
{
    DoubleBufferedProducer producer;
    std::atomic<size_t> produced{0};
    producer.start(nullptr, kName, [&](size_t) -> std::optional<size_t>
    {
        return produced.fetch_add(1); /// never finishes on its own
    });

    std::promise<void> entering_blocking_next;
    auto consumer = std::async(std::launch::async, [&]
    {
        producer.next(); /// take one buffer, do NOT release it
        producer.next(); /// take the other buffer -> both held, next() will now block
        entering_blocking_next.set_value();
        return producer.next(); /// must block, then be woken by stop() and return std::nullopt
    });

    entering_blocking_next.get_future().wait();
    producer.stop(); /// must wake the (about-to-be) blocked consumer

    ASSERT_EQ(consumer.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "next() did not return after stop() — the blocked consumer deadlocked";
    EXPECT_FALSE(consumer.get().has_value());
}

/// `stop` cannot interrupt a callback that has already started - it only ends the loop between
/// callbacks - so a callback that would otherwise keep producing polls isStopRequested() and gives
/// up. Without that flag being visible to it, stop() here would never return.
TEST(DoubleBufferedProducer, RunningCallbackSeesTheStopRequest)
{
    DoubleBufferedProducer producer;
    std::promise<void> inside_callback;
    std::atomic<bool> entered{false};

    producer.start(nullptr, kName, [&](size_t) -> std::optional<size_t>
    {
        if (!entered.exchange(true))
            inside_callback.set_value();

        while (!producer.isStopRequested()) /// stands for a callback that keeps pulling input blocks
            std::this_thread::yield();

        return std::nullopt;
    });

    inside_callback.get_future().wait();

    auto stopper = std::async(std::launch::async, [&] { producer.stop(); });
    ASSERT_EQ(stopper.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "stop() did not return — the running producer callback never saw the stop request";
}

/// A consumer that stops calling next() (it already got everything it needed) would never see a
/// later producer failure. rethrowIfFailed(), called after stop() has joined the producer, surfaces
/// it instead of letting the error be silently dropped.
TEST(DoubleBufferedProducer, RethrowIfFailedSurfacesErrorAfterConsumerStopsTaking)
{
    DoubleBufferedProducer producer;
    std::promise<void> producer_failed;
    size_t produced = 0; /// touched only by the producer thread
    producer.start(nullptr, kName, [&](size_t) -> std::optional<size_t>
    {
        if (produced == 2)
        {
            producer_failed.set_value();
            throw std::runtime_error("boom");
        }
        return produced++;
    });

    /// Take and release both buffers, then stop consuming while the producer keeps working.
    for (size_t i = 0; i < 2; ++i)
    {
        auto item = producer.next();
        ASSERT_TRUE(item.has_value());
        producer.release(item->index);
    }

    producer_failed.get_future().wait();
    producer.stop();

    EXPECT_THROW(producer.rethrowIfFailed(), std::runtime_error);
}

/// A producer that finished normally leaves nothing to rethrow.
TEST(DoubleBufferedProducer, RethrowIfFailedIsNoOpOnSuccess)
{
    DoubleBufferedProducer producer;
    producer.start(nullptr, kName, [](size_t) -> std::optional<size_t> { return std::nullopt; });

    EXPECT_FALSE(producer.next().has_value());
    producer.stop();

    EXPECT_NO_THROW(producer.rethrowIfFailed());
}
