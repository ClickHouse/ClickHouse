#include "config.h"

#include <gtest/gtest.h>

#if USE_SILK

#include <Common/Exception.h>
#include <Common/SilkFiberMailbox.h>
#include <Common/SilkFiberScheduler.h>
#include <Common/tests/gtest_silk_scheduler.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <poll.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

class SilkFiberMailboxTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        initializeFiberSchedulerForTests();
    }
};

/// Whether the descriptor is readable right now (timeout 0) or within the timeout.
bool isReadable(int fd, int timeout_ms)
{
    pollfd pfd{.fd = fd, .events = POLLIN, .revents = 0};
    int res = ::poll(&pfd, 1, timeout_ms);
    return res == 1 && (pfd.revents & POLLIN);
}

constexpr int wait_timeout_ms = 10000;

}


TEST_F(SilkFiberMailboxTest, DeliversItemsInOrderWithBackpressure)
{
    constexpr size_t capacity = 2;
    constexpr size_t num_items = 200;

    Silk::FiberMailbox<std::unique_ptr<size_t>> mailbox(capacity);
    std::atomic<size_t> pushed{0};

    silk::FiberFuture future;
    ASSERT_EQ(Silk::spawn([&]() -> int
    {
        for (size_t i = 0; i < num_items; ++i)
        {
            if (!mailbox.push(std::make_unique<size_t>(i)))
                return 1;
            pushed.fetch_add(1);
        }
        mailbox.finish();
        return 0;
    }, future), 0);

    size_t popped = 0;
    while (!mailbox.isFinished())
    {
        ASSERT_TRUE(isReadable(mailbox.getFileDescriptor(), wait_timeout_ms)) << "the mailbox did not become readable";

        for (;;)
        {
            /// Checked before the pop: the producer can be at most `capacity` items ahead of the consumer.
            EXPECT_LE(pushed.load(), popped + capacity);

            auto item = mailbox.tryPop();
            if (!item)
                break;

            EXPECT_EQ(**item, popped);
            ++popped;

            /// Give the producer a chance to run ahead, so that the backpressure is actually exercised.
            if (popped % 50 == 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }

    EXPECT_EQ(popped, num_items);
    EXPECT_EQ(future.wait(), 0);
    EXPECT_FALSE(isReadable(mailbox.getFileDescriptor(), 0));
}

TEST_F(SilkFiberMailboxTest, FileDescriptorIsReadableOnlyWhenSomethingIsPending)
{
    Silk::FiberMailbox<std::string> mailbox(4);
    EXPECT_FALSE(isReadable(mailbox.getFileDescriptor(), 0));

    EXPECT_EQ(Silk::runBlocking([&]() -> int { return mailbox.push("hello") ? 0 : 1; }), 0);
    EXPECT_TRUE(isReadable(mailbox.getFileDescriptor(), wait_timeout_ms));

    auto item = mailbox.tryPop();
    ASSERT_TRUE(item.has_value());
    EXPECT_EQ(*item, "hello");
    EXPECT_FALSE(isReadable(mailbox.getFileDescriptor(), 0));
    EXPECT_FALSE(mailbox.tryPop().has_value());
    EXPECT_FALSE(mailbox.isFinished());

    EXPECT_EQ(Silk::runBlocking([&]() -> int { mailbox.finish(); return 0; }), 0);
    EXPECT_TRUE(isReadable(mailbox.getFileDescriptor(), wait_timeout_ms));
    EXPECT_FALSE(mailbox.tryPop().has_value());
    EXPECT_TRUE(mailbox.isFinished());
    EXPECT_FALSE(isReadable(mailbox.getFileDescriptor(), 0));
}

TEST_F(SilkFiberMailboxTest, RethrowsProducerExceptionAfterTheItems)
{
    Silk::FiberMailbox<int> mailbox(8);

    EXPECT_EQ(Silk::runBlocking([&]() -> int
    {
        if (!mailbox.push(1) || !mailbox.push(2))
            return 1;
        try
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Producer failure");
        }
        catch (...)
        {
            mailbox.finish(std::current_exception());
        }
        return 0;
    }), 0);

    ASSERT_TRUE(isReadable(mailbox.getFileDescriptor(), wait_timeout_ms));

    auto first = mailbox.tryPop();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(*first, 1);
    EXPECT_FALSE(mailbox.isFinished());

    auto second = mailbox.tryPop();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(*second, 2);
    EXPECT_FALSE(mailbox.isFinished()) << "the exception has not been taken yet";

    try
    {
        mailbox.tryPop();
        FAIL() << "the producer's exception was not rethrown";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::BAD_ARGUMENTS);
    }

    /// Rethrown exactly once.
    EXPECT_FALSE(mailbox.tryPop().has_value());
    EXPECT_TRUE(mailbox.isFinished());
}

TEST_F(SilkFiberMailboxTest, CloseWakesSuspendedProducer)
{
    constexpr size_t capacity = 2;
    Silk::FiberMailbox<int> mailbox(capacity);
    std::atomic<size_t> pushed{0};
    std::atomic<bool> push_refused{false};

    silk::FiberFuture future;
    ASSERT_EQ(Silk::spawn([&]() -> int
    {
        /// Fills the mailbox and then suspends in the third push until the consumer closes it.
        for (int i = 0; i < 100; ++i)
        {
            if (!mailbox.push(i))
            {
                push_refused = true;
                return 0;
            }
            pushed.fetch_add(1);
        }
        return 1;
    }, future), 0);

    /// Wait until the producer has filled the mailbox. The consumer takes nothing.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(wait_timeout_ms);
    while (pushed.load() < capacity && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_EQ(pushed.load(), capacity);

    /// The producer is now suspended in `push`; wait a little to make sure it really is.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(pushed.load(), capacity);

    mailbox.close();
    EXPECT_EQ(future.wait(), 0);
    EXPECT_TRUE(push_refused.load());
    EXPECT_EQ(pushed.load(), capacity);

    /// The items pushed before `close` are still there for the owner to drain.
    EXPECT_TRUE(mailbox.tryPop().has_value());
    EXPECT_TRUE(mailbox.tryPop().has_value());
    EXPECT_FALSE(mailbox.tryPop().has_value());
}

TEST_F(SilkFiberMailboxTest, PushAfterCloseIsRefused)
{
    Silk::FiberMailbox<int> mailbox(2);
    mailbox.close();
    EXPECT_EQ(Silk::runBlocking([&]() -> int { return mailbox.push(1) ? 1 : 0; }), 0);
    EXPECT_FALSE(mailbox.tryPop().has_value());
}

TEST_F(SilkFiberMailboxTest, FiberConsumer)
{
    /// A fiber may consume too: `tryPop` never suspends, so it is combined with a yield loop here.
    Silk::FiberMailbox<int> mailbox(1);

    silk::FiberFuture producer;
    ASSERT_EQ(Silk::spawn([&]() -> int
    {
        for (int i = 0; i < 10; ++i)
            if (!mailbox.push(i))
                return 1;
        mailbox.finish();
        return 0;
    }, producer), 0);

    std::vector<int> received;
    EXPECT_EQ(Silk::runBlocking([&]() -> int
    {
        while (!mailbox.isFinished())
        {
            if (auto item = mailbox.tryPop())
                received.push_back(*item);
            else
                silk::FiberScheduler::yield();
        }
        return 0;
    }), 0);

    EXPECT_EQ(producer.wait(), 0);
    ASSERT_EQ(received.size(), 10u);
    for (int i = 0; i < 10; ++i)
        EXPECT_EQ(received[i], i);
}

TEST_F(SilkFiberMailboxTest, ZeroCapacityIsRejected)
{
    EXPECT_THROW(Silk::FiberMailbox<int>(0), DB::Exception);
}

#endif
