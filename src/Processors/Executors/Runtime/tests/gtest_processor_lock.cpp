#include <Processors/Executors/Runtime/Engine/State/ProcessorState.h>

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace DB;

TEST(ProcessorLock, LockAndUnlock)
{
    ProcessorLock lock;
    EXPECT_FALSE(lock.isFinished());

    EXPECT_TRUE(lock.tryLock());
    EXPECT_FALSE(lock.tryLock());

    EXPECT_TRUE(lock.tryUnlock(lock.snapshot()));
    EXPECT_TRUE(lock.tryLock());
}

TEST(ProcessorLock, NotifyDoesNotLock)
{
    ProcessorLock lock;

    lock.notify();
    EXPECT_TRUE(lock.tryLock());

    lock.notify();
    EXPECT_FALSE(lock.tryLock());
}

TEST(ProcessorLock, NotifyAfterSnapshotFailsUnlock)
{
    ProcessorLock lock;
    ASSERT_TRUE(lock.tryLock());

    auto snapshot = lock.snapshot();
    lock.notify();
    EXPECT_FALSE(lock.tryUnlock(snapshot));

    EXPECT_TRUE(lock.tryUnlock(lock.snapshot()));
    EXPECT_TRUE(lock.tryLock());
}

TEST(ProcessorLock, FinishIsTerminal)
{
    ProcessorLock lock;
    ASSERT_TRUE(lock.tryLock());

    lock.finish();
    EXPECT_TRUE(lock.isFinished());
    EXPECT_FALSE(lock.tryLock());
    lock.notify();
    EXPECT_TRUE(lock.isFinished());
    EXPECT_FALSE(lock.tryLock());
}

TEST(ProcessorLock, ConcurrentNotifyAcquiresOnce)
{
    constexpr size_t rounds = 200;
    constexpr size_t notifiers = 8;

    for (size_t round = 0; round < rounds; ++round)
    {
        ProcessorLock lock;
        std::atomic<size_t> acquired{0};

        std::vector<std::thread> threads;
        for (size_t i = 0; i < notifiers; ++i)
        {
            threads.emplace_back([&]
            {
                lock.notify();
                if (lock.tryLock())
                    ++acquired;
            });
        }

        for (auto & thread : threads)
            thread.join();

        EXPECT_EQ(1u, acquired.load());
        EXPECT_FALSE(lock.tryLock());
        EXPECT_TRUE(lock.tryUnlock(lock.snapshot()));
    }
}

TEST(ProcessorLock, NoNotificationIsLost)
{
    constexpr size_t notifiers = 8;
    constexpr size_t notifications_per_thread = 20000;

    ProcessorLock lock;
    std::atomic<size_t> pending{0};
    std::atomic<size_t> consumed{0};

    auto run_owner = [&]
    {
        while (true)
        {
            auto snapshot = lock.snapshot();
            consumed += pending.exchange(0);
            if (lock.tryUnlock(snapshot))
                return;
        }
    };

    ASSERT_TRUE(lock.tryLock());

    std::vector<std::thread> threads;
    for (size_t i = 0; i < notifiers; ++i)
    {
        threads.emplace_back([&]
        {
            for (size_t j = 0; j < notifications_per_thread; ++j)
            {
                pending.fetch_add(1);
                lock.notify();
                if (lock.tryLock())
                    run_owner();
            }
        });
    }

    run_owner();

    for (auto & thread : threads)
        thread.join();

    EXPECT_EQ(notifiers * notifications_per_thread, consumed.load());
    EXPECT_EQ(0u, pending.load());
    EXPECT_TRUE(lock.tryLock());
}
