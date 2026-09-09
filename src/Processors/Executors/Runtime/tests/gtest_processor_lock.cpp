#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

using namespace DB;

TEST(ProcessorLock, StatusTransitions)
{
    ProcessorLock lock;
    EXPECT_EQ(ProcessorLock::Status::Idle, lock.status());
    EXPECT_FALSE(lock.isFinished());

    {
        auto round = lock.lockRound();
        lock.setExecuting();
    }
    EXPECT_EQ(ProcessorLock::Status::Executing, lock.status());

    {
        auto round = lock.lockRound();
        lock.setIdle();
    }
    EXPECT_EQ(ProcessorLock::Status::Idle, lock.status());

    lock.finish();
    EXPECT_TRUE(lock.isFinished());
    EXPECT_EQ(ProcessorLock::Status::Finished, lock.status());
}

TEST(ProcessorLock, RoundIsExclusive)
{
    ProcessorLock lock;
    std::atomic<bool> other_entered{false};

    auto round = lock.lockRound();
    std::thread other([&]
    {
        auto inner = lock.lockRound();
        other_entered = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(other_entered);

    round.unlock();
    other.join();
    EXPECT_TRUE(other_entered);
}
