#include "config.h"

#include <gtest/gtest.h>

#if USE_SILK

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/SilkFiberScheduler.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_silk_scheduler.h>

#include <base/defines.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

class SilkFiberSchedulerTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        initializeFiberSchedulerForTests();
    }
};

NO_INLINE DB::ThreadStatus * loadCurrentThread()
{
    return DB::current_thread;
}

}


TEST_F(SilkFiberSchedulerTest, RunBlockingReturnsTaskResult)
{
    EXPECT_EQ(Silk::runBlocking([] { return 0; }), 0);
    EXPECT_EQ(Silk::runBlocking([] { return 42; }), 42);
}

TEST_F(SilkFiberSchedulerTest, SpawnDeliversResultThroughFuture)
{
    silk::FiberFuture future;
    ASSERT_EQ(Silk::spawn([] { return 7; }, future), 0);
    EXPECT_EQ(future.wait(), 7);
}

TEST_F(SilkFiberSchedulerTest, ThrowingTaskReturnsErrorCode)
{
    EXPECT_NE(Silk::runBlocking([]() -> int { throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Task failure"); }), 0);
    EXPECT_NE(Silk::runBlocking([]() -> int { throw std::runtime_error("Task failure"); }), 0);
}

TEST_F(SilkFiberSchedulerTest, FiberGetsUnboundThreadStatus)
{
    std::optional<UInt64> thread_id;
    EXPECT_EQ(Silk::runBlocking([&thread_id]() -> int
    {
        if (const DB::ThreadStatus * thread_status = loadCurrentThread())
            thread_id = thread_status->thread_id;
        return 0;
    }), 0);

    ASSERT_TRUE(thread_id.has_value());
    EXPECT_EQ(*thread_id, DB::ThreadStatus::NO_OS_THREAD);
}

TEST_F(SilkFiberSchedulerTest, CurrentThreadIsPreservedAcrossSuspensions)
{
    constexpr size_t num_fibers = 42;
    constexpr size_t suspensions = 42;

    auto task = []() -> int
    {
        const DB::ThreadStatus * thread_status = loadCurrentThread();
        EXPECT_NE(thread_status, nullptr);
        for (size_t i = 0; i < suspensions; ++i)
        {
            silk::FiberScheduler::yield();
            EXPECT_EQ(loadCurrentThread(), thread_status);
        }
        return 0;
    };

    std::vector<silk::FiberFuture> futures(num_fibers);
    for (size_t i = 0; i < num_fibers; ++i)
    {
        ASSERT_EQ(Silk::spawn(task, futures[i]), 0);
    }

    for (size_t i = 0; i < num_fibers; ++i)
    {
        futures[i].wait();
    }
}

/// A parked fiber must leave no untracked memory outside the per-CPU counters.
TEST_F(SilkFiberSchedulerTest, SuspensionPublishesUntrackedMemory)
{
    EXPECT_EQ(Silk::runBlocking([]() -> int
    {
        DB::ThreadStatus * thread_status = loadCurrentThread();
        EXPECT_NE(thread_status, nullptr);

        DB::CurrentThread::flushUntrackedMemory();
        std::ignore = CurrentMemoryTracker::allocNoThrow(1024);
        EXPECT_EQ(thread_status->per_cpu_untracked_memory.contributed, 0);

        silk::FiberScheduler::yield();
        EXPECT_EQ(thread_status->per_cpu_untracked_memory.contributed, thread_status->untracked_memory.load());
        return 0;
    }), 0);
}

#endif
