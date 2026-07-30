#include "config.h"

#include <gtest/gtest.h>

#if USE_SILK

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/SilkFiberScheduler.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_silk_scheduler.h>

#include <base/defines.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <cstddef>
#include <stdexcept>
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

TEST_F(SilkFiberSchedulerTest, NoOSThreadTagMakesUnboundThreadStatus)
{
    EXPECT_EQ(Silk::runBlocking([]() -> int
    {
        DB::ThreadStatus thread_status(DB::ThreadStatus::NoOSThreadTag{});
        EXPECT_EQ(thread_status.thread_id, DB::ThreadStatus::NO_OS_THREAD);
        return 0;
    }), 0);
}

TEST_F(SilkFiberSchedulerTest, CurrentThreadIsPreservedAcrossSuspensions)
{
    constexpr size_t num_fibers = 42;
    constexpr size_t suspensions = 42;

    auto with_thread_status = []() -> int
    {
        DB::ThreadStatus thread_status(DB::ThreadStatus::NoOSThreadTag{});
        for (size_t i = 0; i < suspensions; ++i)
        {
            silk::FiberScheduler::yield();
            EXPECT_EQ(loadCurrentThread(), &thread_status);
        }
        return 0;
    };

    auto without_thread_status = []() -> int
    {
        for (size_t i = 0; i < suspensions; ++i)
        {
            silk::FiberScheduler::yield();
            EXPECT_EQ(loadCurrentThread(), nullptr);
        }
        return 0;
    };

    std::vector<silk::FiberFuture> futures(2 * num_fibers);
    size_t spawned = 0;
    for (auto task : {+with_thread_status, +without_thread_status})
    {
        const size_t end = spawned + num_fibers;
        for (; spawned != end; ++spawned)
        {
            ASSERT_EQ(Silk::spawn(task, futures[spawned]), 0);
        }
    }

    for (size_t i = 0; i < spawned; ++i)
    {
        futures[i].wait();
    }
}

#endif
