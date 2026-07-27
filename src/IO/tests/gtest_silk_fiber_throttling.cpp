#include "config.h"

#include <gtest/gtest.h>

#if USE_SILK

#include <IO/SilkFiberJob.h>
#include <IO/tests/gtest_silk_environment.h>

#include <Common/CurrentThread.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadStatus.h>
#include <Common/Throttler.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <memory>
#include <thread>
#include <vector>

namespace
{

::testing::Environment * const silk_env = DB::tests::registerSilkEnvironment();

/// Never dereferenced: `CurrentThread::attachReadResource` only stores and
/// compares the link, so an opaque non-null pointer is enough for the test.
int fake_read_queue_tag;
int fake_write_queue_tag;

}

/// The RAII throttling scopes store their state in `ThreadStatus`. A fiber
/// migrates across carrier OS threads on every suspension, so this only works
/// because each fiber owns a `ThreadStatus` that the fiber-switch hooks swap
/// into `current_thread` wherever the fiber resumes. This is the regression
/// test for the load-test failure where scopes attached on one carrier and
/// detached on another: LOGICAL_ERROR from `detachReadThrottler` escaping a
/// scope guard and terminating the server.
TEST(SilkFiberThrottling, ScopesSurviveMigration)
{
    constexpr size_t fibers_per_kind = 42;
    constexpr size_t suspensions = 42;

    struct Params
    {
        DB::SilkFiberJobHeader header;
        bool with_scopes = false;
    };

    auto fiber_main = +[](Params * params) noexcept -> int
    {
        try
        {
            DB::ThreadStatus thread_status(DB::ThreadStatus::NoOSThreadTag{});

            if (params->with_scopes)
            {
                auto read_throttler = std::make_shared<DB::Throttler>(/*max_speed_*/ 1'000'000);
                auto write_throttler = std::make_shared<DB::Throttler>(/*max_speed_*/ 1'000'000);
                DB::ResourceLink read_link{.queue = reinterpret_cast<DB::ISchedulerQueue *>(&fake_read_queue_tag)};
                DB::ResourceLink write_link{.queue = reinterpret_cast<DB::ISchedulerQueue *>(&fake_write_queue_tag)};

                DB::CurrentThread::ReadThrottlingScope read_scope(read_throttler);
                DB::CurrentThread::WriteThrottlingScope write_scope(write_throttler);
                DB::CurrentThread::IOSchedulingScope io_scope(read_link, write_link);

                for (size_t i = 0; i < suspensions; ++i)
                {
                    silk::FiberScheduler::yield();
                    if (DB::CurrentThread::getReadThrottler().get() != read_throttler.get())
                        return 2;
                    if (DB::CurrentThread::getWriteThrottler().get() != write_throttler.get())
                        return 3;
                    if (!(DB::CurrentThread::getReadResourceLink() == read_link))
                        return 4;
                    if (!(DB::CurrentThread::getWriteResourceLink() == write_link))
                        return 5;
                }
            }
            else
            {
                /// Sharing a carrier with throttled fibers must not leak their
                /// state into this fiber - the cross-contamination half of the bug.
                for (size_t i = 0; i < suspensions; ++i)
                {
                    silk::FiberScheduler::yield();
                    if (DB::CurrentThread::getReadThrottler() != nullptr)
                        return 6;
                    if (DB::CurrentThread::getWriteThrottler() != nullptr)
                        return 7;
                    if (DB::CurrentThread::getReadResourceLink() || DB::CurrentThread::getWriteResourceLink())
                        return 8;
                }
            }
            return 0;
        }
        catch (...)
        {
            /// A detach LOGICAL_ERROR from a scope destructor lands here.
            return 1;
        }
    };

    std::vector<silk::FiberFuture> futures(2 * fibers_per_kind);
    for (size_t i = 0; i < futures.size(); ++i)
        ASSERT_EQ(DB::runSilkFiber(fiber_main, Params{{}, /*with_scopes=*/ i % 2 == 0}, 0, &futures[i]), 0);

    for (auto & future : futures)
        EXPECT_EQ(future.wait(), 0);
}

#endif
