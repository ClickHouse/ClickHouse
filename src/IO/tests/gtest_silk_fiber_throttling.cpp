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
            /// Ok: a detach LOGICAL_ERROR from a scope destructor lands here,
            /// surfaced as the fiber's non-zero exit code checked by the test.
            return 1;
        }
    };

    std::vector<silk::FiberFuture> futures(2 * fibers_per_kind);
    for (size_t i = 0; i < futures.size(); ++i)
        ASSERT_EQ(DB::runSilkFiber(fiber_main, Params{{}, /*with_scopes=*/ i % 2 == 0}, DB::SilkFiberCategory::FETCH, &futures[i]), 0);

    for (auto & future : futures)
        EXPECT_EQ(future.wait(), 0);
}

/// A throttled fiber must suspend, not block: `Throttler::sleep` on a fiber
/// has to release the carrier OS thread so other fibers can run on it.
/// With 2x carrier oversubscription and every fiber sleeping S seconds,
/// suspending sleeps all overlap (wall ~ S) while blocking sleeps serialize
/// two fibers per carrier (wall >= 2*S). Silk's carrier count is per-CPU and
/// not configurable, so oversubscription is the deterministic form available.
TEST(SilkFiberThrottling, ThrottleSleepYieldsCarrier)
{
    constexpr UInt64 NS = 1'000'000'000;
    constexpr size_t max_speed = 1000;   /// tokens per second; burst = 1 s worth
    constexpr UInt64 sleep_ns = NS;      /// each fiber's forced throttle sleep

    const size_t carriers_upper_bound = std::max<size_t>(1, std::thread::hardware_concurrency());
    const size_t fiber_count = 2 * carriers_upper_bound;

    struct Params
    {
        DB::SilkFiberJobHeader header;
    };

    auto fiber_main = +[](Params *) noexcept -> int
    {
        try
        {
            DB::ThreadStatus thread_status(DB::ThreadStatus::NoOSThreadTag{});
            /// Fresh bucket holds `max_speed` tokens (1 s burst). Consuming
            /// twice that in one call leaves it 1 s negative -> sleep of ~1 s.
            DB::Throttler throttler(max_speed);
            throttler.throttle(2 * max_speed, /*max_block_ns*/ 5 * NS);
            return 0;
        }
        catch (...)
        {
            /// Ok: surfaced as the fiber's non-zero exit code checked by the test.
            return 1;
        }
    };

    Stopwatch watch;

    std::vector<silk::FiberFuture> futures(fiber_count);
    for (auto & future : futures)
        ASSERT_EQ(DB::runSilkFiber(fiber_main, Params{}, DB::SilkFiberCategory::FETCH, &future), 0);

    for (auto & future : futures)
        EXPECT_EQ(future.wait(), 0);

    const UInt64 elapsed_ns = watch.elapsedNanoseconds();

    /// The rate limit itself is still honored: nothing finished early.
    EXPECT_GE(elapsed_ns, sleep_ns);
    /// Suspending sleeps overlap; blocking sleeps would serialize to >= 2 s.
    EXPECT_LT(elapsed_ns, sleep_ns + sleep_ns / 2);
}

#endif
