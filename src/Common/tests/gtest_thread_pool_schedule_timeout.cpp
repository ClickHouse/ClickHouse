#include <atomic>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <thread>

#include <base/defines.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>

#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace
{

/// A pool that is saturated as soon as one job is scheduled, so the next schedule takes the
/// `job_finished.wait_for` branch of ThreadPoolImpl::scheduleImpl -- the branch that converts
/// `wait_microseconds` into a chrono duration.
class SaturatedPool
{
public:
    SaturatedPool()
        : pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
               /* max_threads */ 1, /* max_free_threads */ 0, /* queue_size */ 1)
    {
        pool.scheduleOrThrowOnError([this]
        {
            std::unique_lock lock(mutex);
            blocker.wait(lock, [this] { return released; });
        });

        /// scheduleOrThrowOnError() takes the single queue slot before it returns, so the pool is already
        /// saturated here: every further schedule has to wait for this job to finish.
        chassert(pool.active() == 1);
    }

    void release()
    {
        {
            std::lock_guard lock(mutex);
            released = true;
        }
        blocker.notify_all();
    }

    ~SaturatedPool()
    {
        release();
        pool.wait();
    }

    ThreadPool pool;

private:
    std::mutex mutex;
    std::condition_variable blocker;
    bool released = false;
};

}

/// `wait_microseconds` is fed by settings such as `lock_acquire_timeout`, whose Int64 microsecond value
/// can be negative. It used to reach the pool through a `uint64_t` parameter, so a negative setting
/// arrived as a huge unsigned count, and `wait_for` overflowed Int64 while multiplying it by 1'000 to
/// reach nanoseconds (UBSan aborted the server; a release build wrapped to a deadline in the past).
/// A negative timeout has already expired, so the schedule must give up immediately instead.
TEST(ThreadPoolScheduleTimeout, NegativeTimeoutGivesUpImmediately)
{
    SaturatedPool saturated;

    Stopwatch watch;
    /// -1e17 microseconds is the value from the stress test report: `lock_acquire_timeout = -1e11` seconds.
    EXPECT_FALSE(saturated.pool.trySchedule([]{}, {}, /* wait_microseconds */ -100000000000000000LL));
    EXPECT_LT(watch.elapsedMilliseconds(), 10000);

    watch.restart();
    EXPECT_FALSE(saturated.pool.trySchedule([]{}, {}, /* wait_microseconds */ std::numeric_limits<Int64>::min()));
    EXPECT_LT(watch.elapsedMilliseconds(), 10000);
}

/// A positive timeout must still be honoured: the schedule waits for it and only then gives up.
TEST(ThreadPoolScheduleTimeout, PositiveTimeoutWaits)
{
    SaturatedPool saturated;

    Stopwatch watch;
    EXPECT_FALSE(saturated.pool.trySchedule([]{}, {}, /* wait_microseconds */ 200000));
    EXPECT_GE(watch.elapsedMicroseconds(), 150000);
}

/// A timeout above the clamp keeps meaning "wait until a thread frees up": it must neither overflow the
/// microseconds -> nanoseconds conversion nor make the schedule fail at once.
TEST(ThreadPoolScheduleTimeout, HugeTimeoutWaitsForAFreeThread)
{
    SaturatedPool saturated;

    std::atomic<bool> scheduled{false};
    std::atomic<bool> job_ran{false};

    std::thread scheduler([&]
    {
        scheduled = saturated.pool.trySchedule([&]{ job_ran = true; }, {}, std::numeric_limits<Int64>::max());
    });

    saturated.release();
    scheduler.join();

    EXPECT_TRUE(scheduled);
    saturated.pool.wait();
    EXPECT_TRUE(job_ran);
}
