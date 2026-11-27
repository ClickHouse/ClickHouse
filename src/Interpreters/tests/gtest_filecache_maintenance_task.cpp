#include <Interpreters/FileCache/FileCacheMaintenanceTask.h>

#include <Core/BackgroundSchedulePool.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <gtest/gtest.h>
#include <condition_variable>
#include <mutex>
#include <stdexcept>

namespace ProfileEvents
{
    extern const Event FilesystemCacheMaintenanceTaskWakeUps;
}

using namespace DB;
using namespace std::chrono_literals;

using TimePoint = FileCacheMaintenanceTask::TimePoint;

namespace
{

/// Counts the calls of a job which runs on a thread of the pool and lets the test wait for them.
class JobRunCounter
{
public:
    size_t increment()
    {
        std::lock_guard lock(mutex);
        ++runs;
        changed.notify_all();
        return runs;
    }

    /// Returns false if `expected_runs` were not reached in a minute.
    bool waitFor(size_t expected_runs)
    {
        std::unique_lock lock(mutex);
        return changed.wait_for(lock, 60s, [&] { return runs >= expected_runs; });
    }

    size_t get()
    {
        std::lock_guard lock(mutex);
        return runs;
    }

private:
    std::mutex mutex;
    std::condition_variable changed;
    size_t runs = 0;
};

BackgroundSchedulePoolPtr createPool()
{
    return BackgroundSchedulePool::create(2, 2, 0, CurrentMetrics::end(), CurrentMetrics::end(), ThreadName::TEST_SCHEDULER);
}

}

TEST(FileCacheMaintenanceTask, RunJobsReturnsTheEarliestNextDueTime)
{
    FileCacheMaintenanceTask task;
    std::vector<String> called_jobs;
    task.addJob("late", [&](TimePoint now) { called_jobs.push_back("late"); return now + 10s; });
    task.addJob("early", [&](TimePoint now) { called_jobs.push_back("early"); return now + 2s; });
    task.addJob("never", [&](TimePoint) { called_jobs.push_back("never"); return FileCacheMaintenanceTask::never; });

    const TimePoint now = FileCacheMaintenanceTask::Clock::now();
    EXPECT_EQ(task.runJobs(now), now + 2s);
    EXPECT_EQ(called_jobs, (std::vector<String>{"late", "early", "never"}));

    FileCacheMaintenanceTask task_without_jobs;
    EXPECT_EQ(task_without_jobs.runJobs(now), FileCacheMaintenanceTask::never);
}

TEST(FileCacheMaintenanceTask, ExceptionInOneJobDoesNotStopTheOthers)
{
    FileCacheMaintenanceTask task;
    bool job_after_exception_called = false;
    task.addJob("throws", [](TimePoint) -> TimePoint { throw std::runtime_error("expected in the test"); });
    task.addJob("after exception", [&](TimePoint now) { job_after_exception_called = true; return now + 1h; });

    const TimePoint now = FileCacheMaintenanceTask::Clock::now();
    EXPECT_EQ(task.runJobs(now), now + 1h);
    EXPECT_TRUE(job_after_exception_called);
}

TEST(FileCacheMaintenanceTask, WakeUpsAreCountedOncePerRun)
{
    FileCacheMaintenanceTask task;
    auto & events = ProfileEvents::global_counters;
    const auto wake_ups_before = events[ProfileEvents::FilesystemCacheMaintenanceTaskWakeUps];

    /// A task which was never started is woken without running, and the first run serves it.
    task.wake();
    task.wake();
    EXPECT_EQ(events[ProfileEvents::FilesystemCacheMaintenanceTaskWakeUps] - wake_ups_before, 1u);

    task.runJobs(FileCacheMaintenanceTask::Clock::now());
    task.wake();
    EXPECT_EQ(events[ProfileEvents::FilesystemCacheMaintenanceTaskWakeUps] - wake_ups_before, 2u);
}

TEST(FileCacheMaintenanceTask, JobReturningNowRunsAgainAtOnce)
{
    auto pool = createPool();
    FileCacheMaintenanceTask task;
    JobRunCounter runs;
    task.addJob("two full batches", [&](TimePoint now) { return runs.increment() < 3 ? now : FileCacheMaintenanceTask::never; });

    task.start(*pool, "test");
    const bool reached = runs.waitFor(3);

    /// The destructor of the pool requires `join`, so a fatal assertion must not precede it.
    task.stop();
    pool->join();
    EXPECT_TRUE(reached);
    EXPECT_EQ(runs.get(), 3);
}

TEST(FileCacheMaintenanceTask, WakeDuringRunCausesAnotherRun)
{
    auto pool = createPool();
    FileCacheMaintenanceTask task;
    JobRunCounter runs;
    task.addJob("wakes the task from its first run", [&](TimePoint)
    {
        if (runs.increment() == 1)
            task.wake();
        return FileCacheMaintenanceTask::never;
    });

    task.start(*pool, "test");
    const bool reached = runs.waitFor(2);

    task.stop();
    pool->join();
    EXPECT_TRUE(reached);
    EXPECT_EQ(runs.get(), 2);
}
