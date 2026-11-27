#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <vector>
#include <base/types.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>

namespace DB
{

class BackgroundSchedulePool;

/// One background task which runs several independent jobs of the filesystem cache.
/// It knows nothing about the jobs: each run calls every job and schedules the next run at the earliest time a job asked for.
class FileCacheMaintenanceTask
{
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    /// Returned by a job which has nothing to do until somebody calls `wake`.
    static constexpr TimePoint never = TimePoint::max();

    /// Does its work if it is due at `now` and returns the time when it is next due.
    /// A returned time which is not after `now` asks for another run at once.
    using Job = std::function<TimePoint(TimePoint now)>;

    /// Jobs run in the order they were added. Not thread-safe: add all jobs before `start`.
    void addJob(String job_name, Job job);

    /// Runs the jobs for the first time at once, so that every job says when it is next due.
    void start(BackgroundSchedulePool & pool, const String & log_name);

    /// Runs the jobs as soon as possible. Takes the lock of the task at most once per run, so cache hits can call it.
    /// Does nothing before `start`, as the first run serves it anyway.
    void wake();

    /// Waits for the current run to finish. No run starts afterwards, `wake` becomes a no-op.
    void stop();

    /// Serves the pending wake request, calls every job and returns the earliest time a job is next due.
    /// Public for tests.
    TimePoint runJobs(TimePoint now);

private:
    struct NamedJob
    {
        String name;
        Job job;
    };

    void run();

    std::vector<NamedJob> jobs;
    BackgroundSchedulePoolTaskHolder task;
    /// Whether `wake` was called since the current run started.
    std::atomic<bool> wake_requested = false;
};

}
