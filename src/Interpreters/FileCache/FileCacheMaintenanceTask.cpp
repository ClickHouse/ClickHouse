#include <Interpreters/FileCache/FileCacheMaintenanceTask.h>

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/StorageID.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event FilesystemCacheMaintenanceTaskWakeUps;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void FileCacheMaintenanceTask::addJob(String job_name, Job job)
{
    if (task)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add job `{}` to a maintenance task which was already started", job_name);

    jobs.push_back({std::move(job_name), std::move(job)});
}

void FileCacheMaintenanceTask::start(BackgroundSchedulePool & pool, const String & log_name)
{
    if (task)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Maintenance task `{}` was already started", log_name);

    task = pool.createTask(StorageID::createEmpty(), log_name, [this] { run(); });
    task->schedule();
}

void FileCacheMaintenanceTask::wake()
{
    /// Loaded before exchanging, so that callers on a hot path do not write a shared cache line.
    if (wake_requested.load(std::memory_order_relaxed) || wake_requested.exchange(true))
        return;

    ProfileEvents::increment(ProfileEvents::FilesystemCacheMaintenanceTaskWakeUps);
    if (task)
        task->schedule();
}

void FileCacheMaintenanceTask::stop()
{
    if (task)
        task->deactivate();
}

void FileCacheMaintenanceTask::run()
{
    const auto next_run_time = runJobs(Clock::now());
    if (next_run_time == never)
        return;

    /// The jobs could take long, so `now` which they got is not used here.
    const auto time_after_jobs = Clock::now();
    if (next_run_time <= time_after_jobs)
    {
        task->schedule();
        return;
    }

    /// Rounded up: a run which starts a part of a millisecond early finds no job due and has to run once more.
    /// Does nothing if `wake` was called during this run: the pool then starts the next run at once.
    task->scheduleAfter(std::chrono::ceil<std::chrono::milliseconds>(next_run_time - time_after_jobs).count());
}

FileCacheMaintenanceTask::TimePoint FileCacheMaintenanceTask::runJobs(TimePoint now)
{
    /// Before the jobs, so that a `wake` while they run schedules another run.
    wake_requested = false;

    TimePoint earliest_next_due_time = never;
    for (const auto & named_job : jobs)
    {
        try
        {
            earliest_next_due_time = std::min(earliest_next_due_time, named_job.job(now));
        }
        catch (...)
        {
            /// Not retried: the job runs again with the next run of the task.
            tryLogCurrentException("FileCacheMaintenanceTask", "Job `" + named_job.name + "` failed");
        }
    }
    return earliest_next_due_time;
}

}
