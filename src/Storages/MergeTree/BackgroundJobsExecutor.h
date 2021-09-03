#pragma once

#include <Storages/MergeTree/MergeMutateExecutor.h>
#include <Common/ThreadPool.h>
#include <Core/BackgroundSchedulePool.h>
#include <pcg_random.hpp>


namespace DB
{

/// Settings for background tasks scheduling. Each background executor has one
/// BackgroundSchedulingPoolTask and depending on execution result may put this
/// task to sleep according to settings. Look at scheduleTask function for details.
struct ExecutableTaskSchedulingSettings
{
    double thread_sleep_seconds_random_part = 1.0;
    double thread_sleep_seconds_if_nothing_to_do = 0.1;
    double task_sleep_seconds_when_no_work_max = 600;
    /// For exponential backoff.
    double task_sleep_seconds_when_no_work_multiplier = 1.1;

    double task_sleep_seconds_when_no_work_random_part = 1.0;

     /// Deprecated settings, don't affect background execution
    double thread_sleep_seconds = 10;
    double task_sleep_seconds_when_no_work_min = 10;
};

class MergeTreeData;

class BackgroundJobAssignee : protected WithContext
{
private:
    MergeTreeData & data;

    /// Settings for execution control of background scheduling task
    ExecutableTaskSchedulingSettings sleep_settings;
    /// Useful for random backoff timeouts generation
    pcg64 rng;

    /// How many times execution of background job failed or we have
    /// no new jobs.
    std::atomic<size_t> no_work_done_count{0};

    /// Scheduling task which assign jobs in background pool
    BackgroundSchedulePool::TaskHolder holder;
    /// Mutex for thread safety
    std::mutex holder_mutex;

public:
    enum class Type
    {
        DataProcessing,
        Moving
    };
    Type type{Type::DataProcessing};

    void start();
    void trigger();
    void postpone();
    void finish();

    void scheduleMergeMutateTask(ExecutableTaskPtr merge_task);
    void scheduleFetchTask(ExecutableTaskPtr fetch_task);
    void scheduleMoveTask(ExecutableTaskPtr move_task);

    /// Just call finish
    virtual ~BackgroundJobAssignee();

    BackgroundJobAssignee(
        MergeTreeData & data_,
        Type type,
        ContextPtr global_context_);

private:
    static String toString(Type type);

    /// Function that executes in background scheduling pool
    void main();
};


}
