#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>

#include <pcg_random.hpp>


namespace DB
{

/// Settings for background tasks scheduling. Each background assignee has one
/// BackgroundSchedulingPoolTask and depending on execution result may put this
/// task to sleep according to settings. Look at scheduleTask function for details.
struct BackgroundTaskSchedulingSettings
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

class BackgroundJobsAssignee : public WithContext
{
private:
    MergeTreeData & data;

    /// Settings for execution control of background scheduling task
    BackgroundTaskSchedulingSettings sleep_settings;
    /// Useful for random backoff timeouts generation
    pcg64 rng;

    /// How many times execution of background job failed or we have
    /// no new jobs.
    size_t no_work_done_count = 0;

    /// Scheduling task which assign jobs in background pool
    BackgroundSchedulePool::TaskHolder holder;
    /// Mutex for thread safety
    std::mutex holder_mutex;

public:
    /// In case of ReplicatedMergeTree the first assignee will be responsible for
    /// polling the replication queue and schedule operations according to the LogEntry type
    /// e.g. merges, mutations and fetches. The same will be for Plain MergeTree except there is no
    /// replication queue, so we will just scan parts and decide what to do.
    /// Moving operations are the same for all types of MergeTree and also have their own timetable.
    enum class Type : uint8_t
    {
        DataProcessing,
        Moving
    };
    Type type{Type::DataProcessing};

    void start();
    void trigger();
    void postpone();
    void finish();

    bool scheduleMergeMutateTask(ExecutableTaskPtr merge_task);
    bool scheduleFetchTask(ExecutableTaskPtr fetch_task);
    bool scheduleMoveTask(ExecutableTaskPtr move_task);
    bool scheduleCommonTask(ExecutableTaskPtr common_task, bool need_trigger);

    /// Just call finish
    ~BackgroundJobsAssignee();

    BackgroundJobsAssignee(
        MergeTreeData & data_,
        Type type,
        ContextPtr global_context_);

private:
    static String toString(Type type);

    /// Function that executes in background scheduling pool
    void threadFunc();
};


}
