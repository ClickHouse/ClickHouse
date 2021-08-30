#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/MergeMutateExecutor.h>
#include <Common/ThreadPool.h>
#include <Core/BackgroundSchedulePool.h>
#include <pcg_random.hpp>


namespace DB
{

/// Settings for background tasks scheduling. Each background executor has one
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

/// Pool type where we must execute new job. Each background executor can have several
/// background pools. When it receives new job it will execute new task in corresponding pool.
enum class PoolType
{
    MERGE_MUTATE,
    MOVE,
    FETCH,
};

using BackgroundJobFunc = std::function<bool()>;

/// Result from background job providers. Function which will be executed in pool and pool type.
struct JobAndPool
{
    BackgroundJobFunc job;
    PoolType pool_type;
};

/// Background jobs executor which execute heavy-weight background tasks for MergTree tables, like
/// background merges, moves, mutations, fetches and so on.
/// Consists of two important parts:
/// 1) Task in background scheduling pool which receives new jobs from storages and put them into required pool.
/// 2) One or more ThreadPool objects, which execute background jobs.
class BackgroundJobExecutor : protected WithContext
{

private:
    MergeTreeData & data;

    /// Name for task in background scheduling pool
    String task_name;
    /// Settings for execution control of background scheduling task
    BackgroundTaskSchedulingSettings sleep_settings;
    /// Useful for random backoff timeouts generation
    pcg64 rng;

    /// How many times execution of background job failed or we have
    /// no new jobs.
    std::atomic<size_t> no_work_done_count{0};

    /// Scheduling task which assign jobs in background pool
    BackgroundSchedulePool::TaskHolder scheduling_task;
    /// Mutex for thread safety
    std::mutex scheduling_task_mutex;
    /// Mutex for pcg random generator thread safety
    std::mutex random_mutex;

    /// Save storage id to prevent use-after-free in destructor
    StorageID storage_id;

public:

    enum class Type
    {
        DataProcessing,
        Moving
    };

    Type type{Type::DataProcessing};

    /// These three functions are thread safe

    /// Start background task and start to assign jobs
    void start();
    /// Schedule background task as soon as possible, even if it sleep at this
    /// moment for some reason.
    void triggerTask();

    void triggerTaskWithDelay();
    /// Finish execution: deactivate background task and wait already scheduled jobs
    void finish();

    void executeMergeMutateTask(BackgroundTaskPtr merge_task);
    void executeFetchTask(BackgroundTaskPtr fetch_task);
    void executeMoveTask(BackgroundTaskPtr move_task);

    /// Just call finish
    virtual ~BackgroundJobExecutor();

    BackgroundJobExecutor(
        MergeTreeData & data_,
        Type type,
        ContextPtr global_context_);

private:

    bool selectTaskAndExecute();

    /// Function that executes in background scheduling pool
    void backgroundTaskFunction();
    /// Recalculate timeouts when we have to check for a new job
    void scheduleTask(bool with_backoff);
    /// Run background task as fast as possible and reset errors counter
    void runTaskWithoutDelay();
    /// Return random add for sleep in case of error
    double getSleepRandomAdd();
};




}
