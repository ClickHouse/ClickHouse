#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
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

/// Result from background job providers. Function which will be executed in pool and pool type.
struct JobAndPool
{
    ThreadPool::Job job;
    PoolType pool_type;
};

/// Background jobs executor which execute heavy-weight background tasks for MergTree tables, like
/// background merges, moves, mutations, fetches and so on.
/// Consists of two important parts:
/// 1) Task in background scheduling pool which receives new jobs from storages and put them into required pool.
/// 2) One or more ThreadPool objects, which execute background jobs.
class IBackgroundJobExecutor
{
protected:
    Context & global_context;

    /// Configuration for single background ThreadPool
    struct PoolConfig
    {
        /// This pool type
        PoolType pool_type;
        /// Max pool size in threads
        size_t max_pool_size;
        /// Metric that we have to increment when we execute task in this pool
        CurrentMetrics::Metric tasks_metric;
    };

private:
    /// Name for task in background scheduling pool
    String task_name;
    /// Settings for execution control of background scheduling task
    BackgroundTaskSchedulingSettings sleep_settings;
    /// Useful for random backoff timeouts generation
    pcg64 rng;

    /// How many times execution of background job failed or we have
    /// no new jobs.
    std::atomic<size_t> no_work_done_count{0};

    /// Pools where we execute background jobs
    std::unordered_map<PoolType, ThreadPool> pools;
    /// Configs for background pools
    std::unordered_map<PoolType, PoolConfig> pools_configs;

    /// Scheduling task which assign jobs in background pool
    BackgroundSchedulePool::TaskHolder scheduling_task;
    /// Mutex for thread safety
    std::mutex scheduling_task_mutex;
    /// Mutex for pcg random generator thread safety
    std::mutex random_mutex;

public:
    /// These three functions are thread safe

    /// Start background task and start to assign jobs
    void start();
    /// Schedule background task as soon as possible, even if it sleep at this
    /// moment for some reason.
    void triggerTask();
    /// Finish execution: deactivate background task and wait already scheduled jobs
    void finish();

    /// Just call finish
    virtual ~IBackgroundJobExecutor();

protected:
    IBackgroundJobExecutor(
        Context & global_context_,
        const BackgroundTaskSchedulingSettings & sleep_settings_,
        const std::vector<PoolConfig> & pools_configs_);

    /// Name for task in background schedule pool
    virtual String getBackgroundTaskName() const = 0;
    /// Get job for background execution
    virtual std::optional<JobAndPool> getBackgroundJob() = 0;

private:
    /// Function that executes in background scheduling pool
    void jobExecutingTask();
    /// Recalculate timeouts when we have to check for a new job
    void scheduleTask(bool with_backoff);
    /// Run background task as fast as possible and reset errors counter
    void runTaskWithoutDelay();
    /// Return random add for sleep in case of error
    double getSleepRandomAdd();
};

/// Main jobs executor: merges, mutations, fetches and so on
class BackgroundJobsExecutor final : public IBackgroundJobExecutor
{
private:
    MergeTreeData & data;
public:
    BackgroundJobsExecutor(
        MergeTreeData & data_,
        Context & global_context_);

protected:
    String getBackgroundTaskName() const override;
    std::optional<JobAndPool> getBackgroundJob() override;
};

/// Move jobs executor, move parts between disks in the background
/// Does nothing in case of default configuration
class BackgroundMovesExecutor final : public IBackgroundJobExecutor
{
private:
    MergeTreeData & data;
public:
    BackgroundMovesExecutor(
        MergeTreeData & data_,
        Context & global_context_);

protected:
    String getBackgroundTaskName() const override;
    std::optional<JobAndPool> getBackgroundJob() override;
};

}
