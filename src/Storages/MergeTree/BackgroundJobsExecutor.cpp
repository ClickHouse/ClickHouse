#include <Storages/MergeTree/BackgroundJobsExecutor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <pcg_random.hpp>
#include <random>

namespace CurrentMetrics
{
    extern const Metric BackgroundPoolTask;
    extern const Metric BackgroundMovePoolTask;
}

namespace DB
{

IBackgroundJobExecutor::IBackgroundJobExecutor(
        Context & global_context_,
        const BackgroundTaskSchedulingSettings & sleep_settings_,
        const std::vector<PoolConfig> & pools_configs_)
    : global_context(global_context_)
    , sleep_settings(sleep_settings_)
    , rng(randomSeed())
{
    for (const auto & pool_config : pools_configs_)
    {
        pools.try_emplace(pool_config.pool_type, pool_config.max_pool_size, 0, pool_config.max_pool_size, false);
        pools_configs.emplace(pool_config.pool_type, pool_config);
    }
}

double IBackgroundJobExecutor::getSleepRandomAdd()
{
    std::lock_guard random_lock(random_mutex);
    return std::uniform_real_distribution<double>(0, sleep_settings.task_sleep_seconds_when_no_work_random_part)(rng);
}

void IBackgroundJobExecutor::scheduleTask()
{
    auto no_work_done_times = no_work_done_count.load(std::memory_order_relaxed);
    /// If we have no jobs or some errors than sleep with backoff
    if (no_work_done_times != 0)
    {
        auto next_time_to_execute = 1000 * (std::min(
                sleep_settings.task_sleep_seconds_when_no_work_max,
                sleep_settings.task_sleep_seconds_when_no_work_min * std::pow(sleep_settings.task_sleep_seconds_when_no_work_multiplier, no_work_done_times))
            + getSleepRandomAdd());

         scheduling_task->scheduleAfter(next_time_to_execute);
    }
    else
    {
        /// We have background jobs, schedule task as soon as possible
        scheduling_task->schedule();
    }

}

namespace
{

/// Tricky function: we have separate thread pool with max_threads in each background executor for each table
/// But we want total background threads to be less than max_threads value. So we use global atomic counter (BackgroundMetric)
/// to limit total number of background threads.
bool incrementMetricIfLessThanMax(std::atomic<Int64> & atomic_value, Int64 max_value)
{
    auto value = atomic_value.load(std::memory_order_relaxed);
    while (value < max_value)
    {
        if (atomic_value.compare_exchange_weak(value, value + 1, std::memory_order_release, std::memory_order_relaxed))
            return true;
    }
    return false;
}

}

void IBackgroundJobExecutor::jobExecutingTask()
try
{
    auto job_and_pool = getBackgroundJob();
    if (job_and_pool) /// If we have job, than try to assign into background pool
    {
        auto & pool_config = pools_configs[job_and_pool->pool_type];
        /// If corresponding pool is not full increment metric and assign new job
        if (incrementMetricIfLessThanMax(CurrentMetrics::values[pool_config.tasks_metric], pool_config.max_pool_size))
        {
            try /// this try required because we have to manually decrement metric
            {
                pools[job_and_pool->pool_type].scheduleOrThrowOnError([this, pool_config, job{std::move(job_and_pool->job)}] ()
                {
                    try /// We don't want exceptions in background pool
                    {
                        job();
                        /// Job done, decrement metric and reset no_work counter
                        CurrentMetrics::values[pool_config.tasks_metric]--;
                        no_work_done_count = 0;
                    }
                    catch (...)
                    {
                        no_work_done_count++;
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                        CurrentMetrics::values[pool_config.tasks_metric]--;
                    }
                    /// Job done, new empty space in pool, schedule background task
                    scheduleTask();
                });
            }
            catch (...)
            {
                /// With our Pool settings scheduleOrThrowOnError shouldn't throw exceptions, but for safety add catch here
                no_work_done_count++;
                tryLogCurrentException(__PRETTY_FUNCTION__);
                CurrentMetrics::values[pool_config.tasks_metric]--;
                scheduleTask();
            }
        }
        else /// Pool is full and we have some work to do, let's try to schedule without backoff
        {
            scheduleTask();
        }
    }
    else /// Nothing to do, no jobs
    {
        no_work_done_count++;
        scheduleTask();
    }

}
catch (...) /// Exception while we looking for a task, reschedule
{
    no_work_done_count++;
    tryLogCurrentException(__PRETTY_FUNCTION__);
    scheduleTask();
}

void IBackgroundJobExecutor::start()
{
    std::lock_guard lock(scheduling_task_mutex);
    if (!scheduling_task)
    {
        scheduling_task = global_context.getSchedulePool().createTask(
            getBackgroundTaskName(), [this]{ jobExecutingTask(); });
    }

    scheduling_task->activateAndSchedule();
}

void IBackgroundJobExecutor::finish()
{
    std::lock_guard lock(scheduling_task_mutex);
    if (scheduling_task)
    {
        scheduling_task->deactivate();
        for (auto & [pool_type, pool] : pools)
            pool.wait();
    }
}

void IBackgroundJobExecutor::triggerTask()
{
    std::lock_guard lock(scheduling_task_mutex);
    if (scheduling_task)
        scheduling_task->schedule();
}

IBackgroundJobExecutor::~IBackgroundJobExecutor()
{
    finish();
}

BackgroundJobsExecutor::BackgroundJobsExecutor(
       MergeTreeData & data_,
       Context & global_context_)
    : IBackgroundJobExecutor(
        global_context_,
        global_context_.getBackgroundProcessingTaskSchedulingSettings(),
        {PoolConfig{PoolType::MERGE_MUTATE, global_context_.getSettingsRef().background_pool_size, CurrentMetrics::BackgroundPoolTask}})
    , data(data_)
{
}

String BackgroundJobsExecutor::getBackgroundTaskName() const
{
    return data.getStorageID().getFullTableName() + " (dataProcessingTask)";
}

std::optional<JobAndPool> BackgroundJobsExecutor::getBackgroundJob()
{
    return data.getDataProcessingJob();
}

BackgroundMovesExecutor::BackgroundMovesExecutor(
       MergeTreeData & data_,
       Context & global_context_)
    : IBackgroundJobExecutor(
        global_context_,
        global_context_.getBackgroundMoveTaskSchedulingSettings(),
      {PoolConfig{PoolType::MOVE, global_context_.getSettingsRef().background_move_pool_size, CurrentMetrics::BackgroundMovePoolTask}})
    , data(data_)
{
}

String BackgroundMovesExecutor::getBackgroundTaskName() const
{
    return data.getStorageID().getFullTableName() + " (dataMovingTask)";
}

std::optional<JobAndPool> BackgroundMovesExecutor::getBackgroundJob()
{
    return data.getDataMovingJob();
}

}
