#include <Storages/MergeTree/BackgroundJobsExecutor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <pcg_random.hpp>
#include <random>

namespace DB
{

IBackgroundJobExecutor::IBackgroundJobExecutor(
        MergeTreeData & data_,
        Context & global_context_,
        const String & task_name_,
        const TaskSleepSettings & sleep_settings_,
        const std::vector<PoolConfig> & pools_configs_)
    : data(data_)
    , global_context(global_context_)
    , task_name(task_name_)
    , sleep_settings(sleep_settings_)
    , rng(randomSeed())
{
    for (const auto & pool_config : pools_configs_)
    {
        pools.try_emplace(pool_config.pool_type, pool_config.max_pool_size, 0, pool_config.max_pool_size, false);
        pools_configs.emplace(pool_config.pool_type, pool_config);
    }
}

namespace
{

bool incrementIfLess(std::atomic<long> & atomic_value, long max_value)
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

void IBackgroundJobExecutor::scheduleTask(bool nothing_to_do)
{
    auto errors = errors_count.load(std::memory_order_relaxed);
    size_t next_time_to_execute = 0;
    if (errors != 0)
    {
        next_time_to_execute += 1000 * (std::min(
                sleep_settings.task_sleep_seconds_when_no_work_max,
                sleep_settings.task_sleep_seconds_when_no_work_min * std::pow(sleep_settings.task_sleep_seconds_when_no_work_multiplier, errors))
            + std::uniform_real_distribution<double>(0, sleep_settings.task_sleep_seconds_when_no_work_random_part)(rng));
    }
    else if (nothing_to_do)
    {
        next_time_to_execute += 1000 * (sleep_settings.thread_sleep_seconds_if_nothing_to_do
            + std::uniform_real_distribution<double>(0, sleep_settings.task_sleep_seconds_when_no_work_random_part)(rng));
    }
    else
    {
        scheduling_task->schedule();
        return;
    }

     scheduling_task->scheduleAfter(next_time_to_execute);
}

void IBackgroundJobExecutor::jobExecutingTask()
try
{
    auto job_and_pool = getBackgroundJob();
    if (job_and_pool)
    {
        auto & pool_config = pools_configs[job_and_pool->pool_type];
        /// If corresponding pool is not full, otherwise try next time
        if (incrementIfLess(CurrentMetrics::values[pool_config.tasks_metric], pool_config.max_pool_size))
        {
            try /// this try required because we have to manually decrement metric
            {
                pools[job_and_pool->pool_type].scheduleOrThrowOnError([this, pool_config, job{std::move(job_and_pool->job)}] ()
                {
                    try /// We don't want exceptions in background pool
                    {
                        job();
                        CurrentMetrics::values[pool_config.tasks_metric]--;
                        errors_count = 0;
                    }
                    catch (...)
                    {
                        errors_count++;
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                        CurrentMetrics::values[pool_config.tasks_metric]--;
                    }
                });
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                CurrentMetrics::values[pool_config.tasks_metric]--;
            }
        }
        scheduleTask(false);
    }
    else /// Nothing to do, no jobs
    {
        scheduleTask(true);
    }
}
catch (...) /// Exception while we looking for a task
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
    scheduleTask(true);
}

void IBackgroundJobExecutor::start()
{
    if (!scheduling_task)
    {
        scheduling_task = global_context.getSchedulePool().createTask(
            data.getStorageID().getFullTableName() + task_name, [this]{ jobExecutingTask(); });
    }

    scheduling_task->activateAndSchedule();
}

void IBackgroundJobExecutor::finish()
{
    if (scheduling_task)
    {
        scheduling_task->deactivate();
        for (auto & [pool_type, pool] : pools)
            pool.wait();
    }
}

void IBackgroundJobExecutor::triggerTask()
{
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
        data_,
        global_context_,
        "(dataProcessingTask)",
        global_context_.getBackgroundProcessingTaskSleepSettings(),
        {PoolConfig{PoolType::MERGE_MUTATE, global_context_.getSettingsRef().background_pool_size, CurrentMetrics::BackgroundPoolTask}})
{
}

std::optional<JobAndPool> BackgroundJobsExecutor::getBackgroundJob()
{
    auto job = data.getDataProcessingJob();
    if (job)
        return JobAndPool{job, PoolType::MERGE_MUTATE};
    return {};
}

BackgroundMovesExecutor::BackgroundMovesExecutor(
       MergeTreeData & data_,
       Context & global_context_)
    : IBackgroundJobExecutor(
        data_,
        global_context_,
        "(dataMovingTask)",
        global_context_.getBackgroundMoveTaskSleepSettings(),
        {PoolConfig{PoolType::MOVE, global_context_.getSettingsRef().background_move_pool_size, CurrentMetrics::BackgroundMovePoolTask}})
{
}

std::optional<JobAndPool> BackgroundMovesExecutor::getBackgroundJob()
{
    auto job = data.getDataMovingJob();
    if (job)
        return JobAndPool{job, PoolType::MOVE};
    return {};
}

}
