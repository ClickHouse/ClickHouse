#include <Storages/MergeTree/BackgroundJobsExecutor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <pcg_random.hpp>
#include <random>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
        if(atomic_value.compare_exchange_weak(value, value + 1, std::memory_order_release, std::memory_order_relaxed))
            return true;
    return false;
}

}


void IBackgroundJobExecutor::scheduleTask(bool nothing_to_do)
{
    auto errors = errors_count.load(std::memory_order_relaxed);
    size_t next_time_to_execute = 0;
    if (errors != 0)
        next_time_to_execute += 1000 * (std::min(
                sleep_settings.task_sleep_seconds_when_no_work_max,
                sleep_settings.task_sleep_seconds_when_no_work_min * std::pow(sleep_settings.task_sleep_seconds_when_no_work_multiplier, errors))
            + std::uniform_real_distribution<double>(0, sleep_settings.task_sleep_seconds_when_no_work_random_part)(rng));
    else if (nothing_to_do)
        next_time_to_execute += 1000 * (sleep_settings.thread_sleep_seconds_if_nothing_to_do + std::uniform_real_distribution<double>(0, sleep_settings.task_sleep_seconds_when_no_work_random_part)(rng)); 
    else
        next_time_to_execute = 1000 * std::uniform_real_distribution<double>(0, sleep_settings.thread_sleep_seconds_random_part)(rng);

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
catch (...) /// Exception while we looking for task
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

void IBackgroundJobExecutor::triggerDataProcessing()
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
        TaskSleepSettings{},
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
        TaskSleepSettings{},
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

//BackgroundJobsExecutor::BackgroundJobsExecutor(
//    MergeTreeData & data_,
//    Context & global_context_)
//    : data(data_)
//    , global_context(global_context_)
//    , max_pool_size(global_context.getSettingsRef().background_pool_size)
//    , data_processing_pool(max_pool_size, 0, max_pool_size, false)
//    , move_pool(global_context.getSettingsRef().background_move_pool_size, 0, max_pool_size, false)
//    , rng(randomSeed())
//{
//    data_processing_task = global_context.getSchedulePool().createTask(
//        data.getStorageID().getFullTableName() + " (dataProcessingTask)", [this]{ dataProcessingTask(); });
//        const auto & config = global_context.getConfigRef();
//        settings.thread_sleep_seconds = config.getDouble("background_processing_pool_thread_sleep_seconds", 10);
//        settings.thread_sleep_seconds_random_part = config.getDouble("background_processing_pool_thread_sleep_seconds_random_part", 1.0);
//        settings.thread_sleep_seconds_if_nothing_to_do = config.getDouble("background_processing_pool_thread_sleep_seconds_if_nothing_to_do", 0.1);
//        settings.task_sleep_seconds_when_no_work_min = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_min", 10);
//        settings.task_sleep_seconds_when_no_work_max = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_max", 600);
//        settings.task_sleep_seconds_when_no_work_multiplier = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_multiplier", 1.1);
//        settings.task_sleep_seconds_when_no_work_random_part = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_random_part", 1.0);
//}
//
//void BackgroundJobsExecutor::dataMovingTask()
//try
//{
//    auto job = data.getDataMovingJob();
//    if (job)
//        move_pool.scheduleOrThrowOnError(job);
//
//    data_moving_task->schedule();
//}
//catch(...)
//{
//    tryLogCurrentException(__PRETTY_FUNCTION__);
//}
//
//
//
//void BackgroundJobsExecutor::dataProcessingTask()
//{
//    if (incrementIfLess(CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask], max_pool_size))
//    {
//        try
//        {
//            auto job = data.getDataProcessingJob();
//            if (job)
//            {
//                data_processing_pool.scheduleOrThrowOnError([this, job{std::move(job)}] ()
//                {
//                    try
//                    {
//                        job();
//                        CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask]--;
//                        errors_count = 0;
//                    }
//                    catch (...)
//                    {
//                        errors_count++;
//                        tryLogCurrentException(__PRETTY_FUNCTION__);
//                        CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask]--;
//                    }
//                });
//                auto errors = errors_count.load(std::memory_order_relaxed);
//                if (errors != 0)
//                {
//                    auto next_time_to_execute = 1000 * (std::min(
//                            settings.task_sleep_seconds_when_no_work_max,
//                            settings.task_sleep_seconds_when_no_work_min * std::pow(settings.task_sleep_seconds_when_no_work_multiplier, errors))
//                        + std::uniform_real_distribution<double>(0, settings.task_sleep_seconds_when_no_work_random_part)(rng));
//                    data_processing_task->scheduleAfter(next_time_to_execute);
//                }
//                else
//                    data_processing_task->scheduleAfter(1000 * (settings.thread_sleep_seconds_if_nothing_to_do + std::uniform_real_distribution<double>(0, settings.thread_sleep_seconds_random_part)(rng)));
//            }
//            else
//            {
//                data_processing_task->scheduleAfter(1000 * (settings.thread_sleep_seconds_if_nothing_to_do + std::uniform_real_distribution<double>(0, settings.task_sleep_seconds_when_no_work_random_part)(rng)));
//                CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask]--;
//            }
//        }
//        catch(...)
//        {
//            CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask]--;
//            data_processing_task->scheduleAfter(1000 * (settings.thread_sleep_seconds_if_nothing_to_do + std::uniform_real_distribution<double>(0, settings.task_sleep_seconds_when_no_work_random_part)(rng)));
//            tryLogCurrentException(__PRETTY_FUNCTION__);
//        }
//    }
//    else
//    {
//        /// Pool overloaded
//        data_processing_task->scheduleAfter(1000 * (settings.thread_sleep_seconds_if_nothing_to_do + std::uniform_real_distribution<double>(0, settings.task_sleep_seconds_when_no_work_random_part)(rng)));
//    }
//}
//
//
//void BackgroundJobsExecutor::startMovingTaskIfNeeded()
//{
//    if (data.areBackgroundMovesNeeded() && !data_moving_task)
//    {
//        data_moving_task = global_context.getSchedulePool().createTask(
//            data.getStorageID().getFullTableName() + " (dataMovingTask)", [this]{ dataMovingTask(); });
//        data_moving_task->activateAndSchedule();
//    }
//}
//
//void BackgroundJobsExecutor::start()
//{
//    if (data_processing_task)
//        data_processing_task->activateAndSchedule();
//    startMovingTaskIfNeeded();
//}
//
//void BackgroundJobsExecutor::triggerDataProcessing()
//{
//    if (data_processing_task)
//        data_processing_task->schedule();
//}
//
//void BackgroundJobsExecutor::finish()
//{
//    data_processing_task->deactivate();
//    data_processing_pool.wait();
//    if (data_moving_task)
//    {
//        data_moving_task->deactivate();
//        move_pool.wait();
//    }
//}

}
