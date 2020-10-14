#include <Storages/MergeTree/BackgroundJobsExecutor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <pcg_random.hpp>
#include <random>

namespace CurrentMetrics
{
    extern const Metric BackgroundPoolTask;
}

namespace DB
{

BackgroundJobsExecutor::BackgroundJobsExecutor(
    MergeTreeData & data_,
    Context & global_context_)
    : data(data_)
    , global_context(global_context_)
    , max_pool_size(global_context.getSettingsRef().background_pool_size)
    , data_processing_pool(max_pool_size, 0, max_pool_size, false)
    , move_pool(global_context.getSettingsRef().background_move_pool_size, 0, max_pool_size, false)
    , rng(randomSeed())
{
    data_processing_task = global_context.getSchedulePool().createTask(
        data.getStorageID().getFullTableName() + " (dataProcessingTask)", [this]{ dataProcessingTask(); });
        const auto & config = global_context.getConfigRef();
        settings.thread_sleep_seconds = config.getDouble("background_processing_pool_thread_sleep_seconds", 10);
        settings.thread_sleep_seconds_random_part = config.getDouble("background_processing_pool_thread_sleep_seconds_random_part", 1.0);
        settings.thread_sleep_seconds_if_nothing_to_do = config.getDouble("background_processing_pool_thread_sleep_seconds_if_nothing_to_do", 0.1);
        settings.task_sleep_seconds_when_no_work_min = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_min", 10);
        settings.task_sleep_seconds_when_no_work_max = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_max", 600);
        settings.task_sleep_seconds_when_no_work_multiplier = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_multiplier", 1.1);
        settings.task_sleep_seconds_when_no_work_random_part = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_random_part", 1.0);
}

void BackgroundJobsExecutor::dataMovingTask()
try
{
    auto job = data.getDataMovingJob();
    if (job)
        move_pool.scheduleOrThrowOnError(job);

    data_moving_task->schedule();
}
catch(...)
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
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

void BackgroundJobsExecutor::dataProcessingTask()
{
    if (incrementIfLess(CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask], max_pool_size))
    {
        try
        {
            auto job = data.getDataProcessingJob();
            if (job)
            {
                data_processing_pool.scheduleOrThrowOnError([this, job{std::move(job)}] ()
                {
                    try
                    {
                        job();
                        CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask]--;
                        errors_count = 0;
                    }
                    catch (...)
                    {
                        errors_count++;
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                        CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask]--;
                    }
                });
                auto errors = errors_count.load(std::memory_order_relaxed);
                if (errors != 0)
                {
                    auto next_time_to_execute = 1000 * (std::min(
                            settings.task_sleep_seconds_when_no_work_max,
                            settings.task_sleep_seconds_when_no_work_min * std::pow(settings.task_sleep_seconds_when_no_work_multiplier, errors))
                        + std::uniform_real_distribution<double>(0, settings.task_sleep_seconds_when_no_work_random_part)(rng));
                    data_processing_task->scheduleAfter(next_time_to_execute);
                }
                else
                    data_processing_task->scheduleAfter(1000 * (settings.thread_sleep_seconds_if_nothing_to_do + std::uniform_real_distribution<double>(0, settings.thread_sleep_seconds_random_part)(rng)));
            }
            else
            {
                data_processing_task->scheduleAfter(1000 * (settings.thread_sleep_seconds_if_nothing_to_do + std::uniform_real_distribution<double>(0, settings.task_sleep_seconds_when_no_work_random_part)(rng)));
                CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask]--;
            }
        }
        catch(...)
        {
            CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask]--;
            data_processing_task->scheduleAfter(1000 * (settings.thread_sleep_seconds_if_nothing_to_do + std::uniform_real_distribution<double>(0, settings.task_sleep_seconds_when_no_work_random_part)(rng)));
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
    else
    {
        /// Pool overloaded
        data_processing_task->scheduleAfter(1000 * (settings.thread_sleep_seconds_if_nothing_to_do + std::uniform_real_distribution<double>(0, settings.task_sleep_seconds_when_no_work_random_part)(rng)));
    }
}


void BackgroundJobsExecutor::startMovingTaskIfNeeded()
{
    if (data.areBackgroundMovesNeeded() && !data_moving_task)
    {
        data_moving_task = global_context.getSchedulePool().createTask(
            data.getStorageID().getFullTableName() + " (dataMovingTask)", [this]{ dataMovingTask(); });
        data_moving_task->activateAndSchedule();
    }
}

void BackgroundJobsExecutor::start()
{
    if (data_processing_task)
        data_processing_task->activateAndSchedule();
    startMovingTaskIfNeeded();
}

void BackgroundJobsExecutor::triggerDataProcessing()
{
    if (data_processing_task)
        data_processing_task->schedule();
}

void BackgroundJobsExecutor::finish()
{
    data_processing_task->deactivate();
    data_processing_pool.wait();
    if (data_moving_task)
    {
        data_moving_task->deactivate();
        move_pool.wait();
    }
}

}
