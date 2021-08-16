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
    extern const Metric BackgroundFetchesPoolTask;
}

namespace DB
{



namespace ExecutorBuilder
{

static MergeTreeBackgroundExecutor buildMergeMutateExecutor(ContextPtr context, BackgroundJobExecutor * parent)
{
    return MergeTreeBackgroundExecutor(
        [context] () { return context->getSettingsRef().background_pool_size; },
        [context] () { return context->getSettingsRef().background_pool_size; },
        [] () -> std::atomic<CurrentMetrics::Value> & { return CurrentMetrics::values[CurrentMetrics::BackgroundPoolTask]; },
        [parent] () { parent->triggerTaskWithDelay(); },
        [parent] () { parent->triggerTaskWithDelay(); }
    );
}

static MergeTreeBackgroundExecutor buildFetchExecutor(ContextPtr context, BackgroundJobExecutor * parent)
{
    return MergeTreeBackgroundExecutor(
        [context] () { return context->getSettingsRef().background_fetches_pool_size; },
        [context] () { return context->getSettingsRef().background_fetches_pool_size; },
        [] () -> std::atomic<CurrentMetrics::Value> & { return CurrentMetrics::values[CurrentMetrics::BackgroundFetchesPoolTask]; },
        [parent] () { parent->triggerTaskWithDelay(); },
        [parent] () { parent->triggerTaskWithDelay(); }
    );
}


static MergeTreeBackgroundExecutor buildMovesExecutor(ContextPtr context, BackgroundJobExecutor * parent)
{
    return MergeTreeBackgroundExecutor(
        [context] () { return context->getSettingsRef().background_move_pool_size; },
        [context] () { return context->getSettingsRef().background_move_pool_size; },
        [] () -> std::atomic<CurrentMetrics::Value> & { return CurrentMetrics::values[CurrentMetrics::BackgroundMovePoolTask]; },
        [parent] () { parent->triggerTaskWithDelay(); },
        [parent] () { parent->triggerTaskWithDelay(); }
    );
}

}


BackgroundJobExecutor::BackgroundJobExecutor(MergeTreeData & data_, ContextPtr global_context_)
    : WithContext(global_context_)
    , data(data_)
    , sleep_settings(global_context_->getBackgroundMoveTaskSchedulingSettings())
    , rng(randomSeed())
    , merge_mutate_executor(ExecutorBuilder::buildMergeMutateExecutor(global_context_, this))
    , fetch_executor(ExecutorBuilder::buildFetchExecutor(global_context_, this))
    , moves_executor(ExecutorBuilder::buildMovesExecutor(global_context_, this))
{

}

double BackgroundJobExecutor::getSleepRandomAdd()
{
    std::lock_guard random_lock(random_mutex);
    return std::uniform_real_distribution<double>(0, sleep_settings.task_sleep_seconds_when_no_work_random_part)(rng);
}

void BackgroundJobExecutor::runTaskWithoutDelay()
{
    no_work_done_count = 0;
    /// We have background jobs, schedule task as soon as possible
    scheduling_task->schedule();
}

void BackgroundJobExecutor::scheduleTask(bool with_backoff)
{
    size_t next_time_to_execute;
    if (with_backoff)
    {
        auto no_work_done_times = no_work_done_count.fetch_add(1, std::memory_order_relaxed);

        next_time_to_execute = 1000 * (std::min(
                sleep_settings.task_sleep_seconds_when_no_work_max,
                sleep_settings.thread_sleep_seconds_if_nothing_to_do * std::pow(sleep_settings.task_sleep_seconds_when_no_work_multiplier, no_work_done_times))
            + getSleepRandomAdd());
    }
    else
    {
        no_work_done_count = 0;
        next_time_to_execute = 1000 * sleep_settings.thread_sleep_seconds_if_nothing_to_do;
    }

    scheduling_task->scheduleAfter(next_time_to_execute, false);
}

void BackgroundJobExecutor::executeMergeMutateTask(BackgroundTaskPtr merge_task)
{
    merge_mutate_executor.trySchedule(merge_task);
}


void BackgroundJobExecutor::executeFetchTask(BackgroundTaskPtr fetch_task)
{
    fetch_executor.trySchedule(fetch_task);
}


void BackgroundJobExecutor::executeMoveTask(BackgroundTaskPtr move_task)
{
    fetch_executor.trySchedule(move_task);
}

void BackgroundJobExecutor::start()
{
    std::lock_guard lock(scheduling_task_mutex);
    if (!scheduling_task)
    {
        scheduling_task = getContext()->getSchedulePool().createTask(
            "BackgroundJobExecutor", [this]{ backgroundTaskFunction(); });
    }

    scheduling_task->activateAndSchedule();
}

void BackgroundJobExecutor::finish()
{
    // FIXME ?
    // std::lock_guard lock(scheduling_task_mutex);
    if (scheduling_task)
    {
        scheduling_task->deactivate();
        // for (auto & [pool_type, pool] : pools)
        //     pool.wait();

        merge_mutate_executor.wait();
        fetch_executor.wait();
        moves_executor.wait();
    }
}

void BackgroundJobExecutor::triggerTask()
{
    std::lock_guard lock(scheduling_task_mutex);
    if (scheduling_task)
    {
        runTaskWithoutDelay();
    }

}

void BackgroundJobExecutor::triggerTaskWithDelay()
{
    std::lock_guard lock(scheduling_task_mutex);
    if (scheduling_task)
        scheduleTask(/* with_backoff = */ true);
}

void BackgroundJobExecutor::backgroundTaskFunction()
try
{
    bool result = selectTaskAndExecute();
    scheduleTask(/* with_backoff = */ !result);
}
catch (...) /// Catch any exception to avoid thread termination.
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
    scheduleTask(/* with_backoff = */ true);
}

BackgroundJobExecutor::~BackgroundJobExecutor()
{
    finish();
}


bool BackgroundJobExecutor::selectTaskAndExecute()
{
    if (counter % 2 == 0)
        return data.scheduleDataProcessingJob(*this);
    else
        return data.scheduleDataMovingJob(*this);
}


}
