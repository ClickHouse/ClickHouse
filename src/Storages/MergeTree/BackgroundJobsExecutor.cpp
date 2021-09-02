#include <Storages/MergeTree/BackgroundJobsExecutor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <pcg_random.hpp>
#include <random>

namespace DB
{

BackgroundJobAssignee::BackgroundJobAssignee(MergeTreeData & data_, BackgroundJobAssignee::Type type_, ContextPtr global_context_)
    : WithContext(global_context_)
    , data(data_)
    , sleep_settings(global_context_->getBackgroundMoveTaskSchedulingSettings())
    , rng(randomSeed())
    , type(type_)
{
}

void BackgroundJobAssignee::trigger()
{
    std::lock_guard lock(holder_mutex);

    if (!holder)
        return;

    no_work_done_count = 0;
    /// We have background jobs, schedule task as soon as possible
    holder->schedule();
}

void BackgroundJobAssignee::postpone()
{
    std::lock_guard lock(holder_mutex);

    if (!holder)
        return;

    auto no_work_done_times = no_work_done_count.fetch_add(1, std::memory_order_relaxed);
    double random_addition = std::uniform_real_distribution<double>(0, sleep_settings.task_sleep_seconds_when_no_work_random_part)(rng);

    size_t next_time_to_execute = 1000 * (std::min(
            sleep_settings.task_sleep_seconds_when_no_work_max,
            sleep_settings.thread_sleep_seconds_if_nothing_to_do * std::pow(sleep_settings.task_sleep_seconds_when_no_work_multiplier, no_work_done_times))
        + random_addition);

    holder->scheduleAfter(next_time_to_execute, false);
}


void BackgroundJobAssignee::scheduleMergeMutateTask(ExecutableTaskPtr merge_task)
{
    bool res = getContext()->getMergeMutateExecutor()->trySchedule(merge_task);
    if (res)
        trigger();
    else
        postpone();
}


void BackgroundJobAssignee::scheduleFetchTask(ExecutableTaskPtr fetch_task)
{
    bool res = getContext()->getFetchesExecutor()->trySchedule(fetch_task);
    if (res)
        trigger();
    else
        postpone();
}


void BackgroundJobAssignee::scheduleMoveTask(ExecutableTaskPtr move_task)
{
    bool res = getContext()->getMovesExecutor()->trySchedule(move_task);
    if (res)
        trigger();
    else
        postpone();
}


String BackgroundJobAssignee::toString(Type type)
{
    switch (type)
    {
        case Type::DataProcessing:
            return "DataProcessing";
        case Type::Moving:
            return "Moving";
    }
}

void BackgroundJobAssignee::start()
{
    std::lock_guard lock(holder_mutex);
    if (!holder)
        holder = getContext()->getSchedulePool().createTask("BackgroundJobAssignee:" + toString(type), [this]{ main(); });

    holder->activateAndSchedule();
}

void BackgroundJobAssignee::finish()
{
    /// No lock here, because scheduled tasks could call trigger method
    if (holder)
    {
        holder->deactivate();

        auto storage_id = data.getStorageID();

        getContext()->getMovesExecutor()->removeTasksCorrespondingToStorage(storage_id);
        getContext()->getFetchesExecutor()->removeTasksCorrespondingToStorage(storage_id);
        getContext()->getMergeMutateExecutor()->removeTasksCorrespondingToStorage(storage_id);
    }
}


void BackgroundJobAssignee::main()
try
{
    bool succeed = false;
    switch (type)
    {
        case Type::DataProcessing:
            succeed = data.scheduleDataProcessingJob(*this);
            break;
        case Type::Moving:
            succeed = data.scheduleDataMovingJob(*this);
            break;
    }

    if (!succeed)
        postpone();
}
catch (...) /// Catch any exception to avoid thread termination.
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
    postpone();
}

BackgroundJobAssignee::~BackgroundJobAssignee()
{
    finish();
}

}
