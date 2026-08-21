#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/CurrentMetrics.h>
#include <Common/LockGuardWithStopWatch.h>
#include <Common/randomSeed.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <random>


namespace DB
{

BackgroundJobsAssignee::BackgroundJobsAssignee(IBackgroundOperation & data_, const StorageID & storage_id_, BackgroundJobsAssignee::Type type_, ContextPtr global_context_)
    : WithContext(global_context_)
    , type(type_)
    , data(data_)
    , storage_id(storage_id_)
    , rng(randomSeed())
    , sleep_settings(getSettings())
{
}

BackgroundTaskSchedulingSettings BackgroundJobsAssignee::getSettings() const
{
    switch (type)
    {
        case Type::DataProcessing:
            return getContext()->getBackgroundProcessingTaskSchedulingSettings();
        case Type::Moving:
            return getContext()->getBackgroundMoveTaskSchedulingSettings();
    }
}

void BackgroundJobsAssignee::trigger()
{
    std::lock_guard lock(holder_mutex);

    if (!holder)
        return;

    /// Do not reset backoff factor if some task has appeared,
    /// but decrease it exponentially on every new task.
    no_work_done_count /= 2;
    /// We have background jobs, schedule task as soon as possible
    holder->schedule();
}

void BackgroundJobsAssignee::postpone()
{
    std::lock_guard lock(holder_mutex);

    if (!holder)
        return;

    no_work_done_count += 1;
    double random_addition = std::uniform_real_distribution<double>(0, sleep_settings.task_sleep_seconds_when_no_work_random_part)(rng);

    size_t next_time_to_execute = static_cast<size_t>(
        1000 * (std::min(
            sleep_settings.task_sleep_seconds_when_no_work_max,
            sleep_settings.thread_sleep_seconds_if_nothing_to_do * std::pow(sleep_settings.task_sleep_seconds_when_no_work_multiplier, no_work_done_count))
        + random_addition));

    holder->scheduleAfter(next_time_to_execute, false);
}


bool BackgroundJobsAssignee::scheduleMergeMutateTask(ExecutableTaskPtr merge_task)
{
    bool res = getContext()->getMergeMutateExecutor()->trySchedule(merge_task);
    res ? trigger() : postpone();
    return res;
}


bool BackgroundJobsAssignee::scheduleFetchTask(ExecutableTaskPtr fetch_task)
{
    bool res = getContext()->getFetchesExecutor()->trySchedule(fetch_task);
    res ? trigger() : postpone();
    return res;
}


bool BackgroundJobsAssignee::scheduleMoveTask(ExecutableTaskPtr move_task)
{
    bool res = getContext()->getMovesExecutor()->trySchedule(move_task);
    res ? trigger() : postpone();
    return res;
}


bool BackgroundJobsAssignee::scheduleCommonTask(ExecutableTaskPtr common_task, bool need_trigger)
{
    bool schedule_res = getContext()->getCommonExecutor()->trySchedule(common_task);
    schedule_res && need_trigger ? trigger() : postpone();
    return schedule_res;
}


String BackgroundJobsAssignee::toString(Type type)
{
    switch (type)
    {
        case Type::DataProcessing:
            return "DataProcessing";
        case Type::Moving:
            return "Moving";
    }
}

void BackgroundJobsAssignee::start()
{
    std::lock_guard lock(holder_mutex);
    if (!holder)
        holder = getContext()->getSchedulePool().createTask(storage_id, "BackgroundJobsAssignee:" + toString(type), [this]{ threadFunc(); });

    holder->activateAndSchedule();
}

void BackgroundJobsAssignee::updateStorageID(const StorageID & new_id)
{
    storage_id = new_id;
}

void BackgroundJobsAssignee::finish()
{
    /// No lock here, because scheduled tasks could call trigger method
    if (holder)
    {
        holder->deactivate();

        getContext()->getMovesExecutor()->removeTasksCorrespondingToStorage(storage_id);
        getContext()->getFetchesExecutor()->removeTasksCorrespondingToStorage(storage_id);
        getContext()->getMergeMutateExecutor()->removeTasksCorrespondingToStorage(storage_id);
        getContext()->getCommonExecutor()->removeTasksCorrespondingToStorage(storage_id);
    }
}


void BackgroundJobsAssignee::threadFunc()
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

BackgroundJobsAssignee::~BackgroundJobsAssignee()
{
    try
    {
        finish();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
