#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Common/LockGuardWithStopWatch.h>
#include <Common/randomSeed.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <random>


namespace DB
{

namespace ErrorCodes
{
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char mt_background_jobs_assignee_throw_after_task_created[];
}

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
        case Type::Streaming:
            return getContext()->getBackgroundStreamingTaskSchedulingSettings();
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
        1000 * (data.getBiasBackoffSeconds() + std::min(
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
        case Type::Streaming:
            return "Streaming";
    }
}

bool BackgroundJobsAssignee::createHolderIfNeeded(const StorageID & current_storage_id)
{
    if (holder)
        return false;

    switch (type)
    {
    case Type::DataProcessing:
    case Type::Moving:
        holder = getContext()->getSchedulePool()->createTask(current_storage_id, "BackgroundJobsAssignee:" + toString(type), [this]{ threadFunc(); });
        break;
    case Type::Streaming:
        holder = getContext()->getStreamingSchedulePool()->createTask(current_storage_id, "BackgroundJobsAssignee:" + toString(type), [this]{ threadFunc(); });
        break;
    }

    return true;
}

bool BackgroundJobsAssignee::start()
{
    /// Either the task is created and activated, or the assignee is left exactly as it was: a holder
    /// created by this call is destroyed again if activating it throws, so that a caller which rolls
    /// back on the exception does not have to know whether the failure came before or after the
    /// allocation. Declared before the lock so that it is destroyed after the lock is released, for
    /// the same reason `finish` releases `holder_mutex` before `deactivate`: the lock order with the
    /// task's own mutexes. Destroying the holder deactivates the task, which waits for a run of
    /// `threadFunc` that may already have started; that run does not touch the storage because the
    /// workers are disabled while a `table_readonly` toggle is in flight.
    /// Read the cached id before taking holder_mutex so that the two locks are never nested.
    const auto current_storage_id = getStorageID();

    BackgroundSchedulePoolTaskHolder failed_holder;
    bool created = false;
    {
        std::lock_guard lock(holder_mutex);
        created = createHolderIfNeeded(current_storage_id);
        try
        {
            holder->activateAndSchedule();

            /// Models a scheduling failure after the task was allocated and is already live in the pool.
            fiu_do_on(FailPoints::mt_background_jobs_assignee_throw_after_task_created,
            {
                throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure while activating a background jobs assignee task");
            });
        }
        catch (...)
        {
            if (created)
                failed_holder = std::move(holder);
            throw;
        }
    }
    return created;
}

void BackgroundJobsAssignee::updateStorageID(const StorageID & new_id)
{
    std::lock_guard lock(storage_id_mutex);
    storage_id = new_id;
}

StorageID BackgroundJobsAssignee::getStorageID() const
{
    std::lock_guard lock(storage_id_mutex);
    return storage_id;
}

void BackgroundJobsAssignee::finish()
{
    /// Move the holder to a local variable under the lock, then release the lock
    /// before calling deactivate(). We cannot hold holder_mutex during deactivate()
    /// because it waits for the background task (threadFunc) to finish, and threadFunc
    /// calls trigger()/postpone() which also lock holder_mutex — that would deadlock.
    BackgroundSchedulePoolTaskHolder local_holder;
    {
        std::lock_guard lock(holder_mutex);
        local_holder = std::move(holder);
    }

    if (local_holder)
    {
        local_holder->deactivate();

        const auto current_storage_id = getStorageID();
        getContext()->getMovesExecutor()->removeTasksCorrespondingToStorage(current_storage_id);
        getContext()->getFetchesExecutor()->removeTasksCorrespondingToStorage(current_storage_id);
        getContext()->getMergeMutateExecutor()->removeTasksCorrespondingToStorage(current_storage_id);
        getContext()->getCommonExecutor()->removeTasksCorrespondingToStorage(current_storage_id);
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
        case Type::Streaming:
            succeed = data.scheduleStreamingJob(*this);
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
