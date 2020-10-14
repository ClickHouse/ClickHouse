#include <Storages/MergeTree/BackgroundJobsExecutor.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

BackgroundJobsExecutor::BackgroundJobsExecutor(
    MergeTreeData & data_,
    Context & global_context_)
    : data(data_)
    , global_context(global_context_)
    , data_processing_pool(global_context.getSettingsRef().background_pool_size, 0, 10000, false)
    , move_pool(global_context.getSettingsRef().background_move_pool_size, 0, 10000, false)
{
    data_processing_task = global_context.getSchedulePool().createTask(
        data.getStorageID().getFullTableName() + " (dataProcessingTask)", [this]{ dataProcessingTask(); });
}

void BackgroundJobsExecutor::dataMovingTask()
try
{
    auto job = data.getDataMovingJob();
    if (job)
        move_pool.scheduleOrThrowOnError(*job);

    data_moving_task->schedule();
}
catch(...)
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
}

void BackgroundJobsExecutor::dataProcessingTask()
try
{
    auto job = data.getDataProcessingJob();
    if (job)
        data_processing_pool.scheduleOrThrowOnError(*job);

    data_processing_task->schedule();
}
catch (...)
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
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
