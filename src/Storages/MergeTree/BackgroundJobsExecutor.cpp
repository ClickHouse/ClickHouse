#include <Storages/MergeTree/BackgroundJobsExecutor.h>

namespace DB
{

BackgroundJobsExecutor::BackgroundJobsExecutor(
    MergeTreeData & data_,
    Context & global_context)
    : data(data_)
    , data_processing_pool(global_context.getSettingsRef().background_pool_size, 0, 10000, false)
    , move_pool(global_context.getSettingsRef().background_move_pool_size, 0, 10000, false)
{
    data_processing_task = global_context.getSchedulePool().createTask(
        data.getStorageID().getFullTableName() + " (dataProcessingTask)", [this]{ dataProcessingTask(); });
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

void BackgroundJobsExecutor::start()
{
    if (data_processing_task)
        data_processing_task->activateAndSchedule();
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
}

}
