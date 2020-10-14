#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/ThreadPool.h>
#include <Storages/MergeTree/MergeTreeBackgroundJob.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{

class BackgroundJobsExecutor
{
private:
    MergeTreeData & data;
    Context & global_context;
    ThreadPool data_processing_pool;
    ThreadPool move_pool;

    BackgroundSchedulePool::TaskHolder data_processing_task;
    BackgroundSchedulePool::TaskHolder data_moving_task;

    void dataProcessingTask();
    void dataMovingTask();

public:
    BackgroundJobsExecutor(
        MergeTreeData & data_,
        Context & global_context_);

    void startMovingTaskIfNeeded();
    void triggerDataProcessing();
    void triggerMovesProcessing();
    void start();
    void finish();
};
   
}
