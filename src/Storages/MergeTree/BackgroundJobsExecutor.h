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
    ThreadPool data_processing_pool;
    ThreadPool move_pool;

    BackgroundSchedulePool::TaskHolder data_processing_task;
    BackgroundSchedulePool::TaskHolder move_processing_task;

    void dataProcessingTask();

public:
    BackgroundJobsExecutor(
        MergeTreeData & data_,
        Context & global_context_);

    void triggerDataProcessing();
    void triggerMovesProcessing();
    void start();
    void finish();
};
   
}
