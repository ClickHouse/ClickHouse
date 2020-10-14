#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/ThreadPool.h>
#include <Core/BackgroundSchedulePool.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <pcg_random.hpp>

namespace DB
{

class BackgroundJobsExecutor
{
private:
    MergeTreeData & data;
    Context & global_context;
    size_t max_pool_size;
    ThreadPool data_processing_pool;
    ThreadPool move_pool;
    std::atomic<size_t> errors_count{0};
    pcg64 rng;
    BackgroundProcessingPool::PoolSettings settings;

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
