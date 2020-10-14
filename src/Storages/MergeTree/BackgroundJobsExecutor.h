#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/ThreadPool.h>
#include <Core/BackgroundSchedulePool.h>
#include <pcg_random.hpp>

namespace CurrentMetrics
{
    extern const Metric BackgroundPoolTask;
}

namespace DB
{
enum PoolType
{
    MERGE_MUTATING,
    MOVING,

}

struct PoolSettings
{
    double thread_sleep_seconds = 10;
    double thread_sleep_seconds_random_part = 1.0;
    double thread_sleep_seconds_if_nothing_to_do = 0.1;

    /// For exponential backoff.
    double task_sleep_seconds_when_no_work_min = 10;
    double task_sleep_seconds_when_no_work_max = 600;
    double task_sleep_seconds_when_no_work_multiplier = 1.1;
    double task_sleep_seconds_when_no_work_random_part = 1.0;

    CurrentMetrics::Metric tasks_metric = CurrentMetrics::BackgroundPoolTask;

    PoolSettings() noexcept {}
};

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
    PoolSettings settings;

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
