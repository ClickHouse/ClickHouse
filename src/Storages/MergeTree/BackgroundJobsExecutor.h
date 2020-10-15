#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/ThreadPool.h>
#include <Core/BackgroundSchedulePool.h>
#include <pcg_random.hpp>

namespace CurrentMetrics
{
    extern const Metric BackgroundPoolTask;
    extern const Metric BackgroundMovePoolTask;
}

namespace DB
{

struct TaskSleepSettings
{
    double thread_sleep_seconds = 10;
    double thread_sleep_seconds_random_part = 1.0;
    double thread_sleep_seconds_if_nothing_to_do = 0.1;

    /// For exponential backoff.
    double task_sleep_seconds_when_no_work_min = 10;
    double task_sleep_seconds_when_no_work_max = 600;
    double task_sleep_seconds_when_no_work_multiplier = 1.1;

    double task_sleep_seconds_when_no_work_random_part = 1.0;
};

enum PoolType
{
    MERGE_MUTATE,
    FETCH,
    MOVE,
    LOW_PRIORITY,
};

struct PoolConfig
{
    PoolType pool_type;
    size_t max_pool_size;
    CurrentMetrics::Metric tasks_metric;
};

struct JobAndPool
{
    ThreadPool::Job job;
    PoolType pool_type;
};

class IBackgroundJobExecutor
{
    Context & global_context;
private:
    String task_name;
    TaskSleepSettings sleep_settings;
    pcg64 rng;

    std::atomic<size_t> errors_count{0};

    std::unordered_map<PoolType, ThreadPool> pools;
    std::unordered_map<PoolType, PoolConfig> pools_configs;

    BackgroundSchedulePool::TaskHolder scheduling_task;

public:
    void start();
    void triggerTask();
    void finish();

    virtual ~IBackgroundJobExecutor();

protected:
    IBackgroundJobExecutor(
        Context & global_context_,
        const TaskSleepSettings & sleep_settings_,
        const std::vector<PoolConfig> & pools_configs_);

    virtual String getBackgroundJobName() const = 0;
    virtual std::optional<JobAndPool> getBackgroundJob() = 0;

private:
    void jobExecutingTask();
    void scheduleTask(bool nothing_to_do);
};

class BackgroundJobsExecutor final : public IBackgroundJobExecutor
{
private:
    MergeTreeData & data;
public:
    BackgroundJobsExecutor(
        MergeTreeData & data_,
        Context & global_context_);

protected:
    String getBackgroundJobName() const override;
    std::optional<JobAndPool> getBackgroundJob() override;
};

class BackgroundMovesExecutor final : public IBackgroundJobExecutor
{
private:
    MergeTreeData & data;
public:
    BackgroundMovesExecutor(
        MergeTreeData & data_,
        Context & global_context_);

protected:
    String getBackgroundJobName() const override;
    std::optional<JobAndPool> getBackgroundJob() override;
};

}
