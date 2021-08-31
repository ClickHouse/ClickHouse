#pragma once

#include <deque>
#include <functional>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include <common/shared_ptr_helper.h>
#include <Common/ThreadPool.h>
#include <Storages/MergeTree/ExecutableTask.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class LambdaAdapter : public shared_ptr_helper<LambdaAdapter>, public ExecutableTask
{
public:

    template <typename T>
    explicit LambdaAdapter(T && inner_, MergeTreeData & data_) : inner(inner_), data(data_) {}

    bool execute() override
    {
        res = inner();
        return false;
    }

    void onCompleted() override
    {
        data.triggerBackgroundOperationTask(!res);
    }

    StorageID getStorageID() override
    {
        return data.getStorageID();
    }

private:
    bool res = false;
    std::function<bool()> inner;
    MergeTreeData & data;
};


class MergeTreeBackgroundExecutor : public shared_ptr_helper<MergeTreeBackgroundExecutor>
{
public:

    using CountGetter = std::function<size_t()>;
    using Callback = std::function<void()>;


    MergeTreeBackgroundExecutor()
    {
        scheduler = ThreadFromGlobalPool([this]() { schedulerThreadFunction(); });
    }

    ~MergeTreeBackgroundExecutor()
    {
        wait();
    }

    void setThreadsCount(CountGetter && getter)
    {
        threads_count_getter = getter;
    }

    void setTasksCount(CountGetter && getter)
    {
        max_task_count_getter = getter;
    }

    void setMetric(CurrentMetrics::Metric metric_)
    {
        metric = metric_;
    }

    bool trySchedule(ExecutableTaskPtr task)
    {
        std::lock_guard lock(mutex);

        if (shutdown_suspend)
            return false;

        auto & value = CurrentMetrics::values[metric];
        if (value.load() >= static_cast<int64_t>(max_task_count_getter()))
            return false;

        CurrentMetrics::add(metric);

        tasks.emplace_back(task);
        ++scheduled_tasks_count;
        has_tasks.notify_one();
        return true;
    }

    void removeTasksCorrespondingToStorage(StorageID id);

    void wait()
    {
        {
            std::lock_guard lock(mutex);
            shutdown_suspend = true;
            has_tasks.notify_all();
        }

        if (scheduler.joinable())
            scheduler.join();

        pool.wait();
    }

private:

    void updatePoolConfiguration()
    {
        const auto max_threads = threads_count_getter();
        pool.setMaxFreeThreads(0);
        pool.setMaxThreads(max_threads);
        pool.setQueueSize(max_threads);
    }

    void decrementTasksCount()
    {
        --scheduled_tasks_count;
        CurrentMetrics::sub(metric);
    }

    void schedulerThreadFunction();


    CountGetter threads_count_getter;
    CountGetter max_task_count_getter;
    CurrentMetrics::Metric metric;

    std::deque<ExecutableTaskPtr> tasks;

    std::mutex remove_mutex;

    std::mutex currently_executing_mutex;
    std::map<ExecutableTaskPtr, std::future<void>> currently_executing;

    std::mutex mutex;
    std::condition_variable has_tasks;

    std::atomic_size_t scheduled_tasks_count{0};
    std::atomic_bool shutdown_suspend{false};

    ThreadPool pool;
    ThreadFromGlobalPool scheduler;
};

}
