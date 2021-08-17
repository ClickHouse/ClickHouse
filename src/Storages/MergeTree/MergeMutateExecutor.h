#pragma once

#include <deque>
#include <functional>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include <common/shared_ptr_helper.h>
#include <Common/ThreadPool.h>
#include <Storages/MergeTree/BackgroundTask.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{


/// Tricky function: we have separate thread pool with max_threads in each background executor for each table
/// But we want total background threads to be less than max_threads value. So we use global atomic counter (BackgroundMetric)
/// to limit total number of background threads.
inline bool incrementMetricIfLessThanMax(std::atomic<Int64> & atomic_value, Int64 max_value)
{
    auto value = atomic_value.load(std::memory_order_relaxed);
    while (value < max_value)
    {
        if (atomic_value.compare_exchange_weak(value, value + 1, std::memory_order_release, std::memory_order_relaxed))
            return true;
    }
    return false;
}


class LambdaAdapter : public shared_ptr_helper<LambdaAdapter>, public BackgroundTask
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

    bool trySchedule(BackgroundTaskPtr task)
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

    void removeTasksCorrespondingToStorage(StorageID id)
    {
        /// Stop scheduler thread and pool
        auto lock = getUniqueLock();
        /// Get lock to the tasks
        std::lock_guard second_lock(mutex);

        auto erased = std::erase_if(tasks, [id = std::move(id)] (auto task) -> bool { return task->getStorageID() == id; });
        (void) erased;
    }


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

        // if (last_exception)
        //     std::rethrow_exception(last_exception);
    }

private:

    using ExecutorSuspender = std::unique_lock<MergeTreeBackgroundExecutor>;
    friend class std::unique_lock<MergeTreeBackgroundExecutor>;

    ExecutorSuspender getUniqueLock()
    {
        return ExecutorSuspender(*this);
    }

    std::mutex lock_mutex;

    void lock()
    {
        lock_mutex.lock();
        suspend();
    }

    void unlock()
    {
        resume();
        lock_mutex.unlock();
    }

    void suspend()
    {
        {
            std::unique_lock lock(mutex);
            shutdown_suspend = true;
            has_tasks.notify_one();
        }
        scheduler.join();
        pool.wait();
    }


    void resume()
    {
        shutdown_suspend = false;
        scheduler = ThreadFromGlobalPool([this]() { schedulerThreadFunction(); });
    }


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

    using TasksQueue = std::deque<BackgroundTaskPtr>;
    TasksQueue tasks;

    std::mutex mutex;
    std::condition_variable has_tasks;

    std::atomic_size_t scheduled_tasks_count{0};
    std::atomic_bool shutdown_suspend{false};

    std::exception_ptr last_exception{nullptr};

    ThreadPool pool;
    ThreadFromGlobalPool scheduler;
};

}
