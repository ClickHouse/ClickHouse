#pragma once

#include <deque>
#include <functional>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include <Common/ThreadPool.h>
#include <Storages/MergeTree/BackgroundTask.h>


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



class MergeTreeBackgroundExecutor
{
public:

    using CountGetter = std::function<size_t()>;
    using GlobalMetricGetter = std::function<std::atomic<Int64> &()>;

    MergeTreeBackgroundExecutor(
        CountGetter threads_count_getter_,
        CountGetter max_task_count_getter_,
        GlobalMetricGetter current_tasks_count_getter_)
        : threads_count_getter(threads_count_getter_)
        , max_task_count_getter(max_task_count_getter_)
        , current_tasks_count_getter(current_tasks_count_getter_)
    {
        updatePoolConfiguration();
        scheduler = ThreadFromGlobalPool([this]() { schedulerThreadFunction(); });
    }

    ~MergeTreeBackgroundExecutor()
    {
        wait();
    }

    bool trySchedule(BackgroundTaskPtr task)
    {
        if (!incrementMetricIfLessThanMax(current_tasks_count_getter(), max_task_count_getter()))
            return false;

        std::lock_guard lock(mutex);

        if (shutdown)
            return false;

        tasks.emplace_back(task);
        ++scheduled_tasks_count;
        has_tasks.notify_one();
        return true;
    }


    void wait()
    {
        {
            std::lock_guard lock(mutex);
            shutdown = true;
            has_tasks.notify_all();
        }

        pool.wait();

        if (scheduler.joinable())
            scheduler.join();


        if (last_exception)
            std::rethrow_exception(last_exception);
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

        auto & current_metric = current_tasks_count_getter();
        --current_metric;
    }

    void schedulerThreadFunction()
    {
        while (true)
        {
            BackgroundTaskPtr current;
            {
                std::unique_lock lock(mutex);
                has_tasks.wait(lock, [this](){ return !tasks.empty() || shutdown; });

                if (shutdown)
                    break;

                current = std::move(tasks.front());
                tasks.pop_front();

                /// This is needed to increase / decrease the number of threads at runtime
                updatePoolConfiguration();
            }

            try
            {
                pool.scheduleOrThrowOnError([this, task = std::move(current)] ()
                {
                    try
                    {
                        if (task->execute())
                        {
                            std::lock_guard guard(mutex);
                            tasks.emplace_back(task);
                            has_tasks.notify_one();
                            return;
                        }

                        // current.reset();
                        decrementTasksCount();
                    }
                    catch(...)
                    {
                        decrementTasksCount();
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                    }
                });
            }
            catch (...)
            {
                std::lock_guard lock(mutex);
                tryLogCurrentException(__PRETTY_FUNCTION__);
                last_exception = std::current_exception();
            }
        }
    }

    CountGetter threads_count_getter;
    CountGetter max_task_count_getter;
    GlobalMetricGetter current_tasks_count_getter;

    using TasksQueue = std::deque<BackgroundTaskPtr>;
    TasksQueue tasks;

    std::mutex mutex;
    std::condition_variable has_tasks;

    std::atomic_size_t scheduled_tasks_count{0};
    std::atomic_bool shutdown{false};

    std::exception_ptr last_exception{nullptr};

    ThreadPool pool;
    ThreadFromGlobalPool scheduler;
};



}
