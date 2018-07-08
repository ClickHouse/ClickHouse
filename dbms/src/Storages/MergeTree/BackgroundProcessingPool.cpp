#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTracker.h>
#include <Common/randomSeed.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Interpreters/DNSCacheUpdater.h>

#include <pcg_random.hpp>
#include <random>


namespace CurrentMetrics
{
    extern const Metric BackgroundPoolTask;
    extern const Metric MemoryTrackingInBackgroundProcessingPool;
}

namespace DB
{

static constexpr double thread_sleep_seconds = 10;
static constexpr double thread_sleep_seconds_random_part = 1.0;

/// For exponential backoff.
static constexpr double task_sleep_seconds_when_no_work_min = 10;
static constexpr double task_sleep_seconds_when_no_work_max = 600;
static constexpr double task_sleep_seconds_when_no_work_multiplier = 1.1;
static constexpr double task_sleep_seconds_when_no_work_random_part = 1.0;


void BackgroundProcessingPoolTaskInfo::wake()
{
    if (removed)
        return;

    Poco::Timestamp current_time;

    {
        std::unique_lock<std::mutex> lock(pool.tasks_mutex);

        auto next_time_to_execute = iterator->first;
        auto this_task_handle = iterator->second;

        /// If this task was done nothing at previous time and it has to sleep, then cancel sleep time.
        if (next_time_to_execute > current_time)
            next_time_to_execute = current_time;

        pool.tasks.erase(iterator);
        iterator = pool.tasks.emplace(next_time_to_execute, this_task_handle);
    }

    /// Note that if all threads are currently do some work, this call will not wakeup any thread.
    pool.wake_event.notify_one();
}


BackgroundProcessingPool::BackgroundProcessingPool(int size_) : size(size_)
{
    LOG_INFO(&Logger::get("BackgroundProcessingPool"), "Create BackgroundProcessingPool with " << size << " threads");

    threads.resize(size);
    for (auto & thread : threads)
        thread = std::thread([this] { threadFunction(); });
}


BackgroundProcessingPool::TaskHandle BackgroundProcessingPool::addTask(const Task & task)
{
    TaskHandle res = std::make_shared<TaskInfo>(*this, task);

    Poco::Timestamp current_time;

    {
        std::unique_lock<std::mutex> lock(tasks_mutex);
        res->iterator = tasks.emplace(current_time, res);
    }

    wake_event.notify_all();

    return res;
}

void BackgroundProcessingPool::removeTask(const TaskHandle & task)
{
    if (task->removed.exchange(true))
        return;

    /// Wait for all executions of this task.
    {
        std::unique_lock<std::shared_mutex> wlock(task->rwlock);
    }

    {
        std::unique_lock<std::mutex> lock(tasks_mutex);
        tasks.erase(task->iterator);
    }
}

BackgroundProcessingPool::~BackgroundProcessingPool()
{
    try
    {
        shutdown = true;
        wake_event.notify_all();
        for (std::thread & thread : threads)
            thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void BackgroundProcessingPool::threadFunction()
{
    setThreadName("BackgrProcPool");

    MemoryTracker memory_tracker;
    memory_tracker.setMetric(CurrentMetrics::MemoryTrackingInBackgroundProcessingPool);
    current_memory_tracker = &memory_tracker;

    pcg64 rng(randomSeed());
    std::this_thread::sleep_for(std::chrono::duration<double>(std::uniform_real_distribution<double>(0, thread_sleep_seconds_random_part)(rng)));

    while (!shutdown)
    {
        bool done_work = false;
        TaskHandle task;

        try
        {
            Poco::Timestamp min_time;

            {
                std::unique_lock<std::mutex> lock(tasks_mutex);

                if (!tasks.empty())
                {
                    for (const auto & time_handle : tasks)
                    {
                        if (!time_handle.second->removed)
                        {
                            min_time = time_handle.first;
                            task = time_handle.second;
                            break;
                        }
                    }
                }
            }

            if (shutdown)
                break;

            if (!task)
            {
                std::unique_lock<std::mutex> lock(tasks_mutex);
                wake_event.wait_for(lock,
                    std::chrono::duration<double>(thread_sleep_seconds
                        + std::uniform_real_distribution<double>(0, thread_sleep_seconds_random_part)(rng)));
                continue;
            }

            /// No tasks ready for execution.
            Poco::Timestamp current_time;
            if (min_time > current_time)
            {
                std::unique_lock<std::mutex> lock(tasks_mutex);
                wake_event.wait_for(lock, std::chrono::microseconds(
                    min_time - current_time + std::uniform_int_distribution<uint64_t>(0, thread_sleep_seconds_random_part * 1000000)(rng)));
            }

            std::shared_lock<std::shared_mutex> rlock(task->rwlock);

            if (task->removed)
                continue;

            {
                CurrentMetrics::Increment metric_increment{CurrentMetrics::BackgroundPoolTask};
                done_work = task->function();
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            DNSCacheUpdater::incrementNetworkErrorEventsIfNeeded();
        }

        if (shutdown)
            break;

        {
            std::unique_lock<std::mutex> lock(tasks_mutex);

            if (task->removed)
                continue;

            if (done_work)
                task->count_no_work_done = 0;
            else
                ++task->count_no_work_done;

            /// If task has done work, it could be executed again immediately.
            /// If not, add delay before next run.

            Poco::Timestamp next_time_to_execute;   /// current time
            if (!done_work)
                next_time_to_execute += 1000000 * (std::min(
                        task_sleep_seconds_when_no_work_max,
                        task_sleep_seconds_when_no_work_min * std::pow(task_sleep_seconds_when_no_work_multiplier, task->count_no_work_done))
                    + std::uniform_real_distribution<double>(0, task_sleep_seconds_when_no_work_random_part)(rng));

            tasks.erase(task->iterator);
            task->iterator = tasks.emplace(next_time_to_execute, task);
        }
    }

    current_memory_tracker = nullptr;
}

}
