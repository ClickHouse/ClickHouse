#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/MemoryTracker.h>
#include <Common/randomSeed.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Common/CurrentThread.h>
#include <Interpreters/DNSCacheUpdater.h>

#include <ext/scope_guard.h>
#include <pcg_random.hpp>
#include <random>


namespace DB
{

void BackgroundProcessingPoolTaskInfo::wake()
{
    Poco::Timestamp current_time;

    {
        std::unique_lock lock(pool.tasks_mutex);

        /// This will ensure that iterator is valid. Must be done under the same mutex when the iterator is invalidated.
        if (removed)
            return;

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


BackgroundProcessingPool::BackgroundProcessingPool(int size_,
        const PoolSettings & pool_settings,
        const char * log_name,
        const char * thread_name_)
    : size(size_)
    , thread_name(thread_name_)
    , settings(pool_settings)
{
    logger = &Logger::get(log_name);
    LOG_INFO(logger, "Create " << log_name << " with " << size << " threads");

    threads.resize(size);
    for (auto & thread : threads)
        thread = ThreadFromGlobalPool([this] { threadFunction(); });
}


BackgroundProcessingPool::TaskHandle BackgroundProcessingPool::createTask(const Task & task)
{
    return std::make_shared<TaskInfo>(*this, task);
}

void BackgroundProcessingPool::startTask(const TaskHandle & task)
{
    Poco::Timestamp current_time;

    {
        std::unique_lock lock(tasks_mutex);
        task->iterator = tasks.emplace(current_time, task);
    }

    wake_event.notify_all();
}

BackgroundProcessingPool::TaskHandle BackgroundProcessingPool::addTask(const Task & task)
{
    TaskHandle res = createTask(task);
    startTask(res);
    return res;
}

void BackgroundProcessingPool::removeTask(const TaskHandle & task)
{
    if (task->removed.exchange(true))
        return;

    /// Wait for all executions of this task.
    {
        std::unique_lock wlock(task->rwlock);
    }

    {
        std::unique_lock lock(tasks_mutex);
        tasks.erase(task->iterator);
        /// Note that the task may be still accessible through TaskHandle (shared_ptr).
    }
}

BackgroundProcessingPool::~BackgroundProcessingPool()
{
    try
    {
        shutdown = true;
        wake_event.notify_all();
        for (auto & thread : threads)
            thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void BackgroundProcessingPool::threadFunction()
{
    setThreadName(thread_name);

    {
        std::lock_guard lock(tasks_mutex);

        if (thread_group)
        {
            /// Put all threads to one thread pool
            CurrentThread::attachTo(thread_group);
        }
        else
        {
            CurrentThread::initializeQuery();
            thread_group = CurrentThread::getGroup();
        }
    }

    SCOPE_EXIT({ CurrentThread::detachQueryIfNotDetached(); });
    if (auto * memory_tracker = CurrentThread::getMemoryTracker())
        memory_tracker->setMetric(settings.memory_metric);

    pcg64 rng(randomSeed());
    std::this_thread::sleep_for(std::chrono::duration<double>(std::uniform_real_distribution<double>(0, settings.thread_sleep_seconds_random_part)(rng)));

    while (!shutdown)
    {
        TaskResult task_result = TaskResult::ERROR;
        TaskHandle task;

        try
        {
            Poco::Timestamp min_time;

            {
                std::unique_lock lock(tasks_mutex);

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
                std::unique_lock lock(tasks_mutex);
                wake_event.wait_for(lock,
                    std::chrono::duration<double>(settings.thread_sleep_seconds
                        + std::uniform_real_distribution<double>(0, settings.thread_sleep_seconds_random_part)(rng)));
                continue;
            }

            /// No tasks ready for execution.
            Poco::Timestamp current_time;
            if (min_time > current_time)
            {
                std::unique_lock lock(tasks_mutex);
                wake_event.wait_for(lock, std::chrono::microseconds(
                    min_time - current_time + std::uniform_int_distribution<uint64_t>(0, settings.thread_sleep_seconds_random_part * 1000000)(rng)));
            }

            std::shared_lock rlock(task->rwlock);

            if (task->removed)
                continue;

            {
                CurrentMetrics::Increment metric_increment{settings.tasks_metric};
                task_result = task->function();
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        if (shutdown)
            break;

        {
            std::unique_lock lock(tasks_mutex);

            if (task->removed)
                continue;

            if (task_result == TaskResult::SUCCESS)
                task->count_no_work_done = 0;
            else
                ++task->count_no_work_done;

            /// If task has done work, it could be executed again immediately.
            /// If not, add delay before next run.

            Poco::Timestamp next_time_to_execute;   /// current time
            if (task_result == TaskResult::ERROR)
                next_time_to_execute += 1000000 * (std::min(
                        settings.task_sleep_seconds_when_no_work_max,
                        settings.task_sleep_seconds_when_no_work_min * std::pow(settings.task_sleep_seconds_when_no_work_multiplier, task->count_no_work_done))
                    + std::uniform_real_distribution<double>(0, settings.task_sleep_seconds_when_no_work_random_part)(rng));
            else if (task_result == TaskResult::NOTHING_TO_DO)
                next_time_to_execute += 1000000 * settings.thread_sleep_seconds_if_nothing_to_do;

            tasks.erase(task->iterator);
            task->iterator = tasks.emplace(next_time_to_execute, task);
        }
    }
}

}
