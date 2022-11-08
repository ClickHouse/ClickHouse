#include "BackgroundSchedulePool.h"
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>
#include <chrono>


namespace DB
{

BackgroundSchedulePoolTaskInfo::BackgroundSchedulePoolTaskInfo(
    BackgroundSchedulePool & pool_, const std::string & log_name_, const BackgroundSchedulePool::TaskFunc & function_)
    : pool(pool_), log_name(log_name_), function(function_)
{
}

bool BackgroundSchedulePoolTaskInfo::schedule()
{
    std::lock_guard lock(schedule_mutex);

    if (deactivated || scheduled)
        return false;

    scheduleImpl(lock);
    return true;
}

bool BackgroundSchedulePoolTaskInfo::scheduleAfter(size_t milliseconds, bool overwrite)
{
    std::lock_guard lock(schedule_mutex);

    if (deactivated || scheduled)
        return false;
    if (delayed && !overwrite)
        return false;

    pool.scheduleDelayedTask(shared_from_this(), milliseconds, lock);
    return true;
}

void BackgroundSchedulePoolTaskInfo::deactivate()
{
    std::lock_guard lock_exec(exec_mutex);
    std::lock_guard lock_schedule(schedule_mutex);

    if (deactivated)
        return;

    deactivated = true;
    scheduled = false;

    if (delayed)
        pool.cancelDelayedTask(shared_from_this(), lock_schedule);
}

void BackgroundSchedulePoolTaskInfo::activate()
{
    std::lock_guard lock(schedule_mutex);
    deactivated = false;
}

bool BackgroundSchedulePoolTaskInfo::activateAndSchedule()
{
    std::lock_guard lock(schedule_mutex);

    deactivated = false;
    if (scheduled)
        return false;

    scheduleImpl(lock);
    return true;
}

void BackgroundSchedulePoolTaskInfo::execute()
{
    Stopwatch watch;
    CurrentMetrics::Increment metric_increment{pool.tasks_metric};

    std::lock_guard lock_exec(exec_mutex);

    {
        std::lock_guard lock_schedule(schedule_mutex);

        if (deactivated)
            return;

        scheduled = false;
        executing = true;
    }

    function();
    UInt64 milliseconds = watch.elapsedMilliseconds();

    /// If the task is executed longer than specified time, it will be logged.
    static constexpr UInt64 slow_execution_threshold_ms = 200;

    if (milliseconds >= slow_execution_threshold_ms)
        LOG_TRACE(&Poco::Logger::get(log_name), "Execution took {} ms.", milliseconds);

    {
        std::lock_guard lock_schedule(schedule_mutex);

        executing = false;

        /// In case was scheduled while executing (including a scheduleAfter which expired) we schedule the task
        /// on the queue. We don't call the function again here because this way all tasks
        /// will have their chance to execute

        if (scheduled)
            pool.scheduleTask(shared_from_this());
    }
}

void BackgroundSchedulePoolTaskInfo::scheduleImpl(std::lock_guard<std::mutex> & schedule_mutex_lock)
{
    scheduled = true;

    if (delayed)
        pool.cancelDelayedTask(shared_from_this(), schedule_mutex_lock);

    /// If the task is not executing at the moment, enqueue it for immediate execution.
    /// But if it is currently executing, do nothing because it will be enqueued
    /// at the end of the execute() method.
    if (!executing)
        pool.scheduleTask(shared_from_this());
}

Coordination::WatchCallback BackgroundSchedulePoolTaskInfo::getWatchCallback()
{
     return [task = shared_from_this()](const Coordination::WatchResponse &)
     {
        task->schedule();
     };
}


BackgroundSchedulePool::BackgroundSchedulePool(size_t size_, CurrentMetrics::Metric tasks_metric_, const char *thread_name_)
    : tasks_metric(tasks_metric_)
    , thread_name(thread_name_)
{
    LOG_INFO(&Poco::Logger::get("BackgroundSchedulePool/" + thread_name), "Create BackgroundSchedulePool with {} threads", size_);

    threads.resize(size_);
    for (auto & thread : threads)
        thread = ThreadFromGlobalPool([this] { threadFunction(); });

    delayed_thread = ThreadFromGlobalPool([this] { delayExecutionThreadFunction(); });
}


void BackgroundSchedulePool::increaseThreadsCount(size_t new_threads_count)
{
    const size_t old_threads_count = threads.size();

    if (new_threads_count < old_threads_count)
    {
        LOG_WARNING(&Poco::Logger::get("BackgroundSchedulePool/" + thread_name),
            "Tried to increase the number of threads but the new threads count ({}) is not greater than old one ({})", new_threads_count, old_threads_count);
        return;
    }

    threads.resize(new_threads_count);
    for (size_t i = old_threads_count; i < new_threads_count; ++i)
        threads[i] = ThreadFromGlobalPool([this] { threadFunction(); });
}


BackgroundSchedulePool::~BackgroundSchedulePool()
{
    try
    {
        {
            std::lock_guard lock_tasks(tasks_mutex);
            std::lock_guard lock_delayed_tasks(delayed_tasks_mutex);

            shutdown = true;
        }

        tasks_cond_var.notify_all();
        delayed_tasks_cond_var.notify_all();

        LOG_TRACE(&Poco::Logger::get("BackgroundSchedulePool/" + thread_name), "Waiting for threads to finish.");
        delayed_thread.join();

        for (auto & thread : threads)
            thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


BackgroundSchedulePool::TaskHolder BackgroundSchedulePool::createTask(const std::string & name, const TaskFunc & function)
{
    return TaskHolder(std::make_shared<TaskInfo>(*this, name, function));
}

void BackgroundSchedulePool::scheduleTask(TaskInfoPtr task_info)
{
    {
        std::lock_guard tasks_lock(tasks_mutex);
        tasks.push_back(std::move(task_info));
    }

    tasks_cond_var.notify_one();
}

void BackgroundSchedulePool::scheduleDelayedTask(const TaskInfoPtr & task, size_t ms, std::lock_guard<std::mutex> & /* task_schedule_mutex_lock */)
{
    Poco::Timestamp current_time;

    {
        std::lock_guard lock(delayed_tasks_mutex);

        if (task->delayed)
            delayed_tasks.erase(task->iterator);

        task->iterator = delayed_tasks.emplace(current_time + (ms * 1000), task);
        task->delayed = true;
    }

    delayed_tasks_cond_var.notify_all();
}


void BackgroundSchedulePool::cancelDelayedTask(const TaskInfoPtr & task, std::lock_guard<std::mutex> & /* task_schedule_mutex_lock */)
{
    {
        std::lock_guard lock(delayed_tasks_mutex);
        delayed_tasks.erase(task->iterator);
        task->delayed = false;
    }

    delayed_tasks_cond_var.notify_all();
}


void BackgroundSchedulePool::attachToThreadGroup()
{
    std::lock_guard lock(delayed_tasks_mutex);

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


void BackgroundSchedulePool::threadFunction()
{
    setThreadName(thread_name.c_str());

    attachToThreadGroup();

    while (!shutdown)
    {
        TaskInfoPtr task;

        {
            std::unique_lock<std::mutex> tasks_lock(tasks_mutex);

            tasks_cond_var.wait(tasks_lock, [&]()
            {
                return shutdown || !tasks.empty();
            });

            if (!tasks.empty())
            {
                task = tasks.front();
                tasks.pop_front();
            }
        }

        if (task)
            task->execute();
    }
}


void BackgroundSchedulePool::delayExecutionThreadFunction()
{
    setThreadName((thread_name + "/D").c_str());

    attachToThreadGroup();

    while (!shutdown)
    {
        TaskInfoPtr task;
        bool found = false;

        {
            std::unique_lock lock(delayed_tasks_mutex);

            while (!shutdown)
            {
                Poco::Timestamp min_time;

                if (!delayed_tasks.empty())
                {
                    auto t = delayed_tasks.begin();
                    min_time = t->first;
                    task = t->second;
                }

                if (!task)
                {
                    delayed_tasks_cond_var.wait(lock);
                    continue;
                }

                Poco::Timestamp current_time;

                if (min_time > current_time)
                {
                    delayed_tasks_cond_var.wait_for(lock, std::chrono::microseconds(min_time - current_time));
                    continue;
                }
                else
                {
                    /// We have a task ready for execution
                    found = true;
                    break;
                }
            }
        }

        if (found)
            task->schedule();
    }
}

}
