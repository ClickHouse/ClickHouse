#include <Common/BackgroundSchedulePool.h>
#include <Common/MemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <common/logger_useful.h>
#include <chrono>

namespace CurrentMetrics
{
    extern const Metric BackgroundSchedulePoolTask;
    extern const Metric MemoryTrackingInBackgroundSchedulePool;
}

namespace DB
{


// TaskNotification

class TaskNotification final : public Poco::Notification
{
public:
    explicit TaskNotification(const BackgroundSchedulePool::TaskHandle & task) : task(task) {}
    void execute() { task->execute(); }

private:
    BackgroundSchedulePool::TaskHandle task;
};


// BackgroundSchedulePool::TaskInfo

BackgroundSchedulePool::TaskInfo::TaskInfo(BackgroundSchedulePool & pool, const std::string & name, const Task & function):
    name(name),
    pool(pool),
    function(function),
    iterator(pool.delayed_tasks.end()) {}


bool BackgroundSchedulePool::TaskInfo::schedule()
{
    std::lock_guard lock(mutex);

    if (removed || scheduled)
        return false;

    scheduled = true;

    if (isScheduledWithDelay())
        pool.cancelDelayedTask(shared_from_this());

    pool.queue.enqueueNotification(new TaskNotification(shared_from_this()));
    return true;
}


bool BackgroundSchedulePool::TaskInfo::scheduleAfter(size_t ms)
{
    std::lock_guard lock(mutex);

    if (removed || scheduled)
        return false;

    pool.scheduleDelayedTask(shared_from_this(), ms);
    return true;
}


void BackgroundSchedulePool::TaskInfo::pause()
{
    invalidate();
}


void BackgroundSchedulePool::TaskInfo::resume()
{
    std::lock_guard lock(mutex);
    removed = false;
}


void BackgroundSchedulePool::TaskInfo::invalidate()
{
    if (removed)
        return;

    std::lock_guard lock(mutex);
    removed = true;
    scheduled = false;

    if (isScheduledWithDelay())
        pool.cancelDelayedTask(shared_from_this());
}


void BackgroundSchedulePool::TaskInfo::execute()
{
    std::lock_guard lock(mutex);

    if (removed)
        return;

    scheduled = false;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::BackgroundSchedulePoolTask};

    Stopwatch watch;
    function();
    UInt64 milliseconds = watch.elapsedMilliseconds();

    /// If the task is executed longer than specified time, it will be logged.
    static const int32_t slow_execution_threshold_ms = 50;

    if (milliseconds >= slow_execution_threshold_ms)
        LOG_INFO(&Logger::get("BackgroundSchedulePool"), "Executing " << name << " took " << milliseconds << " ms.");
}


// BackgroundSchedulePool

BackgroundSchedulePool::BackgroundSchedulePool(size_t size)
    : size(size)
{
    LOG_INFO(&Logger::get("BackgroundSchedulePool"), "Create BackgroundSchedulePool with " << size << " threads");

    threads.resize(size);
    for (auto & thread : threads)
        thread = std::thread([this] { threadFunction(); });

    delayed_thread = std::thread([this] { delayExecutionThreadFunction(); });
}


BackgroundSchedulePool::~BackgroundSchedulePool()
{
    try
    {
        shutdown = true;
        wakeup_event.notify_all();
        queue.wakeUpAll();

        delayed_thread.join();

        LOG_TRACE(&Logger::get("BackgroundSchedulePool"), "Waiting for threads to finish.");
        for (std::thread & thread : threads)
            thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


BackgroundSchedulePool::TaskHandle BackgroundSchedulePool::addTask(const std::string & name, const Task & task)
{
    return std::make_shared<TaskInfo>(*this, name, task);
}


void BackgroundSchedulePool::removeTask(const TaskHandle & task)
{
    task->invalidate();
}


void BackgroundSchedulePool::scheduleDelayedTask(const TaskHandle & task, size_t ms)
{
    Poco::Timestamp current_time;

    {
        std::lock_guard lock(delayed_tasks_lock);

        if (task->iterator != delayed_tasks.end())
            delayed_tasks.erase(task->iterator);

        task->iterator = delayed_tasks.emplace(current_time + (ms * 1000), task);
    }

    wakeup_event.notify_all();
}


void BackgroundSchedulePool::cancelDelayedTask(const TaskHandle & task)
{
    {
        std::lock_guard lock(delayed_tasks_lock);
        delayed_tasks.erase(task->iterator);
        task->iterator = delayed_tasks.end();
    }

    wakeup_event.notify_all();
}


void BackgroundSchedulePool::threadFunction()
{
    setThreadName("BackgrSchedPool");

    MemoryTracker memory_tracker;
    memory_tracker.setMetric(CurrentMetrics::MemoryTrackingInBackgroundSchedulePool);
    current_memory_tracker = &memory_tracker;

    while (!shutdown)
    {
        if (Poco::AutoPtr<Poco::Notification> notification = queue.waitDequeueNotification())
        {
            TaskNotification & task_notification = static_cast<TaskNotification &>(*notification);
            task_notification.execute();
        }
    }

    current_memory_tracker = nullptr;
}


void BackgroundSchedulePool::delayExecutionThreadFunction()
{
    setThreadName("BckSchPoolDelay");

    while (!shutdown)
    {
        Poco::Timestamp min_time;
        TaskHandle task;

        {
            std::lock_guard lock(delayed_tasks_lock);

            if (!delayed_tasks.empty())
            {
                auto t = delayed_tasks.begin();
                min_time = t->first;
                task = t->second;
            }
        }

        if (shutdown)
            break;

        if (!task)
        {
            std::unique_lock lock(delayed_tasks_lock);
            wakeup_event.wait(lock);
            continue;
        }

        Poco::Timestamp current_time;
        if (min_time > current_time)
        {
            std::unique_lock lock(delayed_tasks_lock);
            wakeup_event.wait_for(lock, std::chrono::microseconds(min_time - current_time));
        }
        else
        {
            task->schedule();
        }
    }
}

}
