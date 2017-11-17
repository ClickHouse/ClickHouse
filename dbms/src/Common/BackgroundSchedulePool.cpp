#include <Common/BackgroundSchedulePool.h>
#include <Common/MemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
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
    Guard g(lock);

    if (removed || scheduled)
        return false;

    scheduled = true;

    if (iterator != pool.delayed_tasks.end())
        pool.cancelDelayedTask(shared_from_this());

    pool.queue.enqueueNotification(new TaskNotification(shared_from_this()));
    return true;
}

bool BackgroundSchedulePool::TaskInfo::scheduleAfter(size_t ms)
{
    Guard g(lock);

    if (removed || scheduled)
        return false;

    pool.scheduleDelayedTask(shared_from_this(), ms);
    return true;
}

bool BackgroundSchedulePool::TaskInfo::pause(bool value)
{
    Guard g(lock);

    if (removed == value)
        return false;

    if (value)
        invalidate();
    else
        removed = false;

    return true;
}

void BackgroundSchedulePool::TaskInfo::invalidate()
{
    if (removed)
        return;

    Guard g(lock);
    removed = true;
    scheduled = false;

    if (iterator != pool.delayed_tasks.end())
        pool.cancelDelayedTask(shared_from_this());
}

void BackgroundSchedulePool::TaskInfo::execute()
{
    Guard g(lock);

    if (removed)
        return;

    scheduled = false;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::BackgroundSchedulePoolTask};

    auto start = std::chrono::steady_clock::now();

    function();

    auto diff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();

    /// If the task is executed longer than specified time, it will be logged.
    static const int32_t slow_execution_threshold_ms = 50;

    if (diff_ms >= slow_execution_threshold_ms)
        LOG_INFO(&Logger::get("BackgroundSchedulePool"), "Executing " << name << " took: " << diff_ms << " ms");
}

// BackgroundSchedulePool

BackgroundSchedulePool::BackgroundSchedulePool(size_t size) :
    size(size)
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
        wake_event.notify_all();
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
        std::lock_guard<std::mutex> lock(delayed_tasks_lock);

        if (task->iterator != delayed_tasks.end())
            delayed_tasks.erase(task->iterator);

        task->iterator = delayed_tasks.emplace(current_time + (ms * 1000), task);
    }

    wake_event.notify_all();
}

void BackgroundSchedulePool::cancelDelayedTask(const TaskHandle & task)
{
    {
        std::lock_guard<std::mutex> lock(delayed_tasks_lock);
        delayed_tasks.erase(task->iterator);
        task->iterator = delayed_tasks.end();
    }

    wake_event.notify_all();
}

void BackgroundSchedulePool::threadFunction()
{
    setThreadName("BackgroundSchedulePool");

    MemoryTracker memory_tracker;
    memory_tracker.setMetric(CurrentMetrics::MemoryTrackingInBackgroundSchedulePool);
    current_memory_tracker = &memory_tracker;

    while (!shutdown)
    {
        Poco::AutoPtr<Poco::Notification> notification(queue.waitDequeueNotification());

        if (notification)
        {
            TaskNotification * pn = dynamic_cast<TaskNotification*>(notification.get());
            pn->execute();
        }
    }

    current_memory_tracker = nullptr;
}


void BackgroundSchedulePool::delayExecutionThreadFunction()
{
    setThreadName("BackgroundSchedulePoolDelay");

    while (!shutdown)
    {
        Poco::Timestamp min_time;
        TaskHandle task;

        {
            std::lock_guard<std::mutex> lock(delayed_tasks_lock);

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
            std::unique_lock<std::mutex> lock(delayed_tasks_lock);
            wake_event.wait(lock);
            continue;
        }

        Poco::Timestamp current_time;
        if (min_time > current_time)
        {
            std::unique_lock<std::mutex> lock(delayed_tasks_lock);
            wake_event.wait_for(lock, std::chrono::microseconds(min_time - current_time));
        }
        else
        {
            task->schedule();
        }
    }
}

}
