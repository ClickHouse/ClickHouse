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
    explicit TaskNotification(const BackgroundSchedulePool::TaskHandle & task) : task_(task) {}
    void execute() {task_->execute();}

private:
    BackgroundSchedulePool::TaskHandle task_;
};

// BackgroundSchedulePool::TaskInfo

BackgroundSchedulePool::TaskInfo::TaskInfo(BackgroundSchedulePool & pool, const std::string & name, const Task & function):
    name_(name),
    pool_(pool),
    function_(function),
    iterator_(pool.delayed_tasks_.end()) {}

bool BackgroundSchedulePool::TaskInfo::schedule()
{
    Guard g(lock_);

    if (removed_ || scheduled_)
        return false;

    scheduled_ = true;

    if (iterator_ != pool_.delayed_tasks_.end())
        pool_.cancelDelayedTask(shared_from_this());

    pool_.queue_.enqueueNotification(new TaskNotification(shared_from_this()));
    return true;
}

bool BackgroundSchedulePool::TaskInfo::scheduleAfter(size_t ms)
{
    Guard g(lock_);

    if (removed_ || scheduled_)
        return false;

    pool_.scheduleDelayedTask(shared_from_this(), ms);
    return true;
}

bool BackgroundSchedulePool::TaskInfo::pause(bool value)
{
    Guard g(lock_);

    if (removed_ == value)
        return false;

    if (value)
        invalidate();
    else
        removed_ = false;

    return true;
}

void BackgroundSchedulePool::TaskInfo::invalidate()
{
    if (removed_)
        return;

    Guard g(lock_);
    removed_ = true;
    scheduled_ = false;

    if (iterator_ != pool_.delayed_tasks_.end())
        pool_.cancelDelayedTask(shared_from_this());
}

void BackgroundSchedulePool::TaskInfo::execute()
{
    Guard g(lock_);

    if (removed_)
        return;

    scheduled_ = false;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::BackgroundSchedulePoolTask};

    auto start = std::chrono::steady_clock::now();

    function_();

    auto diff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();

    /// If the task is executed longer than specified time, it will be logged.
    static const int32_t slow_execution_threshold_ms = 50;

    if (diff_ms >= slow_execution_threshold_ms)
        LOG_INFO(&Logger::get("BackgroundSchedulePool"), "executing " << name_ << " tooked: "<< diff_ms << " ms");
}

// BackgroundSchedulePool

BackgroundSchedulePool::BackgroundSchedulePool(size_t size) :
    size_(size)
{
    LOG_INFO(&Logger::get("BackgroundSchedulePool"), "Create BackgroundSchedulePool with " << size << " threads");

    threads_.resize(size);
    for (auto & thread : threads_)
        thread = std::thread([this] { threadFunction(); });

    delayed_thread_ = std::thread([this] { delayExecutionThreadFunction(); });
}

BackgroundSchedulePool::~BackgroundSchedulePool()
{
    try
    {
        shutdown_ = true;
        wake_event_.notify_all();
        queue_.wakeUpAll();

        delayed_thread_.join();

        for (std::thread & thread : threads_)
            thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

BackgroundSchedulePool::TaskHandle BackgroundSchedulePool::addTask(const std::string& name, const Task & task)
{
    return std::make_shared<TaskInfo>(*this, name, task);
}

void BackgroundSchedulePool::removeTask(const TaskHandle & task)
{
    task->invalidate();
}

void BackgroundSchedulePool::scheduleDelayedTask(const TaskHandle& task, size_t ms)
{
    Poco::Timestamp current_time;

    {
        std::lock_guard<std::mutex> lock(delayed_tasks_lock_);

        if (task->iterator_ != delayed_tasks_.end())
            delayed_tasks_.erase(task->iterator_);

        task->iterator_ = delayed_tasks_.emplace(current_time+(ms*1000), task);
    }

    wake_event_.notify_all();
}

void BackgroundSchedulePool::cancelDelayedTask(const TaskHandle& task)
{
    {
        std::lock_guard<std::mutex> lock(delayed_tasks_lock_);
        delayed_tasks_.erase(task->iterator_);
        task->iterator_ = delayed_tasks_.end();
    }

    wake_event_.notify_all();
}

void BackgroundSchedulePool::threadFunction()
{
    setThreadName("BackgroundSchedulePool");

    MemoryTracker memory_tracker;
    memory_tracker.setMetric(CurrentMetrics::MemoryTrackingInBackgroundSchedulePool);
    current_memory_tracker = &memory_tracker;

    while (!shutdown_)
    {
        Poco::AutoPtr<Poco::Notification> notification(queue_.waitDequeueNotification());

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

    while (!shutdown_)
    {
        Poco::Timestamp min_time;
        TaskHandle task;

        {
            std::lock_guard<std::mutex> lock(delayed_tasks_lock_);

            if (!delayed_tasks_.empty())
            {
                auto t = delayed_tasks_.begin();
                min_time = t->first;
                task = t->second;
            }
        }

        if (shutdown_)
            break;

        if (!task)
        {
            std::unique_lock<std::mutex> lock(delayed_tasks_lock_);
            wake_event_.wait(lock);
            continue;
        }

        Poco::Timestamp current_time;
        if (min_time > current_time)
        {
            std::unique_lock<std::mutex> lock(delayed_tasks_lock_);
            wake_event_.wait_for(lock, std::chrono::microseconds(min_time - current_time));
        }
        else
        {
            task->schedule();
        }
    }
}

}
