#pragma once

#include <thread>
#include <set>
#include <map>
#include <list>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <functional>
#include <Poco/Event.h>
#include <Poco/Timestamp.h>
#include <Core/Types.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace CurrentMetrics
{
    extern const Metric BackgroundPoolTask;
    extern const Metric MemoryTrackingInBackgroundProcessingPool;
}

namespace DB
{

class BackgroundProcessingPool;
class BackgroundProcessingPoolTaskInfo;

enum class BackgroundProcessingPoolTaskResult
{
    SUCCESS,
    ERROR,
    NOTHING_TO_DO,
};


/** Using a fixed number of threads, perform an arbitrary number of tasks in an infinite loop.
  * In this case, one task can run simultaneously from different threads.
  * Designed for tasks that perform continuous background work (for example, merge).
  * `Task` is a function that returns a bool - did it do any work.
  * If not, then the next time will be done later.
  */
class BackgroundProcessingPool
{
public:
    /// Returns true, if some useful work was done. In that case, thread will not sleep before next run of this task.
    using TaskResult = BackgroundProcessingPoolTaskResult;
    using Task = std::function<TaskResult()>;
    using TaskInfo = BackgroundProcessingPoolTaskInfo;
    using TaskHandle = std::shared_ptr<TaskInfo>;


    struct PoolSettings
    {
        double thread_sleep_seconds = 10;
        double thread_sleep_seconds_random_part = 1.0;
        double thread_sleep_seconds_if_nothing_to_do = 0.1;

        /// For exponential backoff.
        double task_sleep_seconds_when_no_work_min = 10;
        double task_sleep_seconds_when_no_work_max = 600;
        double task_sleep_seconds_when_no_work_multiplier = 1.1;
        double task_sleep_seconds_when_no_work_random_part = 1.0;

        CurrentMetrics::Metric tasks_metric = CurrentMetrics::BackgroundPoolTask;
        CurrentMetrics::Metric memory_metric = CurrentMetrics::MemoryTrackingInBackgroundProcessingPool;

        PoolSettings() noexcept {}
    };

    BackgroundProcessingPool(int size_,
        const PoolSettings & pool_settings = {},
        const char * log_name = "BackgroundProcessingPool",
        const char * thread_name_ = "BackgrProcPool");

    size_t getNumberOfThreads() const
    {
        return size;
    }

    /// Create task and start it.
    TaskHandle addTask(const Task & task);

    /// Create task but not start it.
    TaskHandle createTask(const Task & task);
    /// Start the task that was created but not started. Precondition: task was not started.
    void startTask(const TaskHandle & task);

    void removeTask(const TaskHandle & task);

    ~BackgroundProcessingPool();

protected:
    friend class BackgroundProcessingPoolTaskInfo;

    using Tasks = std::multimap<Poco::Timestamp, TaskHandle>;    /// key is desired next time to execute (priority).
    using Threads = std::vector<ThreadFromGlobalPool>;

    const size_t size;
    const char * thread_name;
    Poco::Logger * logger;

    Tasks tasks;         /// Ordered in priority.
    std::mutex tasks_mutex;

    Threads threads;

    std::atomic<bool> shutdown {false};
    std::condition_variable wake_event;

    /// Thread group used for profiling purposes
    ThreadGroupStatusPtr thread_group;

    void threadFunction();

private:
    PoolSettings settings;
};


class BackgroundProcessingPoolTaskInfo
{
public:
    /// Wake up any thread.
    void wake();

    BackgroundProcessingPoolTaskInfo(BackgroundProcessingPool & pool_, const BackgroundProcessingPool::Task & function_)
        : pool(pool_), function(function_) {}

protected:
    friend class BackgroundProcessingPool;

    BackgroundProcessingPool & pool;
    BackgroundProcessingPool::Task function;

    /// Read lock is hold when task is executed.
    std::shared_mutex rwlock;
    std::atomic<bool> removed {false};

    std::multimap<Poco::Timestamp, std::shared_ptr<BackgroundProcessingPoolTaskInfo>>::iterator iterator;

    /// For exponential backoff.
    size_t count_no_work_done = 0;
};

}
