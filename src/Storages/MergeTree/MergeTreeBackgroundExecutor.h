#pragma once

#include <deque>
#include <functional>
#include <mutex>
#include <future>
#include <condition_variable>
#include <set>
#include <iostream>

#include <boost/circular_buffer.hpp>
#include <boost/noncopyable.hpp>

#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/Stopwatch.h>
#include <base/defines.h>
#include <Storages/MergeTree/IExecutableTask.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct TaskRuntimeData;
using TaskRuntimeDataPtr = std::shared_ptr<TaskRuntimeData>;

/**
 * Has RAII class to determine how many tasks are waiting for the execution and executing at the moment.
 * Also has some flags and primitives to wait for current task to be executed.
 */
struct TaskRuntimeData
{
    TaskRuntimeData(ExecutableTaskPtr && task_, CurrentMetrics::Metric metric_)
        : task(std::move(task_))
        , metric(metric_)
    {
        /// Increment and decrement a metric with sequentially consistent memory order
        /// This is needed, because in unit test this metric is read from another thread
        /// and some invariant is checked. With relaxed memory order we could read stale value
        /// for this metric, that's why test can be failed.
        CurrentMetrics::values[metric].fetch_add(1);
    }

    ~TaskRuntimeData()
    {
        CurrentMetrics::values[metric].fetch_sub(1);
    }

    ExecutableTaskPtr task;
    CurrentMetrics::Metric metric;
    /// Guarded by MergeTreeBackgroundExecutor<>::mutex
    bool is_currently_deleting{false};
    /// Actually autoreset=false is needed only for unit test
    /// where multiple threads could remove tasks corresponding to the same storage
    /// This scenario in not possible in reality.
    Poco::Event is_done{/*autoreset=*/false};
    /// This is equal to task->getPriority() not to do useless virtual calls in comparator
    UInt64 priority{0};

    /// By default priority queue will have max element at top
    static bool comparePtrByPriority(const TaskRuntimeDataPtr & lhs, const TaskRuntimeDataPtr & rhs)
    {
        return lhs->priority > rhs->priority;
    }
};


class OrdinaryRuntimeQueue
{
public:
    TaskRuntimeDataPtr pop()
    {
        auto result = std::move(queue.front());
        queue.pop_front();
        return result;
    }

    void push(TaskRuntimeDataPtr item) { queue.push_back(std::move(item));}

    void remove(StorageID id)
    {
        auto it = std::remove_if(queue.begin(), queue.end(),
            [&] (auto item) -> bool { return item->task->getStorageID() == id; });
        queue.erase(it, queue.end());
    }

    void setCapacity(size_t count) { queue.set_capacity(count); }
    bool empty() { return queue.empty(); }

private:
    boost::circular_buffer<TaskRuntimeDataPtr> queue{0};
};

/// Uses a heap to pop a task with minimal priority
class MergeMutateRuntimeQueue
{
public:
    TaskRuntimeDataPtr pop()
    {
        std::pop_heap(buffer.begin(), buffer.end(), TaskRuntimeData::comparePtrByPriority);
        auto result = std::move(buffer.back());
        buffer.pop_back();
        return result;
    }

    void push(TaskRuntimeDataPtr item)
    {
        item->priority = item->task->getPriority();
        buffer.push_back(std::move(item));
        std::push_heap(buffer.begin(), buffer.end(), TaskRuntimeData::comparePtrByPriority);
    }

    void remove(StorageID id)
    {
        auto it = std::remove_if(buffer.begin(), buffer.end(),
            [&] (auto item) -> bool { return item->task->getStorageID() == id; });
        buffer.erase(it, buffer.end());

        std::make_heap(buffer.begin(), buffer.end(), TaskRuntimeData::comparePtrByPriority);
    }

    void setCapacity(size_t count) { buffer.reserve(count); }
    bool empty() { return buffer.empty(); }

private:
    std::vector<TaskRuntimeDataPtr> buffer{};
};

/**
 *  Executor for a background MergeTree related operations such as merges, mutations, fetches and so on.
 *  It can execute only successors of ExecutableTask interface.
 *  Which is a self-written coroutine. It suspends, when returns true from executeStep() method.
 *
 *  There are two queues of a tasks: pending (main queue for all the tasks) and active (currently executing).
 *  Pending queue is needed since the number of tasks will be more than thread to execute.
 *  Pending tasks are tasks that successfully scheduled to an executor or tasks that have some extra steps to execute.
 *  There is an invariant, that task may occur only in one of these queue. It can occur in both queues only in critical sections.
 *
 *  Pending:                                              Active:
 *
 *  |s| |s| |s| |s| |s| |s| |s| |s| |s| |s|               |s|
 *  |s| |s| |s| |s| |s| |s| |s| |s| |s|                   |s|
 *  |s| |s|     |s|     |s| |s|     |s|                   |s|
 *      |s|             |s| |s|                           |s|
 *      |s|                 |s|
 *                          |s|
 *
 *  Each task is simply a sequence of steps. Heavier tasks have longer sequences.
 *  When a step of a task is executed, we move tasks to pending queue. And take another from the queue's head.
 *  With these architecture all small merges / mutations will be executed faster, than bigger ones.
 *
 *  We use boost::circular_buffer as a container for queues not to do any allocations.
 *
 *  Another nuisance that we faces with is than background operations always interact with an associated Storage.
 *  So, when a Storage want to shutdown, it must wait until all its background operations are finished.
 */
template <class Queue>
class MergeTreeBackgroundExecutor final : boost::noncopyable
{
public:
    MergeTreeBackgroundExecutor(
        String name_,
        size_t threads_count_,
        size_t max_tasks_count_,
        CurrentMetrics::Metric metric_)
        : name(name_)
        , threads_count(threads_count_)
        , max_tasks_count(max_tasks_count_)
        , metric(metric_)
    {
        if (max_tasks_count == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Task count for MergeTreeBackgroundExecutor must not be zero");

        pending.setCapacity(max_tasks_count);
        active.set_capacity(max_tasks_count);

        pool.setMaxThreads(std::max(1UL, threads_count));
        pool.setMaxFreeThreads(std::max(1UL, threads_count));
        pool.setQueueSize(std::max(1UL, threads_count));

        for (size_t number = 0; number < threads_count; ++number)
            pool.scheduleOrThrowOnError([this] { threadFunction(); });
    }

    ~MergeTreeBackgroundExecutor()
    {
        wait();
    }

    /// Handler for hot-reloading
    /// Supports only increasing the number of threads and tasks, because
    /// implementing tasks eviction will definitely be too error-prone and buggy.
    void increaseThreadsAndMaxTasksCount(size_t new_threads_count, size_t new_max_tasks_count);
    size_t getMaxTasksCount() const;

    bool trySchedule(ExecutableTaskPtr task);
    void removeTasksCorrespondingToStorage(StorageID id);
    void wait();

private:
    String name;
    size_t threads_count TSA_GUARDED_BY(mutex) = 0;
    size_t max_tasks_count TSA_GUARDED_BY(mutex) = 0;
    CurrentMetrics::Metric metric;

    void routine(TaskRuntimeDataPtr item);

    /// libc++ does not provide TSA support for std::unique_lock -> TSA_NO_THREAD_SAFETY_ANALYSIS
    void threadFunction() TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Initially it will be empty
    Queue pending TSA_GUARDED_BY(mutex);
    boost::circular_buffer<TaskRuntimeDataPtr> active TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
    std::condition_variable has_tasks TSA_GUARDED_BY(mutex);
    bool shutdown TSA_GUARDED_BY(mutex) = false;
    ThreadPool pool;
    Poco::Logger * log = &Poco::Logger::get("MergeTreeBackgroundExecutor");
};

extern template class MergeTreeBackgroundExecutor<MergeMutateRuntimeQueue>;
extern template class MergeTreeBackgroundExecutor<OrdinaryRuntimeQueue>;

using MergeMutateBackgroundExecutor = MergeTreeBackgroundExecutor<MergeMutateRuntimeQueue>;
using OrdinaryBackgroundExecutor = MergeTreeBackgroundExecutor<OrdinaryRuntimeQueue>;

}
