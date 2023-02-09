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
    extern const int INVALID_CONFIG_PARAMETER;
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

/// Simplest First-in-First-out queue, ignores priority.
class FifoRuntimeQueue
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
            [&] (auto && item) -> bool { return item->task->getStorageID() == id; });
        queue.erase(it, queue.end());
    }

    void setCapacity(size_t count) { queue.set_capacity(count); }
    bool empty() { return queue.empty(); }

private:
    boost::circular_buffer<TaskRuntimeDataPtr> queue{0};
};

/// Uses a heap to pop a task with minimal priority.
class PriorityRuntimeQueue
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
        std::erase_if(buffer, [&] (auto && item) -> bool { return item->task->getStorageID() == id; });
        std::make_heap(buffer.begin(), buffer.end(), TaskRuntimeData::comparePtrByPriority);
    }

    void setCapacity(size_t count) { buffer.reserve(count); }
    bool empty() { return buffer.empty(); }

private:
    std::vector<TaskRuntimeDataPtr> buffer;
};

/// Round-robin queue, ignores priority.
class RoundRobinRuntimeQueue
{
public:
    TaskRuntimeDataPtr pop()
    {
        assert(buffer.size() != unused);
        while (buffer[current] == nullptr)
            current = (current + 1) % buffer.size();
        auto result = std::move(buffer[current]);
        buffer[current] = nullptr; // mark as unused
        unused++;
        if (buffer.size() == unused)
        {
            buffer.clear();
            unused = current = 0;
        }
        else
            current = (current + 1) % buffer.size();
        return result;
    }

    // Inserts element just before round-robin pointer.
    // This way inserted element will be pop()-ed last. It guarantees fairness to avoid starvation.
    void push(TaskRuntimeDataPtr item)
    {
        if (unused == 0)
        {
            buffer.insert(buffer.begin() + current, std::move(item));
            current = (current + 1) % buffer.size();
        }
        else // Optimization to avoid O(N) complexity -- reuse unused elements
        {
            assert(!buffer.empty());
            size_t pos = (current + buffer.size() - 1) % buffer.size();
            while (buffer[pos] != nullptr)
            {
                std::swap(item, buffer[pos]);
                pos = (pos + buffer.size() - 1) % buffer.size();
            }
            buffer[pos] = std::move(item);
            unused--;
        }
    }

    void remove(StorageID id)
    {
        // This is similar to `std::erase_if()`, but we also track movement of `current` element
        size_t saved = 0;
        size_t new_current = 0;
        for (size_t i = 0; i < buffer.size(); i++)
        {
            if (buffer[i] != nullptr && buffer[i]->task->getStorageID() != id) // erase unused and matching elements
            {
                if (i < current)
                    new_current++;
                if (i != saved)
                    buffer[saved] = std::move(buffer[i]);
                saved++;
            }
        }
        buffer.erase(buffer.begin() + saved, buffer.end());
        current = new_current;
        unused = 0;
    }

    void setCapacity(size_t count) { buffer.reserve(count); }
    bool empty() { return buffer.empty(); }

private:
    // Buffer can contain unused elements (nullptrs)
    // Unused elements are created by pop() and reused by push()
    std::vector<TaskRuntimeDataPtr> buffer;

    size_t current = 0; // round-robin pointer
    size_t unused = 0; // number of nullptr elements
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
 *  When a step of a task is executed, we move tasks to pending queue. And take the next task from pending queue.
 *  Next task is choosen from pending tasks using one of the scheduling policies (class Queue):
 *  1) FifoRuntimeQueue. The simplest First-in-First-out queue. Guaranties tasks are executed in order.
 *  2) PriorityRuntimeQueue. Uses heap to select task with smallest priority value.
 *     With this architecture all small merges / mutations will be executed faster, than bigger ones.
 *     WARNING: Starvation is possible in case of overload.
 *  3) RoundRobinRuntimeQueue. Next task is selected, using round-robin pointer, which iterates over all tasks in rounds.
 *     When task is added to pending queue, it is placed just before round-robin pointer
 *     to given every other task an opportunity to execute one step.
 *     With this architecture all merges / mutations are fairly scheduled and never starved.
 *     All decisions regarding priorities are left to components creating tasks (e.g. SimpleMergeSelector).
 *
 *  We use boost::circular_buffer as a container for active queue to avoid allocations.
 *  Another nuisance that we face is that background operations always interact with an associated Storage.
 *  So, when a Storage wants to shutdown, it must wait until all its background operations are finished.
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
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Task count for MergeTreeBackgroundExecutor must not be zero");

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

    /// This method can return stale value of max_tasks_count (no mutex locking).
    /// It's okay because amount of tasks can be only increased and getting stale value
    /// can lead only to some postponing, not logical error.
    size_t getMaxTasksCount() const;

    bool trySchedule(ExecutableTaskPtr task);
    void removeTasksCorrespondingToStorage(StorageID id);
    void wait();

private:
    String name;
    size_t threads_count TSA_GUARDED_BY(mutex) = 0;
    std::atomic<size_t> max_tasks_count = 0;
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

extern template class MergeTreeBackgroundExecutor<FifoRuntimeQueue>;
extern template class MergeTreeBackgroundExecutor<PriorityRuntimeQueue>;
extern template class MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue>;

}
