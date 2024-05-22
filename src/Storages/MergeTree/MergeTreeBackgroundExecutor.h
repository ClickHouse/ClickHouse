#pragma once

#include <deque>
#include <functional>
#include <mutex>
#include <future>
#include <condition_variable>
#include <set>
#include <variant>
#include <utility>

#include <boost/circular_buffer.hpp>
#include <boost/noncopyable.hpp>
#include <Poco/Event.h>

#include <Storages/MergeTree/IExecutableTask.h>
#include <base/defines.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
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
    Priority priority;

    /// By default priority queue will have max element at top
    static bool comparePtrByPriority(const TaskRuntimeDataPtr & lhs, const TaskRuntimeDataPtr & rhs)
    {
        return lhs->priority > rhs->priority;
    }
};

/// Simplest First-in-First-out queue, ignores priority.
class RoundRobinRuntimeQueue
{
public:
    TaskRuntimeDataPtr pop()
    {
        auto result = std::move(queue.front());
        queue.pop_front();
        return result;
    }

    void push(TaskRuntimeDataPtr item)
    {
        queue.push_back(std::move(item));
    }

    void remove(StorageID id)
    {
        auto it = std::remove_if(queue.begin(), queue.end(),
            [&] (auto && item) -> bool { return item->task->getStorageID() == id; });
        queue.erase(it, queue.end());
    }

    void setCapacity(size_t count) { queue.set_capacity(count); }
    bool empty() { return queue.empty(); }

    [[noreturn]] void updatePolicy(std::string_view)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method updatePolicy() is not implemented");
    }

    static constexpr std::string_view name = "round_robin";

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

    [[noreturn]] void updatePolicy(std::string_view)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method updatePolicy() is not implemented");
    }

    static constexpr std::string_view name = "shortest_task_first";

private:
    std::vector<TaskRuntimeDataPtr> buffer;
};

/// Queue that can dynamically change scheduling policy
template <class ... Policies>
class DynamicRuntimeQueueImpl
{
public:
    TaskRuntimeDataPtr pop()
    {
        return std::visit<TaskRuntimeDataPtr>([&] (auto && queue) { return queue.pop(); }, impl);
    }

    void push(TaskRuntimeDataPtr item)
    {
        std::visit([&] (auto && queue) { queue.push(std::move(item)); }, impl);
    }

    void remove(StorageID id)
    {
        std::visit([&] (auto && queue) { queue.remove(id); }, impl);
    }

    void setCapacity(size_t count)
    {
        capacity = count;
        std::visit([&] (auto && queue) { queue.setCapacity(count); }, impl);
    }

    bool empty()
    {
        return std::visit<bool>([&] (auto && queue) { return queue.empty(); }, impl);
    }

    // Change policy. It does nothing if new policy is unknown or equals current policy.
    void updatePolicy(std::string_view name)
    {
        // We use this double lambda trick to generate code for all possible pairs of types of old and new queue.
        // If types are different it moves tasks from old queue to new one using corresponding pop() and push()
        resolve<Policies...>(name, [&] <class NewQueue> (std::in_place_type_t<NewQueue>)
        {
            std::visit([&] (auto && queue)
            {
                if constexpr (std::is_same_v<std::decay_t<decltype(queue)>, NewQueue>)
                    return; // The same policy
                NewQueue new_queue;
                new_queue.setCapacity(capacity);
                while (!queue.empty())
                    new_queue.push(queue.pop());
                impl = std::move(new_queue);
            }, impl);
        });
    }

private:
    // Find policy with specified `name` and call `func()` if found.
    // Tag `std::in_place_type_t<T>` used to help templated lambda to deduce type T w/o creating its instance
    template <class T, class ... Ts, class Func>
    void resolve(std::string_view name, Func && func)
    {
        if (T::name == name)
            return func(std::in_place_type<T>);
        if constexpr (sizeof...(Ts))
            return resolve<Ts...>(name, std::forward<Func>(func));
    }

    std::variant<Policies...> impl;
    size_t capacity;
};

// Avoid typedef and alias to facilitate forward declaration
class DynamicRuntimeQueue : public DynamicRuntimeQueueImpl<RoundRobinRuntimeQueue, PriorityRuntimeQueue> {};

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
 *  Next task is chosen from pending tasks using one of the scheduling policies (class Queue):
 *  1) RoundRobinRuntimeQueue. Uses boost::circular_buffer as FIFO-queue. Next task is taken from queue's head and after one step
 *     enqueued into queue's tail. With this architecture all merges / mutations are fairly scheduled and never starved.
 *     All decisions regarding priorities are left to components creating tasks (e.g. SimpleMergeSelector).
 *  2) PriorityRuntimeQueue. Uses heap to select task with smallest priority value.
 *     With this architecture all small merges / mutations will be executed faster, than bigger ones.
 *     WARNING: Starvation is possible in case of overload.
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
        CurrentMetrics::Metric metric_,
        CurrentMetrics::Metric max_tasks_metric_,
        std::string_view policy = {});

    ~MergeTreeBackgroundExecutor();

    /// Handler for hot-reloading
    /// Supports only increasing the number of threads and tasks, because
    /// implementing tasks eviction will definitely be too error-prone and buggy.
    void increaseThreadsAndMaxTasksCount(size_t new_threads_count, size_t new_max_tasks_count);

    size_t getMaxThreads() const;

    /// This method can return stale value of max_tasks_count (no mutex locking).
    /// It's okay because amount of tasks can be only increased and getting stale value
    /// can lead only to some postponing, not logical error.
    size_t getMaxTasksCount() const;

    bool trySchedule(ExecutableTaskPtr task);
    void removeTasksCorrespondingToStorage(StorageID id);
    void wait();

    /// Update scheduling policy for pending tasks. It does nothing if `new_policy` is the same or unknown.
    void updateSchedulingPolicy(std::string_view new_policy)
    {
        std::lock_guard lock(mutex);
        pending.updatePolicy(new_policy);
    }

private:
    String name;
    size_t threads_count TSA_GUARDED_BY(mutex) = 0;
    std::atomic<size_t> max_tasks_count = 0;
    CurrentMetrics::Metric metric;
    CurrentMetrics::Increment max_tasks_metric;

    void routine(TaskRuntimeDataPtr item);

    /// libc++ does not provide TSA support for std::unique_lock -> TSA_NO_THREAD_SAFETY_ANALYSIS
    void threadFunction() TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Initially it will be empty
    Queue pending TSA_GUARDED_BY(mutex);
    boost::circular_buffer<TaskRuntimeDataPtr> active TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
    std::condition_variable has_tasks TSA_GUARDED_BY(mutex);
    bool shutdown TSA_GUARDED_BY(mutex) = false;
    std::unique_ptr<ThreadPool> pool;
    LoggerPtr log = getLogger("MergeTreeBackgroundExecutor");
};

extern template class MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue>;
extern template class MergeTreeBackgroundExecutor<PriorityRuntimeQueue>;
extern template class MergeTreeBackgroundExecutor<DynamicRuntimeQueue>;

}
