#pragma once

#include <deque>
#include <functional>
#include <atomic>
#include <mutex>
#include <future>
#include <condition_variable>
#include <set>
#include <iostream>

#include <boost/circular_buffer.hpp>

#include <common/shared_ptr_helper.h>
#include <common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/Stopwatch.h>
#include <Storages/MergeTree/IExecutableTask.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/**
 * Has RAII class to determine how many tasks are waiting for the execution and executing at the moment.
 * Also has some flags and primitives to wait for current task to be executed.
 */
struct TaskRuntimeData
{
    TaskRuntimeData(ExecutableTaskPtr && task_, CurrentMetrics::Metric metric_)
        : task(std::move(task_))
        , increment(std::move(metric_))
    {}

    ExecutableTaskPtr task;
    CurrentMetrics::Increment increment;
    std::atomic_bool is_currently_deleting{false};
    /// Actually autoreset=false is needed only for unit test
    /// where multiple threads could remove tasks corresponding to the same storage
    /// This scenario in not possible in reality.
    Poco::Event is_done{/*autoreset=*/false};
};

using TaskRuntimeDataPtr = std::shared_ptr<TaskRuntimeData>;


class IRuntimeQueue
{
public:
    virtual TaskRuntimeDataPtr pop() = 0;
    virtual void push(TaskRuntimeDataPtr item) = 0;
    virtual void remove(StorageID id) = 0;
    virtual void setCapacity(size_t count) = 0;
    virtual bool empty() = 0;
    virtual ~IRuntimeQueue() = default;
};

class OrdinaryRuntimeQueue : public IRuntimeQueue
{
public:
    TaskRuntimeDataPtr pop() override
    {
        auto result = std::move(queue.front());
        queue.pop_front();
        return result;
    }

    void push(TaskRuntimeDataPtr item) override
    {
        queue.push_back(std::move(item));
    }

    void remove(StorageID id) override
    {
        auto it = std::remove_if(queue.begin(), queue.end(),
            [&] (auto item) -> bool { return item->task->getStorageID() == id; });
        queue.erase(it, queue.end());
    }

    void setCapacity(size_t count) override
    {
        queue.set_capacity(count);
    }

    bool empty() override { return queue.empty(); }

private:
    boost::circular_buffer<TaskRuntimeDataPtr> queue{0};
};


/// Consists of two circular buffers
/// One for merges, one for mutations
class MergeMutateRuntimeQueue : public IRuntimeQueue
{
public:
    TaskRuntimeDataPtr pop() override;

    void push(TaskRuntimeDataPtr item) override
    {
        if (item->task->getType() == IExecutableTask::Type::MERGE)
            merges.push_back(std::move(item));
        else if (item->task->getType() == IExecutableTask::Type::MUTATE)
            mutations.push_back(std::move(item));
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Bad type of task for MergeMutateExecutor");
    }

    void remove(StorageID id) override
    {
        auto it = std::remove_if(merges.begin(), merges.end(),
            [&] (auto item) -> bool { return item->task->getStorageID() == id; });
        merges.erase(it, merges.end());

        it = std::remove_if(mutations.begin(), mutations.end(),
            [&] (auto item) -> bool { return item->task->getStorageID() == id; });
        mutations.erase(it, mutations.end());
    }

    void setCapacity(size_t count) override
    {
        merges.set_capacity(count);
        mutations.set_capacity(count);
    }

    bool empty() override { return merges.empty() && mutations.empty(); }

private:
    enum class State : uint8_t
    {
        NO_TASKS = 0,
        ONLY_MERGES = 1,
        ONLY_MUTATIONS = 2,
        BOTH = 3
    };
    bool choise{false};
    boost::circular_buffer<TaskRuntimeDataPtr> merges{0};
    boost::circular_buffer<TaskRuntimeDataPtr> mutations{0};
};

/**
 *  Executor for a background MergeTree related operations such as merges, mutations, fetches an so on.
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
 *  So, when a Storage want to shutdown, it must wait until all its background operaions are finished.
 */
template <class Queue>
class MergeTreeBackgroundExecutor final : public shared_ptr_helper<MergeTreeBackgroundExecutor<Queue>>, private Queue
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

    ~MergeTreeBackgroundExecutor() override
    {
        wait();
    }

    bool trySchedule(ExecutableTaskPtr task);
    void removeTasksCorrespondingToStorage(StorageID id);
    void wait();

private:

    String name;
    size_t threads_count{0};
    size_t max_tasks_count{0};
    CurrentMetrics::Metric metric;

    /**
     * Has RAII class to determine how many tasks are waiting for the execution and executing at the moment.
     * Also has some flags and primitives to wait for current task to be executed.
     */
    struct TaskRuntimeData
    {
        TaskRuntimeData(ExecutableTaskPtr && task_, CurrentMetrics::Metric metric_)
            : task(std::move(task_))
            , increment(std::move(metric_))
        {}

        ExecutableTaskPtr task;
        CurrentMetrics::Increment increment;
        bool is_currently_deleting{false};
        /// Actually autoreset=false is needed only for unit test
        /// where multiple threads could remove tasks corresponding to the same storage
        /// This scenario in not possible in reality.
        Poco::Event is_done{/*autoreset=*/false};
    };

    using TaskRuntimeDataPtr = std::shared_ptr<TaskRuntimeData>;

    void routine(TaskRuntimeDataPtr item);
    void threadFunction();

    /// Initially it will be empty
    Queue pending{};
    boost::circular_buffer<TaskRuntimeDataPtr> active{0};
    std::mutex mutex;
    std::condition_variable has_tasks;
    std::atomic_bool shutdown{false};
    ThreadPool pool;
};

extern template class MergeTreeBackgroundExecutor<MergeMutateRuntimeQueue>;
extern template class MergeTreeBackgroundExecutor<OrdinaryRuntimeQueue>;

using MergeMutateBackgroundExecutor = MergeTreeBackgroundExecutor<MergeMutateRuntimeQueue>;
using OrdinaryBackgroundExecutor = MergeTreeBackgroundExecutor<OrdinaryRuntimeQueue>;

}
