#pragma once
#include <vector>
#include <queue>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// A set of task queues for multithreaded processing.
/// Every threads has its dedicated queue and uses it preferably.
/// When there are no tasks left in dedicated queue it steals tasks from other threads
template <typename Task>
class TaskQueue
{
public:
    void init(size_t num_threads) { queues.resize(num_threads); }

    /// Push a task into thread's dedicated queue
    void push(Task * task, size_t thread_num)
    {
        queues[thread_num].push(task);
        use_queues = std::max(use_queues, thread_num + 1);
        ++num_tasks;
    }

    /// Returns thread number to pop task from.
    /// First it check dedicated queue, and only if it is empty, it steal from other threads
    size_t getAnyThreadWithTasks(size_t from_thread)
    {
        if (num_tasks == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TaskQueue is empty");

        if (from_thread >= use_queues)
            from_thread = 0;

        for (size_t i = 0, max_iterations = use_queues; i < max_iterations; ++i)
        {
            if (!queues[from_thread].empty())
                return from_thread;

            ++from_thread;
            if (from_thread == use_queues)
            {
                use_queues--; // lazy update: less expensive than updating on every pop
                from_thread = 0;
            }
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "TaskQueue is empty");
    }

    /// Pop a task from the specified queue or steal from others
    Task * pop(size_t thread_num)
    {
        auto thread_with_tasks = getAnyThreadWithTasks(thread_num);

        Task * task = queues[thread_with_tasks].front();
        queues[thread_with_tasks].pop();

        --num_tasks;
        return task;
    }

    size_t size() const { return num_tasks; }
    bool empty() const { return num_tasks == 0; }

private:
    using Queue = std::queue<Task *>;
    std::vector<Queue> queues;
    size_t num_tasks = 0;
    size_t use_queues = 0; // For optimization, to avoid searching for empty queue every time
};

}
