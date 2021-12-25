#pragma once
#include <vector>
#include <queue>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename Task>
class TaskQueue
{
public:
    void init(size_t num_threads) { queues.resize(num_threads); }

    void push(Task * task, size_t thread_num)
    {
        queues[thread_num].push(task);
        ++num_tasks;
    }

    size_t getAnyThreadWithTasks(size_t from_thread = 0)
    {
        if (num_tasks == 0)
            throw Exception("TaskQueue is empty.", ErrorCodes::LOGICAL_ERROR);

        for (size_t i = 0; i < queues.size(); ++i)
        {
            if (!queues[from_thread].empty())
                return from_thread;

            ++from_thread;
            if (from_thread >= queues.size())
                from_thread = 0;
        }

        throw Exception("TaskQueue is empty.", ErrorCodes::LOGICAL_ERROR);
    }

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
};

}
