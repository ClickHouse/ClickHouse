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

template <typename Task>
class TaskQueue
{
    using Time = size_t;
    static constexpr Time ignore_thread_mask = 127;

public:
    void init(size_t num_threads)
    {
        queues.resize(num_threads);
        times.assign(num_threads, 0);
    }

    void push(Task * task, size_t thread_num)
    {
        queues[thread_num].push(task);
        ++num_tasks;
    }

    size_t getAnyThreadWithTasks(size_t from_thread = 0)
    {
        for (size_t i = 0; i < queues.size(); ++i)
        {
            if (!queues[from_thread].empty())
                return from_thread;

            ++from_thread;
            if (from_thread >= queues.size())
                from_thread = 0;
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "TaskQueue is empty");
    }

    size_t getOldestThreadWithTasks()
    {
        size_t oldest_thread = 0;
        while (oldest_thread < queues.size() && queues[oldest_thread].empty())
            ++oldest_thread;

        if (oldest_thread == queues.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TaskQueue is empty");

        for (size_t thread = oldest_thread + 1; thread < queues.size(); ++thread)
            if (!queues[thread].empty() && times[thread] < times[oldest_thread])
                oldest_thread = thread;

        return oldest_thread;
    }

    Task * pop(size_t thread_num)
    {
        if (num_tasks == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TaskQueue is empty");

        Time current_time = next_time;
        ++next_time;

        size_t thread_with_tasks;

        if (current_time & ignore_thread_mask)
            thread_with_tasks = getAnyThreadWithTasks(thread_num);
        else
            thread_with_tasks = getOldestThreadWithTasks();

        Task * task = queues[thread_with_tasks].front();
        queues[thread_with_tasks].pop();
        times[thread_with_tasks] = current_time;

        --num_tasks;
        return task;
    }

    size_t size() const { return num_tasks; }
    bool empty() const { return num_tasks == 0; }

private:
    using Queue = std::queue<Task *>;
    std::vector<Queue> queues;
    std::vector<Time> times;
    size_t num_tasks = 0;
    Time next_time = 1;
};

}
