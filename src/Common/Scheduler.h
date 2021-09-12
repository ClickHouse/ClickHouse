#pragma once

#include <cstdint>
#include <cassert>
#include <mutex>
#include <unordered_map>
#include <queue>
#include <memory>
#include <iostream>

#include <Common/ExponentiallySmoothedCounter.h>
#include <Common/Stopwatch.h>


namespace DB
{

/// Scheduler is a sophisticated version of priority queue.
template <typename Payload>
class Scheduler
{
public:
    struct ResourceConstraint
    {
        std::string key;
        size_t max_concurrency;
    };

    struct Task
    {
        /// Lower is more priority. This priority is strict: lower priority operations are preempted.
        size_t priority = 0;

        /// For operations with the same priority - choose operations with higher weight proportionally more frequently.
        size_t weight = 1;

        /// Keys to limit concurrency and to count resource usage.
        std::vector<ResourceConstraint> resource_keys;

        Payload data;
    };

private:
    /// Window size for exponential smoothing.
    static inline constexpr double smooth_interval = 1;

    std::mutex mutex;

    struct Stat
    {
        ExponentiallySmoothedSum counter;
        size_t concurrency = 0;
    };

    using Stats = std::unordered_map<std::string, Stat>;
    Stats stats;

    struct PriorityKey
    {
        size_t hard_priority = 0;
        ExponentiallySmoothedSum counter;

        bool operator< (const PriorityKey & other) const
        {
            /// Negation is for priority_queue.
            return other.hard_priority < hard_priority
                || (other.hard_priority == hard_priority
                    && other.counter.less(counter, smooth_interval));
        }
    };

    struct TaskWithPriorityKey
    {
        Task task;
        PriorityKey priority_key;

        bool operator< (const TaskWithPriorityKey & other) const
        {
            return priority_key < other.priority_key;
        }
    };

    using Queue = std::priority_queue<TaskWithPriorityKey>;
    using Queues = std::unordered_map<std::string, Queue>;

    Queues waiting_queues;
    Queue ready_queue;

public:
    void push(Task task)
    {
        std::lock_guard lock(mutex);

        TaskWithPriorityKey task_with_key;

        task_with_key.priority_key.hard_priority = task.priority;

        std::optional<typename Stats::const_iterator> max_deficit;
        size_t max_deficit_value = 0;

        for (const auto & constraint : task.resource_keys)
        {
            auto it = stats.try_emplace(constraint.key).first;
            Stat & stat = it->second;

            /// Sum all resource usage counters together to form priority.
            task_with_key.priority_key.counter.merge(stat.counter, smooth_interval);

            if (stat.concurrency + 1 > constraint.max_concurrency
                && stat.concurrency + 1 - constraint.max_concurrency > max_deficit_value)
            {
                max_deficit_value = stat.concurrency - constraint.max_concurrency;
                max_deficit = it;
            }
        }

        task_with_key.task = std::move(task);

        /// If there is deficit, put it in a queue with max deficit.
        if (max_deficit)
            waiting_queues[max_deficit.value()->first].push(std::move(task_with_key));
        else
            ready_queue.push(std::move(task_with_key));
    }

    /// Measures how long task was executing and updates the counters.
    struct Handle
    {
        Task task;
        Stopwatch watch;
        Scheduler & parent;

        Handle(Task task_, Scheduler & parent_, std::lock_guard<std::mutex> &) : task(std::move(task_)), parent(parent_)
        {
            for (const auto & constraint : task.resource_keys)
            {
                Stat & stat = parent.stats[constraint.key];
                ++stat.concurrency;
            }
        }

        ~Handle()
        {
            watch.stop();
            double elapsed_seconds = watch.elapsedSeconds();
            double current_seconds = watch.currentSeconds();

            std::lock_guard lock(parent.mutex);

            std::optional<typename Queues::iterator> min_deficit;
            size_t min_deficit_value = 0;

            for (const auto & constraint : task.resource_keys)
            {
                Stat & stat = parent.stats[constraint.key];
                --stat.concurrency;
                stat.counter.add(elapsed_seconds / task.weight, current_seconds, smooth_interval);

                /// Find some waiting task to be moved to ready queue.
                if (auto queue_it = parent.waiting_queues.find(constraint.key); queue_it != parent.waiting_queues.end())
                {
                    auto & queue = queue_it->second;
                    assert(!queue.empty()); /// Empty queues are deleted.

                    const TaskWithPriorityKey & candidate_task = queue.top();

                    size_t candidate_task_max_concurrency = 0;
                    for (const auto & resource_key : candidate_task.task.resource_keys)
                    {
                        if (resource_key.key == constraint.key)
                        {
                            candidate_task_max_concurrency = resource_key.max_concurrency;
                            break;
                        }
                    }

                    size_t current_deficit = stat.concurrency - candidate_task_max_concurrency;
                    if (!min_deficit || current_deficit < min_deficit_value
                        || (current_deficit == min_deficit_value && min_deficit.value()->second.top().priority_key < candidate_task.priority_key))
                    {
                        min_deficit_value = current_deficit;
                        min_deficit = queue_it;
                    }
                }
            }

            /// Maybe move some task to ready queue.
            if (min_deficit)
            {
                auto queue_it = *min_deficit;
                parent.ready_queue.push(queue_it->second.top());
                queue_it->second.pop();

                if (queue_it->second.empty())
                    parent.waiting_queues.erase(queue_it);
            }

            /// Maybe cleanup stats.
            for (auto it = parent.stats.begin(); it != parent.stats.end();)
            {
                if (it->second.concurrency == 0 && it->second.counter.get(current_seconds, smooth_interval) < 1e-9 /* arbitrary */)
                    it = parent.stats.erase(it);
                else
                    ++it;
            }

        }
    };

    using HandlePtr = std::shared_ptr<Handle>;

    HandlePtr pop()
    {
        std::lock_guard lock(mutex);

        if (ready_queue.empty())
            return {};

        for (const auto & stat : stats)
            std::cerr << stat.first << ", " << stat.second.counter.get(Stopwatch().currentSeconds(), smooth_interval) << ", " << stat.second.concurrency << "\n";

        HandlePtr res = std::make_shared<Handle>(ready_queue.top().task, *this, lock);
        ready_queue.pop();
        return res;
    }
};

}
