#pragma once

#include <cstdint>
#include <cassert>
#include <mutex>
#include <unordered_map>
#include <queue>
#include <memory>

#include <iostream>
#include <fmt/format.h>

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

        bool better(const PriorityKey & other) const
        {
            return hard_priority < other.hard_priority
                || (hard_priority == other.hard_priority
                    && counter.less(other.counter, smooth_interval));
        }

        bool operator< (const PriorityKey & other) const
        {
            /// Negation is for priority_queue.
            return other.better(*this);
        }
    };

    struct TaskWithPriorityKey
    {
        Task task;
        PriorityKey priority_key;

        bool better(const TaskWithPriorityKey & other) const
        {
            return priority_key.better(other.priority_key);
        }

        bool operator< (const TaskWithPriorityKey & other) const
        {
            /// Negation is for priority_queue.
            return other.better(*this);
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

            if (stat.concurrency >= constraint.max_concurrency
                && stat.concurrency - constraint.max_concurrency >= max_deficit_value)
            {
                max_deficit_value = stat.concurrency - constraint.max_concurrency;
                max_deficit = it;
            }
        }

        task_with_key.task = std::move(task);

        /// If there is deficit, put it in a queue with max deficit.
        if (max_deficit)
        {
            std::cerr << "Selected queue " << max_deficit.value()->first << "\n";
            waiting_queues[max_deficit.value()->first].push(std::move(task_with_key));
        }
        else
        {
            for (const auto & constraint : task_with_key.task.resource_keys)
                ++stats[constraint.key].concurrency;

            for (const auto & stat : stats)
                std::cerr << fmt::format("push, {}, stat {}, concurrency {}\n", stat.first, stat.second.counter.get(Stopwatch().currentSeconds(), smooth_interval), stat.second.concurrency);

            ready_queue.push(std::move(task_with_key));
        }
    }

    /// Measures how long task was executing and updates the counters.
    struct Handle
    {
        Task task;
        Stopwatch watch;
        Scheduler & parent;

        Handle(Task task_, Scheduler & parent_) : task(std::move(task_)), parent(parent_)
        {
        }

        ~Handle()
        {
            watch.stop();
            double elapsed_seconds = watch.elapsedSeconds();
            double current_seconds = watch.currentSeconds();

            std::lock_guard lock(parent.mutex);

            std::optional<typename Queues::iterator> best_priority;

            for (const auto & constraint : task.resource_keys)
            {
                Stat & stat = parent.stats[constraint.key];
                --stat.concurrency;
                stat.counter.add(elapsed_seconds / task.weight, current_seconds, smooth_interval);
            }

            /// Find some waiting task to be moved to ready queue.
            for (auto queue_it = parent.waiting_queues.begin(); queue_it != parent.waiting_queues.end(); ++queue_it)
            {
                std::cerr << "Waiting queue " << queue_it->first << "\n";

                const TaskWithPriorityKey & candidate_task = queue_it->second.top();

                bool suitable = true;
                for (const auto & constraint : candidate_task.task.resource_keys)
                {
                    std::cerr << fmt::format("suitable: {}, {}, stat {}, concurrency {}\n", parent.stats[constraint.key].concurrency < constraint.max_concurrency, constraint.key, candidate_task.priority_key.counter.get(current_seconds, smooth_interval), parent.stats[constraint.key].concurrency);

                    if (parent.stats[constraint.key].concurrency >= constraint.max_concurrency)
                    {
                        suitable = false;
                        break;
                    }
                }

                if (suitable
                    && (!best_priority || candidate_task.priority_key.better(best_priority.value()->second.top().priority_key)))
                {
                    best_priority = queue_it;
                }
            }

            /// Maybe move some task to ready queue.
            if (best_priority)
            {
                auto queue_it = *best_priority;
                auto & elem = queue_it->second.top();

                for (const auto & constraint : elem.task.resource_keys)
                {
                    ++parent.stats[constraint.key].concurrency;

                    const auto & stat = parent.stats[constraint.key];
                    std::cerr << fmt::format("best, {}, stat {}, concurrency {}\n", constraint.key, elem.priority_key.counter.get(current_seconds, smooth_interval), stat.concurrency);
                }

                parent.ready_queue.push(elem);
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
            std::cerr << fmt::format("pop, {}, stat {}, concurrency {}\n", stat.first, ready_queue.top().priority_key.counter.get(Stopwatch().currentSeconds(), smooth_interval), stat.second.concurrency);

        HandlePtr res = std::make_shared<Handle>(ready_queue.top().task, *this);
        ready_queue.pop();
        return res;
    }
};

}
