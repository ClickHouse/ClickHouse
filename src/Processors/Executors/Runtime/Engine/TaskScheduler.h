#pragma once

#include <Processors/Executors/Runtime/Engine/Poller.h>
#include <Processors/Executors/Runtime/Engine/Task.h>
#include <Processors/Executors/Runtime/Engine/WorkStealingQueue.h>

#include <atomic>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

class TaskScheduler
{
    struct GuardedQueue
    {
        std::mutex mutex;
        WorkStealingQueue queue;
        size_t lifo_pops = 0;
    };

    std::optional<Task> takeFromLocal(GuardedQueue & own);
    std::optional<Task> takeFromGlobal(GuardedQueue & own);
    std::optional<Task> steal(size_t worker_id);
    std::optional<Task> keepAndPopFirst(GuardedQueue & own, WorkStealingQueue & taken);

public:
    TaskScheduler(Poller & poller_, size_t max_workers);

    void push(Task task, size_t worker_id);
    void push(Task task);
    void push(AsyncTask task);

    std::optional<Task> tryPop(size_t worker_id);
    size_t poll(size_t worker_id, int timeout_ms);
    void drain(size_t worker_id);

    size_t queued() const;
    size_t total() const;

private:
    Poller & poller;
    std::vector<GuardedQueue> local;
    GuardedQueue global;
    std::atomic<size_t> queued_count = 0;
    std::atomic<size_t> total_count = 0;
};

}
