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
    struct alignas(128) GlobalState
    {
        std::mutex mutex;
        WorkStealingQueue queue;
        std::atomic<size_t> queue_size = 0;
    };

    struct alignas(128) LocalState
    {
        std::mutex mutex;
        WorkStealingQueue queue;
        std::atomic<size_t> queue_size = 0;

        size_t pops_count = 0;
        size_t lifo_used_count = 0;
        bool pushed_since_last_pop = false;
    };
    using LocalStates = std::vector<LocalState>;

    void pushToLocalQueue(LocalState & own, Task task);
    void pushToGlobalQueue(Task task);
    std::optional<Task> takeFromLocal(LocalState & own);
    std::optional<Task> takeFromGlobal(LocalState & own, size_t max_to_take);
    std::optional<Task> steal(LocalState & own);

public:
    TaskScheduler(Poller & poller_, size_t max_workers);

    void push(Task task, size_t worker_id);
    void push(Task task);
    void push(AsyncTask task);

    std::optional<Task> tryPop(size_t worker_id);
    size_t poll(size_t worker_id, int timeout_ms);
    void drain(size_t worker_id);

    bool hasTasksForOthers(size_t worker_id) const;
    size_t queued() const;
    size_t total() const;

private:
    Poller & poller;
    GlobalState global;
    LocalStates local;
};

}
