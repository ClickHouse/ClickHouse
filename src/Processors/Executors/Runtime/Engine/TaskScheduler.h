#pragma once

#include <Processors/Executors/Runtime/Engine/Poller.h>
#include <Processors/Executors/Runtime/Engine/Task.h>
#include <Processors/Executors/Runtime/Engine/TaskQueue.h>

#include <atomic>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

class TaskScheduler
{
    struct alignas(128) AsyncState
    {
        Poller & poller;
        std::atomic<size_t> tasks_count = 0;
    };

    struct alignas(128) GlobalState
    {
        std::mutex mutex;
        TaskQueue queue;
        std::atomic<size_t> tasks_count = 0;
    };

    struct alignas(128) LocalState
    {
        std::mutex mutex;
        TaskQueue queue;
        std::atomic<size_t> tasks_count = 0;

        std::optional<Task> next;
        size_t next_streak = 0;
        size_t picks_count = 0;
    };
    using LocalStates = std::vector<LocalState>;

    void pushToLocalQueue(LocalState & own, Task task);
    void pushToGlobalQueue(Task task);
    std::optional<Task> takeFromNext(LocalState & own);
    std::optional<Task> takeFromLocal(LocalState & own);
    std::optional<Task> takeFromGlobal();
    std::optional<Task> takeFromOthers(const LocalState & own);

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
    AsyncState async;
    GlobalState global;
    LocalStates local;
};

}
