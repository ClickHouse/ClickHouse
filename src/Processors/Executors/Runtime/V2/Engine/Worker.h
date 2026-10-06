#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Tasks/TaskRunner.h>
#include <Processors/Executors/Runtime/V2/Engine/Tasks/LocalTasks.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerSlot.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>

#include <atomic>
#include <optional>

namespace DB
{

namespace Runtime::V2
{

class WorkerPool;

class Worker
{
    std::optional<Task> pickTask();
    std::optional<Task> tryPickTask();
    std::optional<Task> takeFromNext();
    std::optional<Task> takeFromLocal();
    std::optional<Task> takeFromGlobal();
    std::optional<Task> takeFromOthers();
    void pushToNext(Task task);
    void pushToLocal(Task task);
    void wakeOrSpawn(size_t threads_needed);
    size_t runTask(Task task);

public:
    Worker(WorkerPool & pool_, size_t worker_id_);

    void run(WorkerSlot & slot, std::atomic_bool * yield_flag);

    std::optional<Task> steal();
    size_t poll(int timeout_ms);
    void drain();
    size_t taskCount() const;

    void park();
    void wake();

private:
    WorkerPool & pool;
    const size_t worker_id;
    TaskRunner runner;

    Wyrd::Mutex mutex;
    LocalTasks local_tasks;
    Wyrd::Atomic<size_t> local_tasks_count = 0;

    std::optional<Task> next;
    size_t next_streak = 0;
    size_t picks_count = 0;

    Wyrd::Atomic<bool> wake_up = false;
};

}

}
