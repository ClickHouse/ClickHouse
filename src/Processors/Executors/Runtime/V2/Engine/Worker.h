#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Tasks/TaskRunner.h>
#include <Processors/Executors/Runtime/V2/Engine/Tasks/LocalTasks.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerSlot.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>

#include <array>
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
    std::optional<Task> takeFromLocal();
    std::optional<Task> takeFromGlobal();
    std::optional<Task> takeFromPoller();
    std::optional<Task> takeFromOthers();
    void pushToLocal(const Task & task);
    void wakeOrSpawn(size_t threads_needed);
    size_t runTask(Task task);

public:
    Worker(WorkerPool & pool_, size_t worker_id_);

    void run(WorkerSlot & slot, std::atomic_bool * yield_flag);

    std::optional<Task> steal();
    std::optional<Task> poll(int timeout_ms);
    size_t drain();

    void park();
    void wake();

private:
    WorkerPool & pool;
    const size_t worker_id;
    TaskRunner runner;

    LocalTasks local_tasks;
    std::array<Task, LocalTasks::capacity / 2> offload_buffer{};

    Wyrd::Atomic<bool> wake_up = false;
};

}

}
