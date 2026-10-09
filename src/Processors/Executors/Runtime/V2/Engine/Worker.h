#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Polling/PollRequest.h>
#include <Processors/Executors/Runtime/V2/Engine/Tasks/TaskRunner.h>
#include <Processors/Executors/Runtime/V2/Engine/Tasks/LocalTasks.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerSlot.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>

#include <array>
#include <atomic>
#include <optional>
#include <span>

namespace DB
{

namespace Runtime::V2
{

class WorkerPool;

class Worker
{
    /// Tasks search.
    bool keepRunning(WorkerSlot & slot, std::atomic_bool * yield_flag);
    std::optional<Task> pickTask(WorkerSlot & slot, std::atomic_bool * yield_flag);
    std::optional<Task> tryPickTask();
    std::optional<Task> tryPickTaskBeforePark();
    std::optional<Task> tryStealTasks(size_t rounds);
    Task pickFromBatch(std::span<const Task> batch);

    /// Tasks population.
    void pushManyLifo(std::span<const Task> tasks);
    void pushMany(std::span<const PollRequest> requests);
    void wakeFor(Int64 extra_work);
    void runTask(Task task);
    void finishRun();

public:
    Worker(WorkerPool & pool_, size_t worker_id_);

    void run(WorkerSlot & slot, std::atomic_bool * yield_flag);

    UInt32 stealFifo(std::span<Task> out);
    void handOverTasks();
    void wake();

private:
    WorkerPool & pool;
    const size_t worker_id;
    TaskRunner runner;

    LocalTasks local_tasks;
    std::array<Task, LocalTasks::capacity> tmp_task_buffer{};

    Wyrd::Atomic<UInt32> wakeups = 0;
};

}

}
