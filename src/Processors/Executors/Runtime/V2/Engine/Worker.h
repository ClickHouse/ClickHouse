#pragma once

#include <Processors/Executors/Runtime/V2/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerPool.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerSlot.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingPipeline.h>
#include <Processors/Executors/Runtime/V2/Engine/ProcessorRunner.h>

#include <atomic>
#include <optional>

namespace DB
{

namespace Runtime::V2
{

class Worker
{
    std::optional<Task> pickTask();
    void wakeOrSpawn(size_t threads_needed);
    size_t runTask(Task task);

public:
    Worker(size_t worker_id_, TaskScheduler & scheduler_, WorkersCoordinator & coordinator_, ExecutingPipeline & pipeline_, WorkerPool & pool_);

    void run(WorkerSlot & slot, std::atomic_bool * yield_flag);

private:
    const size_t worker_id;
    TaskScheduler & scheduler;
    WorkersCoordinator & coordinator;
    ExecutingPipeline & pipeline;
    WorkerPool & pool;
    ProcessorRunner runner;
};

}

}
