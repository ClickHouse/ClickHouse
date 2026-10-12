#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/ProcessorRunner.h>
#include <Processors/Executors/Runtime/V2/Engine/Metrics.h>
#include <Processors/Executors/Runtime/V2/Engine/Queues/LocalQueue.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerSlot.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>

#include <array>
#include <atomic>
#include <span>

namespace DB
{

namespace Runtime::V2
{

class WorkerPool;

class Worker
{
    /// Search.
    bool keepRunning(WorkerSlot & slot, std::atomic_bool * yield_flag);
    ExecutingProcessor * pick(WorkerSlot & slot, std::atomic_bool * yield_flag);
    ExecutingProcessor * tryPick();
    ExecutingProcessor * pickFromBatch(std::span<ExecutingProcessor *> batch, bool source_not_empty);
    size_t trySteal(size_t rounds);
    bool anyWorkHasAppeared() const;

    /// Population.
    void pushManyLifo(std::span<ExecutingProcessor * const> many);
    void wakeWorker();
    void execute(ExecutingProcessor & executing);
    void finishRun();

public:
    Worker(WorkerPool & pool_, size_t worker_id_);

    void run(WorkerSlot & slot, std::atomic_bool * yield_flag);
    const WorkerMetrics & getMetrics() const;

    UInt32 stealFifo(std::span<ExecutingProcessor *> out);
    void handOver();
    void wake();

private:
    WorkerPool & pool;
    const size_t worker_id;
    ProcessorRunner runner;

    WorkerMetrics metrics;
    LocalQueue local_tasks;
    std::array<ExecutingProcessor *, LocalQueue::capacity> tmp_buffer{};

    Wyrd::Atomic<UInt32> wakeups = 0;
};

}

}
