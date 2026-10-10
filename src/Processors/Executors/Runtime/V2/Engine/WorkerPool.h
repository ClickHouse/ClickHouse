#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/Async/Poller.h>
#include <Processors/Executors/Runtime/V2/ExecutionOptions.h>
#include <Processors/Executors/Runtime/V2/Engine/Metrics.h>
#include <Processors/Executors/Runtime/V2/Engine/Queues/GlobalQueue.h>
#include <Processors/Executors/Runtime/V2/Engine/Worker.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingPipeline.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Thread.h>
#include <Common/ISlotControl.h>

#include <atomic>
#include <deque>
#include <exception>
#include <optional>
#include <vector>

namespace DB
{

namespace Runtime::V2
{

class WorkerPool
{
    friend class Worker;
    friend class WorkersCoordinator;

    SlotAllocationPtr allocateCPU(bool concurrency_control, SlotCount initial_max);
    void initializeExecution();
    void runSlot(AcquiredSlotPtr slot, std::atomic_bool * yield_flag);
    void spawn(AcquiredSlotPtr slot);
    void grow(size_t threads_needed);
    void preempt(size_t worker_id);
    void resume(size_t worker_id);

public:
    WorkerPool(ExecutingPipeline & pipeline_, const ExecutionOptions & options_, size_t max_threads_, bool concurrency_control);
    ~WorkerPool();

    void run();
    void runUntil(std::atomic_bool * yield_flag);

    void fail(std::exception_ptr exception);
    void stop();
    bool isStopped() const;

    void join();
    void rethrowIfFailed();
    Metrics getMetrics() const;

private:
    ExecutingPipeline & pipeline;
    const ExecutionOptions & options;
    const size_t max_threads;

    /// Scheduling state
    std::deque<Worker> workers;
    WorkersCoordinator coordinator;
    GlobalQueue global_tasks;
    Poller poller;

    /// CPU slots
    size_t requested_threads;
    SlotAllocationPtr cpu_slots;
    AcquiredSlotPtr single_slot;

    /// Worker threads
    std::vector<std::optional<Wyrd::Thread>> threads;
    Wyrd::Mutex spawn_mutex;
    Wyrd::Atomic<size_t> workers_count = 0;

    /// First failure
    Wyrd::Atomic<bool> failed = false;
    std::exception_ptr exception;
};

}

}
