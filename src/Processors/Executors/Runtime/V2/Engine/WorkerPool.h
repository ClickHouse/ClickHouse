#pragma once

#include <Processors/Executors/Runtime/V2/ExecutionOptions.h>
#include <Processors/Executors/Runtime/V2/Engine/Metrics.h>
#include <Processors/Executors/Runtime/V2/Engine/Poller.h>
#include <Processors/Executors/Runtime/V2/Engine/Queues/AsyncReadyQueue.h>
#include <Processors/Executors/Runtime/V2/Engine/Queues/GlobalQueue.h>
#include <Processors/Executors/Runtime/V2/Engine/Worker.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingPipeline.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Thread.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/ISlotControl.h>
#include <Common/VectorWithMemoryTracking.h>

#include <atomic>
#include <exception>
#include <optional>

namespace DB
{

namespace Runtime::V2
{

enum class WakeOutcome : UInt8
{
    Skipped,
    Woken,
    Spawned,
    AllBusy,
};

class WorkerPool
{
    friend class Worker;
    friend class WorkersCoordinator;
    friend class Poller;

    SlotAllocationPtr allocateCPU(bool concurrency_control, SlotCount initial_max);
    void initializeExecution();
    void runSlot(AcquiredSlotPtr slot, std::atomic_bool * yield_flag);
    void spawn(AcquiredSlotPtr slot);
    void spawnPoller();
    size_t grow(size_t threads_needed);
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
    WakeOutcome wakeWorker();
    PoolMetrics getMetrics() const;

private:
    ExecutingPipeline & pipeline;
    const ExecutionOptions & options;
    const size_t max_threads;

    /// Scheduling state
    DequeWithMemoryTracking<Worker> workers;
    WorkersCoordinator coordinator;
    GlobalQueue global_tasks;
    AsyncReadyQueue async_ready_tasks;
    Poller poller;

    /// CPU slots
    size_t requested_threads;
    SlotAllocationPtr cpu_slots;
    AcquiredSlotPtr single_slot;

    /// Worker threads
    VectorWithMemoryTracking<std::optional<Wyrd::Thread>> threads;
    Wyrd::Mutex spawn_mutex;
    Wyrd::Atomic<size_t> workers_count = 0;

    /// Poller thread
    std::optional<Wyrd::Thread> poller_thread;
    Wyrd::Atomic<bool> poller_spawned = false;

    /// First failure
    Wyrd::Atomic<bool> failed = false;
    std::exception_ptr exception;
};

}

}
