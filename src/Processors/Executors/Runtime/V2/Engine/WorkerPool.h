#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Polling/Poller.h>
#include <Processors/Executors/Runtime/V2/Engine/Tasks/GlobalTasks.h>
#include <Processors/Executors/Runtime/V2/Engine/Worker.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingPipeline.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Thread.h>
#include <Common/ISlotControl.h>

#include <atomic>
#include <deque>
#include <optional>
#include <vector>

class MemoryTracker;

namespace DB
{

struct MemoryReservation;

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
    WorkerPool(ExecutingPipeline & pipeline_, size_t max_threads_, bool concurrency_control);
    ~WorkerPool();

    void run();
    void runUntil(std::atomic_bool * yield_flag);
    void stop();
    bool isStopped() const;
    void join();

private:
    ExecutingPipeline & pipeline;
    const size_t max_threads;

    /// Scheduling state
    std::deque<Worker> workers;
    WorkersCoordinator coordinator;
    GlobalTasks global_tasks;
    Poller poller;

    /// CPU slots
    size_t requested_threads;
    SlotAllocationPtr cpu_slots;
    AcquiredSlotPtr single_slot;

    /// Worker threads
    std::vector<std::optional<Wyrd::Thread>> threads;
    Wyrd::Mutex spawn_mutex;
    Wyrd::Atomic<size_t> workers_count = 0;

    /// Memory reservations
    MemoryReservation * reservation = nullptr;
    MemoryTracker * tracker = nullptr;
};

}

}
