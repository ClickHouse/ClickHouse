#pragma once

#include <Processors/Executors/Runtime/V2/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingPipeline.h>
#include <Common/ISlotControl.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Thread.h>

#include <atomic>
#include <memory>
#include <mutex>
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
    void runSlot(AcquiredSlotPtr slot, std::atomic_bool * yield_flag);
    void spawn(AcquiredSlotPtr slot);

public:
    WorkerPool(TaskScheduler & scheduler_, WorkersCoordinator & coordinator_, ExecutingPipeline & pipeline_, size_t max_threads_, bool concurrency_control);
    ~WorkerPool();

    void run();
    void runUntil(std::atomic_bool * yield_flag);
    void grow(size_t threads_needed);
    void stop();

private:
    TaskScheduler & scheduler;
    WorkersCoordinator & coordinator;
    ExecutingPipeline & pipeline;

    /// CPU slots
    const size_t max_threads;
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
