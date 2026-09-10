#pragma once

#include <Processors/Executors/Runtime/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/Pipeline/ExecutingPipeline.h>
#include <Common/ISlotControl.h>
#include <Common/ThreadPool_fwd.h>

#include <atomic>
#include <memory>
#include <mutex>

class MemoryTracker;

namespace DB
{

struct MemoryReservation;

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
    std::unique_ptr<ThreadPool> pool;
    std::mutex spawn_mutex;
    std::atomic<size_t> workers_count = 0;

    /// Memory reservations
    MemoryReservation * reservation = nullptr;
    MemoryTracker * tracker = nullptr;
};

}
