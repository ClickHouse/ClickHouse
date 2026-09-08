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

public:
    WorkerPool(TaskScheduler & scheduler_, WorkersCoordinator & coordinator_, ExecutingPipeline & pipeline_, size_t max_threads_, bool concurrency_control);
    ~WorkerPool();

    void run();
    void runUntil(std::atomic_bool * yield_flag);
    void grow();
    void stop();

private:
    TaskScheduler & scheduler;
    WorkersCoordinator & coordinator;
    ExecutingPipeline & pipeline;

    /// CPU slots: the allocation and the one slot kept across runUntil calls
    const size_t max_threads;
    SlotAllocationPtr cpu_slots;
    AcquiredSlotPtr single_slot;

    /// Threads: spawn_mutex serializes the growers; workers_count is raised by run and runUntil before any other worker exists and lowered by a leaving worker without it
    std::unique_ptr<ThreadPool> pool;
    std::mutex spawn_mutex;
    std::atomic<size_t> workers_count = 0;

    /// Memory reservation of the query, both null without a process list element
    MemoryReservation * reservation = nullptr;
    MemoryTracker * tracker = nullptr;
};

}
