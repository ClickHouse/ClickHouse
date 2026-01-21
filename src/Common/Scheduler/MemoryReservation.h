#pragma once

#include <Common/Scheduler/ResourceAllocation.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Common/CurrentMetrics.h>

#include <memory>
#include <mutex>

class MemoryTracker;

namespace DB
{

/// Represents a memory reservation that is used with:
/// CREATE RESOURCE memory (MEMORY RESERVATION)
/// CREATE WORKLOAD all SETTINGS max_memory = '1Gi'
/// SELECT ... SETTINGS workload = 'all', reserve_memory = '100Mi'
/// The scheduler sees usage through its `allocated = max(actual_size, reserve_memory)`
/// and guarantees that total allocated memory does not exceed related workload `max_memory` limits.
/// The MemoryTracker of a query is the source of truth and MemoryReservation synchronizes with it in safe points.
/// During such syncs it may issue both increase and decrease requests to the scheduler,
/// as well as respond to kill (evict) requests from the scheduler.
struct MemoryReservation : public ResourceAllocation
{
public:
    // Blocks until reservation is admitted iff reserved_size > 0
    MemoryReservation(ResourceLink link, const String & id_, ResourceCost reserved_size);
    ~MemoryReservation() override;

    // Sync actual size with MemoryTracker, issues and waits increase/decrease requests as needed.
    void syncWithMemoryTracker(const MemoryTracker * memory_tracker);

private:
    void throwIfNeeded();
    void syncWithScheduler();

    // Interaction with the scheduler thread
    void killAllocation(const std::exception_ptr & reason) override;
    void increaseApproved(const IncreaseRequest & increase) override;
    void decreaseApproved(const DecreaseRequest & decrease) override;
    void allocationFailed(const std::exception_ptr & reason) override;

    const ResourceCost reserved_size; // value of `reserve_memory` query setting

    /// Protects all the fields in this allocation that may be accessed from the scheduler thread.
    /// NOTE: Lock ordering: first queue.mutex, then allocation.mutex
    std::mutex mutex;
    std::condition_variable cv;

    std::exception_ptr kill_reason;
    std::exception_ptr fail_reason;
    bool increase_enqueued = false;
    bool decrease_enqueued = false;
    bool removed = false;
    ResourceCost allocated_size = 0; // equals ResourceAllocation::allocated, which is private and controlled by the scheduler
    ResourceCost actual_size = 0; // real size of the resource used by the allocation

    /// Introspection
    CurrentMetrics::Increment approved_increment;
    CurrentMetrics::Increment demand_increment;
};

using MemoryReservationPtr = std::unique_ptr<MemoryReservation>;

}
