#pragma once

#include <Common/Scheduler/ResourceAllocation.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Common/CurrentMetrics.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <base/defines.h>

class MemoryTracker;

namespace DB
{

class ISpillable;

/// `MemoryReservation` bridges a running query and the memory scheduler: the scheduler caps each
/// workload's memory while the query's `MemoryTracker` stays the source of truth. It backs:
///   CREATE RESOURCE memory (MEMORY RESERVATION)
///   CREATE WORKLOAD all SETTINGS max_memory = '1Gi'
///   SELECT ... SETTINGS workload = 'all', reserve_memory = '100Mi'
/// The scheduler sees usage as `allocated = max(actual_size, reserve_memory)` and guarantees the
/// total `allocated` under each workload does not exceed its `max_memory` limit.
/// A single scheduler thread (top) serves many query/pipeline threads (bottom):
///
///    SpaceSharedScheduler    <-- dedicated thread + EventQueue (root)
///             |
///      AllocationLimit       <-- caps max_memory
///             |
///    Fair / Precedence       <-- share / order sibling workloads
///             |
///      AllocationQueue       <-- leaf; reservations attach here (IAllocationQueue)
///             |  ^ requests  : insert / increase / decrease / remove
///  - - - - - -+- - - - - - - - - - - - - - -  scheduler thread / query threads
///             |  v approvals : increase / decrease / kill / fail
///     MemoryReservation      <-- owned by QueryStatus (a ResourceAllocation)
///             |                  syncWithMemoryTracker()
///      PipelineExecutor      <-- drives query execution
///             |                  read on each sync point
///       MemoryTracker        <-- actual bytes used (source of truth)
///
/// The `workload` setting resolves (via `WorkloadResourceManager`) to a `ResourceLink` naming that
/// workload's `AllocationQueue`. A reservation's life: construct (`insertAllocation`; blocks until
/// admitted when `reserve_memory > 0`), sync (`syncWithMemoryTracker` issues at most one increase or
/// decrease per call; may be killed under pressure), destruct (`removeAllocation`, wait for removal).
/// Requests bubble up to the root and are approved on the scheduler thread; the scheduler-side nodes
/// (`AllocationLimit`, `Fair`/`PrecedenceAllocation`, `AllocationQueue`) form one `ISpaceSharedNode`
/// subtree per `WorkloadNode`. See also `IAllocationQueue`, `ISpaceSharedNode`,
/// `IncreaseRequest`/`DecreaseRequest`.
struct MemoryReservation : public ResourceAllocation
{
public:
    // Blocks until reservation is admitted iff reserved_size > 0
    MemoryReservation(ResourceLink link, const String & id_, ResourceCost reserved_size, ResourceCost min_bytes_to_spill_);
    ~MemoryReservation() override;

    // Sync actual size with MemoryTracker, issues and waits increase/decrease requests as needed.
    void syncWithMemoryTracker(const MemoryTracker * memory_tracker);

    ResourceCost getTotalReclaimable();
    /// Reclaimable memory of the query's spillable processors, keyed by the object that owns the
    /// state so that processors sharing it are counted once.
    void updateReclaimable(const ISpillable * spillable, ResourceCost total_bytes);
    void removeReclaimable(const ISpillable * spillable);

    [[nodiscard]] ResourceCost takeSpillRequest(const ISpillable * spillable, ResourceCost spillable_bytes);
    void finishSpill(const ISpillable * spillable, ResourceCost settled_bytes, ResourceCost new_spillable_memory_bytes, const MemoryTracker * memory_tracker);

private:
    void throwIfNeeded();
    void reportReclaimable(ResourceCost total);

    // Unlinks this allocation from the scheduler and waits until removal completes.
    // Used both by the destructor and by the constructor when admission fails, so a throwing
    // constructor never leaves a dangling pointer in the scheduler.
    void detachFromQueue();

    // Interaction with the scheduler thread
    void killAllocation(const std::exception_ptr & reason) override;
    void spillAllocation(ResourceCost additional_bytes) override;
    void increaseApproved(const IncreaseRequest & increase) override;
    void decreaseApproved(const DecreaseRequest & decrease) override;
    void allocationFailed(const std::exception_ptr & reason) override;

    const ResourceCost reserved_size;
    const ResourceCost min_bytes_to_spill;

    /// Protects all the fields in this allocation that may be accessed from the scheduler thread.
    /// Lock ordering: AllocationQueue::mutex -> MemoryReservation::mutex (scheduler thread acquires
    /// AllocationQueue::mutex first, then calls callbacks that acquire this mutex).
    /// User-thread paths release this mutex before calling queue operations.
    std::mutex mutex;
    std::condition_variable cv;

    std::exception_ptr kill_reason;
    std::exception_ptr fail_reason;
    bool removed = false;
    ResourceCost allocated_size = 0; // equals ResourceAllocation::allocated, which is private and controlled by the scheduler
    ResourceCost actual_size = 0; // real size of the resource used by the allocation
    ResourceCost enqueued_demand = 0; // amount added to demand_increment when increase was enqueued
    ResourceCost enqueued_decrease = 0; // size of the in-flight decrease request

    /// Helper struct. Holds postponed ProfileEvents increments to be executed from a query thread.
    struct Metrics
    {
        UInt64 increases = 0;
        UInt64 decreases = 0;
        UInt64 failed = 0;
        UInt64 killed = 0;
        void apply();
    } metrics;

    /// Scheduler requested spilling
    ResourceCost enqueued_spill = 0;
    /// Number of processors that do spilling in parallel
    size_t spills_in_flight = 0;

    /// Reclaimable bytes per spillable object
    std::unordered_map<const ISpillable *, ResourceCost> reclaimable;
    /// Map of in progress objects, to avoid spilling them again (since multiple processors can share the same spilling object)
    std::unordered_set<const ISpillable *> reclaimable_in_progress;
    /// Sum of the map values
    ResourceCost reclaimable_total = 0;
    /// Last total sent to the scheduler (small updates are not sent)
    ResourceCost reported_reclaimable = 0;

    /// Introspection
    CurrentMetrics::Increment approved_increment;
    CurrentMetrics::Increment demand_increment;
    CurrentMetrics::Increment reclaimable_increment;
};

using MemoryReservationPtr = std::unique_ptr<MemoryReservation>;

}
