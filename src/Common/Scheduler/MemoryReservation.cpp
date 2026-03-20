#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Scheduler/IAllocationQueue.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <base/defines.h>


namespace ProfileEvents
{
    extern const Event MemoryReservationAdmitMicroseconds;
    extern const Event MemoryReservationIncreaseMicroseconds;
    extern const Event MemoryReservationIncreases;
    extern const Event MemoryReservationDecreases;
    extern const Event MemoryReservationKilled;
    extern const Event MemoryReservationFailed;
}

namespace CurrentMetrics
{
    extern const Metric MemoryReservationApproved;
    extern const Metric MemoryReservationDemand;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_RESERVATION_KILLED;
    extern const int MEMORY_RESERVATION_FAILED;
}

MemoryReservation::MemoryReservation(ResourceLink link, const String & id_, ResourceCost reserved_size_)
    : ResourceAllocation(*link.allocation_queue, id_)
    , reserved_size(reserved_size_)
    , approved_increment(CurrentMetrics::MemoryReservationApproved, 0)
    , demand_increment(CurrentMetrics::MemoryReservationDemand, 0)
{
    chassert(link.allocation_queue);
    actual_size = reserved_size;

    if (reserved_size > 0)
    {
        // Scheduler may call increaseApproved() immediately after insert, so set state beforehand
        increase_enqueued = true;
        enqueued_demand = reserved_size;
        demand_increment.add(enqueued_demand);
    }

    queue.insertAllocation(*this, reserved_size);

    if (reserved_size > 0)
    {
        std::unique_lock lock(mutex);
        auto admit_timer = CurrentThread::getProfileEvents().timer(ProfileEvents::MemoryReservationAdmitMicroseconds);
        cv.wait(lock, [this] { return kill_reason || fail_reason || actual_size <= allocated_size; });
        throwIfNeeded();
    }
}

MemoryReservation::~MemoryReservation()
{
    {
        std::unique_lock lock(mutex);
        if (removed)
        {
            chassert(allocated_size == 0);
            return;
        }
        if (fail_reason)
        {
            return;
        }
        actual_size = 0;
    }

    // removeAllocation handles everything on the scheduler thread:
    // cancels any pending increase, prepares decrease to zero.
    queue.removeAllocation(*this);

    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [this]() { return removed || fail_reason; });
        metrics.apply();
    }
}

void MemoryReservation::syncWithMemoryTracker(const MemoryTracker * memory_tracker)
{
    ResourceCost pending_increase = 0;
    ResourceCost pending_decrease = 0;
    {
        std::unique_lock lock(mutex);

        // Serialization: block all threads while an increase is pending.
        // Multiple query threads may call syncWithMemoryTracker concurrently
        // (the MemoryTracker reflects total memory across all threads).
        // By blocking here we ensure at most one increase is in flight at a time.
        if (increase_enqueued)
            cv.wait(lock, [this] { return !increase_enqueued || kill_reason || fail_reason; });

        throwIfNeeded();

        // Make sure reservation size is always respected
        ResourceCost new_actual_size = std::max(memory_tracker->get(), reserved_size);

        if (new_actual_size == actual_size)
            return;

        actual_size = new_actual_size;

        if (!fail_reason && actual_size > allocated_size && !increase_enqueued)
        {
            chassert(!removed);
            pending_increase = actual_size - allocated_size;
            increase_enqueued = true;
            enqueued_demand = pending_increase;
            demand_increment.add(enqueued_demand);
        }
        else if (!fail_reason && actual_size < allocated_size && !decrease_enqueued)
        {
            chassert(!removed);
            pending_decrease = allocated_size - actual_size;
            decrease_enqueued = true;
        }
    }

    // Called outside mutex to respect lock ordering (AllocationQueue::mutex -> this mutex).
    if (pending_increase > 0)
        queue.increaseAllocation(*this, pending_increase);
    else if (pending_decrease > 0)
        queue.decreaseAllocation(*this, pending_decrease);

    {
        std::unique_lock lock(mutex);
        // Wait on increase to make sure memory is reserved when requested.
        // Decrease is not waited for — it will be processed asynchronously.
        if (actual_size > allocated_size)
        {
            auto increase_timer = CurrentThread::getProfileEvents().timer(ProfileEvents::MemoryReservationIncreaseMicroseconds);
            cv.wait(lock, [this] { return kill_reason || fail_reason || actual_size <= allocated_size; });
        }

        metrics.apply();
        throwIfNeeded();
    }
}

void MemoryReservation::throwIfNeeded()
{
    if (kill_reason)
        throw Exception(ErrorCodes::MEMORY_RESERVATION_KILLED, "Kill reason: {}", getExceptionMessage(kill_reason, /* with_stacktrace = */ false));
    if (fail_reason)
        throw Exception(ErrorCodes::MEMORY_RESERVATION_FAILED, "Fail reason: {}", getExceptionMessage(fail_reason, /* with_stacktrace = */ false));
}

void MemoryReservation::Metrics::apply()
{
    if (increases)
        ProfileEvents::increment(ProfileEvents::MemoryReservationIncreases, increases);
    if (decreases)
        ProfileEvents::increment(ProfileEvents::MemoryReservationDecreases, decreases);
    if (failed)
        ProfileEvents::increment(ProfileEvents::MemoryReservationFailed, failed);
    if (killed)
        ProfileEvents::increment(ProfileEvents::MemoryReservationKilled, killed);
    increases = 0;
    decreases = 0;
    failed = 0;
    killed = 0;
}

void MemoryReservation::killAllocation(const std::exception_ptr & reason)
{
    std::unique_lock lock(mutex);
    metrics.killed++;
    kill_reason = reason;
    cv.notify_all(); // notify syncWithMemoryTracker
}

void MemoryReservation::increaseApproved(const IncreaseRequest & increase)
{
    std::unique_lock lock(mutex);
    metrics.increases++;
    allocated_size += increase.size;
    approved_increment.add(increase.size);
    demand_increment.sub(enqueued_demand);
    enqueued_demand = 0;
    increase_enqueued = false;
    cv.notify_all();
}

void MemoryReservation::decreaseApproved(const DecreaseRequest & decrease)
{
    std::unique_lock lock(mutex);
    metrics.decreases++;
    chassert(allocated_size >= decrease.size);
    allocated_size -= decrease.size;
    approved_increment.sub(decrease.size);
    decrease_enqueued = false;
    if (decrease.removing_allocation)
        removed = true;
    cv.notify_all();
}

void MemoryReservation::allocationFailed(const std::exception_ptr & reason)
{
    std::unique_lock lock(mutex);
    metrics.failed++;
    fail_reason = reason;
    removed = true; // failed allocation are auto-removed by the scheduler
    if (increase_enqueued)
        demand_increment.sub(enqueued_demand);
    approved_increment.sub(allocated_size);
    allocated_size = 0;
    cv.notify_all(); // notify dtor (e.g. for removal of pending allocation or queue purge) or syncWithMemoryTracker
}

}
