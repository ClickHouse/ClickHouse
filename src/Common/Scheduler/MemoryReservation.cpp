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
        demand_increment.add(reserved_size);
    }

    // Lock ordering requires queue.mutex before allocation.mutex, so do not hold mutex here
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
    ResourceCost last_size = actual_size;
    actual_size = 0;
    if (allocated_size > 0) // Running allocation
    {
        queue.decreaseAllocation(*this, allocated_size);
        decrease_enqueued = true;
        cv.wait(lock, [this]() { return allocated_size == 0; });
    }
    else
    {
        queue.decreaseAllocation(*this, last_size);
        decrease_enqueued = true;
        // It can be either approved and decreased later or failed (i.e. canceled) right away
        cv.wait(lock, [this]() { return bool(fail_reason) || (!increase_enqueued && !decrease_enqueued && allocated_size == 0); });
    }
    chassert(removed);
    metrics.apply();
}

void MemoryReservation::syncWithMemoryTracker(const MemoryTracker * memory_tracker)
{
    std::unique_lock lock(mutex);
    throwIfNeeded();

    // Make sure reservation size is always respected
    ResourceCost new_actual_size = std::max(memory_tracker->get(), reserved_size);

    if (new_actual_size != actual_size)
    {
        actual_size = new_actual_size;
        syncWithScheduler();

        // Do not wait on decrease, but wait on increase to make sure memory is reserved when requested
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

void MemoryReservation::syncWithScheduler()
{
    if (!fail_reason && actual_size > allocated_size && !increase_enqueued)
    {
        chassert(!removed);
        ResourceCost increase_size = actual_size - allocated_size;
        queue.increaseAllocation(*this, increase_size);
        increase_enqueued = true;
        demand_increment.add(increase_size);
    }
    else if (!fail_reason && actual_size < allocated_size && !decrease_enqueued)
    {
        chassert(!removed);
        queue.decreaseAllocation(*this, allocated_size - actual_size);
        decrease_enqueued = true;
    }
    else if (actual_size == allocated_size)
    {
        cv.notify_all(); // notify dtor or syncWithMemoryTracker
    }
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
    demand_increment.sub(increase.size);
    increase_enqueued = false;
    syncWithScheduler();
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
    syncWithScheduler();
}

void MemoryReservation::allocationFailed(const std::exception_ptr & reason)
{
    std::unique_lock lock(mutex);
    metrics.failed++;
    fail_reason = reason;
    removed = true; // failed allocation are auto-removed by the scheduler
    if (increase_enqueued)
        demand_increment.sub(actual_size - allocated_size);
    approved_increment.sub(allocated_size);
    allocated_size = 0;
    cv.notify_all(); // notify dtor (e.g. for removal of pending allocation or queue purge) or syncWithMemoryTracker
}

}
