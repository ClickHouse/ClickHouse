#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Scheduler/IAllocationQueue.h>
#include <Common/MemoryTracker.h>
#include <base/defines.h>


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
{
    std::unique_lock lock(mutex);
    chassert(link.allocation_queue);
    actual_size = reserved_size;
    queue.insertAllocation(*this, reserved_size);
    if (reserved_size > 0)
        increase_enqueued = true;
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
        cv.wait(lock, [this]() { return allocated_size == 0; });
    }
    else
    {
        queue.decreaseAllocation(*this, last_size);
        // It can be either approved and decreased later or failed (i.e. canceled) right away
        cv.wait(lock, [this]() { return bool(fail_reason) || (!increase_enqueued && !decrease_enqueued && allocated_size == 0); });
    }
    chassert(removed);
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
        cv.wait(lock, [this] { return kill_reason || fail_reason || actual_size <= allocated_size; });
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

void MemoryReservation::syncWithScheduler()
{
    if (!fail_reason && actual_size > allocated_size && !increase_enqueued)
    {
        chassert(!removed);
        queue.increaseAllocation(*this, actual_size - allocated_size);
        increase_enqueued = true;
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
    kill_reason = reason;
    cv.notify_all(); // notify syncWithMemoryTracker
}

void MemoryReservation::increaseApproved(const IncreaseRequest & increase)
{
    std::unique_lock lock(mutex);
    allocated_size += increase.size;
    increase_enqueued = false;
    syncWithScheduler();
}

void MemoryReservation::decreaseApproved(const DecreaseRequest & decrease)
{
    std::unique_lock lock(mutex);
    chassert(allocated_size >= decrease.size);
    allocated_size -= decrease.size;
    decrease_enqueued = false;
    if (decrease.removing_allocation)
        removed = true;
    syncWithScheduler();
}

void MemoryReservation::allocationFailed(const std::exception_ptr & reason)
{
    std::unique_lock lock(mutex);
    fail_reason = reason;
    removed = true; // failed allocation are auto-removed by the scheduler
    allocated_size = 0;
    cv.notify_all(); // notify dtor (e.g. for removal of pending allocation or queue purge) or syncWithMemoryTracker
}

}
