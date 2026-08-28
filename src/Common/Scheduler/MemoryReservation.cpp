#include <Common/Scheduler/MemoryReservation.h>
#include <Common/Scheduler/IAllocationQueue.h>
#include <Common/MemoryTracker.h>
#include <Common/MemorySpillScheduler.h>
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
        bool admitted = false;
        {
            std::unique_lock lock(mutex);
            auto admit_timer = CurrentThread::getProfileEvents().timer(ProfileEvents::MemoryReservationAdmitMicroseconds);
            cv.wait(lock, [this] { return kill_reason || fail_reason || actual_size <= allocated_size; });
            // Flush deferred profile-event counters before potentially throwing,
            // so failure metrics (e.g. MemoryReservationFailed) are not lost.
            metrics.apply();
            admitted = !kill_reason && !fail_reason;
        }

        if (!admitted)
        {
            // `insertAllocation` above linked this object into the scheduler. Throwing straight
            // from the constructor would skip `~MemoryReservation`, so `removeAllocation` would
            // never run and the scheduler would keep a dangling pointer to a destroyed object
            // (the base `~ResourceAllocation` only has debug-only checks). Unlink first, then
            // report the failure.
            detachFromQueue();
            std::unique_lock lock(mutex);
            throwIfNeeded();
        }
    }
}

MemoryReservation::~MemoryReservation()
{
    detachFromQueue();
}

void MemoryReservation::detachFromQueue()
{
    {
        std::unique_lock lock(mutex);
        if (removed)
        {
            chassert(allocated_size == 0);
            metrics.apply();
            return;
        }
        if (fail_reason)
        {
            metrics.apply();
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
    std::shared_ptr<MemorySpillScheduler> recovery_scheduler;
    UInt64 observed_recovery_epoch = 0;
    {
        std::unique_lock lock(mutex);

        // Normal growth serializes all query threads. A parked request opens a narrow recovery lane:
        // the query may run spill/release work, but it still cannot acquire more reserved memory.
        if (increase_enqueued && !growth_recovery_active)
            cv.wait(lock, [this] { return !increase_enqueued || kill_reason || fail_reason || growth_recovery_active; });

        throwIfNeeded();

        if (increase_enqueued && growth_recovery_active)
        {
            recovery_scheduler = memory_spill_scheduler.lock();
            observed_recovery_epoch = recovery_epoch;
        }

        // Make sure reservation size is always respected.
        ResourceCost new_actual_size = std::max(memory_tracker->get(), reserved_size);
        if (new_actual_size != actual_size)
        {
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
    }

    // Called outside mutex to respect lock ordering (AllocationQueue::mutex -> this mutex).
    if (pending_increase > 0)
        queue.increaseAllocation(*this, pending_increase);
    else if (pending_decrease > 0)
        queue.decreaseAllocation(*this, pending_decrease);

    if (recovery_scheduler && observed_recovery_epoch != 0)
    {
        const auto result = recovery_scheduler->getForcedSpillResult(observed_recovery_epoch);
        if (result.outcome != MemorySpillScheduler::ForcedSpillOutcome::Pending)
        {
            bool notify_recovery_progress = false;
            {
                std::unique_lock lock(mutex);
                if (growth_recovery_active && recovery_epoch == observed_recovery_epoch
                    && reported_recovery_epoch < observed_recovery_epoch)
                {
                    reported_recovery_epoch = observed_recovery_epoch;
                    notify_recovery_progress = true;
                }
            }
            if (notify_recovery_progress)
                queue.notifyRecoveryProgress(*this);
        }
    }

    {
        std::unique_lock lock(mutex);
        // Wait on increase to make sure memory is reserved when requested. The recovery lane is the
        // sole exception: it returns control to the pipeline only so a spillable processor can run.
        if (actual_size > allocated_size && !growth_recovery_active)
        {
            auto increase_timer = CurrentThread::getProfileEvents().timer(ProfileEvents::MemoryReservationIncreaseMicroseconds);
            cv.wait(lock, [this] { return kill_reason || fail_reason || actual_size <= allocated_size || growth_recovery_active; });
        }

        metrics.apply();
        throwIfNeeded();
    }
}

void MemoryReservation::setMemorySpillScheduler(const std::shared_ptr<MemorySpillScheduler> & scheduler)
{
    std::unique_lock lock(mutex);
    memory_spill_scheduler = scheduler;
}

ResourceAllocation::GrowthPressureAction MemoryReservation::onGrowthPressure()
{
    std::shared_ptr<MemorySpillScheduler> scheduler;
    {
        std::unique_lock lock(mutex);
        scheduler = memory_spill_scheduler.lock();
    }

    if (!scheduler)
        return GrowthPressureAction::Protect;

    const auto spill_request = scheduler->requestForcedSpill();
    {
        std::unique_lock lock(mutex);
        growth_recovery_active = !spill_request.inject_priority;
        recovery_epoch = spill_request.epoch;
        cv.notify_all();
    }
    return spill_request.inject_priority ? GrowthPressureAction::Protect : GrowthPressureAction::Yield;
}

void MemoryReservation::onGrowthPressureResolved()
{
    std::shared_ptr<MemorySpillScheduler> scheduler;
    {
        std::unique_lock lock(mutex);
        growth_recovery_active = false;
        recovery_epoch = 0;
        reported_recovery_epoch = 0;
        scheduler = memory_spill_scheduler.lock();
        cv.notify_all();
    }
    if (scheduler)
        scheduler->finishMemoryPressure();
}

bool MemoryReservation::isGrowthRecoveryActive()
{
    std::unique_lock lock(mutex);
    return growth_recovery_active;
}

ResourceCost MemoryReservation::reconcilePendingIncrease(ResourceCost scheduler_allocated_size, ResourceCost requested_size)
{
    std::unique_lock lock(mutex);
    if (!increase_enqueued)
        return requested_size;

    const ResourceCost reconciled_size
        = actual_size > scheduler_allocated_size ? actual_size - scheduler_allocated_size : 0;
    if (reconciled_size > enqueued_demand)
        demand_increment.add(reconciled_size - enqueued_demand);
    else if (reconciled_size < enqueued_demand)
        demand_increment.sub(enqueued_demand - reconciled_size);
    enqueued_demand = reconciled_size;
    return reconciled_size;
}

void MemoryReservation::increaseCancelled()
{
    std::unique_lock lock(mutex);
    enqueued_demand = 0;
    increase_enqueued = false;
    cv.notify_all();
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
    onGrowthPressureResolved();
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
    {
        // The queue cancels any pending increase as part of the removal path
        // (`processActivation` unlinks from `increasing_allocations` without calling
        // `increaseApproved`). Roll back the demand metric and clear
        // `increase_enqueued` so threads blocked on the serialization barrier in
        // `syncWithMemoryTracker` are released and do not wait forever.
        if (increase_enqueued)
        {
            demand_increment.sub(enqueued_demand);
            enqueued_demand = 0;
            increase_enqueued = false;
        }
        removed = true;
    }
    cv.notify_all();
}

void MemoryReservation::allocationFailed(const std::exception_ptr & reason)
{
    onGrowthPressureResolved();
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
