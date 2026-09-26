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
    extern const int MEMORY_RESERVATION_ACQUISITION_TIMEOUT;
}

MemoryReservation::MemoryReservation(ResourceLink link, const String & id_, ResourceCost reserved_size_)
    : MemoryReservation(link, id_, reserved_size_, std::chrono::steady_clock::time_point::max(), Settings{})
{
}

MemoryReservation::MemoryReservation(ResourceLink link, const String & id_, ResourceCost reserved_size_, Settings settings_)
    : MemoryReservation(link, id_, reserved_size_, std::chrono::steady_clock::time_point::max(), settings_)
{
}

MemoryReservation::MemoryReservation(
    ResourceLink link,
    const String & id_,
    ResourceCost reserved_size_,
    std::chrono::steady_clock::time_point admission_deadline_)
    : MemoryReservation(link, id_, reserved_size_, admission_deadline_, Settings{})
{
}

MemoryReservation::MemoryReservation(
    ResourceLink link,
    const String & id_,
    ResourceCost reserved_size_,
    std::chrono::steady_clock::time_point admission_deadline_,
    Settings settings_)
    : ResourceAllocation(*link.allocation_queue, id_, settings_.pressure_policy)
    , reserved_size(reserved_size_)
    , settings(settings_)
    , approved_increment(CurrentMetrics::MemoryReservationApproved, 0)
    , demand_increment(CurrentMetrics::MemoryReservationDemand, 0)
{
    chassert(link.allocation_queue);
    actual_size = reserved_size;

    if (reserved_size > 0)
    {
        // Scheduler may call increaseApproved() immediately after insert, so set state beforehand
        enqueued_demand = reserved_size;
        demand_increment.add(enqueued_demand);
    }

    queue.insertAllocation(*this, reserved_size);

    if (reserved_size > 0)
    {
        bool admitted = false;
        bool timed_out = false;
        {
            std::unique_lock lock(mutex);
            auto admit_timer = CurrentThread::getProfileEvents().timer(ProfileEvents::MemoryReservationAdmitMicroseconds);
            auto admitted_pred = [this] { return kill_reason || fail_reason || actual_size <= allocated_size; };
            // An infinite deadline (`time_point::max()`) means no timeout: wait_until never fires on time
            // and blocks until the reservation is admitted, killed, or failed.
            timed_out = !cv.wait_until(lock, admission_deadline_, admitted_pred);
            // Flush deferred profile-event counters before potentially throwing,
            // so failure metrics (e.g. MemoryReservationFailed) are not lost.
            metrics.apply();
            admitted = !kill_reason && !fail_reason && actual_size <= allocated_size;
        }

        if (!admitted)
        {
            // `insertAllocation` above linked this object into the scheduler. Throwing straight
            // from the constructor would skip `~MemoryReservation`, so `removeAllocation` would
            // never run and the scheduler would keep a dangling pointer to a destroyed object
            // (the base `~ResourceAllocation` only has debug-only checks). Unlink first, then report
            // the failure.
            detachFromQueue();
            std::unique_lock lock(mutex);
            // A timeout takes precedence over the generic failure. Cancelling a still-pending
            // reservation in `detachFromQueue` routes through `AllocationQueue::processActivation`,
            // which fails it with a generic cancellation error; so when we stopped waiting because the
            // deadline passed, report that as the admission timeout instead of letting `throwIfNeeded`
            // surface the cancellation as `MEMORY_RESERVATION_FAILED`.
            if (timed_out)
                throw Exception(ErrorCodes::MEMORY_RESERVATION_ACQUISITION_TIMEOUT,
                    "Timed out waiting to acquire a memory reservation for workload scheduling (exceeded workload_admission_timeout_ms)");
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
    while (true)
    {
        ResourceCost pending_increase = 0;
        ResourceCost pending_decrease = 0;
        std::shared_ptr<MemorySpillScheduler> recovery_scheduler;
        std::shared_ptr<MemoryRecoveryEpisode> observed_recovery;
        auto recovery_deadline = std::chrono::steady_clock::time_point::max();
        bool recovery_timed_out = false;
        {
            std::unique_lock lock(mutex);

            if (enqueued_demand != 0 && !growth_recovery_active)
                cv.wait(lock, [this] { return enqueued_demand == 0 || kill_reason || fail_reason || growth_recovery_active; });

            throwIfNeeded();

            if (enqueued_demand != 0 && growth_recovery_active)
            {
                recovery_scheduler = memory_spill_scheduler.lock();
                observed_recovery = recovery_episode;
                if (settings.recovery_timeout_ms > 0)
                {
                    recovery_deadline = recovery_started_at + std::chrono::milliseconds(settings.recovery_timeout_ms);
                    recovery_timed_out = std::chrono::steady_clock::now() >= recovery_deadline;
                }
            }

            ResourceCost new_actual_size = std::max(memory_tracker->get(), reserved_size);
            ResourceCost expected_allocated = allocated_size - enqueued_decrease;
            actual_size = new_actual_size;

            if (actual_size > expected_allocated && enqueued_demand == 0)
            {
                chassert(!removed);
                pending_increase = actual_size - expected_allocated;
                enqueued_demand = pending_increase;
                demand_increment.add(enqueued_demand);
            }
            else if (actual_size < expected_allocated && enqueued_decrease == 0)
            {
                chassert(!removed);
                pending_decrease = expected_allocated - actual_size;
                enqueued_decrease = pending_decrease;
            }
        }

        if (pending_increase > 0)
            queue.increaseAllocation(*this, pending_increase);
        else if (pending_decrease > 0)
            queue.decreaseAllocation(*this, pending_decrease);

        if (recovery_scheduler && observed_recovery)
        {
            if (!recovery_timed_out)
            {
                if (recovery_deadline == std::chrono::steady_clock::time_point::max())
                    recovery_scheduler->executeForcedSpill(observed_recovery);
                else
                    recovery_scheduler->executeForcedSpillUntil(observed_recovery, recovery_deadline);
            }

            recovery_scheduler->rethrowIfFailed(observed_recovery);

            if (recovery_deadline != std::chrono::steady_clock::time_point::max())
                recovery_timed_out = std::chrono::steady_clock::now() >= recovery_deadline;

            const auto result = recovery_scheduler->getForcedSpillResult(observed_recovery);
            if (result.outcome != MemorySpillScheduler::ForcedSpillOutcome::Pending || recovery_timed_out)
            {
                bool notify_recovery_progress = false;
                {
                    std::unique_lock lock(mutex);
                    actual_size = std::max(memory_tracker->get(), reserved_size);
                    if (growth_recovery_active
                        && recovery_episode == observed_recovery
                        && !recovery_progress_reported)
                    {
                        recovery_progress_reported = true;
                        growth_recovery_active = false;
                        notify_recovery_progress = true;
                        cv.notify_all();
                    }
                }
                if (notify_recovery_progress)
                {
                    recovery_scheduler->finishMemoryPressure(observed_recovery);
                    queue.notifyRecoveryProgress(*this);
                }
            }
        }

        {
            std::unique_lock lock(mutex);
            if (actual_size > allocated_size - enqueued_decrease && !growth_recovery_active)
            {
                auto increase_timer = CurrentThread::getProfileEvents().timer(ProfileEvents::MemoryReservationIncreaseMicroseconds);
                cv.wait(lock, [this]
                {
                    return kill_reason || fail_reason || actual_size <= allocated_size - enqueued_decrease || growth_recovery_active;
                });
            }

            metrics.apply();
            throwIfNeeded();
            if (!growth_recovery_active)
                return;
        }
    }
}

void MemoryReservation::setMemorySpillScheduler(const std::shared_ptr<MemorySpillScheduler> & scheduler)
{
    std::unique_lock lock(mutex);
    memory_spill_scheduler = scheduler;
}

ResourceAllocation::GrowthPressureAction MemoryReservation::onGrowthPressure()
{
    if (!settings.force_spill_before_eviction)
        return GrowthPressureAction::Protect;

    std::shared_ptr<MemorySpillScheduler> scheduler;
    {
        std::unique_lock lock(mutex);
        scheduler = memory_spill_scheduler.lock();
    }

    if (!scheduler)
        return GrowthPressureAction::Protect;

    auto episode = scheduler->requestForcedSpill();
    {
        std::unique_lock lock(mutex);
        growth_recovery_active = true;
        recovery_episode = std::move(episode);
        recovery_progress_reported = false;
        recovery_started_at = std::chrono::steady_clock::now();
        cv.notify_all();
    }
    return GrowthPressureAction::Yield;
}

void MemoryReservation::onGrowthPressureResolved()
{
    std::shared_ptr<MemorySpillScheduler> scheduler;
    std::shared_ptr<MemoryRecoveryEpisode> episode;
    {
        std::unique_lock lock(mutex);
        growth_recovery_active = false;
        recovery_progress_reported = false;
        recovery_started_at = {};
        scheduler = memory_spill_scheduler.lock();
        episode = recovery_episode;
        cv.notify_all();
    }
    if (scheduler)
        scheduler->finishMemoryPressure(episode);
}

bool MemoryReservation::isGrowthRecoveryActive()
{
    std::unique_lock lock(mutex);
    return growth_recovery_active;
}

ResourceCost MemoryReservation::reconcilePendingIncrease(ResourceCost scheduler_allocated_size, ResourceCost requested_size)
{
    std::unique_lock lock(mutex);
    if (enqueued_demand == 0)
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
    cv.notify_all();
}

void MemoryReservation::throwIfNeeded()
{
    if (kill_reason)
        throw Exception(ErrorCodes::MEMORY_RESERVATION_KILLED, "Kill reason: {}", getExceptionMessage(kill_reason, /* with_stacktrace = */ false));
    if (fail_reason)
        throw Exception(ErrorCodes::MEMORY_RESERVATION_FAILED, "Fail reason: {}", getExceptionMessage(fail_reason, /* with_stacktrace = */ false));

    /// A recovery timeout stops waiting, not ownership of an in-flight spill failure. Keep checking
    /// the originating episode on later reservation sync points until a new safe episode replaces it.
    if (recovery_episode)
    {
        std::lock_guard episode_lock(recovery_episode->mutex);
        if (recovery_episode->exception)
            std::rethrow_exception(recovery_episode->exception);
    }
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
    {
        std::unique_lock lock(mutex);
        metrics.killed++;
        kill_reason = reason;
        cv.notify_all(); // notify syncWithMemoryTracker
    }
    onGrowthPressureResolved();
}

void MemoryReservation::increaseApproved(const IncreaseRequest & increase)
{
    std::unique_lock lock(mutex);
    metrics.increases++;
    allocated_size += increase.size;
    approved_increment.add(increase.size);
    demand_increment.sub(enqueued_demand);
    enqueued_demand = 0;
    cv.notify_all();
}

void MemoryReservation::decreaseApproved(const DecreaseRequest & decrease)
{
    std::unique_lock lock(mutex);
    metrics.decreases++;
    chassert(allocated_size >= decrease.size);
    allocated_size -= decrease.size;
    approved_increment.sub(decrease.size);
    enqueued_decrease = 0;
    if (decrease.removing_allocation)
    {
        // The queue cancels any pending increase as part of the removal path
        // (`processActivation` unlinks from `increasing_allocations` without calling
        // `increaseApproved`). Roll back the demand so threads blocked on the
        // serialization barrier in `syncWithMemoryTracker` are released.
        if (enqueued_demand != 0)
        {
            demand_increment.sub(enqueued_demand);
            enqueued_demand = 0;
        }
        removed = true;
    }
    cv.notify_all();
}

void MemoryReservation::allocationFailed(const std::exception_ptr & reason)
{
    {
        std::unique_lock lock(mutex);
        metrics.failed++;
        fail_reason = reason;
        removed = true; // failed allocation are auto-removed by the scheduler
        if (enqueued_demand != 0)
            demand_increment.sub(enqueued_demand);
        approved_increment.sub(allocated_size);
        allocated_size = 0;
        cv.notify_all(); // notify dtor (e.g. for removal of pending allocation or queue purge) or syncWithMemoryTracker
    }
    onGrowthPressureResolved();
}

}
