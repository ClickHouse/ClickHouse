#include <Interpreters/QuerySlot.h>

#include <Common/Scheduler/IAllocationQueue.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>

namespace ProfileEvents
{
    extern const Event ConcurrentQueryWaitMicroseconds;
    extern const Event ConcurrentQuerySlotsAcquired;
}

namespace CurrentMetrics
{
    extern const Metric ConcurrentQueryScheduled;
    extern const Metric ConcurrentQueryAcquired;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_ACCESS_DENIED;
    extern const int QUERY_SLOT_ACQUISITION_TIMEOUT;
}

QuerySlot::QuerySlot(ResourceLink link_, const String & id_, std::chrono::steady_clock::time_point admission_deadline_)
    : ResourceAllocation(*link_.allocation_queue, id_)
{
    chassert(link_.allocation_queue);

    // Throws if the allocation is rejected right away (e.g. `max_waiting_queries` is reached).
    // In that case the allocation is not linked into the queue, so it is safe to throw from the constructor.
    queue.insertAllocation(*this, 1);

    bool timed_out = false;
    {
        CurrentMetrics::Increment scheduled(CurrentMetrics::ConcurrentQueryScheduled);
        auto timer = CurrentThread::getProfileEvents().timer(ProfileEvents::ConcurrentQueryWaitMicroseconds);
        std::unique_lock lock{mutex};
        // An infinite deadline (`time_point::max()`) means no timeout: wait_until never fires on time and
        // blocks until the slot is granted or the allocation fails, exactly like an untimed wait.
        timed_out = !cv.wait_until(lock, admission_deadline_, [this] { return granted || fail_reason; });
    }

    if (timed_out || fail_reason)
    {
        // The allocation is linked into the scheduler unless it has failed. Throwing straight from the
        // constructor would skip the destructor, so unlink first, then report the failure.
        detachFromQueue();
        if (timed_out)
            throw Exception(ErrorCodes::QUERY_SLOT_ACQUISITION_TIMEOUT,
                "Timed out waiting to acquire a query slot for workload scheduling (exceeded workload_admission_timeout_ms)");
        throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "Unable to obtain a query slot: {}",
            getExceptionMessage(fail_reason, /* with_stacktrace = */ false));
    }

    ProfileEvents::increment(ProfileEvents::ConcurrentQuerySlotsAcquired);
    acquired_slot_increment.emplace(CurrentMetrics::ConcurrentQueryAcquired);
}

QuerySlot::~QuerySlot()
{
    acquired_slot_increment.reset();
    detachFromQueue();
}

void QuerySlot::detachFromQueue()
{
    {
        std::unique_lock lock{mutex};
        if (removed)
            return;
    }

    // Must be called without holding `mutex` (lock order: queue mutex -> this mutex, see callbacks).
    // A pending allocation is canceled, a granted one is released by a decrease to zero.
    queue.removeAllocation(*this);

    std::unique_lock lock{mutex};
    cv.wait(lock, [this] { return removed; });
}

void QuerySlot::increaseApproved(const IncreaseRequest &)
{
    std::scoped_lock lock{mutex};
    granted = true;
    cv.notify_all();
}

void QuerySlot::decreaseApproved(const DecreaseRequest & decrease)
{
    std::scoped_lock lock{mutex};
    if (decrease.removing_allocation)
        removed = true;
    cv.notify_all();
}

void QuerySlot::allocationFailed(const std::exception_ptr & reason)
{
    std::scoped_lock lock{mutex};
    fail_reason = reason;
    removed = true; // Failed allocations are removed by the scheduler
    cv.notify_all();
}

void QuerySlot::killAllocation(const std::exception_ptr &)
{
    // Query slots are never evicted: the `QUERY` resource limits are built without eviction.
    chassert(false);
}

}
