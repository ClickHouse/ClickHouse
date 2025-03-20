#include <mutex>

#include <base/scope_guard.h>

#include <Common/Scheduler/CpuSlotsAllocation.h>
#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event ConcurrencyControlSlotsGranted;
    extern const Event ConcurrencyControlSlotsDelayed;
    extern const Event ConcurrencyControlSlotsAcquired;
    extern const Event ConcurrencyControlSlotsAcquiredNonCompeting;
    extern const Event ConcurrencyControlQueriesDelayed;
}

namespace CurrentMetrics
{
    extern const Metric ConcurrencyControlAcquired;
    extern const Metric ConcurrencyControlAcquiredNonCompeting;
    extern const Metric ConcurrencyControlSoftLimit;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int RESOURCE_ACCESS_DENIED;
}

void CpuSlotRequest::execute()
{
    chassert(allocation);
    allocation->grant();
}

void CpuSlotRequest::failed(const std::exception_ptr & ptr)
{
    chassert(allocation);
    allocation->failed(ptr);
}

AcquiredCpuSlot::AcquiredCpuSlot(SlotAllocationPtr && allocation_, CpuSlotRequest * request_)
    : allocation(std::move(allocation_))
    , request(request_)
    , acquired_slot_increment(request ? CurrentMetrics::ConcurrencyControlAcquired : CurrentMetrics::ConcurrencyControlAcquiredNonCompeting)
{}

AcquiredCpuSlot::~AcquiredCpuSlot()
{
    if (request)
        request->finish();
}

CpuSlotsAllocation::CpuSlotsAllocation(SlotCount master_slots_, SlotCount worker_slots_, ResourceLink master_link_, ResourceLink worker_link_)
    : master_slots(master_slots_)
    , total_slots(master_slots_ + worker_slots_)
    , noncompeting_slots(!master_link_ * master_slots_ + !worker_link_ * worker_slots_)
    , master_link(master_link_)
    , worker_link(worker_link_)
    , requests(total_slots - noncompeting_slots) // NOTE: it should not be reallocated after initialization because AcquiredCpuSlot holds raw pointer
    , current_request(&requests.front())
{
    for (CpuSlotRequest & request : requests)
        request.allocation = this;

    std::unique_lock lock{schedule_mutex};
    while (allocated < total_slots)
    {
        if (ISchedulerQueue * queue = getCurrentQueue(lock)) // competing slot - use scheduler
        {
            queue->enqueueRequest(current_request);
            break;
        }
        else // noncompeting slot - provide for free
        {
            allocated++;
            noncompeting++;
        }
    }
}

CpuSlotsAllocation::~CpuSlotsAllocation()
{
    if (master_link || worker_link)
    {
        std::unique_lock lock{schedule_mutex};
        if (allocated < total_slots)
        {
            bool canceled = getCurrentQueue(lock)->cancelRequest(current_request);
            if (!canceled)
            {
                // Request was not canceled, it means it is currently processed by the scheduler thread, we have to wait
                allocated = total_slots; // to prevent enqueueing of the next request - see schedule()
                schedule_cv.wait(lock, [this] { return allocated == total_slots + 1; });
            }
        }
    }
}

[[nodiscard]] AcquiredSlotPtr CpuSlotsAllocation::tryAcquire()
{
    // First try acquire non-competing slot (if any)
    SlotCount value = noncompeting.load();
    while (value)
    {
        if (value == exception_value)
        {
            std::scoped_lock lock{schedule_mutex};
            throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "CPU Resource request failed: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));
        }

        if (noncompeting.compare_exchange_strong(value, value - 1))
        {
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquiredNonCompeting, 1);
            return AcquiredSlotPtr(new AcquiredCpuSlot({}, nullptr));
        }
    }

    // If all non-competing slots are already acquired - try acquire granted competing slot
    value = granted.load();
    while (value)
    {
        if (granted.compare_exchange_strong(value, value - 1))
        {
            std::unique_lock lock{schedule_mutex};
            // Grant noncompeting postponed slots if any, see grant()
            while (allocated < total_slots && !getCurrentQueue(lock))
            {
                allocated++;
                noncompeting++;
            }

            // Make and return acquired slot
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquired, 1);
            size_t index = last_acquire_index.fetch_add(1, std::memory_order_relaxed);
            return AcquiredSlotPtr(new AcquiredCpuSlot(shared_from_this(), &requests[index]));
        }
    }

    return {}; // avoid unnecessary locking
}

[[nodiscard]] AcquiredSlotPtr CpuSlotsAllocation::acquire()
{
    // lock-free shortcut
    if (auto result = tryAcquire())
        return result;

    // TODO(serxa): this logic could be optimized with FUTEX_WAIT
    std::unique_lock lock{schedule_mutex};
    waiters++;
    SCOPE_EXIT({ waiters--; });
    while (true) {
        schedule_cv.wait(lock, [this] { return granted > 0 || noncompeting > 0; });
        if (auto result = tryAcquire())
            return result;
        // NOTE: We need retries because there are lock-free code paths that could acquire slot in another thread regardless of schedule_mutex
    }
}

void CpuSlotsAllocation::failed(const std::exception_ptr & ptr)
{
    std::scoped_lock lock{schedule_mutex};
    exception = ptr;
    noncompeting.store(exception_value);
    allocated = total_slots + 1;
    schedule_cv.notify_all();
}

void CpuSlotsAllocation::grant()
{
    std::unique_lock lock{schedule_mutex};

    // Grant one request
    granted++;
    allocated++;
    current_request++;
    if (waiters > 0 || allocated > total_slots)
        schedule_cv.notify_one(); // notify either waiting acquire() caller or destructor, both not possible simultaneously

    // Enqueue another resource request if necessary
    if (allocated < total_slots)
    {
        // TODO(serxa): we should not request more slots if we already have at least 2 granted and not acquired slots to avoid holding unnecessary slots
        if (ISchedulerQueue * queue = getCurrentQueue(lock)) // competing slot - use scheduler
            queue->enqueueRequest(current_request);
        // NOTE: if the next slot is noncompeting - postpone granting it to avoid it being acquired too early
    }
}

ISchedulerQueue * CpuSlotsAllocation::getCurrentQueue(const std::unique_lock<std::mutex> &)
{
    return allocated < master_slots ? master_link.queue : worker_link.queue;
}

}
