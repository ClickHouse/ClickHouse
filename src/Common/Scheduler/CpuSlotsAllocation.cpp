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

CpuSlotsAllocation::CpuSlotsAllocation(SlotCount min_, SlotCount max_, ResourceLink link_)
    : min(min_)
    , max(max_)
    , link(link_)
    , noncompeting(link ? min : max)
    , requests(max - min)
    , allocated(link ? min : max)
{
    for (CpuSlotRequest & request : requests)
        request.allocation = this;
    schedule(false);
}

CpuSlotsAllocation::~CpuSlotsAllocation()
{
    if (link)
    {
        std::unique_lock lock{schedule_mutex};
        if (allocated < max)
        {
            bool canceled = link.queue->cancelRequest(&requests[allocated - min]);
            if (!canceled)
            {
                // Request was not canceled, it means it is currently processed by the scheduler thread, we have to wait
                allocated = max; // to prevent enqueueing of the next request - see schedule()
                schedule_cv.wait(lock, [this] { return allocated == max + 1; });
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

    // If all non-competing slots are already acquired - try acquire granted (competing) slot
    value = granted.load();
    while (value)
    {
        if (granted.compare_exchange_strong(value, value - 1))
        {
            chassert(link);
            ProfileEvents::increment(ProfileEvents::ConcurrencyControlSlotsAcquired, 1);
            size_t index = last_acquire_index.fetch_add(1, std::memory_order_relaxed);
            return AcquiredSlotPtr(new AcquiredCpuSlot(shared_from_this(), &requests[index]));
        }
    }

    return {}; // avoid unnecessary locking
}

void CpuSlotsAllocation::grant()
{
    granted++;
    schedule(true);
}

void CpuSlotsAllocation::failed(const std::exception_ptr & ptr)
{
    std::scoped_lock lock{schedule_mutex};
    exception = ptr;
    noncompeting.store(exception_value);
    allocated = max + 1;
    schedule_cv.notify_one();
}

void CpuSlotsAllocation::schedule(bool next)
{
    std::scoped_lock lock{schedule_mutex};
    if (next)
        allocated++;
    if (allocated < max)
    {
        chassert(link);
        link.queue->enqueueRequest(&requests[allocated - min]);
    }
    if (allocated > max)
        schedule_cv.notify_one(); // notify waiting destructor
}

}
