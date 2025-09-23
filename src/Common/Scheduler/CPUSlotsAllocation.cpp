#include <mutex>

#include <base/scope_guard.h>

#include <Common/Scheduler/CPUSlotsAllocation.h>
#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>

namespace ProfileEvents
{
    extern const Event ConcurrencyControlWaitMicroseconds;
    extern const Event ConcurrencyControlSlotsAcquired;
    extern const Event ConcurrencyControlSlotsAcquiredNonCompeting;
}

namespace CurrentMetrics
{
    extern const Metric ConcurrencyControlScheduled;
    extern const Metric ConcurrencyControlAcquired;
    extern const Metric ConcurrencyControlAcquiredNonCompeting;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_ACCESS_DENIED;
}

// This function is called by the scheduler thread and it makes sure that the next request
// is send while executing the previous one from the scheduler thread to make sure constant competition.
// Unfortunately, when this is the only request all the nodes are deactivated and reactivated again.
// It is considered as busy period end, which can be seen in `system.scheduler`.
// It costs some CPU cycles, but fairness is okay because there is no competition in that case.
void CPUSlotRequest::execute()
{
    chassert(allocation);
    allocation->grant();
}

void CPUSlotRequest::failed(const std::exception_ptr & ptr)
{
    chassert(allocation);
    allocation->failed(ptr);
}

AcquiredCPUSlot::AcquiredCPUSlot(SlotAllocationPtr && allocation_, CPUSlotRequest * request_)
    : allocation(std::move(allocation_))
    , request(request_)
    , acquired_slot_increment(request ? CurrentMetrics::ConcurrencyControlAcquired : CurrentMetrics::ConcurrencyControlAcquiredNonCompeting)
{}

AcquiredCPUSlot::~AcquiredCPUSlot()
{
    if (request)
        request->finish();
}

CPUSlotsAllocation::CPUSlotsAllocation(SlotCount master_slots_, SlotCount worker_slots_, ResourceLink master_link_, ResourceLink worker_link_)
    : master_slots(master_slots_)
    , total_slots(master_slots_ + worker_slots_)
    , noncompeting_slots(!master_link_ * master_slots_ + !worker_link_ * worker_slots_)
    , master_link(master_link_)
    , worker_link(worker_link_)
    , requests(total_slots - noncompeting_slots) // NOTE: it should not be reallocated after initialization because AcquiredCPUSlot holds raw pointer
    , current_request(requests.empty() ? nullptr : &requests.front())
{
    for (CPUSlotRequest & request : requests)
        request.allocation = this;

    std::unique_lock lock{schedule_mutex};
    while (allocated < total_slots)
    {
        if (ISchedulerQueue * queue = getCurrentQueue(lock)) // competing slot - use scheduler
        {
            queue->enqueueRequest(current_request);
            scheduled_slot_increment.emplace(CurrentMetrics::ConcurrencyControlScheduled);
            wait_timer.emplace(CurrentThread::getProfileEvents().timer(ProfileEvents::ConcurrencyControlWaitMicroseconds));
            break;
        }
        else // noncompeting slot - provide for free
        {
            allocated++;
            noncompeting++;
        }
    }
}

CPUSlotsAllocation::~CPUSlotsAllocation()
{
    if (master_link || worker_link)
    {
        std::unique_lock lock{schedule_mutex};
        if (allocated < total_slots)
        {
            if (ISchedulerQueue * queue = getCurrentQueue(lock))
            {
                bool canceled = queue->cancelRequest(current_request);
                if (!canceled)
                {
                    // Request was not canceled, it means it is currently processed by the scheduler thread, we have to wait
                    allocated = total_slots; // to prevent enqueueing of the next request - see grant()
                    schedule_cv.wait(lock, [this] { return allocated == total_slots + 1; });
                }
            }
        }

        // Finish resource requests for unacquired slots
        for (size_t index = last_acquire_index; granted > 0; --granted, ++index)
            requests[index].finish();
    }
}

[[nodiscard]] AcquiredSlotPtr CPUSlotsAllocation::tryAcquire()
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
            return AcquiredSlotPtr(new AcquiredCPUSlot({}, nullptr));
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
            return AcquiredSlotPtr(new AcquiredCPUSlot(shared_from_this(), &requests[index]));
        }
    }

    return {}; // avoid unnecessary locking
}

[[nodiscard]] AcquiredSlotPtr CPUSlotsAllocation::acquire()
{
    while (true)
    {
        if (auto result = tryAcquire())
            return result;
        std::unique_lock lock{schedule_mutex};
        waiters++;
        schedule_cv.wait(lock, [this] { return granted > 0 || noncompeting > 0; });
        waiters--;
    }
}

void CPUSlotsAllocation::failed(const std::exception_ptr & ptr)
{
    std::scoped_lock lock{schedule_mutex};
    exception = ptr;
    noncompeting.store(exception_value);
    allocated = total_slots + 1;
    scheduled_slot_increment.reset();
    wait_timer.reset();
    schedule_cv.notify_all();
}

void CPUSlotsAllocation::grant()
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
        {
            queue->enqueueRequest(current_request);
            return;
        }
        // NOTE: if the next slot is noncompeting - postpone granting it to avoid it being acquired too early
    }

    // If no new request was enqueued - update metrics and profile events
    scheduled_slot_increment.reset();
    wait_timer.reset();
}

ISchedulerQueue * CPUSlotsAllocation::getCurrentQueue(const std::unique_lock<std::mutex> &) const
{
    return allocated < master_slots ? master_link.queue : worker_link.queue;
}

bool CPUSlotsAllocation::isRequesting() const
{
    std::unique_lock lock{schedule_mutex};
    if (allocated < total_slots && getCurrentQueue(lock))
        // Caller should make sure that request will not be dequeued by the scheduler thread. Otherwise, it will be a race condition.
        return current_request->is_linked();
    else
        return false;
}

}
