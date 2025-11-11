#include <Common/Scheduler/ResourceAllocation.h>
#include <Common/Scheduler/IAllocationQueue.h>

namespace DB
{

ResourceAllocation::ResourceAllocation(
    IAllocationQueue & queue_,
    ResourceCost initial_size,
    IncreaseRequest & increase_,
    DecreaseRequest & decrease_)
    : queue(queue_), increase(increase_), decrease(decrease_)
{
    queue.insertAllocation(*this, initial_size);
}

ResourceAllocation::~ResourceAllocation()
{
    std::unique_lock lock(mutex);
    if (is_removed)
        return;
    queue.decreaseAllocation(*this, 0);
    cv.wait(lock, [this]() { return is_removed; });
}

void ResourceAllocation::allocationRemoved()
{
    std::unique_lock lock(mutex);
    is_removed = true;
    cv.notify_all();
}

}
