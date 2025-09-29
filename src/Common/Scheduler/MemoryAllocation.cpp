#include <Common/Scheduler/MemoryAllocation.h>
#include <Common/ErrorCodes.h>
#include <Commom/MemoryTracker.h>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <base/defines.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MemoryAllocation::MemoryAllocation(ResourceCost size_, ResourceLink link_)
    : ResourceAllocation(size_)
    , link(link_)
{}

void MemoryAllocation::reclaim(ResourceCost size)
{
    chassert(allocated_size >= size); // Cannot reclaim more than allocated

    // TODO(serxa): support spilling to disk.
    std::scoped_lock lock(mutex);
    reclaim_requested = true;
}

void MemoryAllocation::ThreadState::update()
{
    std::scoped_lock lock(allocation.mutex);

    if (allocation.reclaim_requested)
    {
        // TODO(serxa): add more details in error message
        throw DB::Exception(
            DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
            "Workload memory limit exceeded");
    }

    Int64 current_size = 0;
    if (auto * tracker = CurrentThread::getMemoryTracker())
    {
        current_size = tracker->get();
    }

    if (current_size != last_tracked_size)
    {
        ResourceCost delta = current_size - last_tracked_size;
        last_tracked_size = current_size;

        // TODO(serxa): communicate delta to the scheduler if MemoryAllocation is not enqueued already.
    }
}

}
