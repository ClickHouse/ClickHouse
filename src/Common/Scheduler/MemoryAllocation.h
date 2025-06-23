#pragma once

#include <Common/Scheduler/ResourceAllocation.h>
#include <Common/Scheduler/ResourceLink.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <mutex>

namespace DB
{

class MemoryAllocation;
using MemoryAllocationPtr = std::shared_ptr<MemoryAllocation>;

/// Implementation of a memory reservation for a query or background task.
/// It checks what MemoryTrackers report for every thread and synchronizes it with what the resource scheduler allows.
class MemoryAllocation : public ResourceAllocation, public std::enable_shared_from_this<MemoryAllocation>, public boost::noncopyable
{
public:
    // A part of allocation that is responsible for one thread
    class ThreadState
    {
    public:
        explicit ThreadState(const MemoryAllocationPtr & allocation_)
            : allocation(allocation_)
        {}

        // This function should be called periodically by every thread that uses this memory allocation.
        // It checks if a reclaim was requested and if so, it triggers the reclaim process (only query termination for now).
        // It checks it thread have allocated or freed memory since last checked and communicate it to the scheduler if necessary.
        void update();

    private:
        MemoryAllocationPtr allocation;
        Int64 last_tracked_size = 0; // memory consumed by this thread (from MemoryTracker)
    };

    explicit MemoryAllocation(ResourceCost size_, ResourceLink link_);

    void reclaim(ResourceCost size) override;

private:
    const ResourceLink link;
    std::mutex mutex;
    bool reclaim_requested = false; // triggers query to stop
};

}
