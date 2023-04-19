#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

#include <Common/CurrentMemoryTracker.h>


#ifdef MEMORY_TRACKER_DEBUG_CHECKS
thread_local bool memory_tracker_always_throw_logical_error_on_allocation = false;
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace
{

MemoryTracker * getMemoryTracker()
{
    if (auto * thread_memory_tracker = DB::CurrentThread::getMemoryTracker())
        return thread_memory_tracker;

    /// Once the main thread is initialized,
    /// total_memory_tracker is initialized too.
    /// And can be used, since MainThreadStatus is required for profiling.
    if (DB::MainThreadStatus::get())
        return &total_memory_tracker;

    return nullptr;
}

}

using DB::current_thread;
thread_local std::function<void(Int64, bool)> CurrentMemoryTracker::before_alloc = nullptr;

thread_local std::function<void(Int64)> CurrentMemoryTracker::before_free  = nullptr;

AllocationTrace CurrentMemoryTracker::allocImpl(Int64 size, bool throw_if_memory_exceeded)
{
#ifdef MEMORY_TRACKER_DEBUG_CHECKS
    if (unlikely(memory_tracker_always_throw_logical_error_on_allocation))
    {
        memory_tracker_always_throw_logical_error_on_allocation = false;
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Memory tracker: allocations not allowed.");
    }
#endif

    if (auto * memory_tracker = getMemoryTracker())
    {
        if (current_thread)
        {
            Int64 will_be = current_thread->untracked_memory + size;

            if (will_be > current_thread->untracked_memory_limit)
            {
                if (before_alloc)
                    before_alloc(will_be, throw_if_memory_exceeded);
                auto res = memory_tracker->allocImpl(will_be, throw_if_memory_exceeded);
                current_thread->untracked_memory = 0;
                return res;
            }
            else
            {
                /// Update after successful allocations,
                /// since failed allocations should not be take into account.
                current_thread->untracked_memory = will_be;
            }
        }
        /// total_memory_tracker only, ignore untracked_memory
        else
        {
            return memory_tracker->allocImpl(size, throw_if_memory_exceeded);
        }

        return AllocationTrace(memory_tracker->getSampleProbability(size));
    }

    return AllocationTrace(0);
}

void CurrentMemoryTracker::check()
{
    if (auto * memory_tracker = getMemoryTracker())
        std::ignore = memory_tracker->allocImpl(0, true);
}

AllocationTrace CurrentMemoryTracker::alloc(Int64 size)
{
    bool throw_if_memory_exceeded = true;
    return allocImpl(size, throw_if_memory_exceeded);
}

AllocationTrace CurrentMemoryTracker::allocNoThrow(Int64 size)
{
    bool throw_if_memory_exceeded = false;
    return allocImpl(size, throw_if_memory_exceeded);
}

AllocationTrace CurrentMemoryTracker::free(Int64 size)
{
    if (auto * memory_tracker = getMemoryTracker())
    {
        if (current_thread)
        {
            current_thread->untracked_memory -= size;
            if (current_thread->untracked_memory < -current_thread->untracked_memory_limit)
            {
                Int64 untracked_memory = current_thread->untracked_memory;
                current_thread->untracked_memory = 0;
                if (before_free)
                    before_free(-untracked_memory);
                return memory_tracker->free(-untracked_memory);
            }
        }
        /// total_memory_tracker only, ignore untracked_memory
        else
        {
            return memory_tracker->free(size);
        }

        return AllocationTrace(memory_tracker->getSampleProbability(size));
    }

    return AllocationTrace(0);
}

void CurrentMemoryTracker::injectFault()
{
    if (auto * memory_tracker = getMemoryTracker())
        memory_tracker->injectFault();
}

