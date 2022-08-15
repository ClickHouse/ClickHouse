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

void CurrentMemoryTracker::allocImpl(Int64 size, bool throw_if_memory_exceeded)
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
                /// Zero untracked before track. If tracker throws out-of-limit we would be able to alloc up to untracked_memory_limit bytes
                /// more. It could be useful to enlarge Exception message in rethrow logic.
                current_thread->untracked_memory = 0;
                memory_tracker->allocImpl(will_be, throw_if_memory_exceeded);
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
            memory_tracker->allocImpl(size, throw_if_memory_exceeded);
        }
    }
}

void CurrentMemoryTracker::check()
{
    if (auto * memory_tracker = getMemoryTracker())
        memory_tracker->allocImpl(0, true);
}

void CurrentMemoryTracker::alloc(Int64 size)
{
    bool throw_if_memory_exceeded = true;
    allocImpl(size, throw_if_memory_exceeded);
}

void CurrentMemoryTracker::allocNoThrow(Int64 size)
{
    bool throw_if_memory_exceeded = false;
    allocImpl(size, throw_if_memory_exceeded);
}

void CurrentMemoryTracker::realloc(Int64 old_size, Int64 new_size)
{
    Int64 addition = new_size - old_size;
    addition > 0 ? alloc(addition) : free(-addition);
}

void CurrentMemoryTracker::free(Int64 size)
{
    if (auto * memory_tracker = getMemoryTracker())
    {
        if (current_thread)
        {
            current_thread->untracked_memory -= size;
            if (current_thread->untracked_memory < -current_thread->untracked_memory_limit)
            {
                memory_tracker->free(-current_thread->untracked_memory);
                current_thread->untracked_memory = 0;
            }
        }
        /// total_memory_tracker only, ignore untracked_memory
        else
        {
            memory_tracker->free(size);
        }
    }
}

