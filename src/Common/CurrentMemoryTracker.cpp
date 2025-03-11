#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerBlockerInThread.h>

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
        /// Ignore untracked_memory if:
        ///  * total_memory_tracker only, or
        ///  * MemoryTrackerBlockerInThread is active.
        ///    memory_tracker->allocImpl needs to be called for these bytes with the same blocker
        ///    state as we currently have.
        ///    E.g. suppose allocImpl is called twice: first for 2 MB with blocker set to
        ///    VariableContext::User, then for 3 MB with no blocker. This should increase the
        ///    Global memory tracker by 5 MB and the User memory tracker by 3 MB. So we can't group
        ///    these two calls into one memory_tracker->allocImpl call. Without this `if`, the first
        ///    allocImpl call would increment untracked_memory, and the second call would
        ///    incorrectly report all 5 MB with no blocker, so the User memory tracker would be
        ///    incorrectly increased by 5 MB instead of 3 MB.
        ///    (Alternatively, we could maintain `untracked_memory` value separately for each
        ///     possible blocker state, i.e. per VariableContext.)
        if (!current_thread || MemoryTrackerBlockerInThread::isBlockedAny())
        {
            return memory_tracker->allocImpl(size, throw_if_memory_exceeded);
        }
        else
        {
            Int64 will_be = current_thread->untracked_memory + size;

            if (will_be > current_thread->untracked_memory_limit)
            {
                auto res = memory_tracker->allocImpl(will_be, throw_if_memory_exceeded);
                current_thread->untracked_memory = 0;
                return res;
            }

            /// Update after successful allocations,
            /// since failed allocations should not be take into account.
            current_thread->untracked_memory = will_be;
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
        if (!current_thread || MemoryTrackerBlockerInThread::isBlockedAny())
        {
            return memory_tracker->free(size);
        }
        else
        {
            current_thread->untracked_memory -= size;
            if (current_thread->untracked_memory < -current_thread->untracked_memory_limit)
            {
                Int64 untracked_memory = current_thread->untracked_memory;
                current_thread->untracked_memory = 0;
                return memory_tracker->free(-untracked_memory);
            }
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

