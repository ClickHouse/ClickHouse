#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>


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

    /// Note, we cannot use total_memory_tracker earlier (i.e. just after static variable initialized without this check),
    /// since the initialization order of static objects is not defined, and total_memory_tracker may not be initialized yet.
    /// So here we relying on MainThreadStatus initialization.
    if (DB::MainThreadStatus::initialized())
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
        if (!current_thread)
        {
            /// total_memory_tracker only, ignore untracked_memory
            return memory_tracker->allocImpl(size, throw_if_memory_exceeded);
        }

        /// Make sure we do memory tracker calls with the correct level in MemoryTrackerBlockerInThread.
        /// E.g. suppose allocImpl is called twice: first for 2 MB with blocker set to
        /// VariableContext::User, then for 3 MB with no blocker. This should increase the
        /// Global memory tracker by 5 MB and the User memory tracker by 3 MB. So we can't group
        /// these two calls into one memory_tracker->allocImpl call.
        VariableContext blocker_level = MemoryTrackerBlockerInThread::getLevel();
        if (blocker_level != current_thread->untracked_memory_blocker_level)
        {
            current_thread->flushUntrackedMemory();
        }
        current_thread->untracked_memory_blocker_level = blocker_level;

        Int64 previous_untracked_memory = current_thread->untracked_memory;
        current_thread->untracked_memory += size;
        if (current_thread->untracked_memory > current_thread->untracked_memory_limit)
        {
            Int64 current_untracked_memory = current_thread->untracked_memory;
            current_thread->untracked_memory = 0;

            try
            {
                return memory_tracker->allocImpl(current_untracked_memory, throw_if_memory_exceeded);
            }
            catch (...)
            {
                current_thread->untracked_memory += previous_untracked_memory;
                throw;
            }
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
    return allocImpl(size, /*throw_if_memory_exceeded=*/ true);
}

AllocationTrace CurrentMemoryTracker::allocNoThrow(Int64 size)
{
    return allocImpl(size, /*throw_if_memory_exceeded=*/ false);
}

AllocationTrace CurrentMemoryTracker::free(Int64 size)
{
    if (auto * memory_tracker = getMemoryTracker())
    {
        if (!current_thread)
        {
            return memory_tracker->free(size);
        }

        VariableContext blocker_level = MemoryTrackerBlockerInThread::getLevel();
        if (blocker_level != current_thread->untracked_memory_blocker_level)
        {
            current_thread->flushUntrackedMemory();
        }
        current_thread->untracked_memory_blocker_level = blocker_level;

        current_thread->untracked_memory -= size;
        if (current_thread->untracked_memory < -current_thread->untracked_memory_limit)
        {
            Int64 untracked_memory = current_thread->untracked_memory;
            current_thread->untracked_memory = 0;
            return memory_tracker->free(-untracked_memory);
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
