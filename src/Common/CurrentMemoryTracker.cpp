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

namespace ProfileEvents
{
    extern const Event MemoryTrackerAllocations;
    extern const Event MemoryTrackerAllocationsBytes;
    extern const Event MemoryTrackerDeallocations;
    extern const Event MemoryTrackerDeallocationsBytes;
}

namespace
{

MemoryTracker * getMemoryTracker()
{
    if (auto * thread_memory_tracker = DB::CurrentThread::getMemoryTracker())
        return thread_memory_tracker;

    /// total_memory_tracker can be used before MainThreadStatus is initialized,
    /// but only after its own initialization and before teardown.
    if (DB::MainThreadStatus::initialized() || isTotalMemoryTrackerInitialized())
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
            ProfileEvents::increment(ProfileEvents::MemoryTrackerAllocations);
            ProfileEvents::increment(ProfileEvents::MemoryTrackerAllocationsBytes, size);
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
                ProfileEvents::increment(ProfileEvents::MemoryTrackerAllocations);
                ProfileEvents::increment(ProfileEvents::MemoryTrackerAllocationsBytes, current_untracked_memory);
                auto res = memory_tracker->allocImpl(current_untracked_memory, throw_if_memory_exceeded);
                current_thread->updateUntrackedMemoryLimit(memory_tracker->get());
                return res;
            }
            catch (...)
            {
                current_thread->untracked_memory += previous_untracked_memory;
                throw;
            }
        }

        return AllocationTrace(current_thread->getEffectiveSampleProbability(size));
    }

    return AllocationTrace(0);
}

void CurrentMemoryTracker::check()
{
    if (auto * memory_tracker = getMemoryTracker())
    {
        ProfileEvents::increment(ProfileEvents::MemoryTrackerAllocations);
        std::ignore = memory_tracker->allocImpl(0, true);
    }
}

Int64 CurrentMemoryTracker::get()
{
    if (auto * memory_tracker = getMemoryTracker())
        return memory_tracker->get();
    return 0;
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
            ProfileEvents::increment(ProfileEvents::MemoryTrackerDeallocations);
            ProfileEvents::increment(ProfileEvents::MemoryTrackerDeallocationsBytes, size);
            return memory_tracker->free(size);
        }

        VariableContext blocker_level = MemoryTrackerBlockerInThread::getLevel();
        if (blocker_level != current_thread->untracked_memory_blocker_level)
        {
            current_thread->flushUntrackedMemory();
        }
        current_thread->untracked_memory_blocker_level = blocker_level;

        current_thread->untracked_memory -= size;
        /// Use `max_untracked_memory` (not `untracked_memory_limit`) to create hysteresis and avoid track/untrack cycles
        if (current_thread->untracked_memory < -current_thread->max_untracked_memory)
        {
            Int64 untracked_memory = current_thread->untracked_memory;
            current_thread->untracked_memory = 0;
            current_thread->updateUntrackedMemoryLimit(memory_tracker->get() + untracked_memory);

            ProfileEvents::increment(ProfileEvents::MemoryTrackerDeallocations);
            ProfileEvents::increment(ProfileEvents::MemoryTrackerDeallocationsBytes, -untracked_memory);
            return memory_tracker->free(-untracked_memory);
        }

        return AllocationTrace(current_thread->getEffectiveSampleProbability(size));
    }

    return AllocationTrace(0);
}

void CurrentMemoryTracker::injectFault()
{
    if (auto * memory_tracker = getMemoryTracker())
        memory_tracker->injectFault();
}
