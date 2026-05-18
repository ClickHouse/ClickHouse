#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>

#include <atomic>
#include <limits>


#ifdef MEMORY_TRACKER_DEBUG_CHECKS
thread_local bool memory_tracker_always_throw_logical_error_on_allocation = false;
#endif

namespace
{
    std::atomic<UInt64> min_allocation_size_to_throw_on_memory_limit{std::numeric_limits<UInt64>::max()};
}

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

    /// total_memory_tracker can be used before MainThreadStatus is initialized,
    /// but only after its own initialization and before teardown.
    if (DB::MainThreadStatus::initialized() || isTotalMemoryTrackerInitialized())
        return &total_memory_tracker;

    return nullptr;
}

}

using DB::current_thread;

AllocationTrace CurrentMemoryTracker::allocImpl(Int64 size, bool enforce_memory_limit)
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
            return memory_tracker->allocImpl(size, enforce_memory_limit);
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
                return memory_tracker->allocImpl(current_untracked_memory, enforce_memory_limit);
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
        std::ignore = memory_tracker->allocImpl(0, true);
}

AllocationTrace CurrentMemoryTracker::alloc(Int64 size)
{
    return allocImpl(size, /*enforce_memory_limit=*/ true);
}

AllocationTrace CurrentMemoryTracker::allocNoThrow(Int64 size)
{
    return allocImpl(size, /*enforce_memory_limit=*/ false);
}

AllocationTrace CurrentMemoryTracker::allocThrow(Int64 size)
{
    const bool enforce_memory_limit = static_cast<UInt64>(size) >= min_allocation_size_to_throw_on_memory_limit.load(std::memory_order_relaxed);
    return allocImpl(size, enforce_memory_limit);
}

void CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(UInt64 value)
{
    min_allocation_size_to_throw_on_memory_limit.store(
        value == 0 ? std::numeric_limits<UInt64>::max() : value,
        std::memory_order_relaxed);
}

UInt64 CurrentMemoryTracker::getMinAllocationSizeBytesToThrow()
{
    const auto value = min_allocation_size_to_throw_on_memory_limit.load(std::memory_order_relaxed);
    return value == std::numeric_limits<UInt64>::max() ? 0 : value;
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

        return AllocationTrace(current_thread->getEffectiveSampleProbability(size));
    }

    return AllocationTrace(0);
}

void CurrentMemoryTracker::injectFault()
{
    if (auto * memory_tracker = getMemoryTracker())
        memory_tracker->injectFault();
}
