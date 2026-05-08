#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/PerCPUMemoryBudget.h>


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
            /// total_memory_tracker only, ignore untracked_memory and budget
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

        /// `charge()` returns true (flush-needed) in two cases:
        ///   * the slice was applied and ended up exactly inside [0, 2*SLICE] — never,
        ///     since we now refuse rather than overcommit;
        ///   * the slice would have escaped [0, 2*SLICE] and we refused to apply.
        /// In the refused case `size` is *not* in the slice, so the counter-charge
        /// below uses `untracked_memory - size` (the previously-accumulated portion
        /// that the slice does carry) instead of the full `untracked_memory`.
        bool flush_per_cpu = DB::PerCPUMemoryBudget::charge(size);
        je_malloc(20);

        if (current_thread->untracked_memory > current_thread->untracked_memory_limit || flush_per_cpu)
        {
            Int64 current_untracked_memory = current_thread->untracked_memory;
            current_thread->untracked_memory = 0;
            Int64 in_slice = flush_per_cpu ? (current_untracked_memory - size) : current_untracked_memory;
            DB::PerCPUMemoryBudget::charge(-in_slice);

            try
            {
                if (current_untracked_memory > 0)
                    return memory_tracker->allocImpl(current_untracked_memory, throw_if_memory_exceeded);
                else
                    return memory_tracker->free(-current_untracked_memory);
            }
            catch (...)
            {
                current_thread->untracked_memory += previous_untracked_memory;
                DB::PerCPUMemoryBudget::charge(previous_untracked_memory);
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
        /// See note in allocImpl: when `charge` refuses (`flush_per_cpu == true`),
        /// the slice does not carry the current `-size`, so counter-charge by
        /// `untracked_memory + size` (the previously-accumulated part).
        bool flush_per_cpu = DB::PerCPUMemoryBudget::charge(-size);

        if (current_thread->untracked_memory < -current_thread->untracked_memory_limit || flush_per_cpu)
        {
            Int64 untracked_memory = current_thread->untracked_memory;
            current_thread->untracked_memory = 0;
            Int64 in_slice = flush_per_cpu ? (untracked_memory + size) : untracked_memory;
            DB::PerCPUMemoryBudget::charge(-in_slice);
            if (untracked_memory > 0)
                return memory_tracker->allocImpl(untracked_memory, /*throw_if_memory_exceeded=*/ false);
            else
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
