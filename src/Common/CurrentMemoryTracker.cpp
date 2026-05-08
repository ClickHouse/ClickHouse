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

/// Per-thread last-seen view of the per-CPU counters. To be moved into
/// `ThreadStatus` later; kept locally for now.
thread_local DB::PerCPUMemoryBudget::PerCPUMemoryBudgetState per_cpu_state;

/// If we have migrated to a new CPU since the last op, rebind state.cpu and
/// signal the caller to flush via the unified branch
/// (`migrated || flush_per_cpu || over_limit`). State's nallocs/nfrees are
/// deliberately *not* refreshed here — `chargeAlloc`/`chargeFree` overwrite
/// them on every call, so the first post-migration op self-corrects the
/// baseline. Any spurious crossing the stale baseline produces is harmless
/// because we're flushing anyway.
inline bool rebindPerCPUStateOnMigration(int cpu)
{
    if (per_cpu_state.cpu == cpu)
        return false;
    per_cpu_state.cpu = cpu;
    return true;
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

        int cpu = DB::PerCPUMemoryBudget::currentCPU();
        bool migrated = (cpu >= 0) && rebindPerCPUStateOnMigration(cpu);

        Int64 previous_untracked_memory = current_thread->untracked_memory;
        current_thread->untracked_memory += size;

        bool flush_per_cpu = (cpu >= 0) && DB::PerCPUMemoryBudget::chargeAlloc(size, per_cpu_state);

        if (current_thread->untracked_memory > current_thread->untracked_memory_limit || flush_per_cpu || migrated)
        {
            Int64 current_untracked_memory = current_thread->untracked_memory;
            current_thread->untracked_memory = 0;

            try
            {
                if (current_untracked_memory > 0)
                    return memory_tracker->allocImpl(current_untracked_memory, throw_if_memory_exceeded);
                else
                    return memory_tracker->free(-current_untracked_memory);
            }
            catch (...)
            {
                /// nallocs / per_cpu_state.nallocs already advanced; leave them.
                /// The next op may flush slightly sooner, no correctness loss.
                current_thread->untracked_memory = previous_untracked_memory;
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

        int cpu = DB::PerCPUMemoryBudget::currentCPU();
        bool migrated = (cpu >= 0) && rebindPerCPUStateOnMigration(cpu);

        current_thread->untracked_memory -= size;
        bool flush_per_cpu = (cpu >= 0) && DB::PerCPUMemoryBudget::chargeFree(size, per_cpu_state);

        if (current_thread->untracked_memory < -current_thread->untracked_memory_limit || flush_per_cpu || migrated)
        {
            Int64 untracked_memory = current_thread->untracked_memory;
            current_thread->untracked_memory = 0;
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
