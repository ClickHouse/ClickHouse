#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/PerCpuUntrackedMemory.h>

#if defined(__linux__)
#    include <sched.h>
#endif


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

#if defined(__linux__)

/// Resolve the CPU index once per call. The same value is reused for the
/// paired `drain` on overflow so that overflow-and-flush race against
/// migration cleanly.
inline int currentCpu()
{
    int cpu = ::sched_getcpu();
    if (cpu < 0)
        return 0;
    int n = DB::PerCpuUntrackedMemory::cpuCount();
    if (n > 0 && cpu >= n)
        cpu %= n;
    return cpu;
}

#endif

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

    auto * memory_tracker = getMemoryTracker();
    if (!memory_tracker)
        return AllocationTrace(0);

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
    bool blocker_changed = blocker_level != current_thread->untracked_memory_blocker_level;

#if defined(__linux__)
    if (likely(DB::PerCpuUntrackedMemory::isEnabled()))
    {
        int cpu = currentCpu();

        if (blocker_changed)
        {
            Int64 drained = DB::PerCpuUntrackedMemory::drain(cpu);
            if (drained > 0)
                std::ignore = memory_tracker->allocImpl(drained, /*throw_if_memory_exceeded=*/false);
            else if (drained < 0)
                std::ignore = memory_tracker->free(-drained);
            current_thread->untracked_memory_blocker_level = blocker_level;
        }

        Int64 new_local = DB::PerCpuUntrackedMemory::add(cpu, size);
        if (new_local > current_thread->untracked_memory_limit)
        {
            Int64 to_flush = DB::PerCpuUntrackedMemory::drain(cpu);
            if (to_flush > 0)
            {
                try
                {
                    return memory_tracker->allocImpl(to_flush, throw_if_memory_exceeded);
                }
                catch (...)
                {
                    /// Re-stage the drained delta. May land on a different CPU
                    /// after migration; bounded by `untracked_memory_limit`.
                    DB::PerCpuUntrackedMemory::add(cpu, to_flush - size);
                    throw;
                }
            }
            else if (to_flush < 0)
            {
                std::ignore = memory_tracker->free(-to_flush);
            }
        }
        return AllocationTrace(current_thread->getEffectiveSampleProbability(size));
    }
#endif

    /// Fallback: legacy per-thread path.
    if (blocker_changed)
        current_thread->flushUntrackedMemory();
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

    return AllocationTrace(current_thread->getEffectiveSampleProbability(size));
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
    auto * memory_tracker = getMemoryTracker();
    if (!memory_tracker)
        return AllocationTrace(0);

    if (!current_thread)
        return memory_tracker->free(size);

    VariableContext blocker_level = MemoryTrackerBlockerInThread::getLevel();
    bool blocker_changed = blocker_level != current_thread->untracked_memory_blocker_level;

#if defined(__linux__)
    if (likely(DB::PerCpuUntrackedMemory::isEnabled()))
    {
        int cpu = currentCpu();

        if (blocker_changed)
        {
            Int64 drained = DB::PerCpuUntrackedMemory::drain(cpu);
            if (drained > 0)
                std::ignore = memory_tracker->allocImpl(drained, /*throw_if_memory_exceeded=*/false);
            else if (drained < 0)
                std::ignore = memory_tracker->free(-drained);
            current_thread->untracked_memory_blocker_level = blocker_level;
        }

        Int64 new_local = DB::PerCpuUntrackedMemory::add(cpu, -size);
        if (new_local < -current_thread->untracked_memory_limit)
        {
            Int64 to_flush = DB::PerCpuUntrackedMemory::drain(cpu);
            if (to_flush < 0)
                return memory_tracker->free(-to_flush);
            else if (to_flush > 0)
                std::ignore = memory_tracker->allocImpl(to_flush, /*throw_if_memory_exceeded=*/false);
        }
        return AllocationTrace(current_thread->getEffectiveSampleProbability(size));
    }
#endif

    if (blocker_changed)
        current_thread->flushUntrackedMemory();
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

void CurrentMemoryTracker::injectFault()
{
    if (auto * memory_tracker = getMemoryTracker())
        memory_tracker->injectFault();
}
