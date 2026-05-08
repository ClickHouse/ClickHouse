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

using DB::current_thread;

[[gnu::cold]] AllocationTrace CurrentMemoryTracker::allocWithoutCurrentThread(Int64 size, bool throw_if_memory_exceeded)
{
    if (DB::MainThreadStatus::initialized() || isTotalMemoryTrackerInitialized())
        return total_memory_tracker.allocImpl(size, throw_if_memory_exceeded);
    return AllocationTrace(0);
}

[[gnu::cold]] AllocationTrace CurrentMemoryTracker::freeWithoutCurrentThread(Int64 size)
{
    if (DB::MainThreadStatus::initialized() || isTotalMemoryTrackerInitialized())
        return total_memory_tracker.free(size);
    return AllocationTrace(0);
}

AllocationTrace CurrentMemoryTracker::allocImpl(Int64 size, bool throw_if_memory_exceeded)
{
#ifdef MEMORY_TRACKER_DEBUG_CHECKS
    if (unlikely(memory_tracker_always_throw_logical_error_on_allocation))
    {
        memory_tracker_always_throw_logical_error_on_allocation = false;
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Memory tracker: allocations not allowed.");
    }
#endif

    /// Read `current_thread` (a TLS pointer) once so the hot path doesn't issue
    /// duplicate TLS loads across `getMemoryTracker` plus a follow-up null check.
    auto * const thread = current_thread;
    if (thread == nullptr) [[unlikely]]
        return allocWithoutCurrentThread(size, throw_if_memory_exceeded);

    /// Make sure we do memory tracker calls with the correct level in MemoryTrackerBlockerInThread.
    /// E.g. suppose allocImpl is called twice: first for 2 MB with blocker set to
    /// VariableContext::User, then for 3 MB with no blocker. This should increase the
    /// Global memory tracker by 5 MB and the User memory tracker by 3 MB. So we can't group
    /// these two calls into one memory_tracker->allocImpl call.
    ///
    /// Only update `untracked_memory_blocker_level` when it actually changed: the unconditional
    /// store costs a write per allocation on the hot path where it almost never differs.
    VariableContext blocker_level = MemoryTrackerBlockerInThread::getLevel();
    if (blocker_level != thread->untracked_memory_blocker_level) [[unlikely]]
    {
        thread->flushUntrackedMemory();
        thread->untracked_memory_blocker_level = blocker_level;
    }

    Int64 previous_untracked_memory = thread->untracked_memory;
    thread->untracked_memory += size;
    if (thread->untracked_memory > thread->untracked_memory_limit) [[unlikely]]
    {
        Int64 current_untracked_memory = thread->untracked_memory;
        thread->untracked_memory = 0;

        try
        {
            return thread->memory_tracker.allocImpl(current_untracked_memory, throw_if_memory_exceeded);
        }
        catch (...)
        {
            thread->untracked_memory += previous_untracked_memory;
            throw;
        }
    }

    return AllocationTrace(thread->getEffectiveSampleProbability(size));
}

void CurrentMemoryTracker::check()
{
    if (auto * thread = current_thread)
        std::ignore = thread->memory_tracker.allocImpl(0, true);
    else if (DB::MainThreadStatus::initialized() || isTotalMemoryTrackerInitialized())
        std::ignore = total_memory_tracker.allocImpl(0, true);
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
    auto * const thread = current_thread;
    if (thread == nullptr) [[unlikely]]
        return freeWithoutCurrentThread(size);

    VariableContext blocker_level = MemoryTrackerBlockerInThread::getLevel();
    if (blocker_level != thread->untracked_memory_blocker_level) [[unlikely]]
    {
        thread->flushUntrackedMemory();
        thread->untracked_memory_blocker_level = blocker_level;
    }

    thread->untracked_memory -= size;
    if (thread->untracked_memory < -thread->untracked_memory_limit) [[unlikely]]
    {
        Int64 untracked_memory = thread->untracked_memory;
        thread->untracked_memory = 0;
        return thread->memory_tracker.free(-untracked_memory);
    }

    return AllocationTrace(thread->getEffectiveSampleProbability(size));
}

void CurrentMemoryTracker::injectFault()
{
    if (auto * thread = current_thread)
        thread->memory_tracker.injectFault();
    else if (DB::MainThreadStatus::initialized() || isTotalMemoryTrackerInitialized())
        total_memory_tracker.injectFault();
}
