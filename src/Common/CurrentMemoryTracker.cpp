#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/PerCPUMemory.h>

#include <atomic>
#include <limits>
#include <tuple>


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

        DB::PerCPUMemoryThreadState previous_per_cpu = current_thread->per_cpu_untracked_memory;
        Int64 new_untracked_memory = current_thread->untracked_memory.add(size);
        Int64 previous_untracked_memory = new_untracked_memory - size;

        /// Flush when the per-thread cap is hit, or when the per-CPU budget cannot cover the deferral.
        if (new_untracked_memory > current_thread->untracked_memory_limit
            || !DB::per_cpu_memory.sync(new_untracked_memory, current_thread->per_cpu_untracked_memory))
        {
            DB::per_cpu_memory.release(current_thread->per_cpu_untracked_memory);

            current_thread->untracked_memory.store(0);

            try
            {
                /// We cannot return the AllocationTrace from here, since its sample_probability was calculated on the (batched) flushed size, which may not match the original allocation size.
                if (new_untracked_memory > 0)
                    std::ignore = memory_tracker->allocImpl(new_untracked_memory, enforce_memory_limit, /*query_tracker=*/ nullptr, /*_sample_probability=*/ 0.0);
                else
                    std::ignore = memory_tracker->free(-new_untracked_memory, /*_sample_probability=*/ 0.0);
            }
            catch (...)
            {
                current_thread->untracked_memory.add(previous_untracked_memory);
                DB::per_cpu_memory.rollback(current_thread->per_cpu_untracked_memory, previous_per_cpu);
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

void CurrentMemoryTracker::allocGlobal(Int64 size)
{
    /// Find the current query's process-level tracker (if any): the total tracker uses it
    /// for the global overcommit decision (`OvercommitTracker::needToStopQuery`), so a
    /// reservation that crosses the server memory limit waits for or kills the selected
    /// overcommitted query exactly like a real allocation from this thread would.
    /// Nothing is charged on the query tracker chain itself.
    MemoryTracker * process_tracker = nullptr;
    for (auto * tracker = DB::CurrentThread::getMemoryTracker(); tracker; tracker = tracker->getParent())
    {
        if (tracker->level == VariableContext::Process)
        {
            process_tracker = tracker;
            break;
        }
    }

    /// The reservations counter must be raised before the charge: if an external correction
    /// of the total tracker (`MemoryTracker::updateAllocated`) interleaves, the reservation
    /// is counted twice until the next correction, which errs on the safe side (the tracked
    /// amount stays an upper bound), while the opposite order could leave the corrected
    /// amount below the actual usage after `freeGlobal`.
    MemoryTracker::global_speculative_reservations.fetch_add(size, std::memory_order_relaxed);
    try
    {
        /// The returned trace is intentionally dropped: no actual allocation backs
        /// this reservation, so it must not be reported to the allocation profiler.
        std::ignore = total_memory_tracker.allocImpl(size, /*enforce_memory_limit=*/ true, process_tracker);
    }
    catch (...)
    {
        MemoryTracker::global_speculative_reservations.fetch_sub(size, std::memory_order_relaxed);
        throw;
    }
}

void CurrentMemoryTracker::freeGlobal(Int64 size)
{
    /// The reverse order of `allocGlobal`: subtract from the total tracker first, then lower
    /// the reservations counter, so an interleaved external correction can only overcount.
    std::ignore = total_memory_tracker.free(size);
    MemoryTracker::global_speculative_reservations.fetch_sub(size, std::memory_order_relaxed);
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

        Int64 new_untracked_memory = current_thread->untracked_memory.add(-size);

        /// Flush when the per-thread cap is hit, or when the per-CPU budget cannot cover the deferral.
        if (new_untracked_memory < -current_thread->untracked_memory_limit
            || !DB::per_cpu_memory.sync(new_untracked_memory, current_thread->per_cpu_untracked_memory))
        {
            DB::per_cpu_memory.release(current_thread->per_cpu_untracked_memory);

            current_thread->untracked_memory.store(0);
            /// We cannot return the AllocationTrace from here, since its sample_probability was calculated on the (batched) flushed size, which may not match the original allocation size.
            if (new_untracked_memory > 0)
                std::ignore = memory_tracker->allocImpl(new_untracked_memory, /*enforce_memory_limit=*/ false, /*query_tracker=*/ nullptr, /*_sample_probability=*/ 0.0);
            else
                std::ignore = memory_tracker->free(-new_untracked_memory, /*_sample_probability=*/ 0.0);
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
