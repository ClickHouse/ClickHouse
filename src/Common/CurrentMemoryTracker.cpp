#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

#include <Common/CurrentMemoryTracker.h>


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

namespace CurrentMemoryTracker
{

using DB::current_thread;

namespace
{
    AllocationTrace allocImpl(Int64 size, bool throw_if_memory_exceeded)
    {
        if (auto * memory_tracker = getMemoryTracker())
        {
            if (current_thread)
            {
                current_thread->untracked_memory += size;

                if (current_thread->untracked_memory > current_thread->untracked_memory_limit)
                {
                    /// Zero untracked before track. If tracker throws out-of-limit we would be able to alloc up to untracked_memory_limit bytes
                    /// more. It could be useful to enlarge Exception message in rethrow logic.
                    Int64 tmp = current_thread->untracked_memory;
                    current_thread->untracked_memory = 0;
                    return memory_tracker->allocImpl(tmp, throw_if_memory_exceeded);
                }
            }
            /// total_memory_tracker only, ignore untracked_memory
            else
            {
                return memory_tracker->allocImpl(size, throw_if_memory_exceeded);
            }

            return AllocationTrace(memory_tracker->getSampleProbabilityTotal());
        }

        return AllocationTrace(0);
    }
}

void check()
{
    if (auto * memory_tracker = getMemoryTracker())
        std::ignore = memory_tracker->allocImpl(0, true);
}

AllocationTrace alloc(Int64 size)
{
    bool throw_if_memory_exceeded = true;
    return allocImpl(size, throw_if_memory_exceeded);
}

AllocationTrace allocNoThrow(Int64 size)
{
    bool throw_if_memory_exceeded = false;
    return allocImpl(size, throw_if_memory_exceeded);
}

AllocationTrace realloc(Int64 old_size, Int64 new_size)
{
    Int64 addition = new_size - old_size;
    return addition > 0 ? alloc(addition) : free(-addition);
}

AllocationTrace free(Int64 size)
{
    if (auto * memory_tracker = getMemoryTracker())
    {
        if (current_thread)
        {
            current_thread->untracked_memory -= size;
            if (current_thread->untracked_memory < -current_thread->untracked_memory_limit)
            {
                Int64 untracked_memory = current_thread->untracked_memory;
                current_thread->untracked_memory = 0;
                return memory_tracker->free(-untracked_memory);
            }
        }
        /// total_memory_tracker only, ignore untracked_memory
        else
        {
            return memory_tracker->free(size);
        }

        return AllocationTrace(memory_tracker->getSampleProbabilityTotal());
    }

    return AllocationTrace(0);
}

}
