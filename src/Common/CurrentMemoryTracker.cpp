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
    void allocImpl(Int64 size, bool throw_if_memory_exceeded)
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
                    memory_tracker->allocImpl(tmp, throw_if_memory_exceeded);
                }
            }
            /// total_memory_tracker only, ignore untracked_memory
            else
            {
                memory_tracker->allocImpl(size, throw_if_memory_exceeded);
            }
        }
    }
}

void alloc(Int64 size)
{
    bool throw_if_memory_exceeded = true;
    allocImpl(size, throw_if_memory_exceeded);
}

void allocNoThrow(Int64 size)
{
    bool throw_if_memory_exceeded = false;
    allocImpl(size, throw_if_memory_exceeded);
}

void realloc(Int64 old_size, Int64 new_size)
{
    Int64 addition = new_size - old_size;
    addition > 0 ? alloc(addition) : free(-addition);
}

void free(Int64 size)
{
    if (auto * memory_tracker = getMemoryTracker())
    {
        if (current_thread)
        {
            current_thread->untracked_memory -= size;
            if (current_thread->untracked_memory < -current_thread->untracked_memory_limit)
            {
                memory_tracker->free(-current_thread->untracked_memory);
                current_thread->untracked_memory = 0;
            }
        }
        /// total_memory_tracker only, ignore untracked_memory
        else
        {
            memory_tracker->free(size);
        }
    }
}

}
