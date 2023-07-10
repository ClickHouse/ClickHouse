#pragma once

#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct MemoryTrackerSwitcher
{
    explicit MemoryTrackerSwitcher(MemoryTracker * new_tracker)
    {
        if (!current_thread)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "current_thread is not initialized");

        auto * thread_tracker = CurrentThread::getMemoryTracker();
        prev_untracked_memory = current_thread->untracked_memory;
        prev_memory_tracker_parent = thread_tracker->getParent();

        current_thread->untracked_memory = 0;
        thread_tracker->setParent(new_tracker);
    }

    ~MemoryTrackerSwitcher()
    {
        CurrentThread::flushUntrackedMemory();
        auto * thread_tracker = CurrentThread::getMemoryTracker();

        current_thread->untracked_memory = prev_untracked_memory;
        thread_tracker->setParent(prev_memory_tracker_parent);
    }

    MemoryTracker * prev_memory_tracker_parent = nullptr;
    Int64 prev_untracked_memory = 0;
};

}
