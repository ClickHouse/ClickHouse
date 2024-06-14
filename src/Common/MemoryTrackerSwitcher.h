#pragma once

#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>

namespace DB
{

struct MemoryTrackerSwitcher
{
    explicit MemoryTrackerSwitcher(MemoryTracker * new_tracker)
    {
        /// current_thread is not initialized for the main thread, so simply do not switch anything
        if (!current_thread)
            return;

        auto * thread_tracker = CurrentThread::getMemoryTracker();

        prev_untracked_memory = current_thread->untracked_memory;
        prev_memory_tracker_parent = thread_tracker->getParent();

        current_thread->untracked_memory = 0;
        thread_tracker->setParent(new_tracker);
    }

    ~MemoryTrackerSwitcher()
    {
        /// current_thread is not initialized for the main thread, so simply do not switch anything
        if (!current_thread)
            return;

        CurrentThread::flushUntrackedMemory();
        auto * thread_tracker = CurrentThread::getMemoryTracker();

        /// It is important to set untracked memory after the call of
        /// 'setParent' because it may flush untracked memory to the wrong parent.
        thread_tracker->setParent(prev_memory_tracker_parent);
        current_thread->untracked_memory = prev_untracked_memory;
    }

private:
    MemoryTracker * prev_memory_tracker_parent = nullptr;
    Int64 prev_untracked_memory = 0;
};

}
