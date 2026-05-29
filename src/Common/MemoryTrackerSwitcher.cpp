#include <Common/MemoryTrackerSwitcher.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

namespace DB
{

MemoryTrackerSwitcher::MemoryTrackerSwitcher(MemoryTracker * new_tracker)
{
    /// current_thread is not initialized for the main thread, so simply do not switch anything
    if (!current_thread)
        return;

    prev_untracked_memory = current_thread->untracked_memory;
    prev_untracked_memory_blocker_level = current_thread->untracked_memory_blocker_level;
    prev_memory_tracker = current_thread->memory_tracker;

    current_thread->untracked_memory = 0;
    current_thread->memory_tracker = new_tracker;
}

MemoryTrackerSwitcher::~MemoryTrackerSwitcher()
{
    /// current_thread is not initialized for the main thread, so simply do not switch anything
    if (!current_thread)
        return;

    CurrentThread::flushUntrackedMemory();

    /// It is important to restore untracked memory after repointing the tracker,
    /// otherwise flushUntrackedMemory() above would account it to the wrong tracker.
    current_thread->memory_tracker = prev_memory_tracker;
    current_thread->untracked_memory = prev_untracked_memory;
    current_thread->untracked_memory_blocker_level = prev_untracked_memory_blocker_level;
}

}
