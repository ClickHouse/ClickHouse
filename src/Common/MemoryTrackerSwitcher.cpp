#include <Common/MemoryTrackerSwitcher.h>
#include <Common/CurrentThread.h>

namespace DB
{

MemoryTrackerSwitcher::MemoryTrackerSwitcher(MemoryTracker * new_tracker)
{
    /// current_thread is not initialized for the main thread, so simply do not switch anything
    if (!current_thread)
        return;

    auto * thread_tracker = CurrentThread::getMemoryTracker();

    prev_untracked_memory = current_thread->untracked_memory;
    prev_untracked_memory_blocker_level = current_thread->untracked_memory_blocker_level;
    prev_memory_tracker_parent = thread_tracker->getParent();

    current_thread->untracked_memory = 0;
    thread_tracker->setParent(new_tracker);
}

MemoryTrackerSwitcher::~MemoryTrackerSwitcher()
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
    current_thread->untracked_memory_blocker_level = prev_untracked_memory_blocker_level;
}

}
