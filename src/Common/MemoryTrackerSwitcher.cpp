#include <Common/MemoryTrackerSwitcher.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

namespace DB
{

MemoryTrackerSwitcher::MemoryTrackerSwitcher(MemoryTracker * new_tracker)
{
    /// current_thread is not initialized for the main thread, so simply do not switch anything
    ThreadStatus * cur_thread = current_thread;
    if (!cur_thread)
        return;

    /// Report the deferred allocations to the previous parent instead of setting them aside:
    /// otherwise the bytes deferred before and inside the switched scope are both unreported
    /// at the same time, and together they can exceed the bound of one thread's untracked memory
    /// (`max_untracked_memory`), which `additional_memory_tracking_per_thread` relies on.
    CurrentThread::flushUntrackedMemory();
    auto * thread_tracker = CurrentThread::getMemoryTracker();

    prev_untracked_memory_blocker_level = cur_thread->untracked_memory_blocker_level;
    prev_memory_tracker_parent = thread_tracker->getParent();
    prev_sample_config = cur_thread->getMemorySampleConfig();

    thread_tracker->setParent(new_tracker);
    cur_thread->resolveMemorySampleConfig();
}

MemoryTrackerSwitcher::~MemoryTrackerSwitcher()
{
    /// current_thread is not initialized for the main thread, so simply do not switch anything
    ThreadStatus * cur_thread = current_thread;
    if (!cur_thread)
        return;

    CurrentThread::flushUntrackedMemory();
    auto * thread_tracker = CurrentThread::getMemoryTracker();

    thread_tracker->setParent(prev_memory_tracker_parent);
    cur_thread->untracked_memory_blocker_level = prev_untracked_memory_blocker_level;
    cur_thread->setMemorySampleConfig(prev_sample_config);
}

}
