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

    auto * thread_tracker = CurrentThread::getMemoryTracker();

    prev_memory_tracker_parent = thread_tracker->getParent();
    prev_sample_config = current_thread->getMemorySampleConfig();

    thread_tracker->setParent(new_tracker);
    current_thread->resolveMemorySampleConfig();
}

MemoryTrackerSwitcher::~MemoryTrackerSwitcher()
{
    /// current_thread is not initialized for the main thread, so simply do not switch anything
    if (!current_thread)
        return;

    CurrentThread::flushUntrackedMemory();
    auto * thread_tracker = CurrentThread::getMemoryTracker();

    thread_tracker->setParent(prev_memory_tracker_parent);
    current_thread->setMemorySampleConfig(prev_sample_config);
}

}
