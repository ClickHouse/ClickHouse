#include <Common/MemoryTrackerSwitcher.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

#include <utility>

namespace DB
{

MemoryTrackerSwitcher::MemoryTrackerSwitcher(MemoryTracker * new_tracker)
{
    /// current_thread is not initialized for the main thread, so simply do not switch anything
    ThreadStatus * cur_thread = current_thread;
    if (!cur_thread)
        return;

    auto * thread_tracker = CurrentThread::getMemoryTracker();

    prev_untracked_memory = cur_thread->untracked_memory.load();
    prev_untracked_memory_blocker_level = cur_thread->untracked_memory_blocker_level;
    prev_memory_tracker_parent = thread_tracker->getParent();
    prev_per_cpu = std::move(cur_thread->per_cpu_untracked_memory).save();
    prev_sample_config = cur_thread->getMemorySampleConfig();

    cur_thread->untracked_memory.store(0);
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

    /// It is important to set untracked memory after the call of
    /// 'setParent' because it may flush untracked memory to the wrong parent.
    thread_tracker->setParent(prev_memory_tracker_parent);
    cur_thread->untracked_memory.store(prev_untracked_memory);
    cur_thread->untracked_memory_blocker_level = prev_untracked_memory_blocker_level;
    cur_thread->per_cpu_untracked_memory.restore(prev_per_cpu);
    cur_thread->setMemorySampleConfig(prev_sample_config);
}

}
