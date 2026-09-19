#pragma once

#include <Storages/MergeTree/IMergeTreeCleanupThread.h>
#include <Common/Stopwatch.h>

namespace DB
{

class StorageMergeTree;

class MergeTreeCleanupThread : public IMergeTreeCleanupThread
{
public:
    explicit MergeTreeCleanupThread(StorageMergeTree & storage_);

    /// Shadows IMergeTreeCleanupThread::start() to restart cleanup timers
    /// before activating the background task. This ensures the thread waits
    /// a full interval after the manual cleanup done in startup().
    void start();

    /// Ask for `clearEmptyParts` on the next iteration, without waiting for the period the other
    /// part cleanups share. Only the transition needs a wakeup: a request not yet consumed by
    /// `iterate()` still has one outstanding.
    void requestEmptyPartsCleanup()
    {
        if (!clear_empty_parts_requested.exchange(true, std::memory_order_relaxed))
            wakeup();
    }

private:
    StorageMergeTree & storage;

    AtomicStopwatch time_after_previous_cleanup_parts;
    AtomicStopwatch time_after_previous_cleanup_temporary_directories;

    std::atomic<bool> clear_empty_parts_requested = false;

    /// Returns a number that is directly proportional to the number of cleaned up objects
    Float32 iterate() override;
};

}
