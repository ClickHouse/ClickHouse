#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Common/ActionBlocker.h>
#include <Common/Stopwatch.h>
#include <Common/randomSeed.h>

#include <pcg_random.hpp>

namespace DB
{

class MergeTreeData;

/// Removes obsolete data from a table of type [Replicated]MergeTree.
class IMergeTreeCleanupThread
{
public:
    explicit IMergeTreeCleanupThread(MergeTreeData & data_);

    virtual ~IMergeTreeCleanupThread();

    void start();

    void wakeup();

    void stop();

    void wakeupEarlierIfNeeded();

    ActionLock getCleanupLock() { return cleanup_blocker.cancel(); }

protected:
    MergeTreeData & data;

    String log_name;
    LoggerPtr log;
    BackgroundSchedulePoolTaskHolder task;
    pcg64 rng{randomSeed()};

    UInt64 sleep_ms;

    std::atomic<UInt64> prev_cleanup_timestamp_ms = 0;
    std::atomic<bool> is_running = false;

    AtomicStopwatch wakeup_check_timer;

    ActionBlocker cleanup_blocker;

    void run();

    /// Returns a number this is directly proportional to the number of cleaned up blocks
    virtual Float32 iterate() = 0;
};

}
