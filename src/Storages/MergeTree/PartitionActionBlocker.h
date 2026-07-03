#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include <Common/ActionLock.h>
#include <Common/ActionBlocker.h>

namespace DB
{

/// Locks actions on given MergeTree-family table.
/// Like, ActionBlocker, but has two modes:
/// - 'partition-specific' lock, @see `cancelForPartition()`, and `isCancelledForPartition()`.
///    Only actions on *specific partition* are prohibited to start
///    and must interrupt at firsts chance.
///    There could be arbitrary number of partitions locked simultaneously.
///
/// - 'global' lock, @see `cancel()`, `isCancelled()`, and `cancelForever()`
///    Any new actions for *ALL* partitions are prohibited to start
///    and are required to interrupt at first possible moment.
///    As if all individual partitions were locked with 'partition-specific lock'.
///
/// Any lock can be set multiple times and considered fully un-locked
/// only when it was un-locked same number of times (by destructing/resetting of `ActionLock`).
///
/// There could be any number of locks active at given point on single ActionBlocker instance:
/// - 'global lock' locked `N` times.
/// - `M` 'partition lock's each locked different number of times.
class PartitionActionBlocker
{
public:
    PartitionActionBlocker() = default;

    bool isCancelled() const { return global_blocker.isCancelled(); }
    bool isCancelledForPartition(const std::string & /*partition_id*/) const;

    /// Temporarily blocks corresponding actions (while the returned object is alive)
    friend class ActionLock;
    ActionLock cancel() { return ActionLock(global_blocker); }
    ActionLock cancelForPartition(const std::string & partition_id);

    /// Cancel the actions forever.
    void cancelForever() { global_blocker.cancelForever(); }

    const std::atomic<int> & getCounter() const { return global_blocker.getCounter(); }

    size_t countPartitionBlockers() const;

    /// Cleanup partition blockers
    void compactPartitionBlockers();

    std::string formatDebug() const;

private:
    void compactPartitionBlockersLocked();
    bool isCancelledForPartitionOnlyLocked(const std::string & partition_id) const;

    ActionBlocker global_blocker;

    mutable std::shared_mutex mutex;
    std::unordered_map<std::string, ActionBlocker> partition_blockers;
    size_t cleanup_counter = 0;
};


}
