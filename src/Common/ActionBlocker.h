#pragma once
#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include <Common/ActionLock.h>

namespace DB
{

class PartitionsLocker
{
public:
    void lockPartition(const std::string & partition_id);
    void unLockPartition(const std::string & partition_id);
    bool isPartitionLocked(const std::string & partition_id) const;
    size_t countLockedPartitions() const;
    std::ostream & debugDump(std::ostream & ostr) const;

private:
    mutable std::shared_mutex mutex;
    std::unordered_map<std::string, int> locked_partitions;
};

/// An atomic variable that is used to block and interrupt certain actions.
/// If it is not zero then actions related with it should be considered as interrupted.
/// Uses shared_ptr and the lock uses weak_ptr to be able to "hold" a lock when an object with blocker has already died.
class ActionBlocker
{
public:
    ActionBlocker() : counter(std::make_shared<Counter>(0)), partitions_locker(std::make_shared<PartitionsLocker>()) {}

    bool isCancelled() const { return *counter > 0; }
    bool isCancelledForPartition(const std::string & /*partition_id*/) const;

    /// Temporarily blocks corresponding actions (while the returned object is alive)
    friend class ActionLock;
    ActionLock cancel() { return ActionLock(*this); }
    ActionLock cancelForPartition(const std::string & partition_id) { return ActionLock(*this, partition_id); }

    /// Cancel the actions forever.
    void cancelForever() { ++(*counter); }

    /// Returns reference to counter to allow to watch on it directly.
    const std::atomic<int> & getCounter() const { return *counter; }

private:
    using Counter = std::atomic<int>;
    using CounterPtr = std::shared_ptr<Counter>;

    CounterPtr counter;
    std::shared_ptr<PartitionsLocker> partitions_locker;
};


}
