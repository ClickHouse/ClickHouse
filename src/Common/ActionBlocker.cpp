#include <Common/ActionBlocker.h>

#include <iostream>

namespace DB
{

void PartitionsLocker::lockPartition(const std::string & partition_id)
{
    std::unique_lock lock(mutex);

    ++locked_partitions[partition_id];
}

void PartitionsLocker::unLockPartition(const std::string & partition_id)
{
    std::unique_lock lock(mutex);

    if (auto p = locked_partitions.find(partition_id); p != locked_partitions.end())
    {
        if (--p->second == 0)
            locked_partitions.erase(p);
    }
}

bool PartitionsLocker::isPartitionLocked(const std::string & partition_id) const
{
    std::shared_lock lock(mutex);

    const auto p = locked_partitions.find(partition_id);
    return p != locked_partitions.end();
}

size_t PartitionsLocker::countLockedPartitions() const
{
    std::shared_lock lock(mutex);
    return locked_partitions.size();
}

std::ostream & PartitionsLocker::debugDump(std::ostream & ostr) const
{
    std::shared_lock lock(mutex);

    size_t total_locks = 0;
    for (const auto & i : locked_partitions)
    {
        total_locks += i.second;
    }

    ostr << "Total locks: " << total_locks
            << "\n\ton " << locked_partitions.size() << " partitions:";

    for (const auto & p : locked_partitions)
    {
        ostr << "\n\t'" << p.first << "' locked " << p.second << " times";
    }

    return ostr;
}

bool ActionBlocker::isCancelledForPartition(const std::string & partition_id) const
{
    return isCancelled() || partitions_locker->isPartitionLocked(partition_id);
}

}
