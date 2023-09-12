#include "ActionLock.h"
#include <Common/ActionBlocker.h>


namespace DB
{

ActionLock::ActionLock(const ActionBlocker & blocker) : counter_ptr(blocker.counter)
{
    if (auto counter = counter_ptr.lock())
        ++(*counter);
}

ActionLock::ActionLock(const ActionBlocker & blocker, const std::string & partition_id_)
    : counter_ptr(blocker.counter)
    , partition_id(partition_id_)
    , partitions_locker_ptr(blocker.partitions_locker)
{
    if (auto partitions_locker = partitions_locker_ptr.lock())
        partitions_locker->lockPartition(partition_id);
}

ActionLock::~ActionLock()
{
    if (partition_id.empty())
    {
        if (auto counter = counter_ptr.lock())
            --(*counter);
    }
    else
    {
        if (auto partition_locker = partitions_locker_ptr.lock())
            partition_locker->unLockPartition(partition_id);
    }
}

ActionLock::ActionLock(ActionLock && other) noexcept
{
    *this = std::move(other);
}

ActionLock & ActionLock::operator=(ActionLock && other) noexcept
{
    auto lock_lhs = this->counter_ptr.lock();

    counter_ptr = std::move(other.counter_ptr);
    /// After move other.counter_ptr still points to counter, reset it explicitly
    other.counter_ptr.reset();

    if (lock_lhs && partition_id.empty())
        --(*lock_lhs);

    if (!partition_id.empty())
    {
        if (auto partitions_locker_lhs = this->partitions_locker_ptr.lock())
            partitions_locker_lhs->unLockPartition(partition_id);
    }

    this->partitions_locker_ptr = std::move(other.partitions_locker_ptr);
    other.partitions_locker_ptr.reset();
    this->partition_id = std::move(other.partition_id);

    return *this;
}

}
