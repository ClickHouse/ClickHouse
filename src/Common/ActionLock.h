#pragma once
#include <memory>
#include <atomic>
#include <string>

namespace DB
{

class ActionBlocker;
class PartitionsLocker;
using StorageActionBlockType = size_t;

/// Blocks related action while a ActionLock instance exists
/// ActionBlocker could be destroyed before the lock, in this case ActionLock will safely do nothing in its destructor
class ActionLock
{
public:

    ActionLock() = default;

    explicit ActionLock(const ActionBlocker & blocker);
    explicit ActionLock(const ActionBlocker & blocker, const std::string & partition_id_);

    ActionLock(ActionLock && other) noexcept;
    ActionLock & operator=(ActionLock && other) noexcept;

    ActionLock(const ActionLock & other) = delete;
    ActionLock & operator=(const ActionLock & other) = delete;

    bool expired() const
    {
        return counter_ptr.expired();
    }

    ~ActionLock();

private:
    using Counter = std::atomic<int>;
    using CounterWeakPtr = std::weak_ptr<Counter>;

    CounterWeakPtr counter_ptr;

    std::string partition_id;
    std::weak_ptr<PartitionsLocker> partitions_locker_ptr;
};

}
