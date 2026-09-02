#pragma once
#include <memory>
#include <atomic>


namespace DB
{

class ActionBlocker;
using StorageActionBlockType = size_t;

/// Blocks related action while a ActionLock instance exists
/// ActionBlocker could be destroyed before the lock, in this case ActionLock will safely do nothing in its destructor
class ActionLock
{
public:

    ActionLock() = default;

    explicit ActionLock(const ActionBlocker & blocker);

    ActionLock(ActionLock && other) noexcept;
    ActionLock & operator=(ActionLock && other) noexcept;

    ActionLock(const ActionLock & other) = delete;
    ActionLock & operator=(const ActionLock & other) = delete;

    bool expired() const
    {
        return counter_ptr.expired();
    }

    ~ActionLock()
    {
        if (auto counter = counter_ptr.lock())
            --(*counter);
    }

private:
    using Counter = std::atomic<int>;
    using CounterWeakPtr = std::weak_ptr<Counter>;

    CounterWeakPtr counter_ptr;
};

}
