#pragma once
#include <atomic>
#include <memory>
#include <Common/ActionLock.h>


namespace DB
{

/// An atomic variable that is used to block and interrupt certain actions.
/// If it is not zero then actions related with it should be considered as interrupted.
/// Uses shared_ptr and the lock uses weak_ptr to be able to "hold" a lock when an object with blocker has already died.
class ActionBlocker
{
public:
    ActionBlocker() : counter(std::make_shared<Counter>(0)) {}

    bool isCancelled() const { return *counter > 0; }

    /// Temporarily blocks corresponding actions (while the returned object is alive)
    friend class ActionLock;
    [[nodiscard]] ActionLock cancel() { return ActionLock(*this); }

    /// Cancel the actions forever.
    void cancelForever() { ++(*counter); }

    /// Returns reference to counter to allow to watch on it directly.
    const std::atomic<int> & getCounter() const { return *counter; }

private:
    using Counter = std::atomic<int>;
    using CounterPtr = std::shared_ptr<Counter>;

    CounterPtr counter;
};


}
