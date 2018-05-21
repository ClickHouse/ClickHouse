#pragma once

#include <atomic>
#include <memory>
#include <Common/ActionLock.h>

namespace DB
{

/// An atomic variable that is used to block and interrupt certain actions
/// If it is not zero then actions related with it should be considered as interrupted
class ActionBlocker
{
private:
    using Counter = std::atomic<int>;
    using CounterPtr = std::shared_ptr<Counter>;

    mutable CounterPtr counter;

public:
    ActionBlocker() : counter(std::make_shared<Counter>(0)) {}

    bool isCancelled() const { return *counter > 0; }

    /// Temporarily blocks corresponding actions (while the returned object is alive)
    friend class ActionLock;
    ActionLock cancel() const { return ActionLock(*this); }

    /// Cancel the actions forever.
    void cancelForever() const { ++(*counter); }

    /// Returns reference to counter to allow to watch on it directly.
    auto & getCounter() { return *counter; }
};

}
