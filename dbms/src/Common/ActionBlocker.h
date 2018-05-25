#pragma once

#include <atomic>

namespace DB
{

/// An atomic variable that is used to block and interrupt certain actions
/// If it is not zero then actions related with it should be considered as interrupted
class ActionBlocker
{
private:
    std::atomic<int> counter{0};

public:
    bool isCancelled() const { return counter > 0; }

    /// Temporarily blocks corresponding actions (while the returned object is alive)
    struct LockHolder;
    LockHolder cancel() { return LockHolder(this); }

    /// Cancel the actions forever.
    void cancelForever() { ++counter; }

    /// Returns reference to the counter to allow observing it directly.
    const auto & getCounter() { return counter; }

    /// Blocks related action while a BlockerHolder instance exists
    struct LockHolder
    {
        explicit LockHolder(ActionBlocker * var_ = nullptr) : var(var_)
        {
            if (var)
                ++var->counter;
        }

        LockHolder(LockHolder && other) noexcept
        {
            *this = std::move(other);
        }

        LockHolder & operator=(LockHolder && other) noexcept
        {
            var = other.var;
            other.var = nullptr;
            return *this;
        }

        LockHolder(const LockHolder & other) = delete;
        LockHolder & operator=(const LockHolder & other) = delete;

        ~LockHolder()
        {
            if (var)
                --var->counter;
        }

    private:
        ActionBlocker * var = nullptr;
    };
};

}
