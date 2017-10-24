#pragma once

#include <atomic>

namespace DB
{

/// An atomic variable that is used to block and interrupt certain actions
/// If it is not zero then actions related with it should be considered as interrupted
class ActionBlocker
{
private:
    mutable std::atomic<int> counter{0};

public:
    bool isCancelled() const { return counter > 0; }

    /// Temporarily blocks corresponding actions (while the returned object is alive)
    struct BlockHolder;
    BlockHolder cancel() const { return BlockHolder(this); }

    /// Cancel the actions forever.
    void cancelForever() const { ++counter; }

    /// Returns reference to counter to allow to watch on it directly.
    auto & getCounter() { return counter; }

    /// Blocks related action while a BlockerHolder instance exists
    struct BlockHolder
    {
        explicit BlockHolder(const ActionBlocker * var_ = nullptr) : var(var_)
        {
            if (var)
                ++var->counter;
        }

        BlockHolder(BlockHolder && other) noexcept
        {
            *this = std::move(other);
        }

        BlockHolder & operator=(BlockHolder && other) noexcept
        {
            var = other.var;
            other.var = nullptr;
            return *this;
        }

        BlockHolder(const BlockHolder & other) = delete;
        BlockHolder & operator=(const BlockHolder & other) = delete;

        ~BlockHolder()
        {
            if (var)
                --var->counter;
        }

    private:
        const ActionBlocker * var = nullptr;
    };
};

}
