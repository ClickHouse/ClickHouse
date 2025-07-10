#pragma once

#include <Common/IThrottler.h>
#include <array>
#include <limits>

namespace DB
{

/// A throttler that combines multiple throttlers into a single interface.
/// It allows you to create an array of throttlers with a fixed size and use them collectively.
/// It is different from throttler hierarchy and should be used to combine throttlers that are not related to each other.
/// For example, backup operation should use (a) either remote or local throttler and (b) backup throttler.
/// In such a case it is not possible to use hierarchy, but it is possible to use this class.
template <size_t size>
class ThrottlerArray : public IThrottler
{
public:
    /// Variadic constructor for perfect forwarding and size deduction
    template <typename... ThrottlerArgs>
    explicit ThrottlerArray(ThrottlerArgs && ... args)
        : throttlers{std::forward<ThrottlerArgs>(args)...}
    {
        static_assert(sizeof...(args) == size, "Number of arguments must match template size parameter");
    }

    /// Use `amount` tokens on all throttlers
    UInt64 add(size_t amount) override
    {
        UInt64 total_sleep_ns = 0;
        for (const auto & throttler : throttlers)
        {
            total_sleep_ns += throttler->add(amount);
        }
        return total_sleep_ns;
    }

    /// Check if any throttler is currently throttling
    bool isThrottling() const override
    {
        for (const auto & throttler : throttlers)
        {
            if (throttler->isThrottling())
                return true;
        }
        return false;
    }

    /// Returns the minimum number of tokens available for use.
    Int64 getAvailable() override
    {
        Int64 min_available = std::numeric_limits<Int64>::max();
        for (const auto & throttler : throttlers)
            min_available = std::min(min_available, throttler->getAvailable());
        return min_available;
    }

    /// Returns the minimum speed among all throttlers (the most restrictive one)
    UInt64 getMaxSpeed() const override
    {
        UInt64 min_speed = std::numeric_limits<UInt64>::max();
        for (const auto & throttler : throttlers)
        {
            UInt64 speed = throttler->getMaxSpeed();
            if (speed > 0) // Only consider throttlers with actual speed limits
                min_speed = std::min(min_speed, speed);
        }
        return min_speed == std::numeric_limits<UInt64>::max() ? 0 : min_speed;
    }

    /// Returns the minimum burst size among all throttlers (the most restrictive one)
    UInt64 getMaxBurst() const override
    {
        UInt64 min_burst = std::numeric_limits<UInt64>::max();
        for (const auto & throttler : throttlers)
            min_burst = std::min(min_burst, throttler->getMaxBurst());
        return min_burst;
    }

private:
    std::array<ThrottlerPtr, size> throttlers;
};

/// Adds a new throttler to the existing one, combining them if necessary.
/// Note that it does not modify new throttler and does not modify hierarchy of existing throttlers.
inline void addThrottler(ThrottlerPtr & throttler, const ThrottlerPtr & new_throttler)
{
    if (!throttler)
        throttler = new_throttler; // Replace empty throttler with new one
    else if (new_throttler)
        throttler = std::make_shared<ThrottlerArray<2>>(throttler, new_throttler); // Combine existing and new throttler
}

}
