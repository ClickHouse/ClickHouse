#pragma once

#include <Common/ProfileEvents.h>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <memory>

namespace DB
{

/// Interface for throttling operations, allowing to limit the speed of operations in tokens per second.
/// Tokens are usually refer to bytes, but can be any unit of work.
class IThrottler : public boost::noncopyable
{
public:
    virtual ~IThrottler() = default;

    /// Use `amount` tokens, sleeps if required or throws exception on limit overflow.
    /// Returns duration of sleep in nanoseconds (to distinguish sleeping on different kinds of throttlers for metrics)
    virtual UInt64 add(size_t amount) = 0;

    /// Is throttler already accumulated some sleep time and throttling.
    virtual bool isThrottling() const = 0;

    /// Returns the number of tokens available for use.
    /// NOTE: it might refill the bucket state, that is why it is not const.
    virtual Int64 getAvailable() = 0;

    /// Returns the maximum speed in tokens per second.
    virtual UInt64 getMaxSpeed() const = 0;

    /// Returns the maximum burst size in tokens.
    virtual UInt64 getMaxBurst() const = 0;
};

using ThrottlerPtr = std::shared_ptr<IThrottler>;

}
