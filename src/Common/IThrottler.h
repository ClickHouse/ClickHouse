#pragma once

#include <Poco/Net/Throttler.h>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <memory>

namespace DB
{

/// Interface for throttling operations, allowing to limit the speed of operations in tokens per second.
/// Tokens are usually refer to bytes, but can be any unit of work.
class IThrottler : public Poco::Net::Throttler, public boost::noncopyable
{
public:
    /// Is throttler already accumulated some sleep time and throttling.
    virtual bool isThrottling() const = 0;

    /// Returns the number of tokens available for use.
    /// NOTE: it might refill the bucket state, that is why it is not const.
    virtual Int64 getAvailable() = 0;

    /// Returns the maximum speed in tokens per second.
    virtual UInt64 getMaxSpeed() const = 0;

    /// Returns the maximum burst size in tokens.
    virtual UInt64 getMaxBurst() const = 0;

    /// Charge `amount` tokens for the data that was copied from the OS page cache,
    /// i.e. without performing any block device I/O.
    /// Throttlers that exist to limit the bandwidth of a block device ignore such reads,
    /// all the others account them as usual.
    virtual bool throttleOSPageCacheRead(size_t amount, size_t max_block_ns)
    {
        return throttle(amount, max_block_ns);
    }

    bool throttleOSPageCacheRead(size_t amount)
    {
        return throttleOSPageCacheRead(amount, unlimited_block_ns);
    }
};

using ThrottlerPtr = std::shared_ptr<IThrottler>;

}
