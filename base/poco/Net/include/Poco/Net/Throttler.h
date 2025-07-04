#pragma once

#include <cstddef>
#include <memory>


namespace Poco
{
namespace Net
{

    class Throttler
    /// Throttler is an interface for controlling the rate of data transfer.
    /// It can be used to limit the speed of data being sent or received over a network connection.
    {
    public:
        virtual ~Throttler() = default;

        virtual bool throttle(size_t amount, size_t max_block_us) = 0;
        /// Throttle the transfer of `amount` bytes.
        /// This method should block until it is safe to continue sending or receiving data.
        /// Returns true if blocking was applied, false if no blocking was needed.

        virtual void throttleNonBlocking(size_t amount) = 0;
        /// Throttle the transfer of `amount` bytes in a non-blocking manner.
        /// It never blocks, but updates the internal state of the throttler.

        bool throttle(size_t amount)
        {
            return throttle(amount, size_t(-1));
        }
    };

    using ThrottlerPtr = std::shared_ptr<Throttler>;

}
} // namespace Poco::Net
