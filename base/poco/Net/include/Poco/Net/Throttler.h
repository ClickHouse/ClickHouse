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

        virtual bool throttle(size_t amount, size_t max_block_ns) = 0;
        /// Throttle the transfer of `amount` bytes.
        /// This method should block until it is safe to continue sending or receiving data.
        /// Returns true if blocking was applied, false if no blocking was needed.
        /// It never blocks longer than `max_block_ns` nanoseconds.

        static constexpr size_t unlimited_block_ns = static_cast<size_t>(-1);

        bool throttle(size_t amount)
        {
            return throttle(amount, unlimited_block_ns);
        }
    };

    using ThrottlerPtr = std::shared_ptr<Throttler>;

}
} // namespace Poco::Net
