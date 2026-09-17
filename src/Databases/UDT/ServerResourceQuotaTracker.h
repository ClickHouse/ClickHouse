#pragma once

#include <Core/Types.h>

#include <memory>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

class ServerResourceQuotaTrackerError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        LimitExceeded,
        ArithmeticOverflow,
    };

    ServerResourceQuotaTrackerError(Code code_, std::string_view message);

    const Code code;
};

struct ServerResourceQuotaTrackerState
{
    UInt64 charged_bytes = 0;
    UInt64 maximum_bytes = 0;

    bool operator==(const ServerResourceQuotaTrackerState &) const = default;
};

/// Process-wide prospective accounting for immutable authority roots and
/// other UDT catalog/cache owners. Every database resolves the same server
/// layer and joins this one tracker; database-local quotas are deliberately
/// not summed into another per-database envelope.
class ServerResourceQuotaTracker final : public std::enable_shared_from_this<ServerResourceQuotaTracker>
{
public:
    using Ptr = std::shared_ptr<ServerResourceQuotaTracker>;

    class PreparedReservation final
    {
    public:
        PreparedReservation() noexcept = default;
        PreparedReservation(const PreparedReservation &) = delete;
        PreparedReservation & operator=(const PreparedReservation &) = delete;
        PreparedReservation(PreparedReservation && other) noexcept;
        PreparedReservation & operator=(PreparedReservation && other) noexcept;
        ~PreparedReservation();

        UInt64 getChargedBytes() const noexcept { return charged_bytes; }
        /// Transfers the already-counted bytes to the caller's durable/live
        /// ownership. The caller must later invoke `releaseCommitted` exactly
        /// once when that ownership is actually destroyed.
        void commit() noexcept;

    private:
        PreparedReservation(Ptr owner_, UInt64 charged_bytes_) noexcept;

        friend class ServerResourceQuotaTracker;
        Ptr owner;
        UInt64 charged_bytes = 0;
        bool rollback_on_destroy = false;
    };

    /// Returns the single live process policy. Concurrent callers with a
    /// different maximum fail closed; after all owners disappear a later
    /// server lifetime may establish a new policy.
    static Ptr acquireProcessTracker(UInt64 maximum_bytes);

    PreparedReservation prepare(UInt64 additional_bytes);
    void releaseCommitted(UInt64 bytes) noexcept;
    ServerResourceQuotaTrackerState getState() const noexcept;

private:
    explicit ServerResourceQuotaTracker(UInt64 maximum_bytes_);

    const UInt64 maximum_bytes;
    class Impl;
    std::unique_ptr<Impl> impl;
};

}
