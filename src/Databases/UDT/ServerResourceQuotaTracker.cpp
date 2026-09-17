#include <Databases/UDT/ServerResourceQuotaTracker.h>

#include <DataTypes/UDT/ResourceLimits.h>

#include <Common/CurrentMetrics.h>

#include <exception>
#include <limits>
#include <mutex>
#include <utility>

namespace CurrentMetrics
{
extern const Metric UDTLiveCatalogAndCacheBytes;
}

namespace DB::UDT
{
namespace
{

std::mutex process_tracker_mutex;
std::weak_ptr<ServerResourceQuotaTracker> process_tracker;

}

class ServerResourceQuotaTracker::Impl final
{
public:
    mutable std::mutex mutex;
    UInt64 charged_bytes = 0;
};

ServerResourceQuotaTrackerError::ServerResourceQuotaTrackerError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

ServerResourceQuotaTracker::PreparedReservation::PreparedReservation(Ptr owner_, UInt64 charged_bytes_) noexcept
    : owner(std::move(owner_))
    , charged_bytes(charged_bytes_)
    , rollback_on_destroy(charged_bytes != 0)
{
}

ServerResourceQuotaTracker::PreparedReservation::PreparedReservation(PreparedReservation && other) noexcept
    : owner(std::move(other.owner))
    , charged_bytes(std::exchange(other.charged_bytes, 0))
    , rollback_on_destroy(std::exchange(other.rollback_on_destroy, false))
{
}

ServerResourceQuotaTracker::PreparedReservation &
ServerResourceQuotaTracker::PreparedReservation::operator=(PreparedReservation && other) noexcept
{
    if (this == &other)
        return *this;
    if (rollback_on_destroy)
        owner->releaseCommitted(charged_bytes);
    owner = std::move(other.owner);
    charged_bytes = std::exchange(other.charged_bytes, 0);
    rollback_on_destroy = std::exchange(other.rollback_on_destroy, false);
    return *this;
}

ServerResourceQuotaTracker::PreparedReservation::~PreparedReservation()
{
    if (rollback_on_destroy)
        owner->releaseCommitted(charged_bytes);
}

void ServerResourceQuotaTracker::PreparedReservation::commit() noexcept
{
    rollback_on_destroy = false;
}

ServerResourceQuotaTracker::ServerResourceQuotaTracker(UInt64 maximum_bytes_)
    : maximum_bytes(maximum_bytes_)
    , impl(std::make_unique<Impl>())
{
    const UInt64 implementation_maximum = getResourceImplementationLimits().get(ResourceLimit::LiveCatalogAndCacheBytesPerServer);
    if (maximum_bytes == 0 || maximum_bytes > implementation_maximum)
    {
        throw ServerResourceQuotaTrackerError(
            ServerResourceQuotaTrackerError::Code::InvalidConfiguration,
            "server UDT live catalog/cache byte maximum is zero or exceeds its implementation ceiling");
    }
}

ServerResourceQuotaTracker::Ptr ServerResourceQuotaTracker::acquireProcessTracker(UInt64 maximum_bytes)
{
    std::lock_guard lock(process_tracker_mutex);
    if (auto existing = process_tracker.lock())
    {
        if (existing->maximum_bytes != maximum_bytes)
        {
            throw ServerResourceQuotaTrackerError(
                ServerResourceQuotaTrackerError::Code::InvalidConfiguration,
                "live UDT databases resolved conflicting process-wide catalog/cache byte maxima");
        }
        return existing;
    }

    auto created = Ptr(new ServerResourceQuotaTracker(maximum_bytes));
    process_tracker = created;
    return created;
}

ServerResourceQuotaTracker::PreparedReservation ServerResourceQuotaTracker::prepare(UInt64 additional_bytes)
{
    auto self = shared_from_this();
    if (additional_bytes == 0)
        return PreparedReservation(std::move(self), 0);

    std::lock_guard lock(impl->mutex);
    if (additional_bytes > std::numeric_limits<UInt64>::max() - impl->charged_bytes)
    {
        throw ServerResourceQuotaTrackerError(
            ServerResourceQuotaTrackerError::Code::ArithmeticOverflow, "server UDT live catalog/cache byte charge overflows UInt64");
    }
    const UInt64 prospective = impl->charged_bytes + additional_bytes;
    if (prospective > maximum_bytes)
    {
        throw ServerResourceQuotaTrackerError(
            ServerResourceQuotaTrackerError::Code::LimitExceeded, "server UDT live catalog/cache byte quota is exceeded");
    }
    impl->charged_bytes = prospective;
    CurrentMetrics::set(CurrentMetrics::UDTLiveCatalogAndCacheBytes, static_cast<CurrentMetrics::Value>(prospective));
    return PreparedReservation(std::move(self), additional_bytes);
}

void ServerResourceQuotaTracker::releaseCommitted(UInt64 bytes) noexcept
{
    if (bytes == 0)
        return;
    std::lock_guard lock(impl->mutex);
    if (bytes > impl->charged_bytes)
        std::terminate();
    impl->charged_bytes -= bytes;
    CurrentMetrics::set(CurrentMetrics::UDTLiveCatalogAndCacheBytes, static_cast<CurrentMetrics::Value>(impl->charged_bytes));
}

ServerResourceQuotaTrackerState ServerResourceQuotaTracker::getState() const noexcept
{
    std::lock_guard lock(impl->mutex);
    return {
        .charged_bytes = impl->charged_bytes,
        .maximum_bytes = maximum_bytes,
    };
}

}
