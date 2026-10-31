#pragma once

#include <Poco/Timespan.h>

#include <base/types.h>

#include <algorithm>
#include <cstddef>
#include <limits>

namespace DB
{

/// A millisecond count for the Poco APIs that take a `long` - `Event::tryWait`, `Semaphore::wait`
/// and so on. `long` is 32 bits on Windows, so a `UInt64` has to be narrowed; a value past what it
/// can hold saturates, which for a timeout means "about 24 days" rather than a wrapped-around
/// short wait.
/// NOLINTNEXTLINE(google-runtime-int): `long` is the type the Poco APIs take
inline long toPocoMilliseconds(UInt64 milliseconds)
{
    /// NOLINTNEXTLINE(google-runtime-int)
    return static_cast<long>(std::min<UInt64>(milliseconds, std::numeric_limits<long>::max()));
}

/// A `Poco::Timespan` of `seconds`.
///
/// Worth a named function because the obvious spelling, `Poco::Timespan(seconds, 0)`, picks the
/// `(long seconds, long microseconds)` constructor - and `long` is 32 bits on Windows, so a
/// `size_t` count of seconds is narrowed there but not on Linux. This goes through the
/// single-argument constructor instead, which takes a 64-bit microsecond count everywhere.
inline Poco::Timespan timespanFromSeconds(size_t seconds)
{
    return Poco::Timespan(static_cast<Poco::Timespan::TimeDiff>(seconds) * Poco::Timespan::SECONDS);
}

}
