#pragma once

#include <Poco/Timespan.h>

#include <cstddef>

namespace DB
{

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
