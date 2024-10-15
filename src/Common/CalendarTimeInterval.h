#pragma once

#include <Common/IntervalKind.h>
#include <chrono>

namespace DB
{

/// Represents a duration of calendar time, e.g.:
///  * 2 weeks + 5 minutes + and 21 seconds (aka 605121 seconds),
///  * 1 (calendar) month - not equivalent to any number of seconds!
///  * 3 years + 2 weeks (aka 36 months + 604800 seconds).
///
/// Be careful with calendar arithmetic: it's missing many familiar properties of numbers.
/// E.g. x + y - y is not always equal to x (October 31 + 1 month - 1 month = November 1).
struct CalendarTimeInterval
{
    UInt64 seconds = 0;
    UInt64 months = 0;

    using Intervals = std::vector<std::pair<IntervalKind, UInt64>>;

    CalendarTimeInterval() = default;

    /// Year, Quarter, Month are converted to months.
    /// Week, Day, Hour, Minute, Second are converted to seconds.
    /// Millisecond, Microsecond, Nanosecond throw exception.
    explicit CalendarTimeInterval(const Intervals & intervals);

    /// E.g. for {36 months, 604801 seconds} returns {3 years, 2 weeks, 1 second}.
    Intervals toIntervals() const;

    /// Approximate shortest and longest duration in seconds. E.g. a month is [28, 31] days.
    UInt64 minSeconds() const;
    UInt64 maxSeconds() const;

    /// Checks that the interval has only months or only seconds, throws otherwise.
    void assertSingleUnit() const;
    void assertPositive() const;

    /// Add this interval to the timestamp. First months, then seconds.
    /// Gets weird near month boundaries: October 31 + 1 month = December 1.
    /// The returned timestamp is always 28-31 days greater than t.
    std::chrono::sys_seconds advance(std::chrono::system_clock::time_point t) const;

    /// Rounds the timestamp down to the nearest timestamp "aligned" with this interval.
    /// The interval must satisfy assertSingleUnit() and assertPositive().
    ///  * For months, rounds to the start of a month whose abosolute index is divisible by `months`.
    ///    The month index is 0-based starting from January 1970.
    ///    E.g. if the interval is 1 month, rounds down to the start of the month.
    ///  * For seconds, rounds to a timestamp x such that (x - December 29 1969 (Monday)) is divisible
    ///    by this interval.
    ///    E.g. if the interval is 1 week, rounds down to the start of the week (Monday).
    ///
    /// Guarantees:
    ///  * advance(floor(x)) > x
    ///  * floor(advance(floor(x))) = advance(floor(x))
    std::chrono::sys_seconds floor(std::chrono::system_clock::time_point t) const;

    bool operator==(const CalendarTimeInterval & rhs) const;
    bool operator!=(const CalendarTimeInterval & rhs) const;
};

}
