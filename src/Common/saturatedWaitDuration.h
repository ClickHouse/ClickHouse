#pragma once

#include <algorithm>
#include <chrono>
#include <concepts>
#include <limits>
#include <utility>

#include <base/types.h>

namespace DB
{

/// Largest millisecond count that is safe both to convert to nanoseconds and to add to a
/// steady_clock time point. Half of the Int64 nanosecond range, mirroring
/// AtomicStopwatch::secondsToNanoseconds (src/Common/Stopwatch.h): the remaining half covers
/// steady_clock::now(), which on Linux is nanoseconds since boot, so `now() + duration` cannot
/// wrap either.
inline constexpr Int64 MAX_WAIT_MILLISECONDS = (std::numeric_limits<Int64>::max() / 2) / 1'000'000;

/// Saturating milliseconds -> std::chrono::milliseconds for a count that reaches a nanosecond
/// context: a cv/future wait_for with a predicate, or steady_clock arithmetic. The comparisons are
/// signedness-agnostic so an unsigned count that came from a wrapped signed value is clamped too.
/// A non-positive count becomes zero, which is what a wait already does with it.
template <std::integral T>
inline std::chrono::milliseconds saturatedWaitMilliseconds(T ms)
{
    if (std::cmp_less_equal(ms, 0))
        return std::chrono::milliseconds(0);
    if (std::cmp_greater(ms, MAX_WAIT_MILLISECONDS))
        return std::chrono::milliseconds(MAX_WAIT_MILLISECONDS);
    return std::chrono::milliseconds(static_cast<Int64>(ms));
}

/// Saturating milliseconds -> microseconds for a value that is not a wait but a policy an
/// operator configured, so it must keep its magnitude and its sign: only the multiplication is
/// made total, at the full Int64 range of Poco::Timespan::TimeDiff. Do NOT use the wait bound
/// here, that would change the value the server acts on and reports.
template <std::integral T>
inline Int64 saturatedMicrosecondsFromMilliseconds(T ms)
{
    constexpr Int64 max_ms = std::numeric_limits<Int64>::max() / 1000;
    constexpr Int64 min_ms = std::numeric_limits<Int64>::min() / 1000;
    if (std::cmp_greater(ms, max_ms))
        return std::numeric_limits<Int64>::max();
    if (std::cmp_less(ms, min_ms))
        return std::numeric_limits<Int64>::min();
    return static_cast<Int64>(ms) * 1000;
}

/// Upper-only saturation for a boundary whose callee performs the chrono conversion itself and
/// treats zero as "timer disabled" (NuRaft). The count is converted to unsigned first, exactly as
/// the callee's uint64_t parameter does today, so a nonzero input can never become zero.
template <std::integral T>
inline UInt64 saturatedWaitMillisecondsCountNonZero(T ms)
{
    const UInt64 unsigned_ms = static_cast<UInt64>(ms);
    return std::min<UInt64>(unsigned_ms, static_cast<UInt64>(MAX_WAIT_MILLISECONDS));
}

}
