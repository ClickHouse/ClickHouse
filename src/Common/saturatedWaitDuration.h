#pragma once

#include <algorithm>
#include <chrono>
#include <concepts>
#include <limits>
#include <utility>

#include <base/types.h>

namespace DB
{

/// Largest millisecond count safe both to convert to nanoseconds and to add to a steady_clock time
/// point. Half the Int64 nanosecond range: the other half covers steady_clock::now(), which on
/// Linux is nanoseconds since boot, so `now() + duration` cannot wrap either.
inline constexpr Int64 MAX_WAIT_MILLISECONDS = (std::numeric_limits<Int64>::max() / 2) / 1'000'000;

/// For a count reaching a nanosecond context: a cv/future wait_for with a predicate, or
/// steady_clock arithmetic. Comparisons are signedness-agnostic, so an unsigned count holding a
/// wrapped signed value is clamped too. A non-positive count becomes zero.
template <std::integral T>
inline std::chrono::milliseconds saturatedWaitMilliseconds(T ms)
{
    if (std::cmp_less_equal(ms, 0))
        return std::chrono::milliseconds(0);
    if (std::cmp_greater(ms, MAX_WAIT_MILLISECONDS))
        return std::chrono::milliseconds(MAX_WAIT_MILLISECONDS);
    return std::chrono::milliseconds(static_cast<Int64>(ms));
}

/// For a value that is not a wait and must keep its magnitude and sign: only the multiplication is
/// made total, at the full Int64 range of Poco::Timespan::TimeDiff. The wait bound must not be used
/// here, it would change the value the server acts on and reports.
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

/// Upper-only, for a callee that converts to chrono itself and treats zero as "timer disabled"
/// (NuRaft). Converting to unsigned first, as the callee's uint64_t parameter already does, keeps a
/// nonzero input nonzero.
template <std::integral T>
inline UInt64 saturatedWaitMillisecondsCountNonZero(T ms)
{
    const UInt64 unsigned_ms = static_cast<UInt64>(ms);
    return std::min<UInt64>(unsigned_ms, static_cast<UInt64>(MAX_WAIT_MILLISECONDS));
}

}
