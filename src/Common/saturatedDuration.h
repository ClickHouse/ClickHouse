#pragma once

#include <base/types.h>

#include <chrono>
#include <concepts>
#include <limits>
#include <utility>

namespace DB
{

/// Upper bound (1 year) for a millisecond timeout handed to condition_variable::wait_for().
/// libc++ turns wait_for(d) into steady_clock::now() + d in nanoseconds, multiplying milliseconds by
/// 1'000'000; both huge positive and huge negative values overflow that multiplication. Clamping the
/// result into [0, MAX] keeps the timeout well-defined: a negative timeout means "already expired", for
/// which wait_for() returns immediately, so 0 preserves that without risking overflow.
inline constexpr Int64 MAX_WAIT_TIMEOUT_MILLISECONDS = 365LL * 24 * 60 * 60 * 1000;

template <std::integral T>
std::chrono::milliseconds saturatedMilliseconds(T milliseconds)
{
    if (std::cmp_greater(milliseconds, MAX_WAIT_TIMEOUT_MILLISECONDS))
        return std::chrono::milliseconds(MAX_WAIT_TIMEOUT_MILLISECONDS);
    if (std::cmp_less(milliseconds, 0))
        return std::chrono::milliseconds(0);
    return std::chrono::milliseconds(static_cast<Int64>(milliseconds));
}

/// Same clamp for a seconds-typed timeout. A seconds value must be capped before it becomes a
/// std::chrono::seconds, because wait_for still converts seconds to nanoseconds (x 1'000'000'000);
/// values above ~9.2e9 seconds overflow that Int64 conversion. We must not pre-multiply seconds by
/// 1000 to reuse saturatedMilliseconds (that multiplication overflows too), so clamp in seconds.
inline constexpr Int64 MAX_WAIT_TIMEOUT_SECONDS = MAX_WAIT_TIMEOUT_MILLISECONDS / 1000;

template <std::integral T>
std::chrono::seconds saturatedSeconds(T seconds)
{
    if (std::cmp_greater(seconds, MAX_WAIT_TIMEOUT_SECONDS))
        return std::chrono::seconds(MAX_WAIT_TIMEOUT_SECONDS);
    if (std::cmp_less(seconds, 0))
        return std::chrono::seconds(0);
    return std::chrono::seconds(static_cast<Int64>(seconds));
}

/// A future deadline `base + seconds(count)` that saturates at time_point::max() instead of overflowing
/// the clock's integer rep. Unlike saturatedSeconds() (a 1-year cap for a wait_for() timeout), it preserves
/// any representable future instant, so it suits long-lived expiry timestamps such as a cache TTL. The whole
/// base + count sum is done in a 128-bit intermediate that no valid (base, count) can overflow, then clamped
/// once, so the result is exact for every base (including pre-epoch) and every count.
template <std::integral T>
std::chrono::system_clock::time_point saturatedSecondsFrom(std::chrono::system_clock::time_point base, T count)
{
    using TimePoint = std::chrono::system_clock::time_point;
    using Rep = TimePoint::rep;               /// underlying integer rep (microseconds on libc++)
    using Duration = TimePoint::duration;
    static constexpr Rep rep_max = std::numeric_limits<Rep>::max();
    static constexpr Rep ticks_per_second = Duration::period::den / Duration::period::num;

    if (std::cmp_less_equal(count, 0))
        return base;

    /// Widest operands (count up to ~1.8e19 seconds * 1e6 ticks/s, base_ticks up to ~9.2e18) reach ~1.8e25,
    /// far inside __int128 (~1.7e38), so the sum never overflows for any base sign. Since count > 0 here the
    /// deadline can only pass the positive end, so a single max() clamp suffices.
    using Wide = __int128;
    const Wide deadline = static_cast<Wide>(base.time_since_epoch().count()) + static_cast<Wide>(count) * ticks_per_second;
    if (deadline >= static_cast<Wide>(rep_max))
        return TimePoint::max();
    return TimePoint(Duration(static_cast<Rep>(deadline)));
}

}
