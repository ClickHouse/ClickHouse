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
/// the integer rep. Unlike saturatedSeconds() (a 1-year cap for a wait_for() timeout, where a huge wait is
/// meaningless), this keeps any representable future instant, so it suits long-lived expiry timestamps such
/// as a cache TTL: `count` is only clamped when the resulting instant would exceed the representable range
/// (~292000 years on a microsecond system_clock), which no legitimate TTL reaches. All arithmetic stays in
/// the clock's integer rep and every step saturates, so no intermediate can overflow for any base or count.
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

    /// count seconds -> ticks, saturating (count * ticks_per_second may exceed the rep).
    Rep offset_ticks = rep_max;
    if (std::cmp_less_equal(count, rep_max / ticks_per_second))
        offset_ticks = static_cast<Rep>(count) * ticks_per_second;

    /// base_ticks + offset_ticks, saturating. offset_ticks >= 0, so the sum can only overflow the positive
    /// end, and only when base_ticks > 0 (then rep_max - base_ticks is a safe non-negative bound). For a
    /// zero or pre-epoch base the sum is <= offset_ticks <= rep_max, so it cannot overflow.
    const Rep base_ticks = base.time_since_epoch().count();
    if (base_ticks > 0 && offset_ticks > rep_max - base_ticks)
        return TimePoint::max();
    return TimePoint(Duration(base_ticks + offset_ticks));
}

}
