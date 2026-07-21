#pragma once

#include <base/types.h>

#include <chrono>
#include <concepts>
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

}
