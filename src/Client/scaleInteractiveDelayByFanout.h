#pragma once

#include <cmath>
#include <cstddef>
#include <limits>

#include <base/types.h>
#include <Common/thread_local_rng.h>


namespace DB
{

/// Scale `interactive_delay` by `sqrt(total_fanout)` with per-connection jitter in `[1.0, 2.0)`,
/// to reduce progress/profile event traffic from distributed queries. Each remote server will
/// send updates less frequently, proportional to the square root of the total number of remote
/// connections; the jitter desynchronizes progress reports across connections to avoid TCP incast
/// and make the progress bar smoother.
///
/// The scaled value is saturated to `UINT64_MAX` on overflow: user-controlled `interactive_delay`
/// is an unbounded `UInt64`, so `delay * scale * jitter` as a `double` can exceed the `UInt64`
/// range. Casting such a `double` back to `UInt64` without saturation is undefined behavior
/// (UBSan finding in `Stress test (experimental, serverfuzz, arm_asan_ubsan)`).
inline UInt64 scaleInteractiveDelayByFanout(UInt64 delay, size_t total_fanout)
{
    if (total_fanout <= 1)
        return delay;

    const double scale = std::sqrt(static_cast<double>(total_fanout));
    /// Random jitter in range [1.0, 2.0).
    const double jitter = 1.0 + (thread_local_rng() % 1000) / 1000.0;
    const double scaled = std::ceil(static_cast<double>(delay) * scale * jitter);

    /// `static_cast<UInt64>(x)` is undefined behavior when `x` is outside the `UInt64` range.
    /// `2^64` is exactly representable as `double`, so comparing against it is reliable.
    constexpr double two_pow_64 = 18446744073709551616.0;
    if (scaled >= two_pow_64)
        return std::numeric_limits<UInt64>::max();
    return static_cast<UInt64>(scaled);
}

}
