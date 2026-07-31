#include <gtest/gtest.h>

#include <limits>

#include <Client/scaleInteractiveDelayByFanout.h>

using namespace DB;

TEST(ScaleInteractiveDelayByFanout, NoScalingForSingleConnection)
{
    /// total_fanout <= 1 must return the value unchanged (no random jitter applied).
    EXPECT_EQ(scaleInteractiveDelayByFanout(100'000, 0), 100'000u);
    EXPECT_EQ(scaleInteractiveDelayByFanout(100'000, 1), 100'000u);
}

TEST(ScaleInteractiveDelayByFanout, ScalesWithinJitterBounds)
{
    /// With fanout = 4 (scale = 2) and jitter in [1.0, 2.0), the result must be in
    /// [delay * scale * 1.0, delay * scale * 2.0). We allow a +1 slack for the ceil().
    const UInt64 delay = 10'000'000;
    const size_t fanout = 4;
    const double min_expected = static_cast<double>(delay) * 2.0 * 1.0;
    const double max_expected = static_cast<double>(delay) * 2.0 * 2.0;

    for (int i = 0; i < 1000; ++i)
    {
        const UInt64 result = scaleInteractiveDelayByFanout(delay, fanout);
        EXPECT_GE(result, static_cast<UInt64>(min_expected));
        EXPECT_LE(result, static_cast<UInt64>(max_expected) + 1);
    }
}

TEST(ScaleInteractiveDelayByFanout, SaturatesOnOverflow)
{
    /// With `delay` close to `UINT64_MAX` and any fanout > 1, the scaled `double`
    /// exceeds `UINT64_MAX`. The result must saturate at `UINT64_MAX`, not wrap or
    /// trigger undefined behavior from `static_cast<UInt64>` on an out-of-range `double`.
    const UInt64 uint64_max = std::numeric_limits<UInt64>::max();
    const UInt64 almost_max = uint64_max - 1;

    EXPECT_EQ(scaleInteractiveDelayByFanout(uint64_max, 4), uint64_max);
    EXPECT_EQ(scaleInteractiveDelayByFanout(almost_max, 2), uint64_max);
    /// Even a tiny fanout > 1 with a large enough delay overflows the scaled product.
    EXPECT_EQ(scaleInteractiveDelayByFanout(uint64_max / 2, 16), uint64_max);

    /// Historical UBSan finding reproducer: `delay * scale * jitter` produces a value
    /// slightly above `UINT64_MAX` (see `Stress test (experimental, serverfuzz, arm_asan_ubsan)`).
    /// With any `delay >= UINT64_MAX / 2` and fanout >= 4 (scale >= 2), `delay * scale * jitter`
    /// reliably exceeds `UINT64_MAX` even with minimum jitter 1.0. The saturating cast prevents
    /// the UBSan trap.
    EXPECT_EQ(scaleInteractiveDelayByFanout(uint64_max / 2 + 1, 4), uint64_max);
}

TEST(ScaleInteractiveDelayByFanout, ZeroDelayStaysZero)
{
    for (size_t fanout : {size_t{0}, size_t{1}, size_t{2}, size_t{100}, size_t{10'000}})
        EXPECT_EQ(scaleInteractiveDelayByFanout(0, fanout), 0u);
}
