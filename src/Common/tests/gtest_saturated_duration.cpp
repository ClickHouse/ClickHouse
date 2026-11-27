#include <gtest/gtest.h>

#include <Common/saturatedDuration.h>

#include <chrono>
#include <cstdint>
#include <limits>

using namespace DB;

/// saturatedMilliseconds is the shared clamp applied to every user-controlled millisecond timeout
/// before it reaches a chrono now()+duration deadline or a condition_variable/future wait. The
/// invariant it guarantees: the returned duration always fits in [0, one year], so converting it to
/// a steady_clock nanosecond duration (x * 1'000'000) never overflows Int64 and now()+d never wraps.
GTEST_TEST(SaturatedMilliseconds, ClampsHugePositiveToOneYear)
{
    const auto max = std::chrono::milliseconds(MAX_WAIT_TIMEOUT_MILLISECONDS);

    // Values that already exceed the cap saturate to it.
    ASSERT_EQ(saturatedMilliseconds(std::numeric_limits<Int64>::max()), max);
    ASSERT_EQ(saturatedMilliseconds(std::numeric_limits<UInt64>::max()), max);
    // 1e14 ms is below the SettingFieldTimespan source cap (INT64_MAX / 1000) yet far above the
    // chrono-safe ms->ns range; it must still be clamped here.
    ASSERT_EQ(saturatedMilliseconds(Int64(100000000000000LL)), max);
    ASSERT_EQ(saturatedMilliseconds(MAX_WAIT_TIMEOUT_MILLISECONDS + 1), max);
}

GTEST_TEST(SaturatedMilliseconds, ClampsNegativeToZero)
{
    // A negative timeout means "already expired"; wait_for/now()+d must see 0, not an underflowing
    // negative nanosecond duration. SettingFieldTimespan can carry a huge negative microsecond value.
    ASSERT_EQ(saturatedMilliseconds(Int64(-1)), std::chrono::milliseconds(0));
    ASSERT_EQ(saturatedMilliseconds(std::numeric_limits<Int64>::min()), std::chrono::milliseconds(0));
    ASSERT_EQ(saturatedMilliseconds(Int64(-100000000000000LL)), std::chrono::milliseconds(0));
}

GTEST_TEST(SaturatedMilliseconds, PassesThroughInRangeValues)
{
    ASSERT_EQ(saturatedMilliseconds(Int64(0)), std::chrono::milliseconds(0));
    ASSERT_EQ(saturatedMilliseconds(Int64(30000)), std::chrono::milliseconds(30000));
    ASSERT_EQ(saturatedMilliseconds(MAX_WAIT_TIMEOUT_MILLISECONDS), std::chrono::milliseconds(MAX_WAIT_TIMEOUT_MILLISECONDS));

    // The clamped result, converted to nanoseconds the way libc++ does for a steady_clock wait, fits
    // comfortably in Int64 (the whole point of the clamp).
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(saturatedMilliseconds(std::numeric_limits<Int64>::max()));
    ASSERT_GT(ns.count(), 0);
    ASSERT_LT(ns.count(), std::numeric_limits<Int64>::max());
}

/// saturatedSeconds is the seconds-typed sibling for timeouts kept in seconds (a UInt64 seconds
/// setting must be capped before it becomes a std::chrono::seconds, because wait_for still turns
/// seconds into nanoseconds; values above the cap would overflow that x 1'000'000'000 conversion).
GTEST_TEST(SaturatedSeconds, ClampsHugePositiveToOneYear)
{
    const auto max = std::chrono::seconds(MAX_WAIT_TIMEOUT_SECONDS);

    ASSERT_EQ(saturatedSeconds(std::numeric_limits<Int64>::max()), max);
    ASSERT_EQ(saturatedSeconds(std::numeric_limits<UInt64>::max()), max);
    ASSERT_EQ(saturatedSeconds(MAX_WAIT_TIMEOUT_SECONDS + 1), max);
}

GTEST_TEST(SaturatedSeconds, ClampsNegativeToZero)
{
    ASSERT_EQ(saturatedSeconds(Int64(-1)), std::chrono::seconds(0));
    ASSERT_EQ(saturatedSeconds(std::numeric_limits<Int64>::min()), std::chrono::seconds(0));
}

GTEST_TEST(SaturatedSeconds, PassesThroughInRangeValues)
{
    ASSERT_EQ(saturatedSeconds(Int64(0)), std::chrono::seconds(0));
    ASSERT_EQ(saturatedSeconds(Int64(300)), std::chrono::seconds(300));
    ASSERT_EQ(saturatedSeconds(MAX_WAIT_TIMEOUT_SECONDS), std::chrono::seconds(MAX_WAIT_TIMEOUT_SECONDS));

    // The clamped seconds value, converted to nanoseconds as a steady_clock wait would, fits in Int64.
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(saturatedSeconds(std::numeric_limits<UInt64>::max()));
    ASSERT_GT(ns.count(), 0);
    ASSERT_LT(ns.count(), std::numeric_limits<Int64>::max());
}

/// saturatedSecondsFrom builds a future deadline base + seconds(count) for long-lived expiry timestamps
/// (e.g. the query cache TTL). Unlike the wait_for clamps above it preserves any representable instant and
/// only saturates at time_point::max(); all arithmetic stays in the clock rep and every step saturates, so
/// no intermediate overflows for any base (including pre-epoch) or count.
namespace
{
using SysClock = std::chrono::system_clock;
using SysTimePoint = SysClock::time_point;
constexpr auto sys_max = SysTimePoint::max();
}

GTEST_TEST(SaturatedSecondsFrom, PreservesRepresentableTimeouts)
{
    const SysTimePoint base(std::chrono::seconds(1'000'000'000)); // a fixed post-epoch instant

    // In-range TTLs are added exactly, not truncated to any smaller cap.
    ASSERT_EQ(saturatedSecondsFrom(base, 0), base);
    ASSERT_EQ(saturatedSecondsFrom(base, 60), base + std::chrono::seconds(60));
    // ~3.17 years: well beyond a one-year wait cap, but a perfectly valid, representable expiry.
    ASSERT_EQ(saturatedSecondsFrom(base, Int64(100'000'000)), base + std::chrono::seconds(100'000'000));
}

GTEST_TEST(SaturatedSecondsFrom, ClampsToMaxInsteadOfOverflowing)
{
    const SysTimePoint base(std::chrono::seconds(1'000'000'000));

    // Counts large enough to push the deadline past the representable range saturate to time_point::max().
    ASSERT_EQ(saturatedSecondsFrom(base, std::numeric_limits<Int64>::max()), sys_max);
    ASSERT_EQ(saturatedSecondsFrom(base, std::numeric_limits<UInt64>::max()), sys_max);
    // The largest value a Seconds setting can carry (INT64_MAX / 1'000'000 seconds) must not overflow.
    ASSERT_EQ(saturatedSecondsFrom(base, Int64(9223372036854LL)), sys_max);
}

GTEST_TEST(SaturatedSecondsFrom, NonPositiveCountReturnsBase)
{
    const SysTimePoint base(std::chrono::seconds(1'000'000'000));
    ASSERT_EQ(saturatedSecondsFrom(base, 0), base);
    ASSERT_EQ(saturatedSecondsFrom(base, Int64(-1)), base);
    ASSERT_EQ(saturatedSecondsFrom(base, std::numeric_limits<Int64>::min()), base);
}

GTEST_TEST(SaturatedSecondsFrom, HandlesBoundaryBasesWithoutOverflow)
{
    // A base at or before the epoch (negative rep) must not overflow the internal max()-base style math.
    const SysTimePoint epoch{};
    const SysTimePoint pre_epoch(std::chrono::microseconds(-1));
    const SysTimePoint sys_min = SysTimePoint::min();

    ASSERT_EQ(saturatedSecondsFrom(epoch, 60), epoch + std::chrono::seconds(60));
    ASSERT_EQ(saturatedSecondsFrom(pre_epoch, 60), pre_epoch + std::chrono::seconds(60));
    ASSERT_EQ(saturatedSecondsFrom(sys_min, 0), sys_min);
    ASSERT_EQ(saturatedSecondsFrom(sys_min, 60), sys_min + std::chrono::seconds(60));
    // A base at the ceiling saturates for any positive count rather than wrapping.
    ASSERT_EQ(saturatedSecondsFrom(sys_max, 1), sys_max);
}

GTEST_TEST(SaturatedSecondsFrom, PreEpochBaseWithLargeCountIsExactOrSaturates)
{
    // Regression: a count past rep_max / ticks_per_second combined with a pre-epoch (negative) base must
    // still be handled exactly. The whole base + count sum is evaluated in a 128-bit intermediate, so the
    // result is the exact instant when representable and time_point::max() only when it truly overflows.
    static constexpr Int64 count_over_threshold = 9223372036855LL; // smallest count whose ticks exceed rep_max

    // base = -1us: the true sum (9223372036854999999us) exceeds the representable max, so it saturates.
    ASSERT_EQ(saturatedSecondsFrom(SysTimePoint(std::chrono::microseconds(-1)), count_over_threshold), sys_max);

    // base = -300000us: the true sum (9223372036854700000us) is still representable, so it is returned exactly.
    ASSERT_EQ(saturatedSecondsFrom(SysTimePoint(std::chrono::microseconds(-300000)), count_over_threshold),
              SysTimePoint(std::chrono::microseconds(Int64(9223372036854700000LL))));
}
