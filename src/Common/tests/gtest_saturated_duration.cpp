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
