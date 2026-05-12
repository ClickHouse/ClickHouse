#include <gtest/gtest.h>

#include <Common/Stopwatch.h>

#include <limits>


/// Calling these methods with values whose `seconds * 1e9` overflows `UInt64` used to
/// trigger UBSan (`runtime error: ... is outside the range of representable values of
/// type 'unsigned long'`). After the saturating conversion, the methods must accept
/// any finite non-negative `seconds` without UB and must report "threshold not yet
/// reached" (i.e. `compareAndRestart` returns `false`, `compareAndRestartDeferred`
/// returns an empty lock).
TEST(AtomicStopwatch, CompareAndRestartSaturatesExtremeSeconds)
{
    AtomicStopwatch watch;

    EXPECT_FALSE(watch.compareAndRestart(static_cast<double>(std::numeric_limits<UInt64>::max())));
    EXPECT_FALSE(watch.compareAndRestart(static_cast<double>(std::numeric_limits<Int64>::max())));
    EXPECT_FALSE(watch.compareAndRestart(1e30));
}

TEST(AtomicStopwatch, CompareAndRestartDeferredSaturatesExtremeSeconds)
{
    AtomicStopwatch watch;

    EXPECT_FALSE(static_cast<bool>(watch.compareAndRestartDeferred(static_cast<double>(std::numeric_limits<UInt64>::max()))));
    EXPECT_FALSE(static_cast<bool>(watch.compareAndRestartDeferred(static_cast<double>(std::numeric_limits<Int64>::max()))));
    EXPECT_FALSE(static_cast<bool>(watch.compareAndRestartDeferred(1e30)));
}

TEST(AtomicStopwatch, NonPositiveSecondsTriggerImmediately)
{
    AtomicStopwatch watch;

    EXPECT_TRUE(watch.compareAndRestart(0.0));
    EXPECT_TRUE(static_cast<bool>(watch.compareAndRestartDeferred(0.0)));
    EXPECT_TRUE(watch.compareAndRestart(-1.0));
    EXPECT_TRUE(static_cast<bool>(watch.compareAndRestartDeferred(-1.0)));
}
