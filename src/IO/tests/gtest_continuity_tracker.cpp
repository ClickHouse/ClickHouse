#include <gtest/gtest.h>

#include <IO/ContinuityTracker.h>

#include <limits>

using namespace DB;

namespace
{

constexpr size_t MiB(size_t n) { return n << 20; }
constexpr size_t KiB(size_t n) { return n << 10; }
constexpr size_t NO_CLAMP = std::numeric_limits<size_t>::max();

ContinuityTracker::Options testOptions()
{
    ContinuityTracker::Options o;
    o.near_gap = MiB(2);
    o.trigger = MiB(8);
    o.eof_confidence = MiB(32);
    o.ewma_alpha = 0.5;
    return o;
}

}

TEST(ContinuityTracker, ColdStartSuggestsOneShot)
{
    ContinuityTracker t(testOptions());
    EXPECT_EQ(t.signal(), 0u);
    EXPECT_FALSE(t.suggestedBound(0, NO_CLAMP, NO_CLAMP).has_value());
}

TEST(ContinuityTracker, ContiguousServesExtendRunAndTriggerAtWindow)
{
    ContinuityTracker t(testOptions());

    /// Four contiguous 1 MiB serves -> run = 4 MiB, below the 8 MiB trigger.
    for (size_t i = 0; i < 4; ++i)
        t.onServe(MiB(i), MiB(1));
    EXPECT_EQ(t.currentRun(), MiB(4));
    EXPECT_FALSE(t.suggestedBound(MiB(4), NO_CLAMP, NO_CLAMP).has_value());

    /// Cross the trigger: an 8 MiB run opens a long connection bounded to pos + signal.
    for (size_t i = 4; i < 8; ++i)
        t.onServe(MiB(i), MiB(1));
    EXPECT_EQ(t.currentRun(), MiB(8));
    auto bound = t.suggestedBound(MiB(8), NO_CLAMP, NO_CLAMP);
    ASSERT_TRUE(bound.has_value());
    EXPECT_EQ(*bound, MiB(8) + MiB(8));   /// pos + signal
}

TEST(ContinuityTracker, NearGapStaysInRun)
{
    ContinuityTracker t(testOptions());
    t.onServe(0, MiB(4));                  /// frontier at 4 MiB
    t.onServe(MiB(5), MiB(4));             /// 1 MiB forward gap (<= near_gap) -> bridged
    /// Run span = frontier (9 MiB) - start (0), the bridged gap included.
    EXPECT_EQ(t.currentRun(), MiB(9));
}

TEST(ContinuityTracker, FarSeekResetsRunButCarriesEstimate)
{
    ContinuityTracker t(testOptions());
    t.onServe(0, MiB(16));
    EXPECT_EQ(t.currentRun(), MiB(16));

    t.onSeek(MiB(100));                     /// far seek (> near_gap)
    EXPECT_EQ(t.currentRun(), 0u);          /// run reset
    EXPECT_EQ(t.estimate(), MiB(8));        /// EWMA = 0.5*16 + 0.5*0 = 8 MiB

    /// Right after the far seek the estimate alone still justifies a long
    /// connection - "trust the previous run" - bounded to pos + estimate.
    auto bound = t.suggestedBound(MiB(100), NO_CLAMP, NO_CLAMP);
    ASSERT_TRUE(bound.has_value());
    EXPECT_EQ(*bound, MiB(100) + MiB(8));
}

TEST(ContinuityTracker, RepeatedRandomSeeksDecayToShort)
{
    ContinuityTracker t(testOptions());
    t.onServe(0, MiB(16));                  /// seed the estimate at 16 MiB

    /// Random access: far seeks with tiny reads decay the EWMA below the trigger.
    for (size_t i = 1; i <= 6; ++i)
    {
        t.onSeek(MiB(100 * i));
        t.onServe(MiB(100 * i), KiB(64));
    }
    EXPECT_LT(t.signal(), MiB(8));
    EXPECT_FALSE(t.suggestedBound(MiB(700), NO_CLAMP, NO_CLAMP).has_value());
}

TEST(ContinuityTracker, ConfirmedLongRunOpensToClamp)
{
    ContinuityTracker t(testOptions());
    t.onServe(0, MiB(40));                  /// 40 MiB run >= eof_confidence (32 MiB)

    const size_t object_end = MiB(1024);
    auto bound = t.suggestedBound(MiB(40), object_end, NO_CLAMP);
    ASSERT_TRUE(bound.has_value());
    EXPECT_EQ(*bound, object_end);          /// full-file mode: open to the clamp
}

TEST(ContinuityTracker, BoundClampedToExtent)
{
    ContinuityTracker t(testOptions());
    t.onServe(0, MiB(16));                  /// run 16 MiB (>= trigger, < eof_confidence)

    const size_t extent_end = MiB(16) + MiB(4);   /// only 4 MiB left in the extent
    auto bound = t.suggestedBound(MiB(16), NO_CLAMP, extent_end);
    ASSERT_TRUE(bound.has_value());
    /// pos + signal would be 32 MiB; the extent clamps it to 20 MiB.
    EXPECT_EQ(*bound, extent_end);
}

TEST(ContinuityTracker, TriggerBoundaryIsInclusive)
{
    ContinuityTracker t(testOptions());
    t.onServe(0, MiB(8) - 1);               /// one byte below the trigger
    EXPECT_FALSE(t.suggestedBound(0, NO_CLAMP, NO_CLAMP).has_value());
    t.onServe(MiB(8) - 1, 1);               /// exactly at the trigger
    EXPECT_TRUE(t.suggestedBound(MiB(8), NO_CLAMP, NO_CLAMP).has_value());
}
