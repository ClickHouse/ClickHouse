#include <gtest/gtest.h>

#include <IO/ContinuityTracker.h>

using namespace DB;

namespace
{

constexpr size_t MiB(size_t n) { return n << 20; }
constexpr size_t KiB(size_t n) { return n << 10; }

ContinuityTracker::Options testOptions()
{
    ContinuityTracker::Options o;
    o.near_gap = MiB(2);
    o.ewma_alpha = 0.5;
    return o;
}

}

TEST(ContinuityTracker, ColdStartPredictsZero)
{
    ContinuityTracker t(testOptions());
    EXPECT_EQ(t.currentRun(), 0u);
    EXPECT_EQ(t.estimate(), 0u);
    EXPECT_EQ(t.predictedReach(), 0u);
}

TEST(ContinuityTracker, ContiguousServesGrowRun)
{
    ContinuityTracker t(testOptions());
    for (size_t i = 0; i < 8; ++i)
        t.onServe(MiB(i), MiB(1));
    EXPECT_EQ(t.currentRun(), MiB(8));
    /// No far seek yet, so the estimate is still 0 and the prediction is the run.
    EXPECT_EQ(t.estimate(), 0u);
    EXPECT_EQ(t.predictedReach(), MiB(8));
}

TEST(ContinuityTracker, NearGapStaysInRun)
{
    ContinuityTracker t(testOptions());
    t.onServe(0, MiB(4));                  /// frontier at 4 MiB
    t.onServe(MiB(5), MiB(4));             /// 1 MiB forward gap (<= near_gap) -> bridged
    /// Run span = frontier (9 MiB) - start (0), the bridged gap included.
    EXPECT_EQ(t.currentRun(), MiB(9));
}

TEST(ContinuityTracker, FarForwardGapWithoutSeekBreaksRun)
{
    ContinuityTracker t(testOptions());
    t.onServe(0, MiB(4));                  /// frontier 4 MiB
    t.onServe(MiB(10), MiB(1));            /// 6 MiB forward gap (> near_gap), no seek
    /// The discontinuity closed the old run and started a fresh one.
    EXPECT_EQ(t.currentRun(), MiB(1));
    EXPECT_EQ(t.estimate(), MiB(2));       /// EWMA folded the 4 MiB run: 0.5*4 + 0.5*0
}

TEST(ContinuityTracker, NearGapSeekKeepsRun)
{
    ContinuityTracker t(testOptions());
    t.onServe(0, MiB(4));                  /// frontier 4 MiB, run 4 MiB
    t.onSeek(MiB(5));                       /// forward near gap (<= near_gap) -> kept
    EXPECT_EQ(t.currentRun(), MiB(4));     /// run not reset
    t.onServe(MiB(5), MiB(3));             /// continues the run
    EXPECT_EQ(t.currentRun(), MiB(8));
}

TEST(ContinuityTracker, FarSeekResetsRunButCarriesEstimate)
{
    ContinuityTracker t(testOptions());
    t.onServe(0, MiB(16));
    EXPECT_EQ(t.currentRun(), MiB(16));

    t.onSeek(MiB(100));                     /// far seek (> near_gap)
    EXPECT_EQ(t.currentRun(), 0u);          /// run reset
    EXPECT_EQ(t.estimate(), MiB(8));        /// EWMA = 0.5*16 + 0.5*0 = 8 MiB
    /// Right after the far seek, the prediction is the carried estimate alone.
    EXPECT_EQ(t.predictedReach(), MiB(8));
}

TEST(ContinuityTracker, PredictedReachIsMaxOfRunAndEstimate)
{
    ContinuityTracker t(testOptions());
    t.onServe(0, MiB(16));
    t.onSeek(MiB(100));                     /// estimate -> 8 MiB, run -> 0
    EXPECT_EQ(t.estimate(), MiB(8));

    /// A new short run below the estimate: the estimate dominates the prediction.
    t.onServe(MiB(100), MiB(4));
    EXPECT_EQ(t.currentRun(), MiB(4));
    EXPECT_EQ(t.predictedReach(), MiB(8));

    /// Grow it past the estimate: now the run dominates.
    t.onServe(MiB(104), MiB(8));
    EXPECT_EQ(t.currentRun(), MiB(12));
    EXPECT_EQ(t.predictedReach(), MiB(12));
}

TEST(ContinuityTracker, RepeatedRandomSeeksDecayEstimate)
{
    ContinuityTracker t(testOptions());
    t.onServe(0, MiB(16));                  /// seed the estimate at 16 MiB
    for (size_t i = 1; i <= 6; ++i)
    {
        t.onSeek(MiB(100 * i));
        t.onServe(MiB(100 * i), KiB(64));
    }
    /// The EWMA has decayed far below the seed - random access predicts short.
    EXPECT_LT(t.predictedReach(), MiB(1));
}
