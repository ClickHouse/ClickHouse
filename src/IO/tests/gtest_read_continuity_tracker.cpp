#include <IO/ReadContinuityTracker.h>
#include <gtest/gtest.h>

using namespace DB;

namespace
{
    ReadContinuityTracker makeTracker(size_t bridgeable_gap = 100, double alpha = 0.5)
    {
        return ReadContinuityTracker(ReadContinuityTracker::Options{.bridgeable_gap = bridgeable_gap, .ewma_alpha = alpha});
    }
}

TEST(ReadContinuityTracker, ContiguousServesExtendRun)
{
    auto t = makeTracker();
    t.recordReadRange(0, 50);
    EXPECT_EQ(t.currentRun(), 50u);
    t.recordReadRange(50, 50);   /// exactly continues the frontier
    EXPECT_EQ(t.currentRun(), 100u);
    EXPECT_EQ(t.predictedForwardLength(), 100u);
}

TEST(ReadContinuityTracker, SmallGapBridgesRun)
{
    auto t = makeTracker(/*bridgeable_gap=*/100);
    t.recordReadRange(0, 50);
    t.recordReadRange(120, 30);   /// gap 70 <= bridgeable_gap -> bridged; the run spans [0, 150)
    EXPECT_EQ(t.currentRun(), 150u);
}

TEST(ReadContinuityTracker, LargeGapBreaksRunAndFoldsEstimate)
{
    auto t = makeTracker(/*bridgeable_gap=*/100, /*alpha=*/0.5);
    t.recordReadRange(0, 100);
    t.recordReadRange(300, 50);   /// gap 200 > bridgeable_gap -> closes the 100-run, starts a new one
    EXPECT_EQ(t.currentRun(), 50u);
    EXPECT_EQ(t.estimate(), 50u);   /// 0.5*100 + 0.5*0
    EXPECT_EQ(t.predictedForwardLength(), 50u);
}

TEST(ReadContinuityTracker, ForwardNearSeekKeepsRun)
{
    auto t = makeTracker(/*bridgeable_gap=*/100);
    t.recordReadRange(0, 100);
    t.recordSeek(150);   /// forward 50 <= bridgeable_gap -> run kept
    EXPECT_EQ(t.currentRun(), 100u);
    t.recordReadRange(150, 50);   /// continues the kept run
    EXPECT_EQ(t.currentRun(), 200u);
}

TEST(ReadContinuityTracker, FarSeekFoldsButStillPredictsLong)
{
    auto t = makeTracker(/*bridgeable_gap=*/100, /*alpha=*/0.5);
    t.recordReadRange(0, 100);
    t.recordSeek(1000);   /// far -> fold the 100-run into the estimate, restart
    EXPECT_EQ(t.currentRun(), 0u);
    EXPECT_EQ(t.estimate(), 50u);
    EXPECT_EQ(t.predictedForwardLength(), 50u);   /// still long: trusts the previous run
}

TEST(ReadContinuityTracker, BackwardSeekFolds)
{
    auto t = makeTracker(/*bridgeable_gap=*/100, /*alpha=*/0.5);
    t.recordReadRange(100, 100);   /// run [100, 200)
    t.recordSeek(50);   /// backward -> fold, restart at 50
    EXPECT_EQ(t.currentRun(), 0u);
    EXPECT_EQ(t.estimate(), 50u);
}

TEST(ReadContinuityTracker, RepeatedSeeksDecayEstimate)
{
    auto t = makeTracker(/*bridgeable_gap=*/100, /*alpha=*/0.5);
    t.recordReadRange(0, 100);
    t.recordSeek(1000);
    EXPECT_EQ(t.estimate(), 50u);
    t.recordSeek(2000);   /// zero-span close decays the estimate
    EXPECT_EQ(t.estimate(), 25u);
    t.recordSeek(3000);
    EXPECT_EQ(t.estimate(), 12u);   /// 12.5 truncated
}

TEST(ReadContinuityTracker, PredictedForwardLengthIsMaxOfRunAndEstimate)
{
    auto t = makeTracker(/*bridgeable_gap=*/100, /*alpha=*/0.5);
    t.recordReadRange(0, 100);
    t.recordSeek(1000);   /// estimate 50, run reset
    t.recordReadRange(1000, 20);   /// a small new run of 20
    EXPECT_EQ(t.currentRun(), 20u);
    EXPECT_EQ(t.predictedForwardLength(), 50u);   /// max(20, 50)
    t.recordReadRange(1020, 60);   /// run grows to 80
    EXPECT_EQ(t.currentRun(), 80u);
    EXPECT_EQ(t.predictedForwardLength(), 80u);   /// max(80, 50)
}
