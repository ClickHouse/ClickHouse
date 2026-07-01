#include <IO/ContinuityTracker.h>
#include <gtest/gtest.h>

using namespace DB;

namespace
{
    ContinuityTracker makeTracker(size_t near_gap = 100, double alpha = 0.5)
    {
        return ContinuityTracker(ContinuityTracker::Options{.near_gap = near_gap, .ewma_alpha = alpha});
    }
}

TEST(ContinuityTracker, ContiguousServesExtendRun)
{
    auto t = makeTracker();
    t.onServe(0, 50);
    EXPECT_EQ(t.currentRun(), 50u);
    t.onServe(50, 50);   /// exactly continues the frontier
    EXPECT_EQ(t.currentRun(), 100u);
    EXPECT_EQ(t.predictedReach(), 100u);
}

TEST(ContinuityTracker, SmallGapBridgesRun)
{
    auto t = makeTracker(/*near_gap=*/100);
    t.onServe(0, 50);
    t.onServe(120, 30);   /// gap 70 <= near_gap -> bridged; the run spans [0, 150)
    EXPECT_EQ(t.currentRun(), 150u);
}

TEST(ContinuityTracker, LargeGapBreaksRunAndFoldsEstimate)
{
    auto t = makeTracker(/*near_gap=*/100, /*alpha=*/0.5);
    t.onServe(0, 100);
    t.onServe(300, 50);   /// gap 200 > near_gap -> closes the 100-run, starts a new one
    EXPECT_EQ(t.currentRun(), 50u);
    EXPECT_EQ(t.estimate(), 50u);   /// 0.5*100 + 0.5*0
    EXPECT_EQ(t.predictedReach(), 50u);
}

TEST(ContinuityTracker, ForwardNearSeekKeepsRun)
{
    auto t = makeTracker(/*near_gap=*/100);
    t.onServe(0, 100);
    t.onSeek(150);   /// forward 50 <= near_gap -> run kept
    EXPECT_EQ(t.currentRun(), 100u);
    t.onServe(150, 50);   /// continues the kept run
    EXPECT_EQ(t.currentRun(), 200u);
}

TEST(ContinuityTracker, FarSeekFoldsButStillPredictsLong)
{
    auto t = makeTracker(/*near_gap=*/100, /*alpha=*/0.5);
    t.onServe(0, 100);
    t.onSeek(1000);   /// far -> fold the 100-run into the estimate, restart
    EXPECT_EQ(t.currentRun(), 0u);
    EXPECT_EQ(t.estimate(), 50u);
    EXPECT_EQ(t.predictedReach(), 50u);   /// still long: trusts the previous run
}

TEST(ContinuityTracker, BackwardSeekFolds)
{
    auto t = makeTracker(/*near_gap=*/100, /*alpha=*/0.5);
    t.onServe(100, 100);   /// run [100, 200)
    t.onSeek(50);   /// backward -> fold, restart at 50
    EXPECT_EQ(t.currentRun(), 0u);
    EXPECT_EQ(t.estimate(), 50u);
}

TEST(ContinuityTracker, RepeatedSeeksDecayEstimate)
{
    auto t = makeTracker(/*near_gap=*/100, /*alpha=*/0.5);
    t.onServe(0, 100);
    t.onSeek(1000);
    EXPECT_EQ(t.estimate(), 50u);
    t.onSeek(2000);   /// zero-span close decays the estimate
    EXPECT_EQ(t.estimate(), 25u);
    t.onSeek(3000);
    EXPECT_EQ(t.estimate(), 12u);   /// 12.5 truncated
}

TEST(ContinuityTracker, PredictedReachIsMaxOfRunAndEstimate)
{
    auto t = makeTracker(/*near_gap=*/100, /*alpha=*/0.5);
    t.onServe(0, 100);
    t.onSeek(1000);   /// estimate 50, run reset
    t.onServe(1000, 20);   /// a small new run of 20
    EXPECT_EQ(t.currentRun(), 20u);
    EXPECT_EQ(t.predictedReach(), 50u);   /// max(20, 50)
    t.onServe(1020, 60);   /// run grows to 80
    EXPECT_EQ(t.currentRun(), 80u);
    EXPECT_EQ(t.predictedReach(), 80u);   /// max(80, 50)
}
