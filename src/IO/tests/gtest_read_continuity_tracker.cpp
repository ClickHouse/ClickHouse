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
    t.recordReadRange(50, 50);   /// exactly continues the frontier -> checkpoint
    EXPECT_EQ(t.currentRun(), 100u);
    /// The exact continuation checkpointed the run: estimate = 0.5 * 100 = 50;
    /// frontier(100) + max(0.5 * run(100) + 0.5 * est(50), est(50)) = 175.
    EXPECT_EQ(t.estimate(), 50u);
    EXPECT_EQ(t.predictedEnd(), 175u);
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
    /// frontier(350) + max(0.5 * run(50), estimate(50)) = 400.
    EXPECT_EQ(t.predictedEnd(), 400u);
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
    /// Anchored at the seek target: 1000 + max(0, 50) = 1050 - still predicts a
    /// historical-length run from the NEW position, nothing from the old one.
    EXPECT_EQ(t.predictedEnd(), 1050u);
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

TEST(ReadContinuityTracker, PredictedEndIsRunAnchored)
{
    auto t = makeTracker(/*bridgeable_gap=*/100, /*alpha=*/0.5);
    t.recordReadRange(0, 100);
    t.recordSeek(1000);   /// estimate 50, run reset
    t.recordReadRange(1000, 20);   /// a small new run of 20
    EXPECT_EQ(t.currentRun(), 20u);
    /// frontier(1020) + max(0.5*20 + 0.5*50 = 35, 50) = 1070: the estimate dominates early.
    EXPECT_EQ(t.predictedEnd(), 1070u);
    t.recordReadRange(1020, 60);   /// exact continuation -> checkpoint: est = 0.5*80 + 0.5*50 = 65
    EXPECT_EQ(t.currentRun(), 80u);
    /// frontier(1080) + max(0.5*80 + 0.5*65 = 72, 65) = 1152: the blend overtakes the
    /// estimate smoothly; growth stays run-proportional, never caller-re-anchored.
    EXPECT_EQ(t.predictedEnd(), 1152u);
}

TEST(ReadContinuityTracker, PredictedEndIsZeroBeforeFirstServe)
{
    auto t = makeTracker();
    EXPECT_EQ(t.predictedEnd(), 0u);
}

TEST(ReadContinuityTracker, UnbrokenScanWarmsTheEstimate)
{
    /// Trackers start empty on every reader; without continuation checkpoints an
    /// unbroken scan would never raise the estimate (the run never closes). Each
    /// exact continuation folds the grown run in, so confidence rises with the scan.
    auto t = makeTracker(/*bridgeable_gap=*/100, /*alpha=*/0.5);
    t.recordReadRange(0, 100);       /// starts the run: no evidence yet
    EXPECT_EQ(t.estimate(), 0u);
    t.recordReadRange(100, 100);     /// checkpoint: est = 0.5 * 200 = 100
    EXPECT_EQ(t.estimate(), 100u);
    t.recordReadRange(200, 100);     /// checkpoint: est = 0.5 * 300 + 0.5 * 100 = 200
    EXPECT_EQ(t.estimate(), 200u);
    EXPECT_EQ(t.predictedEnd(), 300u + 250u);   /// frontier + max(0.5 * 300 + 0.5 * 200, 200)

    /// A gapless seek is the same positive signal.
    auto s = makeTracker(/*bridgeable_gap=*/100, /*alpha=*/0.5);
    s.recordReadRange(0, 100);
    s.recordSeek(100);               /// gapless: checkpoint, run kept
    EXPECT_EQ(s.estimate(), 50u);
    EXPECT_EQ(s.currentRun(), 100u);

    /// The first read AFTER A JUMP lands at the frontier but confirms nothing.
    auto j = makeTracker(/*bridgeable_gap=*/100, /*alpha=*/0.5);
    j.recordReadRange(0, 100);
    j.recordSeek(1000);              /// far: fold(est=50), restart
    j.recordReadRange(1000, 20);     /// empty-run continuation: NO checkpoint
    EXPECT_EQ(j.estimate(), 50u);
}

TEST(ReadContinuityTracker, RangesFromThePastAreSkipped)
{
    /// Overlapping feeds re-declare spans an earlier feed already covered (the plan
    /// window slides over the same region); the covered part is a re-declaration,
    /// not a pattern signal - it must neither fold the run nor extend it. Only the
    /// tail past the frontier feeds, and it counts as an exact continuation.
    auto t = makeTracker(/*bridgeable_gap=*/100, /*alpha=*/0.5);
    t.recordReadRange(0, 100);
    t.recordReadRange(0, 100);       /// fully covered -> no-op
    EXPECT_EQ(t.currentRun(), 100u);
    EXPECT_EQ(t.estimate(), 0u);
    t.recordReadRange(50, 100);      /// overlaps the frontier: feeds only [100, 150)
    EXPECT_EQ(t.currentRun(), 150u);
    EXPECT_EQ(t.estimate(), 75u);    /// the clipped tail continued the run -> checkpoint: 0.5 * 150
    t.recordReadRange(500, 50);      /// a far-forward range still breaks the run
    EXPECT_EQ(t.currentRun(), 50u);
    EXPECT_EQ(t.estimate(), 112u);   /// close folded the 150-run: 0.5 * 150 + 0.5 * 75
}

