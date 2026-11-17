#include <gtest/gtest.h>

#include <stdexcept>

#include <Common/WriteOutcomeTracker.h>

using namespace DB;

TEST(WriteOutcomeTracker, ScopeSetsAndRestoresTheCurrentTracker)
{
    EXPECT_EQ(WriteOutcomeTracker::current(), nullptr);

    auto outer = std::make_shared<WriteOutcomeTracker>();
    auto inner = std::make_shared<WriteOutcomeTracker>();
    {
        WriteOutcomeTracker::Scope outer_scope(outer);
        EXPECT_EQ(WriteOutcomeTracker::current(), outer);
        {
            WriteOutcomeTracker::Scope inner_scope(inner);
            EXPECT_EQ(WriteOutcomeTracker::current(), inner);
            EXPECT_EQ(WriteOutcomeTracker::currentRaw(), inner.get());
        }
        EXPECT_EQ(WriteOutcomeTracker::current(), outer);
    }
    EXPECT_EQ(WriteOutcomeTracker::current(), nullptr);
    EXPECT_EQ(WriteOutcomeTracker::currentRaw(), nullptr);
}

TEST(WriteOutcomeTracker, ExitGuardDecidesAnExitWithoutOutcome)
{
    /// No attempt may have been applied.
    auto refused = std::make_shared<WriteOutcomeTracker>();
    {
        WriteOutcomeTracker::ExitGuard guard(refused);
    }
    EXPECT_EQ(refused->getOutcome(), WriteOutcome::NotApplied);

    /// An attempt may have been applied, and the call ends with an exception.
    auto lost = std::make_shared<WriteOutcomeTracker>();
    try
    {
        WriteOutcomeTracker::ExitGuard guard(lost);
        lost->markMayHaveBeenApplied();
        throw std::runtime_error("The query was cancelled in the test");
    }
    catch (const std::runtime_error &) /// NOLINT(bugprone-empty-catch)
    {
    }
    EXPECT_EQ(lost->getOutcome(), WriteOutcome::Unknown);

    /// An outcome that the call set wins.
    auto proven = std::make_shared<WriteOutcomeTracker>();
    {
        WriteOutcomeTracker::ExitGuard guard(proven);
        proven->markMayHaveBeenApplied();
        guard.set(WriteOutcome::Applied);
    }
    EXPECT_EQ(proven->getOutcome(), WriteOutcome::Applied);
}

TEST(WriteOutcomeTracker, ExitGuardDecidesEachCall)
{
    /// A later call of the same write starts without the outcome of the earlier one, but keeps its attempts.
    auto tracker = std::make_shared<WriteOutcomeTracker>();
    {
        WriteOutcomeTracker::ExitGuard guard(tracker);
        tracker->markMayHaveBeenApplied();
    }
    EXPECT_EQ(tracker->getOutcome(), WriteOutcome::Unknown);
    {
        WriteOutcomeTracker::ExitGuard guard(tracker);
        EXPECT_FALSE(tracker->getOutcome().has_value());
    }
    EXPECT_EQ(tracker->getOutcome(), WriteOutcome::Unknown);
}

TEST(WriteOutcomeTracker, ExitGuardWithoutTrackerDoesNothing)
{
    WriteOutcomeTracker::ExitGuard guard(nullptr);
    guard.set(WriteOutcome::Applied);
}
