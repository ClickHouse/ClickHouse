#include <Common/MemoryPressureMonitor.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

constexpr uint64_t SECOND = 1'000'000'000ULL;
/// Ladder generations. Reusing one generation across calls means the ladder did not change, so the
/// reload bypass never triggers; switching to another one stands for a reload that changed it.
constexpr uint32_t LADDER_A = 7;
constexpr uint32_t LADDER_B = 8;

/// Save and restore the shared thresholds so a test that changes them does not leak into others.
struct ScopedThresholds
{
    MemoryPressureThresholds prior;
    ScopedThresholds(UInt64 e, UInt64 h, UInt64 c) : prior(getMemoryPressureThresholds()) { setMemoryPressureThresholds(e, h, c); }
    ~ScopedThresholds() { setMemoryPressureThresholds(prior.elevated_pct, prior.high_pct, prior.critical_pct); }
};

}

TEST(MemoryPressureMonitor, ValidateRejectsInvalidThresholds)
{
    /// Out-of-range (any single value > 100) throws.
    EXPECT_THROW(validateMemoryPressureThresholds(101, 90, 95), DB::Exception);
    EXPECT_THROW(validateMemoryPressureThresholds(75, 101, 95), DB::Exception);
    EXPECT_THROW(validateMemoryPressureThresholds(75, 90, 101), DB::Exception);
    EXPECT_THROW(validateMemoryPressureThresholds(300, 90, 95), DB::Exception);

    /// Non-monotonic (elevated > high etc.) throws.
    EXPECT_THROW(validateMemoryPressureThresholds(90, 75, 95), DB::Exception);
    EXPECT_THROW(validateMemoryPressureThresholds(75, 95, 90), DB::Exception);

    /// Valid edges accepted.
    EXPECT_NO_THROW(validateMemoryPressureThresholds(0, 0, 0));
    EXPECT_NO_THROW(validateMemoryPressureThresholds(100, 100, 100));
    EXPECT_NO_THROW(validateMemoryPressureThresholds(75, 90, 95));   // strictly increasing
    EXPECT_NO_THROW(validateMemoryPressureThresholds(75, 75, 90));   // equality allowed
}

TEST(MemoryPressureMonitor, ThresholdsRoundTrip)
{
    ScopedThresholds guard(50, 70, 90);   /// saves the prior ladder, restores on scope exit

    const auto got = getMemoryPressureThresholds();
    EXPECT_EQ(got.elevated_pct, 50u);
    EXPECT_EQ(got.high_pct, 70u);
    EXPECT_EQ(got.critical_pct, 90u);

    EXPECT_THROW(setMemoryPressureThresholds(90, 75, 95), DB::Exception);
}

/// The thresholds are shared and read live: `classifyMemoryPressure` (and thus every monitor) reflects
/// a change at once. This is what lets a reload update the long-lived per-user monitor.
TEST(MemoryPressureMonitor, ClassifyReflectsSharedThresholdsLive)
{
    ScopedThresholds guard(50, 70, 90);

    EXPECT_EQ(classifyMemoryPressure(0.40), MemoryPressureLevel::Normal);
    EXPECT_EQ(classifyMemoryPressure(0.55), MemoryPressureLevel::Elevated);
    EXPECT_EQ(classifyMemoryPressure(0.75), MemoryPressureLevel::High);
    EXPECT_EQ(classifyMemoryPressure(0.95), MemoryPressureLevel::Critical);

    setMemoryPressureThresholds(80, 90, 95);
    EXPECT_EQ(classifyMemoryPressure(0.55), MemoryPressureLevel::Normal);   /// live change
}

/// End to end: a reload relaxes an in-flight monitor at once. Raise the ladder above the current
/// pressure with no time advance; only the ladder change (not a step-down) can lower the level here.
TEST(MemoryPressureMonitor, ThresholdReloadDropsCooldownImmediately)
{
    ScopedThresholds guard(75, 90, 95);

    MemoryPressureMonitor root;
    MemoryTracker tracker(nullptr, VariableContext::Process, false);
    tracker.setHardLimit(1000);
    MemoryPressureMonitor scoped(tracker, root);

    tracker.adjustWithUntrackedMemory(920);   /// 0.92 -> High under 75 / 90 / 95
    EXPECT_EQ(scoped.currentLevel(), MemoryPressureLevel::High);

    setMemoryPressureThresholds(95, 96, 97);   /// a new ladder, so the level above is stale
    EXPECT_EQ(scoped.currentLevel(), MemoryPressureLevel::Normal);

    tracker.adjustWithUntrackedMemory(-920);
}

/// The reload bypass keys on the ladder itself, not on the act of reloading. A `SYSTEM RELOAD CONFIG`
/// that leaves the ladder alone must keep the sticky level: a server under real pressure would
/// otherwise have its pressure state cleared by an unrelated config edit.
TEST(MemoryPressureMonitor, UnchangedReloadKeepsTheStickyLevel)
{
    ScopedThresholds guard(75, 90, 95);

    MemoryPressureMonitor root;
    MemoryTracker tracker(nullptr, VariableContext::Process, false);
    tracker.setHardLimit(1000);
    MemoryPressureMonitor scoped(tracker, root);

    tracker.adjustWithUntrackedMemory(920);   /// 0.92 -> High under 75 / 90 / 95
    EXPECT_EQ(scoped.currentLevel(), MemoryPressureLevel::High);

    tracker.adjustWithUntrackedMemory(-920);  /// pressure gone, but the level is sticky
    EXPECT_EQ(scoped.currentLevel(), MemoryPressureLevel::High);

    setMemoryPressureThresholds(75, 90, 95);  /// same ladder: a reload that changed nothing
    EXPECT_EQ(scoped.currentLevel(), MemoryPressureLevel::High);
}

/// A scoped monitor classifies its tracker's pressure against its own thresholds. Snap-up is
/// immediate, so a rising pressure gives the classified level on the first sample (no clock needed).
/// The parent (a fresh global monitor over the untracked server total) contributes `Normal` here.
TEST(MemoryPressureMonitor, ScopedMonitorClassifiesLocalPressure)
{
    MemoryPressureMonitor parent;   /// default 75 / 90 / 95
    MemoryTracker tracker(nullptr, VariableContext::Process, false);
    tracker.setHardLimit(1000);
    MemoryPressureMonitor scoped(tracker, parent);

    tracker.adjustWithUntrackedMemory(500);   /// 0.50
    EXPECT_EQ(scoped.currentLevel(), MemoryPressureLevel::Normal);

    tracker.adjustWithUntrackedMemory(300);   /// 0.80
    EXPECT_EQ(scoped.currentLevel(), MemoryPressureLevel::Elevated);

    tracker.adjustWithUntrackedMemory(120);   /// 0.92
    EXPECT_EQ(scoped.currentLevel(), MemoryPressureLevel::High);

    tracker.adjustWithUntrackedMemory(70);    /// 0.99
    EXPECT_EQ(scoped.currentLevel(), MemoryPressureLevel::Critical);

    tracker.adjustWithUntrackedMemory(-1090);
}

/// A monitor never reads below any level above it. Build the production chain (global <- user <-
/// query); a spike on the user tracker lifts the query monitor even though the query tracker is calm.
TEST(MemoryPressureMonitor, EscalatesThroughParentChain)
{
    MemoryPressureMonitor global;   /// watches the untracked server total → Normal in the test

    MemoryTracker user_tracker(nullptr, VariableContext::User, false);
    user_tracker.setHardLimit(1000);
    MemoryPressureMonitor user_monitor(user_tracker, global);

    MemoryTracker query_tracker(&user_tracker, VariableContext::Process, false);
    query_tracker.setHardLimit(1000);
    MemoryPressureMonitor query_monitor(query_tracker, user_monitor);

    /// User at 0.92 (High), query itself calm → the query monitor still reports High.
    user_tracker.adjustWithUntrackedMemory(920);
    EXPECT_EQ(query_monitor.currentLevel(), MemoryPressureLevel::High);
    user_tracker.adjustWithUntrackedMemory(-920);
}

TEST(MemoryPressureMonitor, CooldownAppliesToClassifiedLevels)
{
    PressureCooldown c(/*cooldown_ns_=*/10 * SECOND);

    EXPECT_EQ(c.apply(MemoryPressureLevel::High, SECOND, LADDER_A), MemoryPressureLevel::High);            /// snap up
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 2 * SECOND, LADDER_A), MemoryPressureLevel::High);      /// sticky
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 12 * SECOND, LADDER_A), MemoryPressureLevel::Elevated); /// one step per cooldown
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 13 * SECOND, LADDER_A), MemoryPressureLevel::Elevated); /// next step not due yet
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 23 * SECOND, LADDER_A), MemoryPressureLevel::Normal);
}

TEST(MemoryPressureMonitor, CooldownReSpikeRefreshesTheClock)
{
    PressureCooldown c(/*cooldown_ns_=*/10 * SECOND);

    EXPECT_EQ(c.apply(MemoryPressureLevel::Critical, SECOND, LADDER_A), MemoryPressureLevel::Critical);
    /// A re-spike at the same level refreshes the timestamp: the step-down needs sustained calm.
    EXPECT_EQ(c.apply(MemoryPressureLevel::Critical, 9 * SECOND, LADDER_A), MemoryPressureLevel::Critical);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 12 * SECOND, LADDER_A), MemoryPressureLevel::Critical);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 20 * SECOND, LADDER_A), MemoryPressureLevel::High);
}

TEST(MemoryPressureMonitor, CooldownReloadBypassesStickiness)
{
    PressureCooldown c(/*cooldown_ns_=*/10 * SECOND);

    EXPECT_EQ(c.apply(MemoryPressureLevel::High, SECOND, LADDER_A), MemoryPressureLevel::High);        /// snap up at 1s
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 2 * SECOND, LADDER_A), MemoryPressureLevel::High);  /// sticky, no reload
    /// A reload that changed the ladder drops the sticky level at once, with no cooldown wait.
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 3 * SECOND, LADDER_B), MemoryPressureLevel::Normal);
    /// ...and only once: the new ladder is now the one the level was classified against.
    EXPECT_EQ(c.apply(MemoryPressureLevel::High, 4 * SECOND, LADDER_B), MemoryPressureLevel::High);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 5 * SECOND, LADDER_B), MemoryPressureLevel::High);
}

TEST(MemoryPressureMonitor, CooldownsAreIndependent)
{
    /// Two queries = two scoped cooldowns: one query's spike leaves the other's level untouched.
    PressureCooldown query_a(PressureCooldown::QUERY_COOLDOWN_NS);
    PressureCooldown query_b(PressureCooldown::QUERY_COOLDOWN_NS);

    EXPECT_EQ(query_a.apply(MemoryPressureLevel::Critical, SECOND, LADDER_A), MemoryPressureLevel::Critical);
    EXPECT_EQ(query_b.apply(MemoryPressureLevel::Normal, SECOND, LADDER_A), MemoryPressureLevel::Normal);
    EXPECT_EQ(query_a.apply(MemoryPressureLevel::Normal, 2 * SECOND, LADDER_A), MemoryPressureLevel::Critical);
    EXPECT_EQ(query_b.apply(MemoryPressureLevel::Normal, 2 * SECOND, LADDER_A), MemoryPressureLevel::Normal);
}

/// `MemoryTracker::getPressure` is `amount / hard_limit`, lock-free, and 0 when there is no limit or usage.
TEST(MemoryPressureMonitor, MemoryTrackerGetPressure)
{
    MemoryTracker t(nullptr, VariableContext::Process, false);
    EXPECT_DOUBLE_EQ(t.getPressure(), 0.0);   /// no limit

    t.setHardLimit(1000);
    EXPECT_DOUBLE_EQ(t.getPressure(), 0.0);   /// limit but no usage

    t.adjustWithUntrackedMemory(960);
    EXPECT_NEAR(t.getPressure(), 0.96, 1e-9);
    t.adjustWithUntrackedMemory(-960);
}
