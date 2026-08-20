#include <Common/MemoryPressureMonitor.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

constexpr uint64_t SECOND = 1'000'000'000ULL;

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

    EXPECT_EQ(c.apply(MemoryPressureLevel::High, SECOND), MemoryPressureLevel::High);            /// snap up
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 2 * SECOND), MemoryPressureLevel::High);      /// sticky
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 12 * SECOND), MemoryPressureLevel::Elevated); /// one step per cooldown
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 13 * SECOND), MemoryPressureLevel::Elevated); /// next step not due yet
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 23 * SECOND), MemoryPressureLevel::Normal);
}

TEST(MemoryPressureMonitor, CooldownReSpikeRefreshesTheClock)
{
    PressureCooldown c(/*cooldown_ns_=*/10 * SECOND);

    EXPECT_EQ(c.apply(MemoryPressureLevel::Critical, SECOND), MemoryPressureLevel::Critical);
    /// A re-spike at the same level refreshes the timestamp: the step-down needs sustained calm.
    EXPECT_EQ(c.apply(MemoryPressureLevel::Critical, 9 * SECOND), MemoryPressureLevel::Critical);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 12 * SECOND), MemoryPressureLevel::Critical);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 20 * SECOND), MemoryPressureLevel::High);
}

TEST(MemoryPressureMonitor, CooldownsAreIndependent)
{
    /// Two queries = two scoped cooldowns: one query's spike leaves the other's level untouched.
    PressureCooldown query_a(PressureCooldown::QUERY_COOLDOWN_NS);
    PressureCooldown query_b(PressureCooldown::QUERY_COOLDOWN_NS);

    EXPECT_EQ(query_a.apply(MemoryPressureLevel::Critical, SECOND), MemoryPressureLevel::Critical);
    EXPECT_EQ(query_b.apply(MemoryPressureLevel::Normal, SECOND), MemoryPressureLevel::Normal);
    EXPECT_EQ(query_a.apply(MemoryPressureLevel::Normal, 2 * SECOND), MemoryPressureLevel::Critical);
    EXPECT_EQ(query_b.apply(MemoryPressureLevel::Normal, 2 * SECOND), MemoryPressureLevel::Normal);
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
