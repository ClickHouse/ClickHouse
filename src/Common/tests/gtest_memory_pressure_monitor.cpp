#include <Common/MemoryPressureMonitor.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

#include <atomic>
#include <optional>
#include <thread>
#include <vector>

using namespace DB;

namespace
{

/// `PressureCooldown` works in milliseconds, so the test clock does too.
constexpr uint64_t SECOND_MS = 1000;
/// Threshold generations. Reusing one across calls means the thresholds did not change, so the
/// reload bypass never triggers; switching to another one stands for a reload that changed it.
constexpr uint16_t GENERATION_A = 7;
constexpr uint16_t GENERATION_B = 8;

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

    /// Zero is out of range: an `elevated` of 0 classifies every scope as `Elevated`, including a scope
    /// with no hard limit, whose pressure is 0.
    EXPECT_THROW(validateMemoryPressureThresholds(0, 0, 0), DB::Exception);
    EXPECT_THROW(validateMemoryPressureThresholds(0, 90, 95), DB::Exception);
    EXPECT_THROW(validateMemoryPressureThresholds(75, 0, 95), DB::Exception);

    /// Valid edges accepted.
    EXPECT_NO_THROW(validateMemoryPressureThresholds(1, 1, 1));
    EXPECT_NO_THROW(validateMemoryPressureThresholds(100, 100, 100));
    EXPECT_NO_THROW(validateMemoryPressureThresholds(75, 90, 95));   // strictly increasing
    EXPECT_NO_THROW(validateMemoryPressureThresholds(75, 75, 90));   // equality allowed
}

TEST(MemoryPressureMonitor, ThresholdsRoundTrip)
{
    ScopedThresholds guard(50, 70, 90);   /// saves the prior thresholds, restores on scope exit

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

/// End to end: a reload relaxes an in-flight monitor at once. Raise the thresholds above the current
/// pressure with no time advance; only the threshold change (not a step-down) can lower the level here.
TEST(MemoryPressureMonitor, ThresholdReloadDropsCooldownImmediately)
{
    ScopedThresholds guard(75, 90, 95);

    MemoryPressureMonitor root;
    MemoryTracker tracker(nullptr, VariableContext::Process, false);
    tracker.setHardLimit(1000);
    MemoryPressureMonitor scoped(tracker, root);

    tracker.adjustWithUntrackedMemory(920);   /// 0.92 -> High under 75 / 90 / 95
    EXPECT_EQ(scoped.currentLevel(), MemoryPressureLevel::High);

    setMemoryPressureThresholds(95, 96, 97);   /// new thresholds, so the level above is stale
    EXPECT_EQ(scoped.currentLevel(), MemoryPressureLevel::Normal);

    tracker.adjustWithUntrackedMemory(-920);
}

/// The reload bypass keys on the thresholds themselves, not on the act of reloading. A `SYSTEM RELOAD CONFIG`
/// that leaves them alone must keep the sticky level: a server under real pressure would
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

    setMemoryPressureThresholds(75, 90, 95);  /// same thresholds: a reload that changed nothing
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

/// `EscalatesThroughParentChain` wires the monitors by hand, so it proves the escalation rule but not
/// that anything wires them that way in production. This goes through the real nested-`ThreadGroup`
/// constructor and reads the level back through `CurrentThread::getMemoryPressureMonitor`, the accessor
/// the executor uses. Dropping the `setParent` call in `ThreadGroup`'s constructor fails this test.
TEST(MemoryPressureMonitor, NestedThreadGroupInheritsParentPressure)
{
    ScopedThresholds guard(75, 90, 95);

    /// A ThreadStatus must exist before a group can be attached; the debug build already has one.
    std::optional<ThreadStatus> thread_status_holder;
    if (!current_thread)
        thread_status_holder.emplace();

    ThreadGroupPtr query_group = ThreadGroup::createForQuery(getContext().context);
    query_group->memory_tracker.setHardLimit(1000);
    query_group->memory_tracker.adjustWithUntrackedMemory(920);   /// 0.92 -> High for the outer query

    /// `createForExplainAnalyze` is the public factory over the nested `ThreadGroup(parent)`
    /// constructor. The nested group's own tracker has no limit, so on its own it is `Normal`; only
    /// the parent link can lift it.
    ThreadGroupPtr nested_group = ThreadGroup::createForExplainAnalyze(query_group);
    {
        ThreadGroupSwitcher switcher(nested_group, ThreadName::UNKNOWN, /*allow_existing_group=*/true);
        EXPECT_EQ(CurrentThread::getMemoryPressureMonitor().currentLevel(), MemoryPressureLevel::High);
    }

    query_group->memory_tracker.adjustWithUntrackedMemory(-920);
}

TEST(MemoryPressureMonitor, CooldownAppliesToClassifiedLevels)
{
    PressureCooldown c(/*cooldown_ms_=*/10 * SECOND_MS);

    EXPECT_EQ(c.apply(MemoryPressureLevel::High, SECOND_MS, GENERATION_A), MemoryPressureLevel::High);            /// snap up
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 2 * SECOND_MS, GENERATION_A), MemoryPressureLevel::High);      /// sticky
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 12 * SECOND_MS, GENERATION_A), MemoryPressureLevel::Elevated); /// one step per cooldown
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 13 * SECOND_MS, GENERATION_A), MemoryPressureLevel::Elevated); /// next step not due yet
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 23 * SECOND_MS, GENERATION_A), MemoryPressureLevel::Normal);
}

TEST(MemoryPressureMonitor, CooldownReSpikeRefreshesTheClock)
{
    PressureCooldown c(/*cooldown_ms_=*/10 * SECOND_MS);

    EXPECT_EQ(c.apply(MemoryPressureLevel::Critical, SECOND_MS, GENERATION_A), MemoryPressureLevel::Critical);
    /// A re-spike at the same level refreshes the timestamp: the step-down needs sustained calm.
    EXPECT_EQ(c.apply(MemoryPressureLevel::Critical, 9 * SECOND_MS, GENERATION_A), MemoryPressureLevel::Critical);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 12 * SECOND_MS, GENERATION_A), MemoryPressureLevel::Critical);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 20 * SECOND_MS, GENERATION_A), MemoryPressureLevel::High);
}

TEST(MemoryPressureMonitor, CooldownReloadBypassesStickiness)
{
    PressureCooldown c(/*cooldown_ms_=*/10 * SECOND_MS);

    EXPECT_EQ(c.apply(MemoryPressureLevel::High, SECOND_MS, GENERATION_A), MemoryPressureLevel::High);        /// snap up at 1s
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 2 * SECOND_MS, GENERATION_A), MemoryPressureLevel::High);  /// sticky, no reload
    /// A reload that changed the thresholds drops the sticky level at once, with no cooldown wait.
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 3 * SECOND_MS, GENERATION_B), MemoryPressureLevel::Normal);
    /// ...and only once: the new thresholds are now the ones the level was classified against.
    EXPECT_EQ(c.apply(MemoryPressureLevel::High, 4 * SECOND_MS, GENERATION_B), MemoryPressureLevel::High);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 5 * SECOND_MS, GENERATION_B), MemoryPressureLevel::High);
}

TEST(MemoryPressureMonitor, CooldownsAreIndependent)
{
    /// Two queries = two scoped cooldowns: one query's spike leaves the other's level untouched.
    PressureCooldown query_a(PressureCooldown::SCOPE_COOLDOWN_MS);
    PressureCooldown query_b(PressureCooldown::SCOPE_COOLDOWN_MS);

    EXPECT_EQ(query_a.apply(MemoryPressureLevel::Critical, SECOND_MS, GENERATION_A), MemoryPressureLevel::Critical);
    EXPECT_EQ(query_b.apply(MemoryPressureLevel::Normal, SECOND_MS, GENERATION_A), MemoryPressureLevel::Normal);
    EXPECT_EQ(query_a.apply(MemoryPressureLevel::Normal, 2 * SECOND_MS, GENERATION_A), MemoryPressureLevel::Critical);
    EXPECT_EQ(query_b.apply(MemoryPressureLevel::Normal, 2 * SECOND_MS, GENERATION_A), MemoryPressureLevel::Normal);
}

/// The cooldown is compared in whole milliseconds, with `>=`: one millisecond short is not due.
TEST(MemoryPressureMonitor, CooldownBoundaryIsExact)
{
    constexpr uint64_t cooldown_ms = 10 * SECOND_MS;
    PressureCooldown c(cooldown_ms);

    EXPECT_EQ(c.apply(MemoryPressureLevel::High, SECOND_MS, GENERATION_A), MemoryPressureLevel::High);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, SECOND_MS + cooldown_ms - 1, GENERATION_A), MemoryPressureLevel::High);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, SECOND_MS + cooldown_ms, GENERATION_A), MemoryPressureLevel::Elevated);
}

/// A `Normal` sample over a held `Normal` writes nothing at all - not the timestamp, not the generation.
/// This proves those writes were dead rather than merely unnoticed: after a long `Normal` run, and a
/// threshold change inside it, a spike must snap up on the very next sample, and the cooldown that follows
/// must be measured from that sample. A timestamp left behind at 0 would step the level down at once.
TEST(MemoryPressureMonitor, NormalSamplesKeepNoState)
{
    constexpr uint64_t cooldown_ms = 10 * SECOND_MS;
    PressureCooldown c(cooldown_ms);

    for (uint64_t t = 0; t < 100; ++t)
        EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, t * SECOND_MS, GENERATION_A), MemoryPressureLevel::Normal);
    /// A reload while nothing is held.
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, 100 * SECOND_MS, GENERATION_B), MemoryPressureLevel::Normal);

    const uint64_t spike = 101 * SECOND_MS;
    EXPECT_EQ(c.apply(MemoryPressureLevel::High, spike, GENERATION_B), MemoryPressureLevel::High);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, spike + cooldown_ms - 1, GENERATION_B), MemoryPressureLevel::High);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, spike + cooldown_ms, GENERATION_B), MemoryPressureLevel::Elevated);
}

/// The state is one atomic word updated by `compare_exchange`, so a lost race must not step the level
/// down twice for one cooldown: the winner already moved the timestamp, so the loser recomputes against
/// it and finds nothing due. Hammer one instance from many threads at the same instant, one cooldown
/// after the level was set, and require exactly one step.
TEST(MemoryPressureMonitor, ConcurrentStepDownHappensOnce)
{
    constexpr uint64_t cooldown_ms = 10 * SECOND_MS;
    constexpr size_t thread_count = 16;
    constexpr size_t rounds = 2000;

    PressureCooldown c(cooldown_ms);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Critical, SECOND_MS, GENERATION_A), MemoryPressureLevel::Critical);

    const uint64_t due = SECOND_MS + cooldown_ms;
    std::atomic<bool> start{false};
    std::atomic<size_t> below_high{0};

    std::vector<std::thread> threads;
    threads.reserve(thread_count);
    for (size_t i = 0; i < thread_count; ++i)
        threads.emplace_back([&]
        {
            while (!start.load(std::memory_order_relaxed))
                std::this_thread::yield();
            for (size_t round = 0; round < rounds; ++round)
                if (c.apply(MemoryPressureLevel::Normal, due, GENERATION_A) < MemoryPressureLevel::High)
                    below_high.fetch_add(1, std::memory_order_relaxed);
        });
    start.store(true, std::memory_order_relaxed);
    for (auto & thread : threads)
        thread.join();

    /// No caller ever saw the level below `High`, and it is still `High` afterwards. A double step would
    /// have reported `Elevated`.
    EXPECT_EQ(below_high.load(), 0u);
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, due, GENERATION_A), MemoryPressureLevel::High);
}

/// The production overload reads the clock itself; every case above injects time instead.
TEST(MemoryPressureMonitor, ApplyReadsTheClockItself)
{
    PressureCooldown c(PressureCooldown::SCOPE_COOLDOWN_MS);

    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, GENERATION_A), MemoryPressureLevel::Normal);
    EXPECT_EQ(c.apply(MemoryPressureLevel::High, GENERATION_A), MemoryPressureLevel::High);
    /// Sticky: the 10 s cooldown cannot have elapsed inside this test.
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, GENERATION_A), MemoryPressureLevel::High);

    c.reset();
    EXPECT_EQ(c.apply(MemoryPressureLevel::Normal, GENERATION_A), MemoryPressureLevel::Normal);
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
