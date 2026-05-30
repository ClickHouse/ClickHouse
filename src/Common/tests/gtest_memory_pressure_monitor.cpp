#include <Common/MemoryPressureMonitor.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

constexpr uint64_t SECOND = 1'000'000'000ULL;
constexpr uint64_t COOLDOWN = 60 * SECOND;

}

TEST(MemoryPressureMonitor, NoPressureStaysAtNormal)
{
    FakeMemoryPressureMonitor fake(/*initial_pressure=*/0.0, /*initial_now_ns=*/SECOND);
    ScopedMemoryPressureMonitor scope(fake);

    for (int i = 0; i < 10; ++i)
        EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::Normal);
}

TEST(MemoryPressureMonitor, SnapsUpImmediately)
{
    FakeMemoryPressureMonitor fake(0.0, SECOND);
    ScopedMemoryPressureMonitor scope(fake);

    fake.setPressure(0.80);
    EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::Elevated);

    fake.setPressure(0.92);
    EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::High);

    fake.setPressure(0.97);
    EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::Critical);
}

TEST(MemoryPressureMonitor, StickyDownwardCooldown)
{
    FakeMemoryPressureMonitor fake(0.0, SECOND);
    ScopedMemoryPressureMonitor scope(fake);

    fake.setPressure(0.80);
    EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::Elevated);

    fake.setPressure(0.10);
    /// First sample still inside cooldown → stays Elevated.
    EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::Elevated);

    /// Advance 59 s — still inside cooldown.
    fake.setNowNs(SECOND + 59 * SECOND);
    EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::Elevated);

    /// Cross 60 s boundary — steps down to Normal.
    fake.setNowNs(SECOND + 61 * SECOND);
    EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::Normal);
}

TEST(MemoryPressureMonitor, RecoveryFromCriticalIsThreeCooldowns)
{
    FakeMemoryPressureMonitor fake(0.99, SECOND);
    ScopedMemoryPressureMonitor scope(fake);

    EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::Critical);

    fake.setPressure(0.0);

    /// Critical → High after one cooldown.
    fake.setNowNs(SECOND + COOLDOWN + SECOND);
    EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::High);

    /// High → Elevated after another.
    fake.setNowNs(SECOND + 2 * COOLDOWN + 2 * SECOND);
    EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::Elevated);

    /// Elevated → Normal after a third.
    fake.setNowNs(SECOND + 3 * COOLDOWN + 3 * SECOND);
    EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::Normal);
}

TEST(MemoryPressureMonitor, OscillationPinsLevelHigh)
{
    FakeMemoryPressureMonitor fake(0.80, SECOND);
    ScopedMemoryPressureMonitor scope(fake);
    EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::Elevated);

    /// Bounce 70 / 80 every 10 s for 5 minutes. Pressure goes above the
    /// Elevated threshold every other sample, so the cooldown timer never
    /// gets to complete a clean 60 s window — level stays at Elevated.
    uint64_t t = SECOND;
    for (int i = 0; i < 30; ++i)
    {
        t += 10 * SECOND;
        fake.setNowNs(t);
        fake.setPressure(i & 1 ? 0.70 : 0.80);
        EXPECT_EQ(fake.currentLevel(), MemoryPressureLevel::Elevated);
    }
}

TEST(MemoryPressureMonitor, SetThresholdsRejectsInvalid)
{
    FakeMemoryPressureMonitor fake(0.80, SECOND);
    ScopedMemoryPressureMonitor scope(fake);

    /// Out-of-range (any single value > 100) throws — previously wrapped
    /// silently through `uint8_t` at the call site (e.g. 300 → 44).
    EXPECT_THROW(fake.setThresholds(101, 90, 95), DB::Exception);
    EXPECT_THROW(fake.setThresholds(75, 101, 95), DB::Exception);
    EXPECT_THROW(fake.setThresholds(75, 90, 101), DB::Exception);
    EXPECT_THROW(fake.setThresholds(300, 90, 95), DB::Exception);

    /// Non-monotonic (level_1 > level_2 etc.) throws — previously silently
    /// sorted, masking config typos.
    EXPECT_THROW(fake.setThresholds(90, 75, 95), DB::Exception);
    EXPECT_THROW(fake.setThresholds(75, 95, 90), DB::Exception);

    /// Valid edges accepted.
    EXPECT_NO_THROW(fake.setThresholds(0, 0, 0));
    EXPECT_NO_THROW(fake.setThresholds(100, 100, 100));
    EXPECT_NO_THROW(fake.setThresholds(75, 90, 95));   // strictly increasing
    EXPECT_NO_THROW(fake.setThresholds(75, 75, 90));   // equality allowed
}

TEST(MemoryPressureMonitor, ScopedRestoresPriorMonitor)
{
    /// After the scope ends, `memoryPressureMonitor()` must hand back the
    /// production singleton, not a dangling pointer to the fake. Regression
    /// for the ASan/MSan stack-use-after-return that motivated this
    /// interface design.
    auto * before = &memoryPressureMonitor();
    {
        FakeMemoryPressureMonitor fake(0.99, SECOND);
        ScopedMemoryPressureMonitor scope(fake);
        EXPECT_NE(&memoryPressureMonitor(), before);
        EXPECT_EQ(memoryPressureMonitor().currentLevel(), MemoryPressureLevel::Critical);
    }
    EXPECT_EQ(&memoryPressureMonitor(), before);
}

/// `levelForPressure` maps a ratio to a level with no cooldown / no sticky
/// state — each call is independent (used for transient per-query pressure).
TEST(MemoryPressureMonitor, LevelForPressureIsStatelessThresholdMap)
{
    PressureLevelMachine m; /// default thresholds 75 / 90 / 95
    EXPECT_EQ(m.levelForPressure(0.50), MemoryPressureLevel::Normal);
    EXPECT_EQ(m.levelForPressure(0.80), MemoryPressureLevel::Elevated);
    EXPECT_EQ(m.levelForPressure(0.92), MemoryPressureLevel::High);
    EXPECT_EQ(m.levelForPressure(0.98), MemoryPressureLevel::Critical);
    /// A high reading must NOT persist into the next call (unlike `sample`).
    EXPECT_EQ(m.levelForPressure(0.10), MemoryPressureLevel::Normal);
}

/// The query-level (`Process`) limit governs when it is the most constraining,
/// and the walk reacts to it directly — this is the signal the total-only
/// reader used to miss. Root chain at `parent == nullptr` (non-Global) so the
/// test allocates no real global memory.
TEST(MemoryPressureMonitor, LocalPressureUsesQueryLevelLimit)
{
    EXPECT_DOUBLE_EQ(localMemoryPressureFromChain(nullptr), 0.0);

    MemoryTracker user(nullptr, VariableContext::User, false);
    MemoryTracker query(&user, VariableContext::Process, false);
    MemoryTracker thread(&query, VariableContext::Thread, false);

    /// No limits anywhere → no pressure.
    EXPECT_DOUBLE_EQ(localMemoryPressureFromChain(&thread), 0.0);

    query.setHardLimit(1000);
    thread.adjustWithUntrackedMemory(960); /// propagates up: thread=query=user=960
    EXPECT_NEAR(localMemoryPressureFromChain(&thread), 0.96, 1e-9);
    thread.adjustWithUntrackedMemory(-960);
}

/// When the query level has no limit, the walk escalates to the next level
/// that does (here, the user/`User` tracker).
TEST(MemoryPressureMonitor, LocalPressureFallsBackToUserLevel)
{
    MemoryTracker user(nullptr, VariableContext::User, false);
    MemoryTracker query(&user, VariableContext::Process, false); /// no limit
    MemoryTracker thread(&query, VariableContext::Thread, false);

    user.setHardLimit(2000);
    thread.adjustWithUntrackedMemory(1500); /// user = 1500 / 2000 = 0.75
    EXPECT_NEAR(localMemoryPressureFromChain(&thread), 0.75, 1e-9);
    thread.adjustWithUntrackedMemory(-1500);
}

/// The server total (`Global`) is intentionally skipped here — it is handled,
/// with cooldown smoothing, by the separate total-pressure path. Even a
/// near-limit Global tracker contributes nothing to the local pressure.
TEST(MemoryPressureMonitor, LocalPressureSkipsGlobalLevel)
{
    MemoryTracker total(nullptr, VariableContext::Global);
    MemoryTracker thread(&total, VariableContext::Thread, false);

    total.setHardLimit(1000);
    thread.adjustWithUntrackedMemory(990); /// Global at 99%, but must be ignored
    EXPECT_DOUBLE_EQ(localMemoryPressureFromChain(&thread), 0.0);
    thread.adjustWithUntrackedMemory(-990);
}

/// `MemoryTracker::getPressure` is `amount / hard_limit`, lock-free, and 0 when
/// there is no limit or no usage.
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

/// `setThresholds` publishes atomically and `levelForPressure` (lock-free) reads
/// the new ladder; `getThresholds` round-trips the same values.
TEST(MemoryPressureMonitor, SetThresholdsReflectedInLevelForPressure)
{
    PressureLevelMachine m;
    m.setThresholds(50, 70, 90);

    const auto th = m.getThresholds();
    EXPECT_EQ(th.l1_pct, 50u);
    EXPECT_EQ(th.l2_pct, 70u);
    EXPECT_EQ(th.l3_pct, 90u);

    EXPECT_EQ(m.levelForPressure(0.40), MemoryPressureLevel::Normal);
    EXPECT_EQ(m.levelForPressure(0.55), MemoryPressureLevel::Elevated);
    EXPECT_EQ(m.levelForPressure(0.75), MemoryPressureLevel::High);
    EXPECT_EQ(m.levelForPressure(0.95), MemoryPressureLevel::Critical);
}
