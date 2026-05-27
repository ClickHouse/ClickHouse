#include <Common/MemoryPressureMonitor.h>
#include <Common/Exception.h>

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
