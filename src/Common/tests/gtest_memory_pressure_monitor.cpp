#include <Common/MemoryPressureMonitor.h>

#include <gtest/gtest.h>

#include <atomic>

using namespace DB;

namespace
{

/// Wraps the singleton with controllable pressure + clock for deterministic tests.
struct MonitorFixture
{
    std::atomic<double> pressure{0.0};
    std::atomic<uint64_t> now_ns{1'000'000'000};  /// start at t=1s, not 0
    MemoryPressureMonitor & monitor = MemoryPressureMonitor::instance();

    MonitorFixture()
    {
        monitor.resetForTesting(
            [this] { return pressure.load(); },
            [this] { return now_ns.load(); });
    }

    void setPressure(double p) { pressure.store(p); }

    void advance(uint64_t ns) { now_ns.fetch_add(ns); }

    static constexpr uint64_t SECOND = 1'000'000'000ULL;
    static constexpr uint64_t COOLDOWN = 60 * SECOND;
};

}

TEST(MemoryPressureMonitor, NoPressureStaysAtL0)
{
    MonitorFixture f;
    for (int i = 0; i < 10; ++i)
    {
        f.advance(MonitorFixture::SECOND);
        EXPECT_EQ(f.monitor.effective().window_bytes, size_t{8} << 20);
        EXPECT_EQ(f.monitor.effective().block_bytes, size_t{1} << 20);
        EXPECT_EQ(f.monitor.currentLevel(), 0);
    }
}

TEST(MemoryPressureMonitor, SnapsUpImmediately)
{
    MonitorFixture f;
    f.setPressure(0.80);                              /// → L1
    auto cfg = f.monitor.effective();
    EXPECT_EQ(f.monitor.currentLevel(), 1);
    EXPECT_EQ(cfg.window_bytes, size_t{2} << 20);
    EXPECT_EQ(cfg.block_bytes, size_t{512} << 10);

    f.setPressure(0.92);                              /// → L2
    cfg = f.monitor.effective();
    EXPECT_EQ(f.monitor.currentLevel(), 2);
    EXPECT_EQ(cfg.window_bytes, size_t{512} << 10);
    EXPECT_EQ(cfg.block_bytes, size_t{512} << 10);

    f.setPressure(0.97);                              /// → L3
    cfg = f.monitor.effective();
    EXPECT_EQ(f.monitor.currentLevel(), 3);
    EXPECT_EQ(cfg.window_bytes, size_t{128} << 10);
    EXPECT_EQ(cfg.block_bytes, size_t{128} << 10);
}

TEST(MemoryPressureMonitor, StickyDownwardCooldown)
{
    MonitorFixture f;
    f.setPressure(0.80);                              /// snap to L1
    EXPECT_EQ(f.monitor.effective().window_bytes, size_t{2} << 20);

    f.setPressure(0.10);                              /// pressure released
    /// First sample still inside cooldown → stays L1.
    EXPECT_EQ(f.monitor.effective().window_bytes, size_t{2} << 20);
    EXPECT_EQ(f.monitor.currentLevel(), 1);

    /// Advance 59 s — still inside cooldown.
    f.advance(59 * MonitorFixture::SECOND);
    EXPECT_EQ(f.monitor.effective().window_bytes, size_t{2} << 20);
    EXPECT_EQ(f.monitor.currentLevel(), 1);

    /// Cross 60 s boundary — steps down to L0.
    f.advance(2 * MonitorFixture::SECOND);
    EXPECT_EQ(f.monitor.effective().window_bytes, size_t{8} << 20);
    EXPECT_EQ(f.monitor.currentLevel(), 0);
}

TEST(MemoryPressureMonitor, RecoveryFromCriticalIsThreeCooldowns)
{
    MonitorFixture f;
    f.setPressure(0.99);                              /// snap to L3 on first sample
    (void)f.monitor.effective();
    EXPECT_EQ(f.monitor.currentLevel(), 3);

    f.setPressure(0.0);

    /// L3 → L2 after one cooldown.
    f.advance(MonitorFixture::COOLDOWN + MonitorFixture::SECOND);
    (void)f.monitor.effective();
    EXPECT_EQ(f.monitor.currentLevel(), 2);

    /// L2 → L1 after another cooldown.
    f.advance(MonitorFixture::COOLDOWN + MonitorFixture::SECOND);
    (void)f.monitor.effective();
    EXPECT_EQ(f.monitor.currentLevel(), 1);

    /// L1 → L0 after a third.
    f.advance(MonitorFixture::COOLDOWN + MonitorFixture::SECOND);
    (void)f.monitor.effective();
    EXPECT_EQ(f.monitor.currentLevel(), 0);
}

TEST(MemoryPressureMonitor, OscillationPinsLevelHigh)
{
    MonitorFixture f;
    f.setPressure(0.80);                              /// snap to L1 on first sample
    (void)f.monitor.effective();
    EXPECT_EQ(f.monitor.currentLevel(), 1);

    /// Bounce 70 / 80 every 10 s for 5 minutes. Pressure goes above the L1
    /// threshold every other sample, so the cooldown timer never gets to
    /// complete a clean 60 s window — level stays at L1.
    for (int i = 0; i < 30; ++i)
    {
        f.advance(10 * MonitorFixture::SECOND);
        f.setPressure(i & 1 ? 0.70 : 0.80);
        (void)f.monitor.effective();
        EXPECT_EQ(f.monitor.currentLevel(), 1);
    }
}

TEST(MemoryPressureMonitor, SetThresholdsClampsAndOrders)
{
    MonitorFixture f;

    /// Out-of-order: should be sorted internally so the level ladder stays
    /// monotonic.
    f.monitor.setThresholds(90, 75, 95);
    f.setPressure(0.80);
    EXPECT_EQ(f.monitor.effective().window_bytes, size_t{2} << 20);
    EXPECT_EQ(f.monitor.currentLevel(), 1);

    /// Zero clamped to 1 — pressure 0 stays L0 (pressure < 0.01 is below the floor).
    f.monitor.resetForTesting(
        [&f] { return f.pressure.load(); },
        [&f] { return f.now_ns.load(); });
    f.monitor.setThresholds(0, 0, 0);
    f.setPressure(0.005);
    EXPECT_EQ(f.monitor.currentLevel(), 0);
}
