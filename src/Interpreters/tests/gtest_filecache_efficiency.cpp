#include <gtest/gtest.h>

#include <Interpreters/FileCache/FileCacheEfficiency.h>

using namespace DB;

namespace
{

/// A window that the real time of a test never reaches the end of: the tests move to the next
/// window only with `advance`.
constexpr UInt64 W = 3600;

/// `FileCacheEfficiency` with manual used sizes.
struct TestEfficiency
{
    explicit TestEfficiency(UInt64 window_sec)
        : efficiency(window_sec, [this] { return used_size; }, [this] { return large_used_size; })
    {
    }

    void advance(UInt64 seconds) { efficiency.shiftTimeForTesting(std::chrono::seconds(seconds)); }

    size_t used_size = 0;
    size_t large_used_size = 0;
    FileCacheEfficiency efficiency;
};

void expectSnapshot(const FileCacheEfficiency::Snapshot & snapshot, UInt64 active, UInt64 passive, UInt64 idle)
{
    EXPECT_EQ(snapshot.active_bytes, active);
    EXPECT_EQ(snapshot.passive_bytes, passive);
    EXPECT_EQ(snapshot.idle_bytes, idle);
}

}

TEST(FileCacheEfficiency, SnapshotOfLastFullWindow)
{
    TestEfficiency t(W);
    t.used_size = 300;

    const auto window = t.efficiency.currentWindow();
    EXPECT_EQ(window, 0);
    t.efficiency.addPassiveBytes(window, 200, /*large=*/false);
    t.efficiency.moveToActive(window, 50, /*large=*/false);

    /// The live window is not visible until it ends.
    expectSnapshot(t.efficiency.getSnapshot(), 0, 0, 0);

    t.advance(W);
    expectSnapshot(t.efficiency.getSnapshot(), 50, 150, 100);
}

TEST(FileCacheEfficiency, StaleWindowUpdatesAreIgnored)
{
    TestEfficiency t(W);
    t.used_size = 100;

    t.advance(W);
    EXPECT_EQ(t.efficiency.currentWindow(), 1);
    t.efficiency.addPassiveBytes(/*window=*/0, 100, /*large=*/false);
    t.efficiency.moveToActive(/*window=*/0, 100, /*large=*/false);

    t.advance(W);
    expectSnapshot(t.efficiency.getSnapshot(), 0, 0, 100);
}

TEST(FileCacheEfficiency, WindowsWithoutRotation)
{
    TestEfficiency t(W);
    t.used_size = 100;
    t.efficiency.addPassiveBytes(t.efficiency.currentWindow(), 100, /*large=*/false);

    /// Windows 1 and 2 pass with no call; the last full window (2) had no hits.
    t.advance(3 * W);
    expectSnapshot(t.efficiency.getSnapshot(), 0, 0, 100);
}

TEST(FileCacheEfficiency, NegativeAndInconsistentValuesAreClamped)
{
    TestEfficiency t(W);
    t.used_size = 50;
    const auto window = t.efficiency.currentWindow();
    t.efficiency.addPassiveBytes(window, 10, /*large=*/false);
    t.efficiency.moveToActive(window, 30, /*large=*/false);   /// more than passive: passive < 0, clamp it to 0
    t.advance(W);
    expectSnapshot(t.efficiency.getSnapshot(), 30, 0, 20);

    t.efficiency.addPassiveBytes(t.efficiency.currentWindow(), -20, /*large=*/false);   /// passive < 0: clamp to 0
    t.advance(W);
    expectSnapshot(t.efficiency.getSnapshot(), 0, 0, 50);
}

TEST(FileCacheEfficiency, Disabled)
{
    TestEfficiency t(0);
    t.used_size = 100;
    EXPECT_FALSE(t.efficiency.isEnabled());
    t.efficiency.addPassiveBytes(t.efficiency.currentWindow(), 100, /*large=*/false);
    t.advance(1000);
    EXPECT_EQ(t.efficiency.currentWindow(), 0);
    expectSnapshot(t.efficiency.getSnapshot(), 0, 0, 0);
}

TEST(FileCacheEfficiency, LargeSegments)
{
    TestEfficiency t(W);
    t.used_size = 300;
    t.large_used_size = 200;

    const auto window = t.efficiency.currentWindow();
    t.efficiency.addPassiveBytes(window, 150, /*large=*/true);
    t.efficiency.moveToActive(window, 30, /*large=*/true);
    t.efficiency.addPassiveBytes(window, 50, /*large=*/false);
    t.efficiency.moveToActive(window, 20, /*large=*/false);
    /// A large file segment with 10 active and 40 passive bytes shrinks and is not large anymore.
    t.efficiency.moveToClass(window, 10, 40, /*large=*/false);

    t.advance(W);
    auto snapshot = t.efficiency.getSnapshot();
    expectSnapshot(snapshot, 50, 150, 100);
    EXPECT_EQ(snapshot.large_active_bytes, 20);
    EXPECT_EQ(snapshot.large_passive_bytes, 80);
    EXPECT_EQ(snapshot.large_idle_bytes, 100);

    /// A full window without hits: all large bytes are idle.
    t.advance(2 * W);
    snapshot = t.efficiency.getSnapshot();
    expectSnapshot(snapshot, 0, 0, 300);
    EXPECT_EQ(snapshot.large_active_bytes, 0);
    EXPECT_EQ(snapshot.large_passive_bytes, 0);
    EXPECT_EQ(snapshot.large_idle_bytes, 200);
}
