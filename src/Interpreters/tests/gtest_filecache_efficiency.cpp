#include <gtest/gtest.h>

#include <Interpreters/FileCache/FileCacheEfficiency.h>

using namespace DB;

namespace
{

/// `FileCacheEfficiency` with a manual used size.
struct TestEfficiency
{
    explicit TestEfficiency(UInt64 window_sec)
        : efficiency(window_sec, [this] { return used_size; })
    {
    }

    void advance(UInt64 seconds) { efficiency.shiftTimeForTesting(std::chrono::seconds(seconds)); }

    size_t used_size = 0;
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
    TestEfficiency t(10);
    t.used_size = 300;

    const UInt64 window = t.efficiency.currentWindow();
    EXPECT_EQ(window, 0);
    t.efficiency.addHeldBytes(window, 200);
    t.efficiency.addActiveBytes(window, 50);

    /// The live window is not visible until it ends.
    expectSnapshot(t.efficiency.getSnapshot(), 0, 0, 0);

    t.advance(10);
    expectSnapshot(t.efficiency.getSnapshot(), 50, 150, 100);
}

TEST(FileCacheEfficiency, StaleWindowUpdatesAreIgnored)
{
    TestEfficiency t(10);
    t.used_size = 100;

    t.advance(10);
    EXPECT_EQ(t.efficiency.currentWindow(), 1);
    t.efficiency.addHeldBytes(/*window=*/0, 100);
    t.efficiency.addActiveBytes(/*window=*/0, 100);

    t.advance(10);
    expectSnapshot(t.efficiency.getSnapshot(), 0, 0, 100);
}

TEST(FileCacheEfficiency, WindowsWithoutRotation)
{
    TestEfficiency t(10);
    t.used_size = 100;
    t.efficiency.addHeldBytes(t.efficiency.currentWindow(), 100);

    /// Windows 1 and 2 pass with no call; the last full window (2) had no reads.
    t.advance(30);
    expectSnapshot(t.efficiency.getSnapshot(), 0, 0, 100);
}

TEST(FileCacheEfficiency, NegativeAndInconsistentValuesAreClamped)
{
    TestEfficiency t(10);
    t.used_size = 50;
    const UInt64 window = t.efficiency.currentWindow();
    t.efficiency.addHeldBytes(window, 10);
    t.efficiency.addActiveBytes(window, 30);   /// active > held: passive < 0, clamp it to 0
    t.advance(10);
    expectSnapshot(t.efficiency.getSnapshot(), 30, 0, 20);

    t.efficiency.addHeldBytes(t.efficiency.currentWindow(), -20);   /// held < 0: clamp to 0
    t.advance(10);
    expectSnapshot(t.efficiency.getSnapshot(), 0, 0, 50);
}

TEST(FileCacheEfficiency, Disabled)
{
    TestEfficiency t(0);
    t.used_size = 100;
    EXPECT_FALSE(t.efficiency.isEnabled());
    t.efficiency.addHeldBytes(t.efficiency.currentWindow(), 100);
    t.advance(1000);
    EXPECT_EQ(t.efficiency.currentWindow(), 0);
    expectSnapshot(t.efficiency.getSnapshot(), 0, 0, 0);
}
