#include <gtest/gtest.h>

#include <Interpreters/FileCache/FileCacheEfficiency.h>
#include <Interpreters/FileCache/FileSegmentEfficiency.h>

#include <atomic>
#include <thread>
#include <vector>

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
    t.efficiency.apply(window, FileCacheEfficiency::Delta::move(50, /*large=*/false));

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
    t.efficiency.addPassiveBytes(/*segment_window=*/0, 100, /*large=*/false);
    t.efficiency.apply(/*segment_window=*/0, FileCacheEfficiency::Delta::move(100, /*large=*/false));

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
    t.efficiency.apply(window, FileCacheEfficiency::Delta::move(30, /*large=*/false));   /// more than passive: passive < 0, clamp it to 0
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
    t.efficiency.apply(window, FileCacheEfficiency::Delta::move(30, /*large=*/true));
    t.efficiency.addPassiveBytes(window, 50, /*large=*/false);
    t.efficiency.apply(window, FileCacheEfficiency::Delta::move(20, /*large=*/false));
    /// A large file segment with 10 active and 40 passive bytes shrinks and is not large anymore.
    t.efficiency.apply(
        window, FileCacheEfficiency::Delta::dismiss(10, 40, /*large=*/true) + FileCacheEfficiency::Delta::enroll(10, 40, /*large=*/false));

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

namespace
{

constexpr size_t G = FileSegmentEfficiency::GRANULE_SIZE;

}

TEST(FileSegmentEfficiency, ReadsCountAtAccount)
{
    TestEfficiency t(W);
    t.used_size = 4 * G;
    FileSegmentEfficiency segment(4 * G);
    const ByteRange range{0, 4 * G};

    /// A read only marks its granules; `account` counts them, and a repeated read of a granule adds nothing.
    segment.markRead(range, ByteRange{G + 10, 16});
    EXPECT_TRUE(segment.hasUnaccountedReads());
    segment.account(t.efficiency, 4 * G, /*reserved_bytes=*/4 * G, /*large=*/false);
    EXPECT_FALSE(segment.hasUnaccountedReads());
    segment.markRead(range, ByteRange{G, G});
    segment.account(t.efficiency, 4 * G, 4 * G, /*large=*/false);
    EXPECT_EQ(segment.getInfo(/*current_window=*/0, 4 * G, 4 * G).active_bytes, G);

    t.advance(W);
    expectSnapshot(t.efficiency.getSnapshot(), G, 3 * G, 0);
}

TEST(FileSegmentEfficiency, ReadsCountInTheWindowOfTheirAccount)
{
    TestEfficiency t(W);
    t.used_size = 4 * G;
    FileSegmentEfficiency segment(4 * G);

    /// A read before the end of window 0 that is counted in window 1 counts in window 1.
    segment.markRead(ByteRange{0, 4 * G}, ByteRange{0, G});
    t.advance(W);
    segment.account(t.efficiency, 4 * G, 4 * G, /*large=*/false);
    expectSnapshot(t.efficiency.getSnapshot(), 0, 0, 4 * G);

    t.advance(W);
    expectSnapshot(t.efficiency.getSnapshot(), G, 3 * G, 0);
}

TEST(FileSegmentEfficiency, ConcurrentReadsAndAccounts)
{
    /// Readers mark the granules without a lock while one thread, as the holder of the file segment lock, counts
    /// them: each granule counts once.
    constexpr size_t granules = 256;
    TestEfficiency t(W);
    t.used_size = granules * G;
    FileSegmentEfficiency segment(granules * G);
    const ByteRange range{0, granules * G};

    std::atomic<bool> done = false;
    std::thread accounting([&]
    {
        while (!done.load())
            segment.account(t.efficiency, granules * G, granules * G, /*large=*/false);
    });
    std::vector<std::thread> readers;
    for (size_t reader = 0; reader < 4; ++reader)
        readers.emplace_back([&, reader]
        {
            for (size_t i = 0; i < granules; ++i)
                segment.markRead(range, ByteRange{((i + reader * 64) % granules) * G, G});
        });
    for (auto & thread : readers)
        thread.join();
    done = true;
    accounting.join();
    segment.account(t.efficiency, granules * G, granules * G, /*large=*/false);

    EXPECT_FALSE(segment.hasUnaccountedReads());
    EXPECT_EQ(segment.getInfo(/*current_window=*/0, granules * G, granules * G).active_bytes, granules * G);
    t.advance(W);
    expectSnapshot(t.efficiency.getSnapshot(), granules * G, 0, 0);
}
