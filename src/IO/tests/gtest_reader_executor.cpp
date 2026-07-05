#include <IO/ReaderExecutor.h>
#include <IO/LocalSourceReader.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace ProfileEvents
{
    extern const Event ReaderExecutorSourceRequests;
    extern const Event ReaderExecutorBytesFromSource;
    extern const Event ReaderExecutorRequestedBytes;
    extern const Event ReaderExecutorModeledCostMicroseconds;
    extern const Event ReaderExecutorCacheGetRequests;
    extern const Event ReaderExecutorCachePopulateRequests;
    extern const Event ReaderExecutorIncompleteConnections;
}

using namespace DB;

namespace
{

/// RAII helper: creates a ThreadGroup with its own ProfileEvents counters, attaches the
/// current thread to it, detaches in the destructor -- so a test reads the executor's
/// ProfileEvents in isolation, without interference from other tests.
struct TestThreadGroup
{
    /// Create a ThreadStatus only if none exists (the debug build attaches a
    /// MainThreadStatus; ASan/release may not), else ThreadStatus's ctor asserts.
    std::optional<DB::ThreadStatus> thread_status_holder{
        current_thread ? std::nullopt : std::optional<DB::ThreadStatus>(std::in_place)};
    DB::ThreadGroupPtr thread_group = DB::ThreadGroup::createForQuery(getContext().context);
    DB::ThreadGroupSwitcher switcher{thread_group, ThreadName::UNKNOWN};

    ProfileEvents::Count get(ProfileEvents::Event event) const
    {
        return thread_group->performance_counters[event];
    }
};

/// Byte value at logical offset `i` within a file: deterministic pattern.
unsigned char patternByte(size_t i)
{
    return static_cast<unsigned char>(i % 256);
}

class ReaderExecutorTest : public ::testing::Test
{
protected:
    std::filesystem::path tmp_dir;

    void SetUp() override
    {
        tmp_dir = std::filesystem::temp_directory_path() / "test_reader_executor";
        std::filesystem::create_directories(tmp_dir);
    }

    void TearDown() override { std::filesystem::remove_all(tmp_dir); }

    /// Write `size` bytes following `patternByte` to a new file and return the
    /// matching StoredObject.
    StoredObject makeFile(const std::string & name, size_t size)
    {
        auto path = tmp_dir / name;
        std::ofstream f(path, std::ios::binary);
        for (size_t i = 0; i < size; ++i)
            f.put(static_cast<char>(patternByte(i)));
        f.close();

        StoredObject obj;
        obj.remote_path = path.string();
        obj.bytes_size = size;
        return obj;
    }

    /// Drain the executor and return all bytes it serves, streaming each window's chain.
    static std::vector<char> drain(ReaderExecutor & ex)
    {
        std::vector<char> out;
        while (true)
        {
            ChainedBuffers w = ex.readNextWindow();
            if (w.atEnd())
                break;
            while (!w.atEnd())
            {
                auto span = w.peek();
                out.insert(out.end(), span.data, span.data + span.size);
                w.advance(span.size);
            }
        }
        return out;
    }
};

TEST_F(ReaderExecutorTest, SequentialReadSingleObject)
{
    StoredObjects objects{makeFile("a.bin", 1024)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, /*block_size=*/256);

    EXPECT_EQ(ex.totalSize(), 1024u);
    EXPECT_FALSE(ex.hasUnknownSize());

    auto data = drain(ex);
    ASSERT_EQ(data.size(), 1024u);
    for (size_t i = 0; i < data.size(); ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[i]), patternByte(i)) << "at offset " << i;
    EXPECT_EQ(ex.getPosition(), 1024u);
}

TEST_F(ReaderExecutorTest, WindowNeverExceedsBlockSize)
{
    StoredObjects objects{makeFile("a.bin", 1000)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, /*block_size=*/100);

    size_t total = 0;
    size_t windows = 0;
    while (true)
    {
        ChainedBuffers w = ex.readNextWindow();
        if (w.atEnd())
            break;
        EXPECT_LE(w.totalBytes(), 100u);
        EXPECT_EQ(w.peek().logical_offset, total);
        total += w.totalBytes();
        ++windows;
    }
    EXPECT_EQ(total, 1000u);
    EXPECT_EQ(windows, 10u);
}

TEST_F(ReaderExecutorTest, SeekThenRead)
{
    StoredObjects objects{makeFile("a.bin", 1024)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, /*block_size=*/256);

    ex.seek(500);
    EXPECT_EQ(ex.getPosition(), 500u);

    ChainedBuffers w = ex.readNextWindow();
    ASSERT_FALSE(w.atEnd());
    auto span = w.peek();
    EXPECT_EQ(span.logical_offset, 500u);
    EXPECT_EQ(static_cast<unsigned char>(span.data[0]), patternByte(500));

    /// Seek backward and re-read.
    ex.seek(10);
    ChainedBuffers w2 = ex.readNextWindow();
    ASSERT_FALSE(w2.atEnd());
    auto span2 = w2.peek();
    EXPECT_EQ(span2.logical_offset, 10u);
    EXPECT_EQ(static_cast<unsigned char>(span2.data[0]), patternByte(10));
}

TEST_F(ReaderExecutorTest, MultiObjectConcatenationNeverCrossesBoundary)
{
    StoredObjects objects{makeFile("a.bin", 300), makeFile("b.bin", 200)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, /*block_size=*/256);

    EXPECT_EQ(ex.totalSize(), 500u);

    /// A window must never straddle the object boundary at 300.
    while (true)
    {
        size_t pos = ex.getPosition();
        ChainedBuffers w = ex.readNextWindow();
        if (w.atEnd())
            break;
        if (pos < 300)
            EXPECT_LE(w.peek().logical_offset + w.totalBytes(), 300u) << "window from " << pos << " crossed boundary";
    }
    EXPECT_EQ(ex.getPosition(), 500u);
}

TEST_F(ReaderExecutorTest, MultiObjectDataIsCorrect)
{
    StoredObjects objects{makeFile("a.bin", 300), makeFile("b.bin", 200)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, /*block_size=*/64);

    auto data = drain(ex);
    ASSERT_EQ(data.size(), 500u);
    /// Object A holds pattern[0..299], object B holds pattern[0..199].
    for (size_t i = 0; i < 300; ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[i]), patternByte(i)) << "object A at " << i;
    for (size_t i = 0; i < 200; ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[300 + i]), patternByte(i)) << "object B at " << i;
}

TEST_F(ReaderExecutorTest, EmptyFileIsImmediateEOF)
{
    StoredObjects objects{makeFile("empty.bin", 0)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, /*block_size=*/256);

    EXPECT_EQ(ex.totalSize(), 0u);
    EXPECT_TRUE(ex.readNextWindow().atEnd());
}

TEST_F(ReaderExecutorTest, MissingFileWithUnknownSizeThrows)
{
    /// `DiskLocal::prepareRead` marks an unstatable file `UnknownSize`; the
    /// executor must then open it and surface the real error (e.g. file does not
    /// exist) instead of treating it as an empty read.
    StoredObject missing;
    missing.remote_path = (tmp_dir / "does_not_exist.bin").string();
    missing.bytes_size = StoredObject::UnknownSize;
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), {missing}, /*block_size=*/256);

    EXPECT_ANY_THROW(ex.readNextWindow());
}

TEST_F(ReaderExecutorTest, TruncatedKnownSizeFileThrows)
{
    /// A known-size object whose file is shorter than its declared size is
    /// truncated/corrupt; the executor must throw rather than return a short read.
    StoredObject obj = makeFile("short.bin", 100);
    obj.bytes_size = 1000;  // pretend the object is larger than the file on disk
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), {obj}, /*block_size=*/256);

    EXPECT_ANY_THROW(ex.readNextWindow());
}

/// The metrics tests read the executor's ProfileEvents from a fresh per-test ThreadGroup
/// (starts at zero) -- the same path that feeds `system.events`.
TEST_F(ReaderExecutorTest, ProfileEventsCountSourceReadsAndBytes)
{
    TestThreadGroup tg;

    /// 1 MiB file read in 256 KiB blocks -> 4 source reads, all bytes served.
    constexpr size_t size = 1024 * 1024;
    StoredObjects objects{makeFile("a.bin", size)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, /*block_size=*/256 * 1024);
    drain(ex);

    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorSourceRequests), 4u);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorBytesFromSource), size);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorRequestedBytes), size);
    /// The cache / connection KPI inputs are not implemented in this slice.
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorCacheGetRequests), 0u);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorCachePopulateRequests), 0u);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorIncompleteConnections), 0u);
}

TEST_F(ReaderExecutorTest, ModeledCostMatchesFormula)
{
    TestThreadGroup tg;

    /// Modeled cost = 30ms/source request + 20ms/MiB from source (cache/conn terms 0).
    constexpr size_t size = 1024 * 1024;
    StoredObjects objects{makeFile("a.bin", size)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, /*block_size=*/256 * 1024);
    drain(ex);

    const auto cost = tg.get(ProfileEvents::ReaderExecutorModeledCostMicroseconds);
    const auto requested = tg.get(ProfileEvents::ReaderExecutorRequestedBytes);
    EXPECT_EQ(cost, 30000u * 4 + 20000u);  // 4 reads + 1 MiB
    EXPECT_EQ(requested, size);

    /// The KPI: modeled ms per requested MiB.
    const double ms_per_mib = (static_cast<double>(cost) / 1000.0)
        / (static_cast<double>(requested) / (1024.0 * 1024.0));
    EXPECT_DOUBLE_EQ(ms_per_mib, 140.0);
}

TEST_F(ReaderExecutorTest, ModeledCostScalesWithSourceRequests)
{
    TestThreadGroup tg;

    /// Smaller blocks over the same data -> more source requests -> higher modeled cost,
    /// so the KPI (cost per requested MiB) rises even though the bytes are unchanged.
    constexpr size_t size = 1024 * 1024;
    {
        StoredObjects big_block{makeFile("a.bin", size)};
        ReaderExecutor coarse(std::make_shared<LocalSourceReader>(), big_block, /*block_size=*/1024 * 1024);
        drain(coarse);
    }
    const auto cost_after_coarse = tg.get(ProfileEvents::ReaderExecutorModeledCostMicroseconds);
    const auto requests_after_coarse = tg.get(ProfileEvents::ReaderExecutorSourceRequests);
    {
        StoredObjects small_block{makeFile("b.bin", size)};
        ReaderExecutor fine(std::make_shared<LocalSourceReader>(), small_block, /*block_size=*/64 * 1024);
        drain(fine);
    }
    const auto cost_after_fine = tg.get(ProfileEvents::ReaderExecutorModeledCostMicroseconds);
    const auto requests_after_fine = tg.get(ProfileEvents::ReaderExecutorSourceRequests);

    EXPECT_EQ(requests_after_coarse, 1u);
    EXPECT_EQ(requests_after_fine - requests_after_coarse, 16u);
    EXPECT_GT(cost_after_fine - cost_after_coarse, cost_after_coarse);
}

}
