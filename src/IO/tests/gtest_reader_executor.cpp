#include <IO/ReaderExecutor.h>
#include <IO/LocalSourceReader.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>
#include <string>
#include <vector>

using namespace DB;

namespace
{

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

    /// Drain the executor and return all bytes it serves.
    static std::vector<char> drain(ReaderExecutor & ex)
    {
        std::vector<char> out;
        while (true)
        {
            auto chunk = ex.readNextChunk();
            if (chunk.size == 0)
                break;
            out.insert(out.end(), chunk.data, chunk.data + chunk.size);
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

TEST_F(ReaderExecutorTest, ChunkNeverExceedsBlockSize)
{
    StoredObjects objects{makeFile("a.bin", 1000)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, /*block_size=*/100);

    size_t total = 0;
    size_t chunks = 0;
    while (true)
    {
        auto chunk = ex.readNextChunk();
        if (chunk.size == 0)
            break;
        EXPECT_LE(chunk.size, 100u);
        EXPECT_EQ(chunk.logical_offset, total);
        total += chunk.size;
        ++chunks;
    }
    EXPECT_EQ(total, 1000u);
    EXPECT_EQ(chunks, 10u);
}

TEST_F(ReaderExecutorTest, SeekThenRead)
{
    StoredObjects objects{makeFile("a.bin", 1024)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, /*block_size=*/256);

    ex.seek(500);
    EXPECT_EQ(ex.getPosition(), 500u);

    auto chunk = ex.readNextChunk();
    ASSERT_GT(chunk.size, 0u);
    EXPECT_EQ(chunk.logical_offset, 500u);
    EXPECT_EQ(static_cast<unsigned char>(chunk.data[0]), patternByte(500));

    /// Seek backward and re-read.
    ex.seek(10);
    auto chunk2 = ex.readNextChunk();
    ASSERT_GT(chunk2.size, 0u);
    EXPECT_EQ(chunk2.logical_offset, 10u);
    EXPECT_EQ(static_cast<unsigned char>(chunk2.data[0]), patternByte(10));
}

TEST_F(ReaderExecutorTest, MultiObjectConcatenationNeverCrossesBoundary)
{
    StoredObjects objects{makeFile("a.bin", 300), makeFile("b.bin", 200)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, /*block_size=*/256);

    EXPECT_EQ(ex.totalSize(), 500u);

    /// A chunk must never straddle the object boundary at 300.
    while (true)
    {
        size_t pos = ex.getPosition();
        auto chunk = ex.readNextChunk();
        if (chunk.size == 0)
            break;
        if (pos < 300)
            EXPECT_LE(chunk.logical_offset + chunk.size, 300u) << "chunk from " << pos << " crossed boundary";
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
    auto chunk = ex.readNextChunk();
    EXPECT_EQ(chunk.size, 0u);
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

    EXPECT_ANY_THROW(ex.readNextChunk());
}

TEST_F(ReaderExecutorTest, TruncatedKnownSizeFileThrows)
{
    /// A known-size object whose file is shorter than its declared size is
    /// truncated/corrupt; the executor must throw rather than return a short read.
    StoredObject obj = makeFile("short.bin", 100);
    obj.bytes_size = 1000;  // pretend the object is larger than the file on disk
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), {obj}, /*block_size=*/256);

    EXPECT_ANY_THROW(ex.readNextChunk());
}

TEST_F(ReaderExecutorTest, StatsCountSourceReadsAndBytes)
{
    /// 1 MiB file read in 256 KiB blocks -> 4 source reads, all bytes served.
    constexpr size_t size = 1024 * 1024;
    StoredObjects objects{makeFile("a.bin", size)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, /*block_size=*/256 * 1024);

    drain(ex);

    const auto & stats = ex.getStats();
    EXPECT_EQ(stats.source_requests, 4u);
    EXPECT_EQ(stats.bytes_from_source, size);
    EXPECT_EQ(stats.bytes_requested, size);
    /// The cache / connection KPI inputs are not implemented in this slice.
    EXPECT_EQ(stats.cache_get_requests, 0u);
    EXPECT_EQ(stats.cache_populate_requests, 0u);
    EXPECT_EQ(stats.incomplete_connections, 0u);
}

TEST_F(ReaderExecutorTest, ModeledCostMatchesFormula)
{
    /// Modeled cost = 30ms/source request + 20ms/MiB from source (cache/conn terms 0).
    constexpr size_t size = 1024 * 1024;
    StoredObjects objects{makeFile("a.bin", size)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, /*block_size=*/256 * 1024);

    drain(ex);

    const auto & stats = ex.getStats();
    const size_t expected_cost_us = 30000 * stats.source_requests
        + 20000 * stats.bytes_from_source / (1024 * 1024);
    EXPECT_EQ(expected_cost_us, 30000u * 4 + 20000u);  // 4 reads + 1 MiB
    EXPECT_EQ(ex.modeledCostMicroseconds(), expected_cost_us);

    /// The KPI: modeled ms per requested MiB.
    const double ms_per_mib = (static_cast<double>(ex.modeledCostMicroseconds()) / 1000.0)
        / (static_cast<double>(stats.bytes_requested) / (1024.0 * 1024.0));
    EXPECT_DOUBLE_EQ(ms_per_mib, 140.0);
}

TEST_F(ReaderExecutorTest, ModeledCostScalesWithSourceRequests)
{
    /// Smaller blocks over the same data -> more source requests -> higher modeled cost,
    /// so the KPI (cost per requested MiB) rises even though the bytes are unchanged.
    constexpr size_t size = 1024 * 1024;
    StoredObjects big_block{makeFile("a.bin", size)};
    StoredObjects small_block{makeFile("b.bin", size)};
    ReaderExecutor coarse(std::make_shared<LocalSourceReader>(), big_block, /*block_size=*/1024 * 1024);
    ReaderExecutor fine(std::make_shared<LocalSourceReader>(), small_block, /*block_size=*/64 * 1024);

    drain(coarse);
    drain(fine);

    EXPECT_EQ(coarse.getStats().source_requests, 1u);
    EXPECT_EQ(fine.getStats().source_requests, 16u);
    EXPECT_GT(fine.modeledCostMicroseconds(), coarse.modeledCostMicroseconds());
}

}
