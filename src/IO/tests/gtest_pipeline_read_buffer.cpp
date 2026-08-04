#include <IO/PipelineReadBuffer.h>
#include <IO/ReaderExecutor.h>
#include <IO/LocalSourceReader.h>
#include <IO/ReadHelpers.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Common/CurrentMetrics.h>

#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace CurrentMetrics
{
    extern const Metric ReaderExecutorChainedBufferBytes;
}

using namespace DB;

namespace
{

unsigned char patternByte(size_t i)
{
    return static_cast<unsigned char>(i % 256);
}

class PipelineReadBufferTest : public ::testing::Test
{
protected:
    std::filesystem::path tmp_dir;

    void SetUp() override
    {
        tmp_dir = std::filesystem::temp_directory_path() / "test_pipeline_read_buffer";
        std::filesystem::create_directories(tmp_dir);
    }

    void TearDown() override { std::filesystem::remove_all(tmp_dir); }

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

    std::unique_ptr<PipelineReadBuffer> makeBuffer(const StoredObjects & objects, size_t block_size = 256)
    {
        auto executor = std::make_unique<ReaderExecutor>(
            std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{.block_size = block_size});
        return std::make_unique<PipelineReadBuffer>(std::move(executor));
    }
};

TEST_F(PipelineReadBufferTest, ReadWholeFile)
{
    auto buf = makeBuffer({makeFile("a.bin", 1024)});

    EXPECT_EQ(buf->tryGetFileSize(), std::optional<size_t>(1024));

    std::vector<char> data(1024);
    buf->readStrict(data.data(), data.size());
    for (size_t i = 0; i < data.size(); ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[i]), patternByte(i)) << "at offset " << i;
    EXPECT_TRUE(buf->eof());
    EXPECT_EQ(buf->getPosition(), 1024);
}

TEST_F(PipelineReadBufferTest, SpansBlocksAndObjects)
{
    /// Two objects, small block size: data must be continuous across both block
    /// and object boundaries when read through the standard buffer interface.
    auto buf = makeBuffer({makeFile("a.bin", 300), makeFile("b.bin", 200)}, /*block_size=*/64);

    std::vector<char> data(500);
    buf->readStrict(data.data(), data.size());
    for (size_t i = 0; i < 300; ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[i]), patternByte(i)) << "object A at " << i;
    for (size_t i = 0; i < 200; ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[300 + i]), patternByte(i)) << "object B at " << i;
    EXPECT_TRUE(buf->eof());
}

TEST_F(PipelineReadBufferTest, SeekSetAndRead)
{
    auto buf = makeBuffer({makeFile("a.bin", 1024)});

    buf->seek(500, SEEK_SET);
    EXPECT_EQ(buf->getPosition(), 500);

    char c = 0;
    buf->readStrict(&c, 1);
    EXPECT_EQ(static_cast<unsigned char>(c), patternByte(500));
    EXPECT_EQ(buf->getPosition(), 501);
}

TEST_F(PipelineReadBufferTest, SeekBackwardRereads)
{
    auto buf = makeBuffer({makeFile("a.bin", 1024)});

    std::vector<char> head(400);
    buf->readStrict(head.data(), head.size());

    buf->seek(0, SEEK_SET);
    EXPECT_EQ(buf->getPosition(), 0);

    char c = 0;
    buf->readStrict(&c, 1);
    EXPECT_EQ(static_cast<unsigned char>(c), patternByte(0));
}

TEST_F(PipelineReadBufferTest, SeekCurRelative)
{
    auto buf = makeBuffer({makeFile("a.bin", 1024)});

    buf->seek(100, SEEK_SET);
    buf->seek(50, SEEK_CUR);
    EXPECT_EQ(buf->getPosition(), 150);

    char c = 0;
    buf->readStrict(&c, 1);
    EXPECT_EQ(static_cast<unsigned char>(c), patternByte(150));
}

TEST_F(PipelineReadBufferTest, InvokesProfileCallback)
{
    /// MergeTreeReadPool's slow-read backoff relies on the profile callback being
    /// invoked for each read; the executor path must keep feeding it.
    auto buf = makeBuffer({makeFile("a.bin", 1024)}, /*block_size=*/256);

    size_t calls = 0;
    size_t reported = 0;
    buf->setProfileCallback([&](ReadBufferFromFileBase::ProfileInfo info)
    {
        ++calls;
        reported += info.bytes_read;
    }, CLOCK_MONOTONIC);

    std::vector<char> data(1024);
    buf->readStrict(data.data(), data.size());

    EXPECT_GT(calls, 0u);
    EXPECT_EQ(reported, 1024u);
}

TEST_F(PipelineReadBufferTest, SetReadUntilPositionBoundsRead)
{
    /// A hard bound (as StorageLog sets under the read lock) must stop the read
    /// at the bound even though the file is larger.
    auto buf = makeBuffer({makeFile("a.bin", 1024)}, /*block_size=*/256);
    buf->setReadUntilPosition(500);

    size_t total = 0;
    while (true)
    {
        char tmp[128];
        size_t got = buf->read(tmp, sizeof(tmp));
        if (got == 0)
            break;
        total += got;
    }
    EXPECT_EQ(total, 500u);
}

TEST_F(PipelineReadBufferTest, NarrowingReadUntilAfterBufferingTrims)
{
    /// Fill a block, consume part of it, then narrow the bound into the buffered
    /// region: the already-buffered bytes past the bound must not be returned.
    auto buf = makeBuffer({makeFile("a.bin", 1024)}, /*block_size=*/256);

    char head[128];
    buf->readStrict(head, sizeof(head));
    EXPECT_EQ(buf->getPosition(), 128);

    buf->setReadUntilPosition(200);

    size_t total = 128;
    while (true)
    {
        char tmp[64];
        size_t got = buf->read(tmp, sizeof(tmp));
        if (got == 0)
            break;
        total += got;
    }
    EXPECT_EQ(total, 200u);
}

TEST_F(PipelineReadBufferTest, ExtendReadUntilAfterTrimContinues)
{
    /// Narrow the bound into a buffered block, drain to it, then widen the bound
    /// and continue: the bytes after the old bound must follow with nothing
    /// skipped.
    auto buf = makeBuffer({makeFile("a.bin", 1024)}, /*block_size=*/256);

    char head[128];
    buf->readStrict(head, sizeof(head));            // [0, 128)

    buf->setReadUntilPosition(200);
    size_t mid = 0;
    while (true)
    {
        char tmp[64];
        size_t got = buf->read(tmp, sizeof(tmp));
        if (got == 0)
            break;
        mid += got;
    }
    EXPECT_EQ(mid, 72u);                             // [128, 200)

    buf->setReadUntilEnd();
    std::vector<char> rest;
    while (true)
    {
        char tmp[256];
        size_t got = buf->read(tmp, sizeof(tmp));
        if (got == 0)
            break;
        rest.insert(rest.end(), tmp, tmp + got);
    }

    ASSERT_EQ(rest.size(), 1024u - 200u);            // [200, 1024)
    for (size_t i = 0; i < rest.size(); ++i)
        ASSERT_EQ(static_cast<unsigned char>(rest[i]), patternByte(200 + i)) << "at logical " << (200 + i);
}

TEST_F(PipelineReadBufferTest, ChainedBufferBytesReturnToBaselineAtEOF)
{
    /// One window buffer is alive at a time while streaming, and EOF releases the last
    /// one: window memory must not accumulate across refills.
    const auto baseline = CurrentMetrics::get(CurrentMetrics::ReaderExecutorChainedBufferBytes);
    auto buf = makeBuffer({makeFile("a.bin", 1024)}, /*block_size=*/256);

    std::vector<char> data(256);
    for (size_t window = 0; window < 4; ++window)
    {
        buf->readStrict(data.data(), data.size());
        EXPECT_LE(CurrentMetrics::get(CurrentMetrics::ReaderExecutorChainedBufferBytes) - baseline, 256);
    }
    EXPECT_TRUE(buf->eof());
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::ReaderExecutorChainedBufferBytes), baseline);
}

}
