#include <IO/PipelineReadBuffer.h>
#include <IO/ReaderExecutor.h>
#include <IO/ISourceReader.h>
#include <IO/ReadSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/MMappedFileCache.h>
#include <IO/MMapReadBufferFromFileWithCache.h>
#include <Disks/IO/createReadBufferFromFileBase.h>

#include <gtest/gtest.h>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <Common/VectorWithMemoryTracking.h>

using namespace DB;

namespace
{

/// In-memory source reader for testing. open() materializes the data into a
/// temp file and returns a file-backed ReadBufferFromFileBase; temp file is
/// removed on destruction.
class MemorySourceReader : public ISourceReader
{
public:
    explicit MemorySourceReader(String data_) : data(std::move(data_)) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject &) override
    {
        auto path = std::filesystem::temp_directory_path() / ("test_pipeline_source_" + std::to_string(file_counter++));
        {
            std::ofstream f(path, std::ios::binary);
            f.write(data.data(), data.size());
        }
        temp_files.push_back(path);
        return createReadBufferFromFileBase(path.string(), ReadSettings{});
    }

    String name() const override { return "MemorySourceReader"; }

    ~MemorySourceReader() override
    {
        for (const auto & p : temp_files)
            std::filesystem::remove(p);
    }

private:
    String data;
    size_t file_counter = 0;
    std::vector<std::filesystem::path> temp_files;
};

}

TEST(PipelineReadBuffer, ReadAll)
{
    String content = "Hello, ReaderExecutor! This is a test of the pipeline read buffer.";
    auto source = std::make_shared<MemorySourceReader>(content);

    StoredObjects objects;
    objects.emplace_back("test", "", content.size());

    auto executor = std::make_unique<ReaderExecutor>(
        source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, 20);

    PipelineReadBuffer buf(std::move(executor));

    String result;
    readStringUntilEOF(result, buf);
    EXPECT_EQ(result, content);
}

TEST(PipelineReadBuffer, SeekNegativeOffsetThrows)
{
    /// Standard ReadBufferFromFileBase contract: negative SEEK_SET and
    /// SEEK_CUR that would underflow must throw ARGUMENT_OUT_OF_BOUND.
    /// Pre-fix: signed → unsigned cast wrapped to ~SIZE_MAX.
    String content(1000, 'X');
    auto source = std::make_shared<MemorySourceReader>(content);

    StoredObjects objects;
    objects.emplace_back("test", "", content.size());

    auto executor = std::make_unique<ReaderExecutor>(
        source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, 100);

    PipelineReadBuffer buf(std::move(executor));

    EXPECT_THROW(buf.seek(-1, SEEK_SET), Exception);
    EXPECT_THROW(buf.seek(-1, SEEK_CUR), Exception);

    /// SEEK_CUR landing exactly at 0 is valid.
    buf.seek(10, SEEK_SET);
    EXPECT_NO_THROW(buf.seek(-10, SEEK_CUR));
}

TEST(PipelineReadBuffer, Seek)
{
    String content = "0123456789ABCDEF";
    auto source = std::make_shared<MemorySourceReader>(content);

    StoredObjects objects;
    objects.emplace_back("test", "", content.size());

    auto executor = std::make_unique<ReaderExecutor>(
        source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, 8);

    PipelineReadBuffer buf(std::move(executor));

    buf.seek(10, SEEK_SET);
    String result;
    readStringUntilEOF(result, buf);
    EXPECT_EQ(result, "ABCDEF");
}

TEST(PipelineReadBuffer, GetPosition)
{
    String content(100, 'X');
    auto source = std::make_shared<MemorySourceReader>(content);

    StoredObjects objects;
    objects.emplace_back("test", "", 100);

    auto executor = std::make_unique<ReaderExecutor>(
        source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, 30);

    PipelineReadBuffer buf(std::move(executor));

    EXPECT_EQ(buf.getPosition(), 0);
    buf.next();
    /// After consuming first window (30 bytes), position should be at 30
    /// But the position depends on how many bytes are in working_buffer.
    /// After next(), pos should be at start of working_buffer.
    EXPECT_GE(buf.getPosition(), 0);
    EXPECT_LE(buf.getPosition(), 30);
}

TEST(PipelineReadBuffer, TryGetFileSize)
{
    String content(500, 'Y');
    auto source = std::make_shared<MemorySourceReader>(content);

    StoredObjects objects;
    objects.emplace_back("test", "", 500);

    auto executor = std::make_unique<ReaderExecutor>(
        source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, 100);

    PipelineReadBuffer buf(std::move(executor));

    auto size = buf.tryGetFileSize();
    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(*size, 500);
}

TEST(PipelineReadBuffer, TryGetFileSizeReturnsNulloptForUnknownSize)
{
    /// When the underlying object has `StoredObject::UnknownSize` (e.g. S3
    /// HEAD without `Content-Length`), `tryGetFileSize` must surface as
    /// `nullopt`, not as `~uint64_t::max`. Downstream
    /// `FormatFactory::wrapReadBufferIfNeeded` reads this to decide whether
    /// to wrap with `ParallelReadBuffer`; a max-valued size would enable
    /// parallel reads that can't be satisfied and trip
    /// `UNEXPECTED_END_OF_FILE`.
    String content = "small payload, real bytes unknown to the caller";
    auto source = std::make_shared<MemorySourceReader>(content);

    StoredObjects objects;
    objects.emplace_back("test", "", StoredObject::UnknownSize);

    auto executor = std::make_unique<ReaderExecutor>(
        source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, 100);

    PipelineReadBuffer buf(std::move(executor));

    EXPECT_EQ(buf.tryGetFileSize(), std::nullopt);
}

namespace
{

/// Source reader that returns `MMapReadBufferFromFileWithCache` over a temp
/// file. Used to drive the executor through the mmap path so we can assert it
/// doesn't trip on `set()+next()` returning `false` (mmap has no `nextImpl`).
class MMapSourceReader : public ISourceReader
{
public:
    explicit MMapSourceReader(String data_) : data(std::move(data_)) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject &) override
    {
        auto path = std::filesystem::temp_directory_path() / ("test_pipeline_mmap_" + std::to_string(file_counter++));
        {
            std::ofstream f(path, std::ios::binary);
            f.write(data.data(), data.size());
        }
        temp_files.push_back(path);
        return std::make_unique<MMapReadBufferFromFileWithCache>(cache, path.string(), /*offset=*/0, data.size());
    }

    String name() const override { return "MMapSourceReader"; }

    ~MMapSourceReader() override
    {
        for (const auto & p : temp_files)
            std::filesystem::remove(p);
    }

private:
    String data;
    MMappedFileCache cache{8};
    size_t file_counter = 0;
    std::vector<std::filesystem::path> temp_files;
};

}

TEST(PipelineReadBuffer, MMapSourceDoesNotReturnImmediateEof)
{
    /// Regression: `MMapReadBufferFromFileWithCache` inherits
    /// `supportsExternalBufferMode = true` by default. The executor's
    /// `readIntoBlock` trusts that flag, calls `set(dest, n); next();`, and the
    /// mmap class has no `nextImpl` — so `next()` returns `false` and the
    /// executor sees an immediate EOF on the very first window. After the fix,
    /// `MMapReadBufferFromFileWithCache::supportsExternalBufferMode` returns
    /// `false` and `readIntoBlock` falls through to `buf.read(dest, n)`, which
    /// memcpys out of the mapped region.
    String content(8192, 'M');
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('A' + (i % 26));

    auto source = std::make_shared<MMapSourceReader>(content);

    StoredObjects objects;
    objects.emplace_back("test", "", content.size());

    auto executor = std::make_unique<ReaderExecutor>(
        source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, 1024);

    PipelineReadBuffer buf(std::move(executor));

    String result;
    readStringUntilEOF(result, buf);
    EXPECT_EQ(result.size(), content.size());
    EXPECT_EQ(result, content);
}

TEST(PipelineReadBuffer, MMapReportsNoExternalBufferMode)
{
    /// Direct contract check: the mmap buffer must advertise that it cannot
    /// refill into a caller-supplied external buffer. Without this, any caller
    /// using `set()+next()` (notably `ReaderExecutor::readIntoBlock`) treats
    /// the first call as EOF.
    auto path = std::filesystem::temp_directory_path() / "test_pipeline_mmap_contract";
    {
        std::ofstream f(path, std::ios::binary);
        f.write("hello", 5);
    }
    MMappedFileCache cache{8};
    MMapReadBufferFromFileWithCache buf(cache, path.string(), 0, 5);
    EXPECT_FALSE(buf.supportsExternalBufferMode());
    std::filesystem::remove(path);
}
