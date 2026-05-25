#include <IO/PipelineReadBuffer.h>
#include <IO/ReaderExecutor.h>
#include <IO/ISourceReader.h>
#include <IO/ReadSettings.h>
#include <IO/ReadHelpers.h>
#include <Disks/IO/createReadBufferFromFileBase.h>

#include <gtest/gtest.h>
#include <cstring>
#include <filesystem>
#include <fstream>

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
        source, objects, std::vector<std::shared_ptr<ICacheProvider>>{}, 20);

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
        source, objects, std::vector<std::shared_ptr<ICacheProvider>>{}, 100);

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
        source, objects, std::vector<std::shared_ptr<ICacheProvider>>{}, 8);

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
        source, objects, std::vector<std::shared_ptr<ICacheProvider>>{}, 30);

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
        source, objects, std::vector<std::shared_ptr<ICacheProvider>>{}, 100);

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
        source, objects, std::vector<std::shared_ptr<ICacheProvider>>{}, 100);

    PipelineReadBuffer buf(std::move(executor));

    EXPECT_EQ(buf.tryGetFileSize(), std::nullopt);
}
