#include <IO/PipelineReadBuffer.h>
#include <IO/ReaderExecutor.h>
#include <IO/IFileBasedSourceReader.h>
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
class MemorySourceReader : public IFileBasedSourceReader
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

/// Counts `open` calls on a wrapped source - one per source request on the
/// one-shot read path - so a test can pin that an operation did NOT refetch.
class CountingSourceReader : public IFileBasedSourceReader
{
public:
    explicit CountingSourceReader(std::shared_ptr<IFileBasedSourceReader> inner_) : inner(std::move(inner_)) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override
    {
        ++opens;
        return inner->open(object);
    }

    String name() const override { return inner->name(); }

    size_t opens = 0;

private:
    std::shared_ptr<IFileBasedSourceReader> inner;
};

}

TEST(PipelineReadBuffer, ReadAll)
{
    String content = "Hello, ReaderExecutor! This is a test of the pipeline read buffer.";
    auto source = std::make_shared<MemorySourceReader>(content);

    StoredObjects objects;
    objects.emplace_back("test", "", content.size());

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 20;
    auto executor = std::make_unique<ReaderExecutor>(source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, executor_options);

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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 100;
    auto executor = std::make_unique<ReaderExecutor>(source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, executor_options);

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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 8;
    auto executor = std::make_unique<ReaderExecutor>(source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, executor_options);

    PipelineReadBuffer buf(std::move(executor));

    buf.seek(10, SEEK_SET);
    String result;
    readStringUntilEOF(result, buf);
    EXPECT_EQ(result, "ABCDEF");
}

TEST(PipelineReadBuffer, InBufferSeekIsServedWithoutRefetch)
{
    /// Regression for `seek` absorbing in-buffer seeks. A seek whose target is already
    /// inside the working buffer must be served by repositioning `pos`, not by re-seeking
    /// the executor (which drops the window and refetches). The compressed reader does
    /// exactly this - over-read a block, then seek back to a mark inside it - and a
    /// refetch there both wastes a request and, because a held source connection is
    /// forward-only, breaks long-connection reuse.
    const size_t size = 64 * 1024;
    String content(size, 0);
    for (size_t i = 0; i < size; ++i)
        content[i] = static_cast<char>('A' + (i % 26));
    auto counting = std::make_shared<CountingSourceReader>(std::make_shared<MemorySourceReader>(content));

    StoredObjects objects;
    objects.emplace_back("test", "", size);
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 16 * 1024;
    auto executor = std::make_unique<ReaderExecutor>(
        counting, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, executor_options);
    PipelineReadBuffer buf(std::move(executor));

    /// Fetch one window [0, 16K) and partly consume it: one source open.
    std::vector<char> head(8 * 1024);
    buf.readStrict(head.data(), head.size());
    ASSERT_EQ(counting->opens, 1u);

    /// Seek back to an offset still inside the window and read: served by repositioning.
    buf.seek(2048, SEEK_SET);
    char c = 0;
    buf.readStrict(&c, 1);
    EXPECT_EQ(c, content[2048]);
    EXPECT_EQ(counting->opens, 1u);
}

TEST(PipelineReadBuffer, SaveUpToPositionOnDrainedBufferIsSafe)
{
    /// Regression for a UBSan abort: after EOF the buffer detaches its base pointers
    /// (`detachBuffer`), so `position()` is nullptr. The format segmentation engines
    /// (`fileSegmentationEngineCSVImpl` on an `s3(...)` CSV read via parallel parsing)
    /// then call `loadAtPosition` -> `saveUpToPosition` on it with previously saved
    /// bytes in `memory`, and the zero-size tail copy must not pass the null pointer
    /// to memcpy (undefined behavior even with size 0).
    String content = "abc";
    auto source = std::make_shared<MemorySourceReader>(content);
    StoredObjects objects;
    objects.emplace_back("test", "", content.size());
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 8;
    auto executor = std::make_unique<ReaderExecutor>(
        source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, executor_options);
    PipelineReadBuffer buf(std::move(executor));

    String all;
    readStringUntilEOF(all, buf);
    EXPECT_EQ(all, content);
    EXPECT_TRUE(buf.eof());

    Memory<> memory(4);                     /// old_bytes > 0: the branch that reached memcpy
    char * current = buf.position();
    EXPECT_FALSE(loadAtPosition(buf, memory, current));
    EXPECT_EQ(current, buf.position());
}

TEST(PipelineReadBuffer, GetPosition)
{
    String content(100, 'X');
    auto source = std::make_shared<MemorySourceReader>(content);

    StoredObjects objects;
    objects.emplace_back("test", "", 100);

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 30;
    auto executor = std::make_unique<ReaderExecutor>(source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, executor_options);

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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 100;
    auto executor = std::make_unique<ReaderExecutor>(source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, executor_options);

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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 100;
    auto executor = std::make_unique<ReaderExecutor>(source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, executor_options);

    PipelineReadBuffer buf(std::move(executor));

    EXPECT_EQ(buf.tryGetFileSize(), std::nullopt);
}

namespace
{

/// Source reader that returns `MMapReadBufferFromFileWithCache` over a temp
/// file. Used to drive the executor through the mmap path so we can assert it
/// doesn't trip on `set()+next()` returning `false` (mmap has no `nextImpl`).
class MMapSourceReader : public IFileBasedSourceReader
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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 1024;
    auto executor = std::make_unique<ReaderExecutor>(source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, executor_options);

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
    /// using `set()+next()` (notably `readIntoBlock`) treats
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

TEST(PipelineReadBuffer, HoldConsumedServesBackwardSeekFromMemory)
{
    /// The merge read pattern: consume forward, hop back into just-consumed bytes.
    /// With `hold_consumed` the chain retains a trailing window of consumed nodes,
    /// so the hop re-serves from memory - no executor seek, no source request; a
    /// hop beyond the window falls back to the executor and refetches.
    const size_t size = 64 * 1024;
    String content(size, 0);
    for (size_t i = 0; i < size; ++i)
        content[i] = static_cast<char>('A' + (i % 26));
    auto counting = std::make_shared<CountingSourceReader>(std::make_shared<MemorySourceReader>(content));

    StoredObjects objects;
    objects.emplace_back("test", "", size);
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 8 * 1024;
    executor_options.block_size = 4 * 1024;
    auto executor = std::make_unique<ReaderExecutor>(
        counting, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, executor_options);
    PipelineReadBuffer buf(std::move(executor), /*hold_consumed_=*/16 * 1024);

    std::vector<char> data(24 * 1024);
    buf.readStrict(data.data(), data.size());
    EXPECT_TRUE(std::memcmp(data.data(), content.data(), data.size()) == 0);
    const size_t opens_before = counting->opens;

    /// Hop back inside the hold window.
    buf.seek(8 * 1024, SEEK_SET);
    char c = 0;
    buf.readStrict(&c, 1);
    EXPECT_EQ(c, content[8 * 1024]);
    EXPECT_EQ(counting->opens, opens_before) << "a hop inside the hold window must not touch the source";

    /// Re-consume to the end: byte-exact after the rewind.
    String rest;
    readStringUntilEOF(rest, buf);
    EXPECT_EQ(String(1, c) + rest, content.substr(8 * 1024));

    /// A hop beyond the hold window falls back to the executor and refetches.
    buf.seek(0, SEEK_SET);
    String all;
    readStringUntilEOF(all, buf);
    EXPECT_EQ(all, content);
    EXPECT_GT(counting->opens, opens_before);
}

TEST(PipelineReadBuffer, HoldConsumedClearedByFarSeek)
{
    /// A far (executor-delegated) seek resets the stream; the retention store
    /// resets with it - a later backward hop into pre-jump territory must
    /// refetch rather than serve coverage across the jump hole.
    const size_t size = 64 * 1024;
    String content(size, 0);
    for (size_t i = 0; i < size; ++i)
        content[i] = static_cast<char>('a' + (i % 26));
    auto counting = std::make_shared<CountingSourceReader>(std::make_shared<MemorySourceReader>(content));

    StoredObjects objects;
    objects.emplace_back("test", "", size);
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 8 * 1024;
    executor_options.block_size = 4 * 1024;
    /// Below both hop distances, so the executor RESTARTs (dropping its bank)
    /// rather than serving the back hop from plan-reuse retention - the store
    /// clearing, not the executor's bank, is what this test observes.
    executor_options.min_bytes_for_seek = 4 * 1024;
    auto executor = std::make_unique<ReaderExecutor>(
        counting, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, executor_options);
    PipelineReadBuffer buf(std::move(executor), /*hold_consumed_=*/16 * 1024);

    std::vector<char> head(16 * 1024);
    buf.readStrict(head.data(), head.size());

    buf.seek(48 * 1024, SEEK_SET);   /// far forward: executor path, store cleared
    char c = 0;
    buf.readStrict(&c, 1);
    EXPECT_EQ(c, content[48 * 1024]);

    const size_t opens_before = counting->opens;
    buf.seek(4 * 1024, SEEK_SET);    /// pre-jump territory: must refetch
    buf.readStrict(&c, 1);
    EXPECT_EQ(c, content[4 * 1024]);
    EXPECT_GT(counting->opens, opens_before);
}
