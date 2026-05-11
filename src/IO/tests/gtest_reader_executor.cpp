#include <IO/ReaderExecutor.h>
#include <IO/ISourceReader.h>
#include <IO/ICacheProvider.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/SourceBufferLimit.h>
#include <IO/ReadSettings.h>
#include <IO/Rope.h>
#include <Disks/IO/createReadBufferFromFileBase.h>

#include <gtest/gtest.h>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <unordered_map>

using namespace DB;

namespace
{

/// In-memory source reader for testing.
class MemorySourceReader : public ISourceReader
{
public:
    explicit MemorySourceReader(std::unordered_map<String, String> data_)
        : data(std::move(data_)) {}

    size_t read(const StoredObject & object, size_t offset, size_t size, char * buffer) override
    {
        auto it = data.find(object.remote_path);
        if (it == data.end())
            return 0;
        const auto & content = it->second;
        if (offset >= content.size())
            return 0;
        size_t to_read = std::min(size, content.size() - offset);
        std::memcpy(buffer, content.data() + offset, to_read);
        return to_read;
    }

    String name() const override { return "MemorySourceReader"; }

private:
    std::unordered_map<String, String> data;
};

}

TEST(ReaderExecutor, ReadSingleObjectNoCaches)
{
    String content(1000, 'A');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj_a", content}});

    StoredObjects objects;
    objects.emplace_back("obj_a", "", 1000);

    ReaderExecutor executor(source, objects, {}, 512);

    auto rope = executor.readNextWindow();
    EXPECT_FALSE(rope.empty());
    EXPECT_EQ(rope.range().offset, 0);
    EXPECT_EQ(rope.range().size, 512);

    size_t total = 0;
    for (const auto & node : rope.getNodes())
    {
        for (size_t i = 0; i < node.size; ++i)
            EXPECT_EQ(node.data()[i], 'A');
        total += node.size;
    }
    EXPECT_EQ(total, 512);

    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().offset, 512);
    EXPECT_EQ(rope2.range().size, 488);

    auto rope3 = executor.readNextWindow();
    EXPECT_TRUE(rope3.empty());
}

TEST(ReaderExecutor, ReadMultiObject)
{
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"blob_0", String(300, 'X')},
            {"blob_1", String(200, 'Y')},
        });

    StoredObjects objects;
    objects.emplace_back("blob_0", "", 300);
    objects.emplace_back("blob_1", "", 200);

    ReaderExecutor executor(source, objects, {}, 400);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 0);
    EXPECT_EQ(rope.range().size, 400);

    size_t pos = 0;
    for (const auto & node : rope.getNodes())
    {
        for (size_t i = 0; i < node.size; ++i)
        {
            char expected = (pos + i < 300) ? 'X' : 'Y';
            EXPECT_EQ(node.data()[i], expected)
                << "at logical offset " << (pos + i);
        }
        pos += node.size;
    }
}

TEST(ReaderExecutor, Seek)
{
    String content(1000, 'B');
    content[500] = 'Z';
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 1000);

    ReaderExecutor executor(source, objects, {}, 100);

    executor.seek(500);
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 500);
    EXPECT_EQ(rope.range().size, 100);
    EXPECT_EQ(rope.getNodes()[0].data()[0], 'Z');
}

namespace
{

/// Mock cache that stores data in memory, organized by blocks.
class MockCacheHandle : public ICacheHandle
{
public:
    MockCacheHandle(
        Range range,
        std::unordered_map<size_t, String> & storage_,
        size_t block_size_)
        : storage(storage_), block_size(block_size_)
    {
        size_t start_block = range.offset / block_size;
        size_t end_block = (range.end() + block_size - 1) / block_size;

        for (size_t b = start_block; b < end_block; ++b)
        {
            size_t block_start = b * block_size;
            size_t block_end = std::min(block_start + block_size, range.end());
            block_start = std::max(block_start, range.offset);
            Range block_range{block_start, block_end - block_start};

            if (storage.contains(b))
                result.hit_ranges.push_back(block_range);
            else
                result.miss_ranges.push_back(block_range);
        }
    }

    CacheLookupResult status() const override { return result; }

    Rope get(Range range) override
    {
        size_t block = range.offset / block_size;
        const auto & data = storage.at(block);
        auto buf = std::make_shared<OwnedRopeBuffer>(data.size());
        std::memcpy(buf->data(), data.data(), data.size());

        Rope rope;
        rope.append(RopeNode{buf, 0, data.size(), block * block_size});
        return rope.slice(range);
    }

    bool put(Range range, Rope data) override
    {
        size_t block = range.offset / block_size;
        if (storage.contains(block))
            return false;

        String content;
        for (const auto & node : data.getNodes())
            content.append(node.data(), node.size);
        storage[block] = std::move(content);
        return true;
    }

private:
    std::unordered_map<size_t, String> & storage;
    size_t block_size;
    CacheLookupResult result;
};

class MockCacheProvider : public ICacheProvider
{
public:
    explicit MockCacheProvider(size_t block_size_)
        : block_size(block_size_) {}

    std::unique_ptr<ICacheHandle> lookup(CacheKey, Range range) override
    {
        return std::make_unique<MockCacheHandle>(range, storage, block_size);
    }

    String name() const override { return "MockCache"; }

    bool hasBlock(size_t block_index) const { return storage.contains(block_index) > 0; }

private:
    std::unordered_map<size_t, String> storage;
    size_t block_size;
};

}

TEST(ReaderExecutor, CacheMissPopulatesCache)
{
    String content(1024, 'C');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 1024);

    auto cache = std::make_shared<MockCacheProvider>(512);

    ReaderExecutor executor(source, objects, {cache}, 512);

    /// First read: miss, fetches from source, populates cache
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 512);
    EXPECT_TRUE(cache->hasBlock(0));

    /// Second read
    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().size, 512);
    EXPECT_TRUE(cache->hasBlock(1));
}

TEST(ReaderExecutor, CacheHitSkipsSource)
{
    String source_content(512, 'S');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", source_content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 512);

    auto cache = std::make_shared<MockCacheProvider>(512);

    /// Warm up cache
    {
        ReaderExecutor warmup(source, objects, {cache}, 512);
        warmup.readNextWindow();
    }

    /// Replace source with different content
    String alt_content(512, 'Z');
    auto alt_source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", alt_content}});

    ReaderExecutor executor(alt_source, objects, {cache}, 512);
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 512);

    /// Should have gotten 'S' from cache, not 'Z' from alt_source
    EXPECT_EQ(rope.getNodes()[0].data()[0], 'S');
}

TEST(ReaderExecutor, PrefetchTriggersOnReadNextWindow)
{
    String content(3000, 'P');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 3000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);

    ReaderExecutor executor(source, objects, {}, 1000);
    executor.setPrefetchPool(pool);

    auto rope1 = executor.readNextWindow();
    EXPECT_EQ(rope1.range().size, 1000);

    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().offset, 1000);
    EXPECT_EQ(rope2.range().size, 1000);

    auto rope3 = executor.readNextWindow();
    EXPECT_EQ(rope3.range().offset, 2000);
    EXPECT_EQ(rope3.range().size, 1000);

    auto rope4 = executor.readNextWindow();
    EXPECT_TRUE(rope4.empty());
}

TEST(ReaderExecutor, SeekDiscardsPrefetch)
{
    String content(2000, 'Q');
    content[1500] = 'Z';
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);

    ReaderExecutor executor(source, objects, {}, 500);
    executor.setPrefetchPool(pool);

    auto rope1 = executor.readNextWindow();
    EXPECT_EQ(rope1.range().offset, 0);

    executor.seek(1500);
    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().offset, 1500);
    EXPECT_EQ(rope2.getNodes()[0].data()[0], 'Z');
}

TEST(ReaderExecutor, MergeRangesNoGap)
{
    /// Adjacent ranges — should merge into one
    std::vector<Range> ranges = {{0, 100}, {100, 100}, {200, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 50);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 300);
}

TEST(ReaderExecutor, MergeRangesSmallGap)
{
    /// Small gap (10 bytes) < min_gap (100) — merge
    std::vector<Range> ranges = {{0, 100}, {110, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 100);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 210);
}

TEST(ReaderExecutor, MergeRangesLargeGap)
{
    /// Large gap (500 bytes) > min_gap (100) — don't merge
    std::vector<Range> ranges = {{0, 100}, {600, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 100);
    ASSERT_EQ(merged.size(), 2);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 100);
    EXPECT_EQ(merged[1].offset, 600);
    EXPECT_EQ(merged[1].size, 100);
}

TEST(ReaderExecutor, MergeRangesMixed)
{
    /// Three ranges: first two close, third far away
    std::vector<Range> ranges = {{0, 100}, {120, 100}, {1000, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 50);
    ASSERT_EQ(merged.size(), 2);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 220);
    EXPECT_EQ(merged[1].offset, 1000);
    EXPECT_EQ(merged[1].size, 100);
}

TEST(ReaderExecutor, MergeRangesZeroMinGap)
{
    /// min_gap=0 — no merging
    std::vector<Range> ranges = {{0, 100}, {100, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 0);
    ASSERT_EQ(merged.size(), 2);
}

namespace
{

/// Source reader that tracks open() and read() call counts.
/// open() returns a ReadBufferFromMemory-like wrapper over the in-memory data.
class CountingSourceReader : public ISourceReader
{
public:
    explicit CountingSourceReader(std::unordered_map<String, String> data_)
        : data(std::move(data_)) {}

    size_t read(const StoredObject & object, size_t offset, size_t size, char * buffer) override
    {
        ++stateless_read_count;
        auto it = data.find(object.remote_path);
        if (it == data.end())
            return 0;
        const auto & content = it->second;
        if (offset >= content.size())
            return 0;
        size_t to_read = std::min(size, content.size() - offset);
        std::memcpy(buffer, content.data() + offset, to_read);
        return to_read;
    }

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override
    {
        ++open_count;
        auto it = data.find(object.remote_path);
        if (it == data.end())
            return nullptr;
        /// Return a ReadBufferFromFile-like object backed by a temp file.
        /// For simplicity, we write content to a temp file and open it.
        auto path = std::filesystem::temp_directory_path() / ("test_counting_source_" + std::to_string(open_count));
        {
            std::ofstream f(path, std::ios::binary);
            f.write(it->second.data(), it->second.size());
        }
        temp_files.push_back(path);
        return createReadBufferFromFileBase(path.string(), ReadSettings{});
    }

    String name() const override { return "CountingSourceReader"; }

    std::atomic<size_t> stateless_read_count{0};
    std::atomic<size_t> open_count{0};

    ~CountingSourceReader() override
    {
        for (const auto & p : temp_files)
            std::filesystem::remove(p);
    }

private:
    std::unordered_map<String, String> data;
    std::vector<std::filesystem::path> temp_files;
};

}

TEST(ReaderExecutor, LiveBufferReusesConnection)
{
    /// 2000 bytes, window=500 → 4 sequential readNextWindow calls.
    /// With live buffer: open() called once, then reused for all 4 reads.
    /// Without: read() called 4 times, open() never called.
    String content(2000, 'Q');
    auto source = std::make_shared<CountingSourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 2000);

    auto limit = std::make_shared<SourceBufferLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    String result;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.empty())
            break;
        for (const auto & node : rope.getNodes())
            result.append(node.data(), node.size);
    }

    EXPECT_EQ(result.size(), 2000);
    EXPECT_EQ(result, content);

    /// open() called once (first read opens live buffer).
    EXPECT_EQ(source->open_count.load(), 1);
    /// stateless read() should NOT have been called — all reads went through live buffer.
    EXPECT_EQ(source->stateless_read_count.load(), 0);
}

TEST(ReaderExecutor, LiveBufferFallbackWhenFull)
{
    /// Semaphore with 0 capacity — live buffers disabled.
    /// All reads go through stateless read().
    String content(1000, 'R');
    auto source = std::make_shared<CountingSourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 1000);

    auto limit = std::make_shared<SourceBufferLimit>(0);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    String result;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.empty())
            break;
        for (const auto & node : rope.getNodes())
            result.append(node.data(), node.size);
    }

    EXPECT_EQ(result.size(), 1000);
    EXPECT_EQ(result, content);

    /// open() never called — no slots available.
    EXPECT_EQ(source->open_count.load(), 0);
    /// All reads went through stateless read().
    EXPECT_GE(source->stateless_read_count.load(), 2);
}

TEST(ReaderExecutor, LiveBufferClosedOnSeek)
{
    /// Sequential read opens live buffer, seek closes it and opens a new one.
    String content(2000, 'S');
    content[1000] = 'T';
    auto source = std::make_shared<CountingSourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 2000);

    auto limit = std::make_shared<SourceBufferLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Read first window (opens live buffer).
    auto rope1 = executor.readNextWindow();
    EXPECT_EQ(rope1.range().size, 500);
    EXPECT_EQ(source->open_count.load(), 1);

    /// Seek backwards (closes live buffer, opens new one on next read).
    executor.seek(1000);
    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().offset, 1000);
    EXPECT_EQ(rope2.getNodes()[0].data()[0], 'T');

    /// open() called twice: once for initial read, once after seek.
    EXPECT_EQ(source->open_count.load(), 2);
    EXPECT_EQ(source->stateless_read_count.load(), 0);
}
