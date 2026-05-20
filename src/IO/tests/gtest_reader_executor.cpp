#include <IO/ReaderExecutor.h>
#include <IO/ISourceReader.h>
#include <IO/ICacheProvider.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/SourceBufferLimit.h>
#include <IO/ReadSettings.h>
#include <IO/Rope.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/IO/createReadBufferFromFileBase.h>

#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <unordered_map>

namespace DB::ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
}

using namespace DB;

namespace
{

/// In-memory source reader for testing.
/// open() materializes the requested object into a temp file and returns a
/// file-backed ReadBufferFromFileBase. Temp files are cleaned up on destruction.
class MemorySourceReader : public ISourceReader
{
public:
    explicit MemorySourceReader(std::unordered_map<String, String> data_)
        : data(std::move(data_)) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override
    {
        auto it = data.find(object.remote_path);
        if (it == data.end())
            return nullptr;
        auto path = std::filesystem::temp_directory_path() / ("test_memory_source_" + std::to_string(file_counter++));
        {
            std::ofstream f(path, std::ios::binary);
            f.write(it->second.data(), it->second.size());
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
    std::unordered_map<String, String> data;
    size_t file_counter = 0;
    std::vector<std::filesystem::path> temp_files;
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
        ByteRange range,
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
            ByteRange block_range{block_start, block_end - block_start};

            if (storage.contains(b))
                result.hit_ranges.push_back(block_range);
            else
                result.miss_ranges.push_back(block_range);
        }
    }

    CacheLookupResult status() const override { return result; }

    Rope get(ByteRange range) override
    {
        size_t block = range.offset / block_size;
        const auto & data = storage.at(block);
        auto buf = std::make_shared<OwnedRopeBuffer>(data.size());
        std::memcpy(buf->data(), data.data(), data.size());

        Rope rope;
        rope.append(RopeNode{buf, 0, data.size(), block * block_size});
        return rope.slice(range);
    }

    bool put(ByteRange range, Rope data) override
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

    std::unique_ptr<ICacheHandle> lookup(CacheKey, ByteRange range) override
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

TEST(ReaderExecutor, SeekInsidePrefetchedWindow)
{
    /// After the first window read, a prefetch is in flight for [500, 1000).
    /// Seeking to 750 (inside the prefetched range) must:
    ///   - leave executor.getPosition() == 750 (not 500), and
    ///   - cause the next readNextWindow to return a rope starting at logical 750.
    /// Pre-fix: position was rewound to prefetch_range.offset and the returned
    /// rope still started at 500, so direct callers got bytes from the wrong offset.

    String content(2000, 0);
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('A' + (i % 26));

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    ReaderExecutor executor(source, objects, {}, /*window_size=*/500);
    executor.setPrefetchPool(pool);

    auto rope1 = executor.readNextWindow();
    EXPECT_EQ(rope1.range().offset, 0u);
    EXPECT_EQ(rope1.range().size, 500u);

    executor.seek(750);
    EXPECT_EQ(executor.getPosition(), 750u);

    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().offset, 750u);
    EXPECT_EQ(rope2.range().size, 250u);  /// [750, 1000)
    ASSERT_FALSE(rope2.getNodes().empty());
    EXPECT_EQ(rope2.getNodes().front().data()[0], content[750]);
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
    std::vector<ByteRange> ranges = {{0, 100}, {100, 100}, {200, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 50);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 300);
}

TEST(ReaderExecutor, MergeRangesSmallGap)
{
    /// Small gap (10 bytes) < min_gap (100) — merge
    std::vector<ByteRange> ranges = {{0, 100}, {110, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 100);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 210);
}

TEST(ReaderExecutor, MergeRangesLargeGap)
{
    /// Large gap (500 bytes) > min_gap (100) — don't merge
    std::vector<ByteRange> ranges = {{0, 100}, {600, 100}};
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
    std::vector<ByteRange> ranges = {{0, 100}, {120, 100}, {1000, 100}};
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
    std::vector<ByteRange> ranges = {{0, 100}, {100, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 0);
    ASSERT_EQ(merged.size(), 2);
}

#if USE_SSL
TEST(ReaderExecutor, TotalSizeSaturatesOnUndersizedEncryptedFile)
{
    /// File is 10 bytes; two encryption layers expect 128 bytes of headers
    /// (offset_map.totalSize() < data_start_offset). Pre-fix: unsigned
    /// subtraction underflowed to ~SIZE_MAX, making the executor think the
    /// logical file was enormous. Post-fix: totalSize() saturates to 0;
    /// the next read (or initDecryption) will throw CANNOT_READ_ALL_DATA.
    String content(10, 'A');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/512);
    executor.addDecryptionLayer("layer0", 64, [](UInt128, const String &) { return String{}; });
    executor.addDecryptionLayer("layer1", 64, [](UInt128, const String &) { return String{}; });

    EXPECT_EQ(executor.totalSize(), 0u);
}
#endif

TEST(ReaderExecutor, MergeRangesOverlapping)
{
    /// Overlapping ranges merge into their union regardless of min_gap > 0.
    /// Without the saturating-subtraction fix, gap = sorted[i].offset - prev.end()
    /// underflows on overlap and the merge branch is skipped, leaving overlapping
    /// ranges in the output.
    std::vector<ByteRange> ranges = {{0, 100}, {50, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 10);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0u);
    EXPECT_EQ(merged[0].size, 150u);  /// [0, 100) ∪ [50, 150) = [0, 150)
}

TEST(ReaderExecutor, MergeRangesContained)
{
    /// One range fully contained in another. The union is the wider range;
    /// without the fix the underflow path emits both ranges.
    std::vector<ByteRange> ranges = {{0, 200}, {50, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 10);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0u);
    EXPECT_EQ(merged[0].size, 200u);  /// [0, 200) ∪ [50, 150) = [0, 200)
}

TEST(ReaderExecutor, ShortReadThrows)
{
    /// offset_map sees obj_a as 1000 bytes but the source has only 300.
    /// readFromSource short-reads pr1, which is non-terminal (obj_b follows).
    /// Pre-fix: obj_b's data would silently land at a shifted logical offset.
    /// Fix: throw CANNOT_READ_ALL_DATA with a clear message.
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"obj_a", String(300, 'A')},
            {"obj_b", String(500, 'B')},
        });

    StoredObjects objects;
    objects.emplace_back("obj_a", "", 1000);  /// claims 1000 but actual is 300
    objects.emplace_back("obj_b", "", 500);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/1500);
    EXPECT_THROW(executor.readNextWindow(), Exception);
}

TEST(ReaderExecutor, MergeAcrossCacheHitDropsCachedNode)
{
    /// When mergeRanges combines two miss ranges across a cached hit, the
    /// source fetches the merged range — which now covers the cached block.
    /// The cache hit must be dropped from the result, otherwise it appears
    /// alongside source data for the same logical offsets (duplicate coverage).
    ///
    /// Layout: cache block [100, 200), miss ranges [0, 100) and [200, 300)
    /// merged into [0, 300) with a large min_bytes_for_seek.

    StoredObjects objects;
    objects.emplace_back("obj", "", 300);

    auto cache = std::make_shared<MockCacheProvider>(100);

    /// Warm cache block 1 (offsets [100, 200)). Content is irrelevant —
    /// the bug is about node count, not content.
    {
        String warm_content(300, 'W');
        auto warm_source = std::make_shared<MemorySourceReader>(
            std::unordered_map<String, String>{{"obj", warm_content}});
        ReaderExecutor warmup(warm_source, objects, {cache}, /*window_size=*/100);
        warmup.seek(100);
        warmup.readNextWindow();
        ASSERT_TRUE(cache->hasBlock(1));
    }

    /// Real read: window covers [0, 300); min_bytes_for_seek=8 MiB so the
    /// two miss ranges around the cached block get merged into [0, 300).
    String real_content(300, 'S');
    auto real_source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", real_content}});

    ReaderExecutor executor(
        real_source, objects, {cache},
        /*window_size=*/300,
        /*min_bytes_for_seek=*/8 * 1024 * 1024);
    auto rope = executor.readNextWindow();

    EXPECT_EQ(rope.range().offset, 0u);
    EXPECT_EQ(rope.range().size, 300u);
    /// Without the fix, the surviving cache hit at [100, 200) adds 100 bytes
    /// of duplicate coverage, so totalBytes() reports 400 instead of 300.
    EXPECT_EQ(rope.totalBytes(), 300u);
}

namespace ProfileEvents
{
    extern const Event LiveSourceBufferCreated;
    extern const Event LiveSourceBufferHits;
    extern const Event LiveSourceBufferFallbacks;
    extern const Event LiveSourceBufferBytes;
}

namespace
{

/// Succeeds on the first open() and throws on every subsequent one.
/// Used to drive an asynchronous failure into the prefetch lambda so the
/// future held by ReaderExecutor ends up holding an exception when the
/// destructor calls future.get() via discardPrefetch.
class ThrowOnSecondOpenSourceReader : public ISourceReader
{
public:
    explicit ThrowOnSecondOpenSourceReader(String data_)
        : data(std::move(data_)) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject &) override
    {
        if (call_count.fetch_add(1) > 0)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
                "ThrowOnSecondOpenSourceReader: synthetic failure (would abort in debug if LOGICAL_ERROR)");

        auto path = std::filesystem::temp_directory_path() / ("test_throw_on_second_open_" + std::to_string(file_counter++));
        {
            std::ofstream f(path, std::ios::binary);
            f.write(data.data(), data.size());
        }
        temp_files.push_back(path);
        return createReadBufferFromFileBase(path.string(), ReadSettings{});
    }

    String name() const override { return "ThrowOnSecondOpenSourceReader"; }

    ~ThrowOnSecondOpenSourceReader() override
    {
        for (const auto & p : temp_files)
            std::filesystem::remove(p);
    }

private:
    String data;
    std::atomic<size_t> call_count{0};
    size_t file_counter = 0;
    std::vector<std::filesystem::path> temp_files;
};

/// RAII helper: creates a ThreadGroup with its own ProfileEvents counters,
/// attaches the current thread to it, detaches in destructor.
/// Lets us read per-test ProfileEvents without interference from other tests.
struct TestThreadGroup
{
    /// Create ThreadStatus if none exists (debug build has one, ASan may not).
    std::optional<DB::ThreadStatus> thread_status_holder{current_thread ? std::nullopt : std::optional<DB::ThreadStatus>(std::in_place)};
    DB::ThreadGroupPtr thread_group = DB::ThreadGroup::createForQuery(getContext().context);
    DB::ThreadGroupSwitcher switcher{thread_group, ThreadName::UNKNOWN};

    ProfileEvents::Count get(ProfileEvents::Event event) const
    {
        return thread_group->performance_counters[event].load(std::memory_order_relaxed);
    }
};

}

TEST(ReaderExecutor, DestructorTolerantOfThrowingPrefetch)
{
    /// Pre-fix: ~ReaderExecutor calls discardPrefetch → future.get(), which
    /// re-throws the prefetch lambda's exception. Because ~ReaderExecutor is
    /// implicitly noexcept, this terminates the process.
    /// Post-fix: discardPrefetch catches and logs, scope exit is clean.

    auto source = std::make_shared<ThrowOnSecondOpenSourceReader>(String(2000, 'A'));
    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);

    {
        ReaderExecutor executor(source, objects, {}, /*window_size=*/500);
        executor.setPrefetchPool(pool);

        /// First sync read consumes the 1st open() and primes maybeTriggerPrefetch,
        /// which submits a task whose 2nd open() will throw on the pool thread.
        auto rope = executor.readNextWindow();
        ASSERT_FALSE(rope.empty());

        /// executor's destructor must drain the throwing future without terminating.
    }
    SUCCEED();
}

TEST(ReaderExecutor, LiveBufferReusesConnection)
{
    TestThreadGroup tg;

    /// 2000 bytes, window=500 → 4 sequential readNextWindow calls.
    String content(2000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
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
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferCreated), 1);   /// Opened once.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferHits), 3);      /// Reused for 3 subsequent reads.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferFallbacks), 0); /// Never fell back.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferBytes), 2000);  /// All bytes through live buffer.
}

TEST(ReaderExecutor, LiveBufferFallbackWhenFull)
{
    TestThreadGroup tg;

    /// Semaphore with 0 capacity — all reads go through stateless path.
    String content(1000, 'R');
    auto source = std::make_shared<MemorySourceReader>(
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
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferCreated), 0);   /// Never opened — no slots.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferHits), 0);      /// No live buffer to hit.
    EXPECT_GE(tg.get(ProfileEvents::LiveSourceBufferFallbacks), 2); /// All reads fell back.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferBytes), 0);     /// No bytes through live buffer.
}

TEST(ReaderExecutor, LiveBufferClosedOnSeek)
{
    TestThreadGroup tg;

    /// Sequential read opens live buffer, seek closes it and opens a new one.
    String content(2000, 'S');
    content[1000] = 'T';
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 2000);

    auto limit = std::make_shared<SourceBufferLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Read first window (opens live buffer).
    auto rope1 = executor.readNextWindow();
    EXPECT_EQ(rope1.range().size, 500);

    /// Seek (closes live buffer, next read opens a new one).
    executor.seek(1000);
    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().offset, 1000);
    EXPECT_EQ(rope2.getNodes()[0].data()[0], 'T');

    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferCreated), 2);   /// Opened twice: initial + after seek.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferHits), 0);      /// Seek broke the chain.
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferFallbacks), 0); /// Slots were available.
}
