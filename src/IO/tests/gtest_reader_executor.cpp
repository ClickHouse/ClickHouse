#include <IO/ReaderExecutor.h>
#include <IO/ISourceReader.h>
#include <IO/ICacheProvider.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/SourceBufferLimit.h>
#include <IO/ReadSettings.h>
#include <IO/Rope.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
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
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

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

    size_t put(ByteRange range, Rope data) override
    {
        size_t block = range.offset / block_size;
        if (storage.contains(block))
            return 0;

        String content;
        for (const auto & node : data.getNodes())
            content.append(node.data(), node.size);
        size_t bytes = content.size();
        storage[block] = std::move(content);
        return bytes;
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
    ///   - cause the next readNextWindow to return a rope starting at logical 750
    ///     with content matching the source at offset 750.
    ///
    /// The returned size depends on which branch the executor takes:
    ///   - Wait branch (worker already running): rope is the prefetched [500, 1000)
    ///     sliced to [750, 1000), size 250.
    ///   - Cancel branch (worker hadn't started): a fresh window from position 750
    ///     of size min(window_size, file_size - 750), so the rope spans [750, 1250).
    /// Both are valid outcomes and the test accepts either.

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
    EXPECT_TRUE(rope2.range().size == 250u || rope2.range().size == 500u)
        << "got size " << rope2.range().size;
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

TEST(ReaderExecutor, SeekTriggersPrefetch)
{
    /// After `seek` lands outside the previously-prefetched range, the old
    /// prefetch is discarded AND a new one for the new position must be
    /// queued — without that, the next `readNextWindow` would pay full
    /// source-read latency synchronously.
    String content(4000, 'S');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 4000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500);
    executor.setPrefetchPool(pool);

    /// Before the first readNextWindow nothing has been prefetched yet.
    EXPECT_FALSE(executor.hasInflightPrefetch());

    /// Seek to a position outside any existing prefetch range. Must queue
    /// a fresh prefetch for the new position right away.
    executor.seek(2500);
    EXPECT_TRUE(executor.hasInflightPrefetch())
        << "seek must trigger a new prefetch when prefetch_pool is set";

    /// And the prefetched data is the one we actually consume next.
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 2500u);
    EXPECT_EQ(rope.range().size, 500u);
}

TEST(ReaderExecutor, SeekWithoutPoolDoesNotCrash)
{
    /// Transient `readBigAt` executors have no `prefetch_pool` — the
    /// post-seek `maybeTriggerPrefetch` call must be a clean no-op there.
    String content(1000, 'T');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 1000);

    ReaderExecutor executor(source, objects, {}, 200);
    /// No setPrefetchPool — leave prefetch_pool null.

    executor.seek(400);
    EXPECT_FALSE(executor.hasInflightPrefetch());

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 400u);
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

TEST(ReaderExecutor, DestructorAfterThrownReadNextWindowDoesNotSegfault)
{
    /// Reproduces the production segfault observed in stress tests:
    ///   1. First `readNextWindow` succeeds and queues a prefetch.
    ///   2. Second `readNextWindow` waits on the prefetch via `future.get()`,
    ///      which re-throws the worker's exception. `future.get()` detaches
    ///      the future's `__state_` as its very first step, so afterwards the
    ///      future is unusable.
    ///   3. Pre-fix: the throw skipped `prefetch_handle.reset()`, leaving the
    ///      executor with a non-null `prefetch_handle` whose future has already
    ///      been consumed. `~ReaderExecutor → discardPrefetch → get()` then
    ///      segfaulted dereferencing a null `__assoc_state*` at offset 0x28
    ///      (the `__mut_` slot) inside `pthread_mutex_lock`.
    ///   4. Post-fix: `readNextWindow` takes local ownership of the handle and
    ///      clears `prefetch_handle` BEFORE calling `get`,
    ///      so the destructor never re-touches a half-consumed future.

    auto source = std::make_shared<ThrowOnSecondOpenSourceReader>(String(2000, 'B'));
    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);

    {
        ReaderExecutor executor(source, objects, {}, /*window_size=*/500);
        executor.setPrefetchPool(pool);

        /// 1st readNextWindow: synchronous open (success) + queues a prefetch
        /// whose worker will call `open()` again and throw.
        auto rope1 = executor.readNextWindow();
        ASSERT_FALSE(rope1.empty());

        /// 2nd readNextWindow: waits on the prefetch, which re-throws the
        /// worker's exception. This is the path that previously left the
        /// executor in a poisoned state.
        EXPECT_ANY_THROW(executor.readNextWindow());

        /// Now let the executor go out of scope. Pre-fix this segfaulted in
        /// `discardPrefetch`. Post-fix the destructor finishes cleanly because
        /// `prefetch_handle` was already cleared inside `readNextWindow` before
        /// the throw.
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

TEST(ReaderExecutor, LiveBufferReleasedAtEof)
{
    /// Once the caller reads to EOF, the per-stream `SourceBufferLimit`
    /// slot (and the associated open connection) must be returned even if
    /// the `ReaderExecutor` itself is not yet destroyed. Otherwise a
    /// finished-but-still-held reader pins capacity from the global
    /// budget.
    String content(2000, 'E');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 2000);

    auto limit = std::make_shared<SourceBufferLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Read every window — last call returns an empty rope (EOF).
    auto r1 = executor.readNextWindow();
    EXPECT_EQ(r1.range().size, 500u);
    EXPECT_EQ(limit->getActive().size(), 1u) << "expected one open slot during streaming";

    auto r2 = executor.readNextWindow();
    auto r3 = executor.readNextWindow();
    auto r4 = executor.readNextWindow();
    EXPECT_EQ(r2.range().size, 500u);
    EXPECT_EQ(r3.range().size, 500u);
    EXPECT_EQ(r4.range().size, 500u);
    EXPECT_EQ(limit->getActive().size(), 1u);

    /// EOF — slot must be released by readNextWindow itself.
    auto r5 = executor.readNextWindow();
    EXPECT_TRUE(r5.empty());
    EXPECT_EQ(limit->getActive().size(), 0u)
        << "live buffer slot must be released when EOF is reached";

    /// Idempotent: calling readNextWindow again at EOF is still EOF and
    /// keeps the slot count at zero.
    auto r6 = executor.readNextWindow();
    EXPECT_TRUE(r6.empty());
    EXPECT_EQ(limit->getActive().size(), 0u);
}

TEST(ReaderExecutor, LiveBufferReacquiredAfterSeekBackFromEof)
{
    /// After EOF released the slot, a backward seek and re-read must
    /// re-open the connection and re-acquire a slot.
    String content(1500, 'R');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 1500);

    auto limit = std::make_shared<SourceBufferLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Read to EOF.
    while (!executor.readNextWindow().empty()) {}
    EXPECT_EQ(limit->getActive().size(), 0u);

    /// Seek back to the start and read again — slot must come back.
    executor.seek(0);
    auto r = executor.readNextWindow();
    EXPECT_EQ(r.range().offset, 0u);
    EXPECT_EQ(r.range().size, 500u);
    EXPECT_EQ(limit->getActive().size(), 1u)
        << "slot must be re-acquired after backward seek + read";
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

namespace
{

/// Mock cache that reports hits/misses at FULL block granularity, matching the
/// production behaviour of PageCacheHandle and DiskCacheHandle. The existing
/// MockCacheHandle clips ranges to the request which hides the cache-vs-cache
/// overlap problem these tests target.
///
/// `put` records its arguments in `put_log` and refuses non-disjoint ropes
/// (totalBytes != range.size) — production caches assume disjoint coverage,
/// and DiskCacheHandle::put's flat_buf memcpy would overflow otherwise.
class WideGranularityMockCacheHandle : public ICacheHandle
{
public:
    WideGranularityMockCacheHandle(
        ByteRange requested_,
        std::unordered_map<size_t, String> & storage_,
        std::vector<std::pair<ByteRange, size_t>> & put_log_,
        size_t block_size_)
        : storage(storage_), put_log(put_log_), block_size(block_size_)
    {
        size_t start_block = requested_.offset / block_size;
        size_t end_block = (requested_.end() + block_size - 1) / block_size;
        for (size_t b = start_block; b < end_block; ++b)
        {
            /// FULL block, NOT clipped to requested_.
            ByteRange block_range{b * block_size, block_size};
            if (storage.contains(b))
                result.hit_ranges.push_back(block_range);
            else
                result.miss_ranges.push_back(block_range);
        }
    }

    CacheLookupResult status() const override { return result; }

    Rope get(ByteRange range) override
    {
        Rope rope;
        size_t start_block = range.offset / block_size;
        size_t end_block = (range.end() + block_size - 1) / block_size;
        for (size_t b = start_block; b < end_block; ++b)
        {
            auto it = storage.find(b);
            if (it == storage.end())
                continue;
            const auto & data = it->second;
            auto buf = std::make_shared<OwnedRopeBuffer>(data.size());
            std::memcpy(buf->data(), data.data(), data.size());
            rope.append(RopeNode{buf, 0, data.size(), b * block_size});
        }
        return rope.slice(range);
    }

    size_t put(ByteRange range, Rope data) override
    {
        put_log.emplace_back(range, data.totalBytes());

        /// Production caches assume disjoint coverage. If totalBytes doesn't
        /// match range.size, the chain handed us duplicate coverage — refuse.
        if (data.totalBytes() != range.size)
            return 0;

        size_t bytes_written = 0;
        size_t start_block = range.offset / block_size;
        size_t end_block = (range.end() + block_size - 1) / block_size;
        for (size_t b = start_block; b < end_block; ++b)
        {
            if (storage.contains(b))
                continue;
            ByteRange block_range{b * block_size, block_size};
            Rope slice = data.slice(block_range);
            String content;
            content.resize(slice.totalBytes());
            size_t pos = 0;
            for (const auto & node : slice.getNodes())
            {
                std::memcpy(content.data() + pos, node.data(), node.size);
                pos += node.size;
            }
            bytes_written += content.size();
            storage[b] = std::move(content);
        }
        return bytes_written;
    }

private:
    std::unordered_map<size_t, String> & storage;
    std::vector<std::pair<ByteRange, size_t>> & put_log;
    size_t block_size;
    CacheLookupResult result;
};

class WideGranularityMockCache : public ICacheProvider
{
public:
    WideGranularityMockCache(size_t block_size_, String name_)
        : block_size(block_size_), provider_name(std::move(name_)) {}

    std::unique_ptr<ICacheHandle> lookup(CacheKey, ByteRange range) override
    {
        return std::make_unique<WideGranularityMockCacheHandle>(
            range, storage, put_log, block_size);
    }

    String name() const override { return provider_name; }

    bool hasBlock(size_t block_index) const { return storage.contains(block_index); }

    /// (range argument to put, totalBytes of rope argument to put)
    const std::vector<std::pair<ByteRange, size_t>> & putLog() const { return put_log; }

    /// Pre-fill a block; used to seed cache state before a test.
    void seedBlock(size_t block_index, char fill)
    {
        storage[block_index] = String(block_size, fill);
    }

private:
    std::unordered_map<size_t, String> storage;
    std::vector<std::pair<ByteRange, size_t>> put_log;
    size_t block_size;
    String provider_name;
};

}

/// Sanity: two caches with the SAME granularity, complementary hits. No
/// overlap by construction. Expected to pass even pre-fix.
TEST(ReaderExecutor, ChainTwoTierDisjointHits)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(128 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 128 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "DiskMock");
    page_cache->seedBlock(0, 'P');
    disk_cache->seedBlock(1, 'D');

    ReaderExecutor executor(src, objects, {page_cache, disk_cache},
                             /*window_size=*/128 * 1024,
                             /*min_bytes_for_seek=*/0);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 128u * 1024u);
    EXPECT_EQ(rope.totalBytes(), 128u * 1024u);
}

/// THE BUG: PageCache (64K blocks) hits block 0, misses block 1. DiskCache
/// (4M blocks) holds the whole 4M segment, so for the chain's lookup of
/// [64K, 128K) it reports a hit at [0, 4M) — covering bytes already in
/// PageCache. Returned rope today has duplicate coverage.
TEST(ReaderExecutor, ChainLowerCacheHitCoversUpperHit)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(4 * 1024 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 4 * 1024 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(4 * 1024 * 1024, "DiskMock");
    page_cache->seedBlock(0, 'P');
    disk_cache->seedBlock(0, 'D');

    ReaderExecutor executor(src, objects, {page_cache, disk_cache},
                             /*window_size=*/128 * 1024,
                             /*min_bytes_for_seek=*/0);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 128u * 1024u);
    EXPECT_EQ(rope.totalBytes(), 128u * 1024u)
        << "Returned rope must not contain duplicate coverage from cache-vs-cache overlap";
}

/// Three caches with 64K / 1M / 4M granularities. PageCache hits, MidCache
/// hit covers PageCache, DiskCache not reached. Returned rope must be
/// disjoint and equal to window size.
TEST(ReaderExecutor, ChainThreeTierCascading)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(4 * 1024 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 4 * 1024 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    auto mid_cache  = std::make_shared<WideGranularityMockCache>(1024 * 1024, "MidMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(4 * 1024 * 1024, "DiskMock");
    page_cache->seedBlock(0, 'P');
    mid_cache->seedBlock(0, 'M');
    disk_cache->seedBlock(0, 'D');

    ReaderExecutor executor(src, objects, {page_cache, mid_cache, disk_cache},
                             /*window_size=*/128 * 1024,
                             /*min_bytes_for_seek=*/0);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 128u * 1024u);
    EXPECT_EQ(rope.totalBytes(), 128u * 1024u);
}

/// After a read where PageCache hits but DiskCache is cold, the DiskCache
/// must be filled with its full segment range (cached hit bytes + source
/// bytes combined into one disjoint rope for the put).
TEST(ReaderExecutor, ChainLowerCacheFilledFullyAfterRead)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(4 * 1024 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 4 * 1024 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(4 * 1024 * 1024, "DiskMock");
    page_cache->seedBlock(0, 'P');

    ReaderExecutor executor(src, objects, {page_cache, disk_cache},
                             /*window_size=*/128 * 1024,
                             /*min_bytes_for_seek=*/0);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.totalBytes(), 128u * 1024u);
    EXPECT_TRUE(disk_cache->hasBlock(0))
        << "DiskCache must be filled with the full segment after the read";
}

/// Every put across the chain must receive a rope with disjoint coverage —
/// totalBytes == range.size. A rope with duplicate nodes would overflow
/// DiskCacheHandle::put's flat_buf.
TEST(ReaderExecutor, ChainPutReceivesDisjointRope)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(4 * 1024 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 4 * 1024 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(4 * 1024 * 1024, "DiskMock");
    page_cache->seedBlock(0, 'P');

    ReaderExecutor executor(src, objects, {page_cache, disk_cache},
                             /*window_size=*/128 * 1024,
                             /*min_bytes_for_seek=*/0);

    executor.readNextWindow();

    ASSERT_FALSE(disk_cache->putLog().empty()) << "DiskCache put must be called";
    for (const auto & [range, total] : disk_cache->putLog())
        EXPECT_EQ(total, range.size)
            << "put received non-disjoint rope: range=[" << range.offset
            << ", " << range.end() << "), totalBytes=" << total;
}

/// Cache block extends past the window's tail. Returned rope must end at
/// the window boundary, not at the block boundary.
TEST(ReaderExecutor, ChainHitExtendsBeyondWindowEnd)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(128 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 128 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    page_cache->seedBlock(0, 'P');

    ReaderExecutor executor(src, objects, {page_cache},
                             /*window_size=*/50 * 1024,
                             /*min_bytes_for_seek=*/0);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 0u);
    EXPECT_EQ(rope.range().size, 50u * 1024u);
    EXPECT_EQ(rope.totalBytes(), 50u * 1024u);
}

/// Window starts inside a cache block. Bytes before window.offset must not
/// appear in the returned rope.
TEST(ReaderExecutor, ChainHitExtendsBeforeWindowStart)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(128 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 128 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    page_cache->seedBlock(0, 'P');

    ReaderExecutor executor(src, objects, {page_cache},
                             /*window_size=*/40 * 1024,
                             /*min_bytes_for_seek=*/0);

    executor.seek(10 * 1024);
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 10u * 1024u);
    EXPECT_EQ(rope.range().size, 40u * 1024u);
    EXPECT_EQ(rope.totalBytes(), 40u * 1024u);
}

/// Cache miss range extends past the window end (cache block is larger than
/// the window's tail). Source fetch goes past the window to fill the block;
/// cache stores the full block; user rope is exactly window size.
TEST(ReaderExecutor, ChainWindowEndCacheMissExtendsPast)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(128 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 128 * 1024);

    auto disk_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "DiskMock");

    ReaderExecutor executor(src, objects, {disk_cache},
                             /*window_size=*/50 * 1024,
                             /*min_bytes_for_seek=*/0);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 50u * 1024u);
    EXPECT_EQ(rope.totalBytes(), 50u * 1024u);
    EXPECT_TRUE(disk_cache->hasBlock(0))
        << "Cache block must be filled past the window end (intentional read-ahead)";
}
