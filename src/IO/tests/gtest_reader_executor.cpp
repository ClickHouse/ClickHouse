#include <IO/ReaderExecutor.h>
#include <IO/tests/ReaderExecutorInspector.h>
#include <IO/PlanSchedule.h>
#include <IO/BufferSourceReader.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/ICacheProvider.h>
#include <IO/IntervalSet.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/LocalSourceReader.h>
#include <IO/PipelineReadBuffer.h>

/// The tests adopted from master build `ReaderExecutor::Options` with designated
/// initializers for the few fields they care about; this branch's Options has more.
#pragma clang diagnostic ignored "-Wmissing-designated-field-initializers"

#include <Interpreters/Cache/EncryptionHeaderCache.h>
#include <IO/LongConnectionLimit.h>
#include <IO/ReadSettings.h>
#include <IO/ChainedBuffers.h>
#include <IO/PageCacheProvider.h>
#include <Core/Defines.h>
#include <Common/PageCache.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryPressureMonitor.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Disks/IO/createReadBufferFromFileBase.h>

#include <IO/DiskCacheProvider.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheSettings.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Interpreters/Context.h>
#include <Core/ServerUUID.h>
#include <Common/QueryScope.h>
#include <Common/scope_guard_safe.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <chrono>
#include <future>
#include <latch>
#include <limits>
#include <memory>
#include <optional>
#include <semaphore>
#include <thread>
#include <unordered_map>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_READ_ALL_DATA;
}

namespace DB::FileCacheSetting
{
    extern const FileCacheSettingsString path;
    extern const FileCacheSettingsUInt64 max_size;
    extern const FileCacheSettingsUInt64 max_elements;
    extern const FileCacheSettingsUInt64 max_file_segment_size;
    extern const FileCacheSettingsUInt64 boundary_alignment;
    extern const FileCacheSettingsBool load_metadata_asynchronously;
    extern const FileCacheSettingsFileCachePolicy cache_policy;
}

using namespace DB;
#include "config.h"
#if USE_SSL
#include <IO/ReaderExecutorDecryptor.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/WriteBufferFromString.h>
#endif

namespace ProfileEvents
{
    extern const Event ReaderExecutorLongConnectionOpened;
    extern const Event ReaderExecutorLongConnectionHits;
    extern const Event ReaderExecutorLongConnectionFallbacks;
    extern const Event ReaderExecutorBytesPushedToCacheSync;
    extern const Event ReaderExecutorBytesFromSource;
    extern const Event ReaderExecutorBytesFromPageCache;
    extern const Event ReaderExecutorBytesFromFilesystemCache;
    extern const Event ReaderExecutorSourceRequests;
    extern const Event ReaderExecutorRequestedBytes;
    extern const Event ReaderExecutorModeledCostMicroseconds;
    extern const Event ReaderExecutorCacheGetRequests;
    extern const Event ReaderExecutorCachePopulateRequests;
    extern const Event ReaderExecutorIncompleteConnections;
    extern const Event ReaderExecutorMachineInterrupted;
    extern const Event ReaderExecutorPartialCollects;
    extern const Event ReaderExecutorBytesPromoted;
}

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

}

namespace
{

/// Mock pool that runs every submitted job synchronously on the calling
/// thread and returns a `Done`-state handle (the machine then holds the
/// produced chain). Eliminates worker-thread timing from prefetch-related tests.
class SyncPrefetchPool : public PrefetchThreadPool
{
public:
    SyncPrefetchPool() : PrefetchThreadPool(NoWorkers{}) {}
    std::shared_ptr<JobHandle> submitJob(std::function<void()> task) override
    {
        task();
        return makeCompletedJobHandleForTest();
    }
};

}


namespace
{

/// In-memory source reader for testing.
/// open() materializes the requested object into a temp file and returns a
/// file-backed ReadBufferFromFileBase. Temp files are cleaned up on destruction.
class MemorySourceReader : public IFileBasedSourceReader
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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 512;
    ReaderExecutor executor(source, objects, {}, executor_options);

    auto chain = executor.readNextWindow();
    EXPECT_FALSE(chain.empty());
    EXPECT_EQ(chain.range().offset, 0);
    EXPECT_EQ(chain.range().size, 512);

    size_t total = 0;
    for (const auto & node : chain.getNodes())
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

TEST(ReaderExecutor, DisplayServesHoleyBankPrefix)
{
    /// The bank can be HOLEY - the wait-bank appends only the gaps each live writer could
    /// serve, so a failed middle leaves disjoint chunks. `Display::coverage` counts the bank
    /// per interval, and `Display::read` must serve the same shape: the claimed contiguous
    /// prefix serves (never an empty window - the caller reads empty as EOF mid-extent), and
    /// the unserved chunk beyond the hole SURVIVES the consuming trim for the next window.
    String content(1000, '\0');
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('A' + i % 26);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 1000);

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 512;
    ReaderExecutor executor(source, objects, {}, executor_options);

    auto r1 = executor.readNextWindow();
    ASSERT_EQ(r1.range().size, 512u);

    /// Bank [512, 612) and [700, 800) with the true bytes, hole at [612, 700).
    inspect(executor).bankBytes(512, std::string_view(content).substr(512, 100));
    inspect(executor).bankBytes(700, std::string_view(content).substr(700, 100));

    /// The banked prefix serves; an empty chain here is the false-EOF regression.
    auto r2 = executor.readNextWindow();
    ASSERT_EQ(r2.range().offset, 512u);
    ASSERT_EQ(r2.range().size, 100u);

    /// The chunk beyond the hole is still banked - the trim consumed only the delivered prefix.
    const auto ivs = inspect(executor).bankIntervals();
    ASSERT_EQ(ivs.size(), 1u);
    EXPECT_EQ(ivs.front().offset, 700u);
    EXPECT_EQ(ivs.front().size, 100u);

    /// The rest of the file reads back intact: the hole is fetched, the banked tail is served
    /// from the bank (not re-fetched), and the bytes match.
    String collected(content.substr(0, 612));
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            collected.append(node.data(), node.size);
    }
    EXPECT_EQ(collected, content);
}

TEST(ReaderExecutor, UnknownSizeLatchedEofStillFetchesBelowEndGaps)
{
    /// A size-unknown EOF latch records that AN end was seen, not where. A pool lead can
    /// latch it while its window's bytes are discarded (the cache refused the put), leaving
    /// a below-end gap the serve must re-fetch - refusing to launch because of the latch
    /// silently truncates the read. Constructed state: latch + serve below the true end.
    String content(1000, '\0');
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('a' + i % 26);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", StoredObject::UnknownSize);

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 512;
    ReaderExecutor executor(source, objects, {}, executor_options);

    auto r1 = executor.readNextWindow();
    ASSERT_EQ(r1.range().size, 512u);

    inspect(executor).latchEof();

    /// The engine must still fetch [512, ...) from the source; empty is silent truncation.
    auto r2 = inspect(executor).serveWindowAt(512);
    ASSERT_FALSE(r2.empty());
    EXPECT_EQ(r2.range().offset, 512u);
    String got;
    for (const auto & node : r2.getNodes())
        got.append(node.data(), node.size);
    EXPECT_EQ(got, content.substr(512, got.size()));
    EXPECT_GT(got.size(), 0u);
}

/// T4's epoch reset: the lane's ahead cursor (`attempted_end`) is GLOBAL and monotone within
/// a plan, so a backward seek that rebuilds the plan MUST reset it - a stale cursor above the
/// new plan's jobs would retire them all for the launch scan and silently kill read-ahead for
/// the whole post-seek plan (content stays correct: the pump is cursor-blind). The plan
/// window is shrunk below the pre-seek cursor so the mutant (no reset) is observable.
TEST(ReaderExecutor, AheadRelaunchesAfterBackwardSeek)
{
    const size_t file = 16 * 1024;
    String content(file, '\0');
    for (size_t i = 0; i < file; ++i)
        content[i] = static_cast<char>('A' + i % 26);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file);

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 1024;
    executor_options.plan_look_ahead_max_window = 4096;   /// post-seek jobs end BELOW the old cursor
    executor_options.prefetch_pool = pool;
    executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
    ReaderExecutor executor(source, objects, {}, executor_options);

    /// Forward: advance the ahead cursor well past 4096.
    String head;
    for (size_t w = 0; w < 8; ++w)
    {
        auto chain = executor.readNextWindow();
        for (const auto & node : chain.getNodes())
            head.append(node.data(), node.size);
    }
    ASSERT_EQ(head, content.substr(0, head.size()));
    ASSERT_GE(head.size(), 8 * 1024u);

    /// Backward seek out of any in-flight window: the plan rebuilds; the cursor must too.
    executor.seek(0);
    auto chain = executor.readNextWindow();
    ASSERT_EQ(chain.range().offset, 0u);
    EXPECT_TRUE(inspect(executor).hasInflightPrefetch())
        << "a stale ahead cursor would retire every post-seek job and kill read-ahead";
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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 400;
    ReaderExecutor executor(source, objects, {}, executor_options);

    auto chain = executor.readNextWindow();
    EXPECT_EQ(chain.range().offset, 0);
    EXPECT_EQ(chain.range().size, 400);

    size_t pos = 0;
    for (const auto & node : chain.getNodes())
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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 100;
    ReaderExecutor executor(source, objects, {}, executor_options);

    executor.seek(500);
    auto chain = executor.readNextWindow();
    EXPECT_EQ(chain.range().offset, 500);
    EXPECT_EQ(chain.range().size, 100);
    EXPECT_EQ(chain.getNodes()[0].data()[0], 'Z');
}

namespace
{

/// Held read buffer over a run of resident blocks in `MockCacheProvider`'s storage.
/// Re-readable; clamps `read(sub)` to its own range so a shared-storage view never
/// reaches into a neighbouring hit's blocks. Reads each call from the LIVE storage
/// (so eviction/regrowth is reflected).
class MockCacheReader : public CacheReader
{
public:
    MockCacheReader(ByteRange range_in_file, std::unordered_map<size_t, String> & storage_, size_t block_size_)
        : range_member(range_in_file), storage(storage_), block_size(block_size_) {}

    ByteRange range() const override { return range_member; }

    ChainedBuffers read(ByteRange sub) override
    {
        ChainedBuffers result;
        const size_t lo = std::max(sub.offset, range_member.offset);
        const size_t hi = std::min(sub.end(), range_member.end());
        if (lo >= hi)
            return result;

        const size_t first_block = lo / block_size;
        const size_t last_block = (hi - 1) / block_size;
        for (size_t b = first_block; b <= last_block; ++b)
        {
            auto it = storage.find(b);
            if (it == storage.end())
                continue;
            const auto & data = it->second;
            auto buf = std::make_shared<OwnedChainedBuffer>(data.size());
            std::memcpy(buf->data(), data.data(), data.size());
            ChainedBuffers block_chain;
            block_chain.append(ChainedBufferNode{buf, 0, data.size(), b * block_size});
            result.append(block_chain.slice(ByteRange{lo, hi - lo}));
        }
        return result;
    }

private:
    ByteRange range_member;
    std::unordered_map<size_t, String> & storage;
    size_t block_size;
};

/// Held write buffer over a block-aligned miss range in `MockCacheProvider`. `write`
/// stores whole blocks into the mock store (first-writer-wins, mirroring the legacy
/// `put`), advancing `committed` even when the bytes were already present (so
/// `complete` converges). `read` serves the committed prefix; `pin` is a no-op (the
/// block mock has no evictable in-flight segment).
class MockCacheWriter : public CacheWriter
{
public:
    MockCacheWriter(ByteRange aligned_range, std::unordered_map<size_t, String> & storage_, size_t block_size_)
        : range_member(aligned_range), storage(storage_), block_size(block_size_) {}

    ByteRange range() const override { return range_member; }
    IntervalSet committed() const override { return committed_ranges; }

    size_t write(ChainedBuffers data) override
    {
        size_t bytes_written = 0;
        for (size_t offset = range_member.offset; offset < range_member.end(); offset += block_size)
        {
            const size_t b = offset / block_size;
            const ByteRange block_range{offset, std::min(block_size, range_member.end() - offset)};

            /// Already committed by us — skip.
            if (committed_ranges.subtract(block_range).empty())
                continue;
            /// Only act on a block `data` fully covers (block-aligned delivery).
            if (!data.covers(block_range))
                continue;

            if (!storage.contains(b))
            {
                ChainedBuffers slice = data.slice(block_range);
                String content;
                content.resize(slice.totalBytes());
                slice.copyTo(content.data(), block_range);
                bytes_written += content.size();
                storage[b] = std::move(content);
            }
            /// Advance `committed` even on a first-writer-wins loss, so `complete` converges.
            committed_ranges.add(block_range);
        }
        return bytes_written;
    }

    ChainedBuffers read(ByteRange sub) override
    {
        ChainedBuffers result;
        const size_t lo = std::max(sub.offset, range_member.offset);
        const size_t hi = std::min(sub.end(), range_member.end());
        if (lo >= hi)
            return result;

        const size_t first_block = lo / block_size;
        const size_t last_block = (hi - 1) / block_size;
        for (size_t b = first_block; b <= last_block; ++b)
        {
            auto it = storage.find(b);
            if (it == storage.end())
                continue;
            const auto & data = it->second;
            auto buf = std::make_shared<OwnedChainedBuffer>(data.size());
            std::memcpy(buf->data(), data.data(), data.size());
            ChainedBuffers block_chain;
            block_chain.append(ChainedBufferNode{buf, 0, data.size(), b * block_size});
            result.append(block_chain.slice(ByteRange{lo, hi - lo}));
        }
        return result;
    }

private:
    ByteRange range_member;
    std::unordered_map<size_t, String> & storage;
    size_t block_size;
    IntervalSet committed_ranges;
};

class MockCacheProvider : public ICacheProvider
{
public:
    explicit MockCacheProvider(size_t block_size_)
        : block_size(block_size_) {}

    String name() const override { return "MockCache"; }
    CacheTier tier() const override { return CacheTier::FilesystemCache; }

    /// Read-only residency probe: classify each block as hit/miss against the LIVE
    /// store (never mutating it). Hits coalesce adjacent blocks into one entry and
    /// carry a held read buffer; misses are ONE ENTRY PER BLOCK (a block is this
    /// mock's cell) with no writer.
    CacheViewPtr planResidencyView(const StoredObject &, size_t, ByteRange range_in_file) override
    {
        auto view = std::make_unique<CacheView>();
        if (range_in_file.size == 0)
            return view;

        const size_t start_block = range_in_file.offset / block_size;
        const size_t end_block = (range_in_file.end() + block_size - 1) / block_size;

        bool run_active = false;
        ByteRange run_range{0, 0};
        auto flush_hit_run = [&]()
        {
            if (!run_active)
                return;
            view->hit_entries.push_back(HitEntry{
                run_range, std::make_unique<MockCacheReader>(run_range, storage, block_size)});
            run_active = false;
        };

        for (size_t b = start_block; b < end_block; ++b)
        {
            const ByteRange block_range{b * block_size, block_size};
            if (storage.contains(b))
            {
                if (!run_active)
                {
                    run_active = true;
                    run_range = block_range;
                }
                else
                    run_range.size = block_range.end() - run_range.offset;
            }
            else
            {
                flush_hit_run();
                view->miss_entries.push_back(MissEntry{block_range, /*writer=*/nullptr});
            }
        }
        flush_hit_run();
        return view;
    }

    void openWriteBuffers(const StoredObject &, size_t, CacheView & view) override
    {
        for (auto & entry : view.miss_entries)
            entry.writer = std::make_unique<MockCacheWriter>(entry.range, storage, block_size);
    }

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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 512;
    ReaderExecutor executor(source, objects, {cache}, executor_options);

    /// First read: miss, fetches from source, populates cache
    auto chain = executor.readNextWindow();
    EXPECT_EQ(chain.range().size, 512);
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
        ReaderExecutor::Options warmup_options;
        warmup_options.window_size = 512;
        ReaderExecutor warmup(source, objects, {cache}, warmup_options);
        warmup.readNextWindow();
    }

    /// Replace source with different content
    String alt_content(512, 'Z');
    auto alt_source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", alt_content}});

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 512;
    ReaderExecutor executor(alt_source, objects, {cache}, executor_options);
    auto chain = executor.readNextWindow();
    EXPECT_EQ(chain.range().size, 512);

    /// Should have gotten 'S' from cache, not 'Z' from alt_source
    EXPECT_EQ(chain.getNodes()[0].data()[0], 'S');
}

TEST(ReaderExecutor, PrefetchTriggersOnReadNextWindow)
{
    String content(3000, 'P');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 3000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 1000;
    executor_options.prefetch_pool = pool;
    ReaderExecutor executor(source, objects, {}, executor_options);

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

TEST(ReaderExecutor, PrefetchBoxRoundTripServesAllBytes)
{
    /// Cold sequential scan driven by a REAL prefetch pool. The source-connection
    /// cluster is moved foreground -> job at submit and reclaimed back at consume /
    /// cancel-queued every window. Assert the round-trip is faithful: the prefetch
    /// box move (foreground <-> in-flight job and back) serves every byte in order
    /// with no data corruption across the connection-ownership transfer.
    String content(8000, 0);
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('A' + (i % 26));
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 8000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    auto limit = std::make_shared<LongConnectionLimit>(10);
    TestThreadGroup tg;
    String result;
    {
        /// min_bytes_for_seek=0: contiguous windows continue the open connection.
        ReaderExecutor::Options executor_options;
        executor_options.window_size = 1000;
        executor_options.min_bytes_for_seek = 0;
        executor_options.prefetch_pool = pool;
        executor_options.long_connection_limit = limit;
        ReaderExecutor executor(source, objects, {}, executor_options);

        while (true)
        {
            auto chain = executor.readNextWindow();
            if (chain.empty())
                break;
            for (const auto & node : chain.getNodes())
                result.append(node.data(), node.size);
            /// The cluster lives in exactly one place (foreground or the in-flight job),
            /// so at most one connection slot is ever active - never two.
            EXPECT_LE(limit->getActiveCount(), 1u);
        }
    }

    EXPECT_EQ(result, content);   /// no corruption across the box move
}

TEST(ReaderExecutor, SeekInsidePrefetchedWindow)
{
    /// After the first window read, a prefetch is in flight for [500, 1000).
    /// Seeking to 750 (inside the prefetched range) must:
    ///   - leave executor.getPosition() == 750 (not 500), and
    ///   - cause the next readNextWindow to return a chain starting at logical 750
    ///     with content matching the source at offset 750.
    ///
    /// The returned size depends on which branch the executor takes:
    ///   - Wait branch (worker already running): chain is the prefetched [500, 1000)
    ///     sliced to [750, 1000) - size 250, or less when the takeover interrupted
    ///     the worker mid-window and a shorter prefix past 750 was served.
    ///   - Cancel branch (worker hadn't started): a fresh window from position 750
    ///     of size min(window_size, file_size - 750), so the chain spans [750, 1250).
    /// All are valid outcomes and the test accepts any.

    String content(2000, 0);
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('A' + (i % 26));

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 500;
    executor_options.prefetch_pool = pool;
    ReaderExecutor executor(source, objects, {}, executor_options);

    auto rope1 = executor.readNextWindow();
    EXPECT_EQ(rope1.range().offset, 0u);
    EXPECT_EQ(rope1.range().size, 500u);

    executor.seek(750);
    EXPECT_EQ(executor.getPosition(), 750u);

    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().offset, 750u);
    EXPECT_TRUE((rope2.range().size >= 1 && rope2.range().size <= 250u) || rope2.range().size == 500u)
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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 500;
    executor_options.prefetch_pool = pool;
    ReaderExecutor executor(source, objects, {}, executor_options);

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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 500;
    executor_options.prefetch_pool = pool;
    ReaderExecutor executor(source, objects, {}, executor_options);

    /// Before the first readNextWindow nothing has been prefetched yet.
    EXPECT_FALSE(inspect(executor).hasInflightPrefetch());

    /// Seek to a position outside any existing prefetch range. Must queue
    /// a fresh prefetch for the new position right away.
    executor.seek(2500);
    EXPECT_TRUE(inspect(executor).hasInflightPrefetch())
        << "seek must trigger a new prefetch when prefetch_pool is set";

    /// And the prefetched data is the one we actually consume next.
    auto chain = executor.readNextWindow();
    EXPECT_EQ(chain.range().offset, 2500u);
    EXPECT_EQ(chain.range().size, 500u);
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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 200;
    ReaderExecutor executor(source, objects, {}, executor_options);
    /// No `prefetch_pool` in Options — sync path.

    executor.seek(400);
    EXPECT_FALSE(inspect(executor).hasInflightPrefetch());

    auto chain = executor.readNextWindow();
    EXPECT_EQ(chain.range().offset, 400u);
}

TEST(ReaderExecutor, PrefetchWindowRespondsToMemoryPressure)
{
    /// Read-ahead is speculative, so it tracks memory pressure: suppressed entirely at
    /// High/Critical. At Normal/Elevated the ASK is no longer window-gated for a bypass
    /// bottom (the worker's one-window residue cap self-limits the fetch instead), so the
    /// machine window spans the plan remainder - EOF-bounded here. Uses the stateless path
    /// (a present-but-zero-capacity long_connection_limit, so no slot is acquired) so the
    /// window read is observable.
    struct Reading { size_t sync_window; bool scheduled; size_t prefetch_window; };
    auto measure = [](double pressure) -> Reading
    {
        FakeMemoryPressureMonitor fake(pressure, /*initial_now_ns=*/1'000'000'000ULL);
        ScopedMemoryPressureMonitor scope(fake);

        auto source = std::make_shared<MemorySourceReader>(
            std::unordered_map<String, String>{{"obj", String(1u << 20, 'p')}});   // 1 MiB
        StoredObjects objects;
        objects.emplace_back("obj", "", 1u << 20);

        auto pool = std::make_shared<PrefetchThreadPool>(2);
        auto limit = std::make_shared<LongConnectionLimit>(0);   // present but no slots -> stateless window reads
        ReaderExecutor::Options executor_options;
        executor_options.window_size = 256u << 10;
        executor_options.min_bytes_for_seek = 0;
        executor_options.block_size = 32u << 10;
        executor_options.prefetch_pool = pool;
        executor_options.long_connection_limit = limit;
        ReaderExecutor executor(source, objects, {}, executor_options);

        ChainedBuffers chain = executor.readNextWindow();   // synchronous full-window read, then schedules a prefetch
        return {chain.range().size, inspect(executor).hasInflightPrefetch(), inspect(executor).inflightPrefetchSize()};
    };

    const Reading normal = measure(0.50);
    EXPECT_TRUE(normal.scheduled);
    EXPECT_EQ(normal.prefetch_window, (1u << 20) - normal.sync_window)
        << "Normal: the ask spans the file remainder";

    const Reading elevated = measure(0.80);
    EXPECT_TRUE(elevated.scheduled);
    EXPECT_EQ(elevated.prefetch_window, (1u << 20) - elevated.sync_window)
        << "Elevated: the ask spans the (pressure-shrunk) remainder";

    const Reading high = measure(0.92);
    EXPECT_FALSE(high.scheduled) << "High pressure: prefetch suppressed";
    EXPECT_EQ(high.prefetch_window, 0u);

    const Reading critical = measure(0.99);
    EXPECT_FALSE(critical.scheduled) << "Critical pressure: prefetch suppressed";
    EXPECT_EQ(critical.prefetch_window, 0u);
}

TEST(ReaderExecutor, MergeRangesNoGap)
{
    /// Adjacent ranges — should merge into one
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {100, 100}, {200, 100}};
    auto merged = ReaderExecutorInspector::mergeRanges(ranges, 50);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 300);
}

TEST(ReaderExecutor, MergeRangesSmallGap)
{
    /// Small gap (10 bytes) < min_gap (100) — merge
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {110, 100}};
    auto merged = ReaderExecutorInspector::mergeRanges(ranges, 100);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 210);
}

TEST(ReaderExecutor, MergeRangesLargeGap)
{
    /// Large gap (500 bytes) > min_gap (100) — don't merge
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {600, 100}};
    auto merged = ReaderExecutorInspector::mergeRanges(ranges, 100);
    ASSERT_EQ(merged.size(), 2);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 100);
    EXPECT_EQ(merged[1].offset, 600);
    EXPECT_EQ(merged[1].size, 100);
}

TEST(ReaderExecutor, MergeRangesMixed)
{
    /// Three ranges: first two close, third far away
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {120, 100}, {1000, 100}};
    auto merged = ReaderExecutorInspector::mergeRanges(ranges, 50);
    ASSERT_EQ(merged.size(), 2);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 220);
    EXPECT_EQ(merged[1].offset, 1000);
    EXPECT_EQ(merged[1].size, 100);
}

TEST(ReaderExecutor, MergeRangesZeroMinGap)
{
    /// min_gap=0 — no merging
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {100, 100}};
    auto merged = ReaderExecutorInspector::mergeRanges(ranges, 0);
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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 512;
    ReaderExecutor executor(source, objects, {}, executor_options);
    executor.addDecryptionLayer("layer0", [](UInt128, const String &) { return String{}; });
    executor.addDecryptionLayer("layer1", [](UInt128, const String &) { return String{}; });

    EXPECT_EQ(executor.totalSize(), 0u);
}

namespace
{
    /// Encrypt `plaintext` with the given key/iv at stream offset 0 using
    /// AES_128_CTR. CTR is symmetric — encryption and decryption are the
    /// same operation.
    String aesCtrEncrypt(const String & key, FileEncryption::InitVector iv, const String & plaintext)
    {
        FileEncryption::Encryptor enc(FileEncryption::Algorithm::AES_128_CTR, key, iv);
        enc.setOffset(0);
        String out(plaintext.size(), '\0');
        enc.decrypt(plaintext.data(), plaintext.size(), out.data());
        return out;
    }

    /// Same as `aesCtrEncrypt` but keyed at an arbitrary stream offset —
    /// needed when reproducing a legacy stacked-encryption write where an
    /// outer layer's keystream covers `[inner_header (64), inner_ciphertext]`
    /// at offsets `[0, inner_ciphertext_size + 64)`. CTR is position-
    /// addressable, so encrypting two contiguous chunks at adjacent offsets
    /// produces the same ciphertext as encrypting the concatenation.
    String aesCtrEncryptAt(const String & key, FileEncryption::InitVector iv,
        size_t stream_offset, const char * data, size_t size)
    {
        FileEncryption::Encryptor enc(FileEncryption::Algorithm::AES_128_CTR, key, iv);
        enc.setOffset(stream_offset);
        String out(size, '\0');
        enc.decrypt(data, size, out.data());
        return out;
    }

    /// Build the on-disk encrypted byte stream:
    ///   Header(64 bytes) + ciphertext
    String makeEncryptedFile(const String & key, FileEncryption::InitVector iv, const String & plaintext)
    {
        String file_bytes;
        {
            WriteBufferFromString wb(file_bytes);
            FileEncryption::Header header;
            header.algorithm = FileEncryption::Algorithm::AES_128_CTR;
            header.key_fingerprint = FileEncryption::calculateKeyFingerprint(key);
            header.init_vector = iv;
            header.write(wb);
            wb.finalize();
        }
        file_bytes += aesCtrEncrypt(key, iv, plaintext);
        return file_bytes;
    }

    /// Read the whole file through the executor and concatenate the served
    /// nodes. The executor serves plaintext - it decrypts each window inside
    /// `readNextWindow` - so the consumer just copies the bytes out.
    String readAll(ReaderExecutor & executor)
    {
        String result;
        while (true)
        {
            auto w = executor.readNextWindow();
            if (w.empty())
                break;
            for (const auto & n : w.getNodes())
                result.append(n.data(), n.size);
        }
        return result;
    }
}

TEST(ReaderExecutor, DecryptsMultiNodeWindow)
{
    /// End-to-end: the executor serves plaintext over a multi-node window
    /// (`readNextWindow` decrypts each node at its logical offset). Plaintext is
    /// larger than CHAINED_BUFFER_BLOCK_SIZE (and a non-multiple total) so several
    /// nodes, including a partial tail, are decrypted.

    String key(16, 'k');
    FileEncryption::InitVector iv(UInt128{0x0123456789abcdefULL});

    const size_t plaintext_size = ReaderExecutor::CHAINED_BUFFER_BLOCK_SIZE * 3 + 12345;
    String plaintext(plaintext_size, '\0');
    for (size_t i = 0; i < plaintext_size; ++i)
        plaintext[i] = static_cast<char>((i * 31 + 7) & 0xFF);  /// distinguishable

    String file_bytes = makeEncryptedFile(key, iv, plaintext);

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    /// Window larger than the plaintext so the entire file is read in one
    /// readNextWindow call — the >3 MiB payload is served as several plaintext
    /// nodes (3 full blocks + 1 partial), each decrypted at its logical offset.
    ReaderExecutor::Options executor_options;
    executor_options.window_size = plaintext_size + ReaderExecutor::CHAINED_BUFFER_BLOCK_SIZE;
    ReaderExecutor executor(source, objects, {}, executor_options);
    executor.addDecryptionLayer("/test",
        [&](UInt128 got_fp, const String &)
        {
            EXPECT_EQ(got_fp, FileEncryption::calculateKeyFingerprint(key));
            return key;
        });
    executor.initDecryption();

    String result = readAll(executor);

    ASSERT_EQ(result.size(), plaintext.size());
    EXPECT_EQ(result, plaintext);
}

TEST(ReaderExecutor, EncryptedEofReleasesLongConnectionSlot)
{
    /// Regression: `atEnd` used to compare the logical `position` against
    /// the physical `offset_map.totalSize()`. For an encrypted file the
    /// physical size is larger by `data_start_offset` bytes, so after the
    /// last plaintext byte `position` is strictly less than
    /// `offset_map.totalSize()` and `atEnd` stays false. That skipped the
    /// EOF branch in `readNextWindow` and left the `LongConnectionLimit`
    /// slot pinned past EOF.
    String key(16, 'k');
    FileEncryption::InitVector iv(UInt128{0xfeedfacecafeULL});
    String plaintext(2048, 'E');
    String file_bytes = makeEncryptedFile(key, iv, plaintext);

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    auto long_connection_limit = std::make_shared<LongConnectionLimit>(4);

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 512;
    executor_options.long_connection_limit = long_connection_limit;
    ReaderExecutor executor(source, objects, {}, executor_options);
    executor.addDecryptionLayer("/test",
        [&](UInt128, const String &) { return key; });
    executor.initDecryption();

    while (true)
    {
        auto w = executor.readNextWindow();
        if (w.empty())
            break;
    }

    EXPECT_EQ(long_connection_limit->getActiveCount(), 0u);
}

TEST(ReaderExecutor, EncryptedSeekInsidePrefetchedWindow)
{
    /// Encrypted twin of `SeekInsidePrefetchedWindow`: the collect trims the kept
    /// prefetched window against the seek target. Everything inside the executor is
    /// PHYSICAL (header-inclusive), so a trim against the logical cursor would slice
    /// the chain 64 bytes (one header) early and serve shifted plaintext - the
    /// unencrypted twin cannot catch that.
    String key(16, 's');
    FileEncryption::InitVector iv(UInt128{0x5eedu});
    String plaintext(2000, '\0');
    for (size_t i = 0; i < plaintext.size(); ++i)
        plaintext[i] = static_cast<char>('A' + (i % 26));
    String file_bytes = makeEncryptedFile(key, iv, plaintext);

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 500;
    executor_options.prefetch_pool = pool;
    ReaderExecutor executor(source, objects, {}, executor_options);
    executor.addDecryptionLayer("/t",
        [&](UInt128, const String &) { return key; });
    executor.initDecryption();

    auto rope1 = executor.readNextWindow();
    EXPECT_EQ(rope1.range().offset, 0u);
    ASSERT_FALSE(rope1.getNodes().empty());
    EXPECT_EQ(rope1.getNodes().front().data()[0], plaintext[0]);

    executor.seek(750);
    EXPECT_EQ(executor.getPosition(), 750u);

    auto rope2 = executor.readNextWindow();
    EXPECT_EQ(rope2.range().offset, 750u);
    ASSERT_FALSE(rope2.getNodes().empty());
    EXPECT_EQ(rope2.getNodes().front().data()[0], plaintext[750]);
}

TEST(ReaderExecutor, DecryptsSmallPayload)
{
    /// Same path but payload smaller than CHAINED_BUFFER_BLOCK_SIZE — exercises the
    /// single-iteration loop.

    String key(16, 'q');
    FileEncryption::InitVector iv(UInt128{42});
    const String plaintext = "Hello, encrypted world!";
    String file_bytes = makeEncryptedFile(key, iv, plaintext);

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 4096;
    ReaderExecutor executor(source, objects, {}, executor_options);
    executor.addDecryptionLayer("/t",
        [&](UInt128, const String &) { return key; });
    executor.initDecryption();

    String result = readAll(executor);
    EXPECT_EQ(result, plaintext);
}

TEST(ReaderExecutor, DecryptsMultiLayer)
{
    /// Two encryption layers stacked, in the layout that a legacy
    /// `DiskEncrypted`-over-`DiskEncrypted` configuration actually
    /// produces on write:
    ///   [outer_h_plain]            -- 64 bytes, in clear
    ///   [outer.encrypt(inner_h)]   -- 64 bytes, ciphertext (NOT plaintext)
    ///   [outer.encrypt(inner.encrypt(plaintext))]
    /// The outer encryption keystream covers the inner header AND payload
    /// — i.e. outer's keystream offset for user-byte P is `P + 64`, while
    /// inner's is `P`. `initDecryption` must peel the outer layer off the
    /// inner header bytes before parsing them; the executor's per-layer
    /// keystream offsets must recover the plaintext.

    String key_inner(16, 'i');
    String key_outer(16, 'o');
    FileEncryption::InitVector iv_inner(UInt128{1});
    FileEncryption::InitVector iv_outer(UInt128{2});

    const String plaintext(ReaderExecutor::CHAINED_BUFFER_BLOCK_SIZE + 500, 'X');

    /// 1. Serialize the inner header bytes.
    String inner_h_bytes;
    {
        WriteBufferFromString wb(inner_h_bytes);
        FileEncryption::Header inner_h;
        inner_h.algorithm = FileEncryption::Algorithm::AES_128_CTR;
        inner_h.key_fingerprint = FileEncryption::calculateKeyFingerprint(key_inner);
        inner_h.init_vector = iv_inner;
        inner_h.write(wb);
        wb.finalize();
    }
    ASSERT_EQ(inner_h_bytes.size(), FileEncryption::Header::kSize);

    /// 2. Inner-encrypt the plaintext at inner keystream offset 0.
    const String inner_ciphertext = aesCtrEncrypt(key_inner, iv_inner, plaintext);

    /// 3. Outer-encrypt `inner_h_bytes` and `inner_ciphertext` as one
    ///    contiguous stream — `inner_h_bytes` at outer-keystream offset 0,
    ///    `inner_ciphertext` at outer-keystream offset 64. CTR is
    ///    position-addressable so this matches the result of outer-
    ///    encrypting the concatenation in one shot.
    const String outer_h_ciphertext = aesCtrEncryptAt(
        key_outer, iv_outer,
        /*stream_offset=*/0,
        inner_h_bytes.data(), inner_h_bytes.size());
    const String outer_payload_ciphertext = aesCtrEncryptAt(
        key_outer, iv_outer,
        /*stream_offset=*/FileEncryption::Header::kSize,
        inner_ciphertext.data(), inner_ciphertext.size());

    /// 4. Assemble the file: plaintext outer header, ciphertext inner
    ///    header, ciphertext payload.
    String file_bytes;
    {
        WriteBufferFromString wb(file_bytes);
        FileEncryption::Header outer_h;
        outer_h.algorithm = FileEncryption::Algorithm::AES_128_CTR;
        outer_h.key_fingerprint = FileEncryption::calculateKeyFingerprint(key_outer);
        outer_h.init_vector = iv_outer;
        outer_h.write(wb);
        wb.finalize();
    }
    file_bytes += outer_h_ciphertext;
    file_bytes += outer_payload_ciphertext;

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    ReaderExecutor::Options executor_options;
    executor_options.window_size = plaintext.size() + 2048;
    ReaderExecutor executor(source, objects, {}, executor_options);
    /// Layers are added outermost-first, innermost-last — same order the
    /// stacked-disk prepareRead chain produces (each layer recurses into
    /// its delegate before appending its own `needDecryption`).
    executor.addDecryptionLayer("/outer",
        [&](UInt128, const String &) { return key_outer; });
    executor.addDecryptionLayer("/inner",
        [&](UInt128, const String &) { return key_inner; });
    executor.initDecryption();

    String result = readAll(executor);
    ASSERT_EQ(result.size(), plaintext.size());
    EXPECT_EQ(result, plaintext);
}

TEST(ReaderExecutorDecryptor, ConcurrentDecryptIsReentrant)
{
    /// `ReaderExecutorDecryptor::decrypt` must be reentrant: it builds fresh
    /// per-call encryptors, so several threads decrypting DISTINCT logical
    /// offsets concurrently must not cross-talk through any shared keystream
    /// offset. Parse a known single-layer header, then have N threads each
    /// decrypt a distinct chunk; assert every chunk matches a single-threaded
    /// reference decrypt of the same chunk.

    String key(16, 'r');
    FileEncryption::InitVector iv(UInt128{0xabcdef0123456789ULL});

    /// Plaintext large enough for many distinct, non-overlapping chunks.
    const size_t chunk_size = 4096;
    const size_t num_threads = 8;
    const size_t plaintext_size = chunk_size * num_threads;
    String plaintext(plaintext_size, '\0');
    for (size_t i = 0; i < plaintext_size; ++i)
        plaintext[i] = static_cast<char>((i * 37 + 11) & 0xFF);

    /// On-disk ciphertext is the payload only (decryptor operates on logical
    /// offsets; the header lives separately in `header_bytes`).
    const String ciphertext = aesCtrEncrypt(key, iv, plaintext);

    /// Serialize the layer's header into a ChainedBuffers for `parseHeaders`.
    String header_str;
    {
        DB::WriteBufferFromString wb(header_str);
        FileEncryption::Header header;
        header.algorithm = FileEncryption::Algorithm::AES_128_CTR;
        header.key_fingerprint = FileEncryption::calculateKeyFingerprint(key);
        header.init_vector = iv;
        header.write(wb);
        wb.finalize();
    }
    ASSERT_EQ(header_str.size(), FileEncryption::Header::kSize);

    DB::ChainedBuffers header_chain;
    {
        auto buf = std::make_shared<DB::OwnedChainedBuffer>(header_str.size());
        memcpy(buf->data(), header_str.data(), header_str.size());
        header_chain.append(DB::ChainedBufferNode{
            .buffer = buf,
            .buffer_offset = 0,
            .size = header_str.size(),
            .offset = 0,
        });
    }

    DB::ReaderExecutorDecryptor decryptor;
    decryptor.addLayer("/r", [&](UInt128 got_fp, const String &)
    {
        EXPECT_EQ(got_fp, FileEncryption::calculateKeyFingerprint(key));
        return key;
    });
    decryptor.parseHeaders(header_chain);
    ASSERT_TRUE(decryptor.initialized());

    /// Single-threaded reference: decrypt each chunk in isolation.
    std::vector<String> reference(num_threads);
    for (size_t t = 0; t < num_threads; ++t)
    {
        const size_t off = t * chunk_size;
        String chunk = ciphertext.substr(off, chunk_size);
        decryptor.decrypt(chunk.data(), chunk.size(), off);
        reference[t] = std::move(chunk);
        EXPECT_EQ(reference[t], plaintext.substr(off, chunk_size));
    }

    /// Concurrent: N threads decrypt distinct offsets at once through the same
    /// const decryptor. A start latch maximises overlap.
    std::vector<String> got(num_threads);
    std::latch start{static_cast<std::ptrdiff_t>(num_threads)};
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&, t]
        {
            const size_t off = t * chunk_size;
            start.arrive_and_wait();
            /// Decrypt many times to widen the race window.
            for (int rep = 0; rep < 64; ++rep)
            {
                String c = ciphertext.substr(off, chunk_size);
                decryptor.decrypt(c.data(), c.size(), off);
                if (rep == 0)
                    got[t] = std::move(c);
                else
                    EXPECT_EQ(c, reference[t]) << "thread " << t << " rep " << rep;
            }
        });
    }
    for (auto & th : threads)
        th.join();

    for (size_t t = 0; t < num_threads; ++t)
        EXPECT_EQ(got[t], reference[t]) << "concurrent decrypt mismatch at thread " << t;
}

#endif

TEST(ReaderExecutor, MergeRangesOverlapping)
{
    /// Overlapping ranges merge into their union regardless of min_gap > 0.
    /// Without the saturating-subtraction fix, gap = sorted[i].offset - prev.end()
    /// underflows on overlap and the merge branch is skipped, leaving overlapping
    /// ranges in the output.
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {50, 100}};
    auto merged = ReaderExecutorInspector::mergeRanges(ranges, 10);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0u);
    EXPECT_EQ(merged[0].size, 150u);  /// [0, 100) ∪ [50, 150) = [0, 150)
}

TEST(ReaderExecutor, MergeRangesContained)
{
    /// One range fully contained in another. The union is the wider range;
    /// without the fix the underflow path emits both ranges.
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 200}, {50, 100}};
    auto merged = ReaderExecutorInspector::mergeRanges(ranges, 10);
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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 1500;
    ReaderExecutor executor(source, objects, {}, executor_options);
    EXPECT_THROW(executor.readNextWindow(), Exception);
}

TEST(ReaderExecutor, CacheHitBetweenColdGapsNoDuplicateCoverage)
{
    /// A cached block sits between two cold gaps. The positional plan returns one
    /// pure run per call - cold gap [0, 100), then the cached block [100, 200)
    /// served from cache, then cold gap [200, 300) - so the read to EOF must
    /// assemble exactly [0, 300) with no duplicate coverage. A cache hit left
    /// overlapping fetched source offsets would inflate totalBytes past the range.
    ///
    /// Layout: cache block [100, 200), cold [0, 100) and [200, 300). The large
    /// min_bytes_for_seek means a window would once have merged the two gaps
    /// across the hit; the plan-gap clamp now stops each read at the hit boundary.

    StoredObjects objects;
    objects.emplace_back("obj", "", 300);

    auto cache = std::make_shared<MockCacheProvider>(100);

    /// Warm cache block 1 (offsets [100, 200)). Content is irrelevant here.
    {
        String warm_content(300, 'W');
        auto warm_source = std::make_shared<MemorySourceReader>(
            std::unordered_map<String, String>{{"obj", warm_content}});
        ReaderExecutor::Options warmup_options;
        warmup_options.window_size = 100;
        ReaderExecutor warmup(warm_source, objects, {cache}, warmup_options);
        warmup.seek(100);
        warmup.readNextWindow();
        ASSERT_TRUE(cache->hasBlock(1));
    }

    String real_content(300, 'S');
    auto real_source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", real_content}});

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 300;
    executor_options.min_bytes_for_seek = 8 * 1024 * 1024;
    ReaderExecutor executor(real_source, objects, {cache}, executor_options);

    /// Drain to EOF: the plan serves the run at the cursor (gap or resident) one
    /// call at a time.
    ChainedBuffers assembled;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        assembled.append(std::move(chain));
    }

    EXPECT_EQ(assembled.range().offset, 0u);
    EXPECT_EQ(assembled.range().size, 300u);
    EXPECT_EQ(assembled.totalBytes(), 300u);
}

namespace CurrentMetrics
{
    extern const Metric ReaderExecutorActive;
}

namespace
{

/// Succeeds on the first open() and throws on every subsequent one.
/// Used to drive an asynchronous failure into the fetch step so the machine
/// held by ReaderExecutor ends up carrying an exception (`failure`) when the
/// destructor drains it via cancelMachine.
class ThrowOnSecondOpenSourceReader : public IFileBasedSourceReader
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

}

TEST(ReaderExecutor, DestructorTolerantOfThrowingPrefetch)
{
    /// ~ReaderExecutor must drain a throwing read-ahead without terminating:
    /// the step's exception is captured into the machine (`failure`) and only
    /// logged by `cancelMachine` - nothing rethrows out of the `noexcept`
    /// destructor.

    auto source = std::make_shared<ThrowOnSecondOpenSourceReader>(String(2000, 'A'));
    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);

    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = 500;
        executor_options.prefetch_pool = pool;
        ReaderExecutor executor(source, objects, {}, executor_options);

        /// First sync read consumes the 1st open() and primes maybeTriggerPrefetch,
        /// which submits a task whose 2nd open() will throw on the pool thread.
        auto chain = executor.readNextWindow();
        ASSERT_FALSE(chain.empty());

        /// executor's destructor must drain the throwing future without terminating.
    }
    SUCCEED();
}

TEST(ReaderExecutor, DestructorAfterThrownReadNextWindowDoesNotSegfault)
{
    /// Reproduces a production segfault observed in stress tests: a collect
    /// that rethrows the worker's exception must not leave the executor
    /// pointing at a half-consumed step (a std::future is detached by its
    /// first `get`). `tryCollectMachine` takes local ownership (clears
    /// `machine`) BEFORE waiting/rethrowing, so the destructor's
    /// `cancelMachine` never re-touches the consumed handle.

    auto source = std::make_shared<ThrowOnSecondOpenSourceReader>(String(2000, 'B'));
    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);

    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = 500;
        executor_options.prefetch_pool = pool;
        ReaderExecutor executor(source, objects, {}, executor_options);

        /// 1st readNextWindow: synchronous open (success) + queues a prefetch
        /// whose worker will call `open()` again and throw.
        auto rope1 = executor.readNextWindow();
        ASSERT_FALSE(rope1.empty());

        /// 2nd readNextWindow: waits on the prefetch, which re-throws the
        /// worker's exception. This is the path that previously left the
        /// executor in a poisoned state.
        EXPECT_ANY_THROW(executor.readNextWindow());

        /// Now let the executor go out of scope: the destructor must finish
        /// cleanly because `machine` was already cleared inside
        /// `readNextWindow` before the throw.
    }
    SUCCEED();
}

TEST(ReaderExecutor, LocalReadUsesFullWindow)
{
    /// Local reads have no LongConnectionLimit / live buffer, so they take the
    /// stateless path. Like stateless remote reads, they keep the full window
    /// (not a single block) so one open amortises its setup over a window
    /// instead of reopening the source per block.
    constexpr size_t file_size = 2 * ReaderExecutor::CHAINED_BUFFER_BLOCK_SIZE;
    String content(file_size, 'L');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    /// No long_connection_limit, no caches -> stateless local path; default 8 MiB window.
    ReaderExecutor executor(source, objects, {});

    auto w = executor.readNextWindow();
    ASSERT_FALSE(w.empty());
    EXPECT_EQ(w.range().size, file_size)
        << "local read must keep the full window (whole 2 MiB file), not shrink to one block";
    EXPECT_EQ(w.getNodes().size(), file_size / ReaderExecutor::CHAINED_BUFFER_BLOCK_SIZE)
        << "the window is split into block-sized chain nodes";
}

TEST(ReaderExecutor, ConfiguredBlockSizeControlsNodeSize)
{
    /// A stateless (local) read keeps the full window but splits it into chain
    /// nodes of the configured block size. With a non-default block size the
    /// window still spans the whole file, while each node is one configured
    /// block - proving `block_size` drives the node granularity and
    /// `window_size` drives the window.
    constexpr size_t configured_block = 256 * 1024;
    constexpr size_t file_size = 4 * configured_block;  /// 1 MiB
    String content(file_size, 'B');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    /// No long_connection_limit, no caches -> stateless local path.
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 4 * 1024 * 1024;
    executor_options.min_bytes_for_seek = 0;
    executor_options.block_size = configured_block;
    ReaderExecutor executor(source, objects, {}, executor_options);

    auto w = executor.readNextWindow();
    ASSERT_FALSE(w.empty());
    EXPECT_EQ(w.range().size, file_size)
        << "window must span the whole file, not be capped at the 256 KiB block";
    EXPECT_EQ(w.getNodes().size(), file_size / configured_block)
        << "the window is split into configured-block-sized chain nodes";
}

TEST(ReaderExecutor, ConsumePathCancelledPrefetchIsStashedForDrain)
{
    /// When a queued read-ahead is revoked on the readNextWindow collect path
    /// (the next read arrives before the worker starts it), the machine must be
    /// stashed in `abandoned_machines` so ~ReaderExecutor waits for the pool
    /// worker to take the cancellation path before the executor's state (and
    /// the enclosing query's memory-tracker chain) is freed. The worker
    /// attaches a ThreadGroupSwitcher to the submitter's group BEFORE checking
    /// cancellation, so dropping the handle here risked a use-after-free.
    String content(2000, 'Z');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    /// Real single-worker pool. Occupy its one worker with a blocking task so
    /// the executor's prefetch stays Queued (the worker can't pull it), which
    /// makes the consume-path tryCancel succeed deterministically.
    auto pool = std::make_shared<PrefetchThreadPool>(1);

    std::promise<void> worker_started;
    std::promise<void> release_worker;
    auto blocker = pool->submitJob([&]
    {
        worker_started.set_value();
        release_worker.get_future().wait();
    });
    ASSERT_TRUE(blocker != nullptr);
    worker_started.get_future().wait();   /// the one worker is now busy in `blocker`

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 500;
    executor_options.min_bytes_for_seek = 0;
    executor_options.prefetch_pool = pool;
    ReaderExecutor executor(source, objects, {}, executor_options);

    /// Window 1: synchronous read, then maybeTriggerPrefetch submits a prefetch
    /// for window 2 that queues behind the blocked worker.
    auto w1 = executor.readNextWindow();
    ASSERT_FALSE(w1.empty());
    ASSERT_TRUE(inspect(executor).hasInflightPrefetch());

    /// Window 2: the prefetch is still Queued, so tryCancel succeeds on the
    /// consume path and the cancelled handle must be stashed for the drain.
    auto w2 = executor.readNextWindow();
    ASSERT_FALSE(w2.empty());
    EXPECT_EQ(inspect(executor).abandonedPrefetchCount(), 1u)
        << "consume-path cancelled prefetch must be stashed for ~ReaderExecutor to drain";

    /// Release the worker so it finishes `blocker`, then pulls the cancelled
    /// prefetch (sets the cancellation exception). ~ReaderExecutor's drain then
    /// get()s it (throws, caught) and returns cleanly.
    release_worker.set_value();
}

/// Same as above, but the synchronous fallback read THROWS after the cancel.
/// The cancelled handle must be stashed BEFORE that read, otherwise it is
/// dropped on the stack unwind and ~ReaderExecutor never waits for the worker
/// (which attaches a ThreadGroupSwitcher to the now-freed group).
TEST(ReaderExecutor, ConsumePathCancelledPrefetchStashedBeforeThrowingSyncRead)
{
    /// First open succeeds (window 1); the second (window 2's fallback read,
    /// no live buffer is kept without a long_connection_limit) throws.
    auto source = std::make_shared<ThrowOnSecondOpenSourceReader>(String(2000, 'Z'));
    StoredObjects objects;
    objects.emplace_back("obj", "", 2000);

    auto pool = std::make_shared<PrefetchThreadPool>(1);
    std::promise<void> worker_started;
    std::promise<void> release_worker;
    auto blocker = pool->submitJob([&]
    {
        worker_started.set_value();
        release_worker.get_future().wait();
    });
    ASSERT_TRUE(blocker != nullptr);
    worker_started.get_future().wait();

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 500;
    executor_options.min_bytes_for_seek = 0;
    executor_options.prefetch_pool = pool;
    ReaderExecutor executor(source, objects, {}, executor_options);

    auto w1 = executor.readNextWindow();
    ASSERT_FALSE(w1.empty());
    ASSERT_TRUE(inspect(executor).hasInflightPrefetch());

    /// Window 2: tryCancel succeeds, then the fallback read throws.
    EXPECT_THROW(executor.readNextWindow(), DB::Exception);
    EXPECT_EQ(inspect(executor).abandonedPrefetchCount(), 1u)
        << "cancelled prefetch must be stashed before the throwing fallback read";

    release_worker.set_value();
}

/// `reader_executor_window_size` / `reader_executor_block_size` of 0 would make
/// `effectiveWindowSize` / `allocateBlocks` produce a zero-size allocation;
/// reject them at construction.
TEST(ReaderExecutor, ConstructorRejectsZeroWindowOrBlockSize)
{
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(10, 'x')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 10);

    ReaderExecutor::Options zero_window_options;
    zero_window_options.window_size = 0;
    EXPECT_THROW(
        ReaderExecutor(source, objects, {}, zero_window_options),
        DB::Exception);

    ReaderExecutor::Options zero_block_options;
    zero_block_options.window_size = 500;
    zero_block_options.min_bytes_for_seek = 0;
    zero_block_options.block_size = 0;
    EXPECT_THROW(
        ReaderExecutor(source, objects, {}, zero_block_options),
        DB::Exception);
}

/// `OffsetMap::build` throws for an unknown-size object in a multi-object
/// pipeline. The live-instance gauge must not be bumped before `build`, or a
/// throwing constructor (which skips `~ReaderExecutor`) leaks the count.
TEST(ReaderExecutor, ConstructorDoesNotLeakActiveMetricWhenBuildThrows)
{
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"a", String(100, 'a')}});
    StoredObjects objects;
    objects.emplace_back("a", "", 100);
    objects.emplace_back("b", "", StoredObject::UnknownSize);

    const auto before = CurrentMetrics::get(CurrentMetrics::ReaderExecutorActive);
    EXPECT_THROW(ReaderExecutor(source, objects, {}), DB::Exception);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::ReaderExecutorActive), before)
        << "a throwing constructor must not leak ReaderExecutorActive";
}

namespace
{

/// FileCache-style cache with fixed-size segments that the test can evict
/// between windows. Mirrors `DiskCacheProvider`'s status miss-head behavior:
///   - partially downloaded segment -> miss head at current write offset,
///   - empty/evicted segment        -> miss head at segment start.
/// Honors pinning via the write buffer's `pin` using FileCache's releasable()
/// rule (use_count()==1).
class EvictableSegmentMockCache : public ICacheProvider
{
public:
    explicit EvictableSegmentMockCache(size_t segment_size_)
        : segment_size(segment_size_) {}

    /// Read-only residency probe (defined out-of-line below, after the buffer
    /// classes): committed prefix is a hit, the segment-aligned tail past the
    /// download frontier is a per-segment miss. MUST NOT mutate the store.
    CacheViewPtr planResidencyView(const StoredObject &, size_t, ByteRange range_in_file) override;

    /// One held write buffer per aligned (segment) miss range; each appends into its
    /// segment append-only (mirroring the old `put`) and pins it via `pin`.
    void openWriteBuffers(const StoredObject &, size_t, CacheView & view) override;

    String name() const override { return "EvictableSegmentMock"; }
    CacheTier tier() const override { return CacheTier::FilesystemCache; }

    /// Evict every segment not currently pinned by a caller; a re-download re-fills
    /// the bytes from scratch, so drop the stored bytes alongside the frontier.
    void evictUnpinned()
    {
        for (auto & [idx, live] : liveness)
            if (live.use_count() == 1)
            {
                downloaded[idx] = 0;
                bytes.erase(idx);
            }
    }

    size_t segmentSize() const { return segment_size; }

    /// Mark a segment fully resident with genuine `fill` bytes - the faithful twin of
    /// setting `downloaded` directly, so reads return real data. Mirrors
    /// `WideGranularityMockCache::seedBlock`.
    void seedSegment(size_t idx, char fill)
    {
        downloaded[idx] = segment_size;
        bytes[idx] = String(segment_size, fill);
    }

    std::shared_ptr<int> & livenessFor(size_t idx)
    {
        auto & p = liveness[idx];
        if (!p)
            p = std::make_shared<int>(0);
        return p;
    }

    /// idx -> bytes downloaded from segment start (0 == empty).
    std::unordered_map<size_t, size_t> downloaded;
    /// idx -> the genuine committed bytes for `[seg_start, seg_start + downloaded[idx])`;
    /// reads return these (no fabricated placeholder). Length tracks `downloaded[idx]`.
    std::unordered_map<size_t, String> bytes;
    /// idx -> liveness token; an extra ref (held by the executor's pin) makes
    /// the segment non-evictable.
    std::unordered_map<size_t, std::shared_ptr<int>> liveness;
    std::vector<std::pair<ByteRange, size_t>> put_log;
    /// idx -> how many times a write buffer was opened for this segment. A segment
    /// straddling several small plan windows is re-opened once per window it spans
    /// (single tier does not fold the plan to the cell boundary), so this rises above
    /// 1 even though the segment is FETCHED once - the segment-open-once probe.
    std::unordered_map<size_t, size_t> open_count;
    /// idx -> true means put() returns 0 (simulates a cache that refuses writes).
    std::unordered_map<size_t, bool> reject_put;

private:
    size_t segment_size;
};

/// Held read buffer over ONE segment's committed prefix `[seg_start, seg_start+dl)`.
/// `read(sub)` re-reads the LIVE `downloaded[idx]` each call (clamped to the hit
/// range), so an eviction between windows makes the hit come up short and the
/// executor heals by refetch. Returns the genuine stored bytes for the committed
/// sub-range.
class EvictableSegmentReadBuffer : public CacheReader
{
public:
    EvictableSegmentReadBuffer(ByteRange hit_range_, size_t seg_idx_, EvictableSegmentMockCache & cache_)
        : hit_range(hit_range_), seg_idx(seg_idx_), cache(cache_) {}

    ByteRange range() const override { return hit_range; }

    ChainedBuffers read(ByteRange sub) override
    {
        ChainedBuffers result;
        const size_t lo = std::max(sub.offset, hit_range.offset);
        const size_t hi = std::min({sub.end(), hit_range.end(), liveCommittedEnd()});
        if (lo >= hi)
            return result;
        auto it = cache.bytes.find(seg_idx);
        if (it == cache.bytes.end())
            return result;
        const size_t seg_start = seg_idx * cache.segmentSize();
        const String & seg_bytes = it->second;
        const size_t local_lo = lo - seg_start;
        const size_t local_hi = std::min(hi - seg_start, seg_bytes.size());
        if (local_lo >= local_hi)
            return result;
        auto buf = std::make_shared<OwnedChainedBuffer>(local_hi - local_lo);
        std::memcpy(buf->data(), seg_bytes.data() + local_lo, local_hi - local_lo);
        result.append(ChainedBufferNode{buf, 0, local_hi - local_lo, seg_start + local_lo});
        return result;
    }

private:
    size_t liveCommittedEnd() const
    {
        const size_t seg = cache.segmentSize();
        const size_t seg_start = seg_idx * seg;
        const size_t dl = cache.downloaded.contains(seg_idx) ? cache.downloaded[seg_idx] : 0;
        return std::min(seg_start + std::min(dl, seg), hit_range.end());
    }

    ByteRange hit_range;
    size_t seg_idx;
    EvictableSegmentMockCache & cache;
};

/// Held write buffer over ONE aligned (segment) miss range. `write` appends into the
/// segment append-only at the live `cwo` (with `reject_put` and the `livenessFor`
/// token), advancing `committed`. `read` serves the genuine committed-prefix bytes.
/// `pin(frontier)`: a partially-downloaded segment returns its liveness token (so it
/// survives `evictUnpinned` while the executor holds it).
class EvictableSegmentWriteBuffer : public CacheWriter
{
public:
    EvictableSegmentWriteBuffer(ByteRange aligned_range_, size_t seg_idx_, EvictableSegmentMockCache & cache_)
        : aligned_range(aligned_range_), seg_idx(seg_idx_), cache(cache_) {}

    ByteRange range() const override { return aligned_range; }
    IntervalSet committed() const override { return committed_ranges; }

    size_t write(ChainedBuffers data) override
    {
        const size_t seg = cache.segmentSize();
        const size_t seg_start = seg_idx * seg;

        if (auto it = cache.reject_put.find(seg_idx); it != cache.reject_put.end() && it->second)
            return 0;

        /// Append-only at the live current write offset, like `FileSegment::write`.
        const size_t cwo = seg_start + (cache.downloaded.contains(seg_idx) ? cache.downloaded[seg_idx] : 0);
        const size_t seg_end = seg_start + seg;
        const size_t write_end_max = std::min(seg_end, aligned_range.end());
        if (cwo >= write_end_max)
            return 0;

        /// Only the contiguous prefix of `data` starting at `cwo` can be appended.
        const ByteRange target{cwo, write_end_max - cwo};
        size_t contiguous = target.size;
        if (auto gaps = data.gaps(target); !gaps.empty())
        {
            const size_t first_gap = gaps.front().offset;
            contiguous = (first_gap > cwo) ? (first_gap - cwo) : 0;
        }
        if (contiguous == 0)
            return 0;

        /// Store the genuine bytes append-only (the frontier and the stored length stay
        /// in lock-step), so a later read returns real data, not a placeholder.
        String & seg_bytes = cache.bytes[seg_idx];
        const size_t local = cwo - seg_start;
        if (seg_bytes.size() < local + contiguous)
            seg_bytes.resize(local + contiguous);
        data.copyTo(seg_bytes.data() + local, ByteRange{cwo, contiguous});

        cache.downloaded[seg_idx] = std::min(seg, (cwo + contiguous) - seg_start);
        cache.livenessFor(seg_idx);
        committed_ranges.add(ByteRange{cwo, contiguous});
        return contiguous;
    }

    ChainedBuffers read(ByteRange sub) override
    {
        ChainedBuffers result;
        const size_t seg = cache.segmentSize();
        const size_t seg_start = seg_idx * seg;
        const size_t dl = cache.downloaded.contains(seg_idx) ? cache.downloaded[seg_idx] : 0;
        const size_t committed_end = seg_start + std::min(dl, seg);
        const size_t lo = std::max({sub.offset, aligned_range.offset, seg_start});
        const size_t hi = std::min({sub.end(), aligned_range.end(), committed_end});
        if (lo >= hi)
            return result;
        auto it = cache.bytes.find(seg_idx);
        if (it == cache.bytes.end())
            return result;
        const String & seg_bytes = it->second;
        const size_t local_lo = lo - seg_start;
        const size_t local_hi = std::min(hi - seg_start, seg_bytes.size());
        if (local_lo >= local_hi)
            return result;
        auto buf = std::make_shared<OwnedChainedBuffer>(local_hi - local_lo);
        std::memcpy(buf->data(), seg_bytes.data() + local_lo, local_hi - local_lo);
        result.append(ChainedBufferNode{buf, 0, local_hi - local_lo, seg_start + local_lo});
        return result;
    }

    CacheSegmentPin pin(size_t frontier) const override
    {
        const size_t seg = cache.segmentSize();
        const size_t idx = frontier / seg;
        const size_t dl = cache.downloaded.contains(idx) ? cache.downloaded[idx] : 0;
        if (dl == 0 || dl >= seg)
            return nullptr;   // nothing partial to pin
        return std::static_pointer_cast<void>(cache.livenessFor(idx));
    }

private:
    ByteRange aligned_range;
    size_t seg_idx;
    EvictableSegmentMockCache & cache;
    IntervalSet committed_ranges;
};

inline CacheViewPtr EvictableSegmentMockCache::planResidencyView(
    const StoredObject &, size_t, ByteRange range_in_file)
{
    auto view = std::make_unique<CacheView>();
    if (range_in_file.size == 0)
        return view;

    const size_t seg = segment_size;
    const size_t first = range_in_file.offset / seg;
    const size_t last = (range_in_file.end() - 1) / seg;
    for (size_t idx = first; idx <= last; ++idx)
    {
        const size_t seg_start = idx * seg;
        const size_t seg_end = seg_start + seg;
        const size_t dl = downloaded.contains(idx) ? downloaded[idx] : 0;

        if (dl >= seg)
        {
            const ByteRange hit{seg_start, seg};
            view->hit_entries.push_back(HitEntry{
                hit, std::make_unique<EvictableSegmentReadBuffer>(hit, idx, *this)});
        }
        else if (dl > 0)
        {
            const ByteRange hit{seg_start, dl};
            view->hit_entries.push_back(HitEntry{
                hit, std::make_unique<EvictableSegmentReadBuffer>(hit, idx, *this)});
            /// Miss is SEGMENT-aligned (head past the frontier, tail at the cell
            /// boundary) like a real cache, so the plan folds the whole touched cell
            /// even past the request -- this is what the executor's cell-aligned
            /// expansion relies on.
            const size_t miss_head = seg_start + dl;
            view->miss_entries.push_back(MissEntry{ByteRange{miss_head, seg_end - miss_head}, /*writer=*/nullptr});
        }
        else
        {
            view->miss_entries.push_back(MissEntry{ByteRange{seg_start, seg}, /*writer=*/nullptr});
        }
    }
    return view;
}

inline void EvictableSegmentMockCache::openWriteBuffers(
    const StoredObject &, size_t, CacheView & view)
{
    for (auto & entry : view.miss_entries)
    {
        /// Each miss cell lies within a single segment (one miss entry per segment
        /// in `planResidencyView`); derive its index from the offset.
        const size_t seg_idx = entry.range.offset / segment_size;
        ++open_count[seg_idx];
        entry.writer = std::make_unique<EvictableSegmentWriteBuffer>(entry.range, seg_idx, *this);
    }
}

} // anonymous namespace

/// T7's unified serve cycle: a HIT run whose plan-pinned view goes STALE (the mock's hit
/// views hold no liveness token, so an eviction sweep can drop a resident segment the plan
/// classified as a hit) must HEAL through the lane's bank - the shared `bankDirectRead` verb,
/// now job-independent - instead of returning an empty window the caller reads as EOF
/// mid-file. Warm segment 1, plan over both segments, evict between windows, read through.
TEST(ReaderExecutor, HitRunHealsStaleView)
{
    TestThreadGroup tg;

    const size_t seg = 4096;
    const size_t file = 2 * seg;
    String content(file, '\0');
    for (size_t i = 0; i < file; ++i)
        content[i] = static_cast<char>('a' + i % 26);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});
    StoredObjects objects;
    objects.emplace_back("file", "", file);

    auto cache = std::make_shared<EvictableSegmentMockCache>(seg);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    /// Warm segment 1 ([4096,8192)) so the main plan classifies it a HIT.
    {
        ReaderExecutor::Options warm_opts;
        warm_opts.window_size = seg;
        warm_opts.min_bytes_for_seek = 0;
        ReaderExecutor warmer(source, objects, caches, warm_opts);
        warmer.seek(seg);
        ASSERT_FALSE(warmer.readNextWindow().empty());
    }

    ReaderExecutor::Options opts;
    opts.window_size = seg;
    opts.min_bytes_for_seek = 0;
    opts.plan_look_ahead_max_window = file;   /// one plan: miss [0,4096) + hit [4096,8192)
    ReaderExecutor executor(source, objects, caches, opts);

    String got;
    const auto consume = [&](ChainedBuffers chain)
    {
        for (const auto & node : chain.getNodes())
            got.append(node.data(), node.size);
    };

    consume(executor.readNextWindow());   /// [0,4096): the miss; the plan (and hit view) built
    ASSERT_EQ(got.size(), seg);

    /// The sweep drops segment 1 - its plan-held VIEW carries no liveness token, so the
    /// bytes vanish under the hit classification (segment 0 survives: its writer pins).
    cache->evictUnpinned();

    consume(executor.readNextWindow());   /// [4096,8192): the stale hit - must heal, not EOF
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        consume(std::move(chain));
    }
    EXPECT_EQ(got, content) << "the stale hit run must heal through the bank, not truncate";
}

TEST(ReaderExecutor, SequentialMidReadEvictionHealsByRefetch)
{
    TestThreadGroup tg;

    /// One 4000-byte object = one 4000-byte cache segment, window 1000.
    String content(4000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 4000);

    auto cache = std::make_shared<EvictableSegmentMockCache>(4000);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    auto limit = std::make_shared<LongConnectionLimit>(10);

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 1000;
    executor_options.min_bytes_for_seek = 0;
    executor_options.long_connection_limit = limit;
    auto executor = std::make_unique<ReaderExecutor>(source, objects, caches, executor_options);

    String result;
    auto consume = [&](ChainedBuffers chain)
    {
        for (const auto & node : chain.getNodes())
            result.append(node.data(), node.size);
    };

    /// Window 1: [0,1000). The fetch extends to the CELL edge (cell-fill shaping), so
    /// the whole segment fills at once - complete, hence unpinned and evictable.
    auto w1 = executor->readNextWindow();
    ASSERT_FALSE(w1.empty());
    consume(std::move(w1));
    ASSERT_EQ(cache->downloaded[0], 4000u) << "the touched cell is fetched whole";

    /// Eviction pressure drops the COMPLETE (unpinned) cell mid-read; the remaining
    /// windows must heal by re-fetching, never truncate or serve stale bytes.
    cache->evictUnpinned();
    ASSERT_EQ(cache->downloaded[0], 0u) << "a complete cell is evictable";

    /// Drain the rest sequentially.
    while (true)
    {
        auto chain = executor->readNextWindow();
        if (chain.empty())
            break;
        consume(std::move(chain));
    }

    EXPECT_EQ(result, content);   /// no corruption / no missing bytes
    /// Destroy the executor so it flushes `stats` into the thread group's ProfileEvents.
    executor.reset();
}

TEST(ReaderExecutor, PrefetchConsumeRebuildsPinAcrossSegmentBoundary)
{
    TestThreadGroup tg;

    /// Windows >= 2 arrive via the machine COLLECT path, where the foreground
    /// rebuilds the Strategy-A pin under the new frontier. Two 2000-byte
    /// segments, window 1000: W3 is the first window of segment 1 - the collect
    /// must re-pin that fresh partial segment, or an eviction sweep right after
    /// drops it. The INLINE pool runs each worker (its inline cache write AND the
    /// next look-ahead) synchronously, so `downloaded[1]` is deterministic at the
    /// eviction point (with a real pool the look-ahead write may not have landed
    /// yet - the segment would be partially filled, which the pin also survives,
    /// but the byte assert would race).
    String content(4000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});
    StoredObjects objects;
    objects.emplace_back("file", "", 4000);

    auto cache = std::make_shared<EvictableSegmentMockCache>(2000);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    auto pool = std::make_shared<SyncPrefetchPool>();
    auto limit = std::make_shared<LongConnectionLimit>(10);
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 1000;
    executor_options.min_bytes_for_seek = 0;
    executor_options.prefetch_pool = pool;
    executor_options.long_connection_limit = limit;
    /// Pin the fill-ahead lead to one window so the prefetch advances window-by-window: this
    /// test validates the cross-segment-boundary pin rebuild on the COLLECT path, which needs a
    /// fresh PARTIAL segment 1 at W3 - the larger default lead would fetch the whole file at once.
    executor_options.fill_ahead_lead = 1000;
    auto executor = std::make_unique<ReaderExecutor>(source, objects, caches, executor_options);

    String result;
    auto consume = [&](ChainedBuffers chain)
    {
        for (const auto & node : chain.getNodes())
            result.append(node.data(), node.size);
    };

    /// W1 [0,1000) sync (no machine in flight yet) -> launches the machine for [1000,2000).
    consume(executor->readNextWindow());
    /// W2 [1000,2000) collect -> fills segment 0 to 2000 (full).
    consume(executor->readNextWindow());
    /// W3 [2000,3000) collect -> first window of segment 1: fills it to cwo=1000 (partial)
    /// and must RE-PIN it at collect; launches the machine for [3000,4000).
    consume(executor->readNextWindow());

    /// Evict everything unpinned. The collect pinned segment 1 at W3 when it was a fresh
    /// partial segment (cwo=1000); the look-ahead worker for [3000,4000) then filled its
    /// second half INLINE at launch (the worker writes its led segments on the fetch thread),
    /// so it is fully downloaded now - but the pin (held until the machine's reap) keeps it
    /// resident through the eviction sweep.
    cache->evictUnpinned();
    EXPECT_EQ(cache->downloaded[1], 2000u) << "consume-path pin did not protect the in-flight segment";

    /// Finish and verify no corruption.
    while (true)
    {
        auto chain = executor->readNextWindow();
        if (chain.empty())
            break;
        consume(std::move(chain));
    }
    EXPECT_EQ(result, content);
}

TEST(ReaderExecutor, SegmentFetchedOnceAcrossSmallWindows)
{
    TestThreadGroup tg;

    /// Segment-open-once holds regardless of how small the plan window is: even with
    /// the window PINNED to window_size (`plan_look_ahead_max_window == window_size`),
    /// the cell-aligned expansion folds each plan out to the touched segment boundary,
    /// so a 2000-byte segment spanning two 1000-byte windows is opened ONCE (its writer
    /// spans the whole cell) and fetched ONCE. Four 2000-byte segments (8000-byte
    /// object), window 1000: open_count == 1 per segment, BytesFromSource == file size.
    String content(8000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});
    StoredObjects objects;
    objects.emplace_back("file", "", 8000);

    auto cache = std::make_shared<EvictableSegmentMockCache>(2000);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    auto pool = std::make_shared<SyncPrefetchPool>();
    auto limit = std::make_shared<LongConnectionLimit>(10);
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 1000;
    executor_options.min_bytes_for_seek = 0;
    executor_options.prefetch_pool = pool;
    executor_options.long_connection_limit = limit;
    /// Fixed-small plan window: the ceiling collapses the generalized window to
    /// window_size, so every plan is exactly one window and each 2000-byte segment
    /// spans two of them.
    executor_options.plan_look_ahead_max_window = 1000;
    auto executor = std::make_unique<ReaderExecutor>(source, objects, caches, executor_options);

    String result;
    while (true)
    {
        auto chain = executor->readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            result.append(node.data(), node.size);
    }
    EXPECT_EQ(result, content);

    /// Each segment opened exactly once -- the plan folds to the cell boundary, so a
    /// segment never straddles a plan edge and its writer is never re-opened.
    for (size_t idx = 0; idx < 4; ++idx)
        EXPECT_EQ(cache->open_count[idx], 1u) << "segment " << idx << " open count";

    /// And fetched exactly once: the file is read from the source whole.
    executor.reset();
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorBytesFromSource), 8000)
        << "the source was re-read";
}

TEST(ReaderExecutor, PinReleasedOnSeek)
{
    String content(8000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});
    StoredObjects objects;
    objects.emplace_back("file", "", 8000);

    auto cache = std::make_shared<EvictableSegmentMockCache>(4000);  /// two segments
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    auto pool = std::make_shared<SyncPrefetchPool>();
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 1000;
    executor_options.min_bytes_for_seek = 0;
    executor_options.prefetch_pool = pool;
    executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
    /// One-window lead: the look-ahead machine enters segment 1 one window at a
    /// time, so the collect at the boundary pins a genuinely PARTIAL segment 1
    /// (an in-flight pin exists only while a cell is mid-fill - the pump's
    /// cursor fetches complete their cell and pin nothing).
    executor_options.fill_ahead_lead = 1000;
    ReaderExecutor executor(source, objects, caches, executor_options);

    String got;
    auto consume = [&](ChainedBuffers chain)
    {
        for (const auto & node : chain.getNodes())
            got.append(node.data(), node.size);
    };

    /// Consume through segment 0 and the first window of segment 1: the machine
    /// collect at the boundary pinned the in-flight partial segment 1.
    for (int i = 0; i < 5; ++i)
    {
        auto chain = executor.readNextWindow();
        ASSERT_FALSE(chain.empty());
        consume(std::move(chain));
    }
    EXPECT_EQ(got, content.substr(0, got.size()));
    cache->evictUnpinned();
    ASSERT_GT(cache->downloaded[1], 0u) << "in-flight partial segment 1 must be pinned";
    ASSERT_LT(cache->downloaded[1], 4000u) << "segment 1 must still be mid-fill for the pin to matter";

    executor.seek(0);                                     /// far seek: pin released
    cache->evictUnpinned();
    EXPECT_EQ(cache->downloaded[1], 0u)
        << "pin should be released on seek, allowing eviction of segment 1";

    auto chain = executor.readNextWindow();                /// [0,1000) re-fetches
    ASSERT_FALSE(chain.empty());
    String after;
    for (const auto & node : chain.getNodes())
        after.append(node.data(), node.size);
    EXPECT_EQ(after, content.substr(0, after.size()));
}

TEST(ReaderExecutor, PutFailedTakesNoPin)
{
    String content(4000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});
    StoredObjects objects;
    objects.emplace_back("file", "", 4000);

    auto cache = std::make_shared<EvictableSegmentMockCache>(4000);
    cache->reject_put[0] = true;            /// segment 0 never accepts writes
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 1000;
    executor_options.min_bytes_for_seek = 0;
    executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
    ReaderExecutor executor(source, objects, caches, executor_options);

    auto chain = executor.readNextWindow();   /// [0,1000)
    ASSERT_FALSE(chain.empty());
    String got;
    for (const auto & node : chain.getNodes())
        got.append(node.data(), node.size);
    EXPECT_EQ(got, content.substr(0, got.size()));   /// data still correct from source
    EXPECT_FALSE(cache->liveness.contains(0));        /// nothing downloaded -> no pin token
}

TEST(ReaderExecutor, TransientReadDoesNotPin)
{
    String content(4000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});
    StoredObjects objects;
    objects.emplace_back("file", "", 4000);

    auto cache = std::make_shared<EvictableSegmentMockCache>(4000);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 1000;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(source, objects, caches, executor_options);
    auto transient = executor.makeTransientForReadAt(0, /*read_size=*/4000);
    ASSERT_TRUE(transient != nullptr);
    auto chain = transient->readNextWindow();
    ASSERT_FALSE(chain.empty());

    /// A `readBigAt` transient does not pin its in-flight segment (it reads its
    /// bounded extent once and is destroyed, so protecting a partial segment serves
    /// nothing), so nothing survives an eviction sweep.
    cache->evictUnpinned();
    EXPECT_EQ((cache->downloaded.contains(0) ? cache->downloaded[0] : 0u), 0u);
}

namespace
{

/// Source whose buffers report right-bounded reads and record (and honor) the
/// right bound requested via setReadUntilPosition, so a test can assert how the
/// executor bounds a source read.
struct BoundLog
{
    std::vector<std::optional<size_t>> read_until;   /// per open() (nullopt = open-ended)
    std::vector<size_t> start_offset;                /// per open() (seek target)
};

class BoundRecordingBuffer : public ReadBufferFromFileBase
{
public:
    BoundRecordingBuffer(const String & data_, BoundLog & log_, size_t idx_)
        : ReadBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0), data(data_), log(log_), idx(idx_) {}

    String getFileName() const override { return "BoundRecordingBuffer"; }
    bool supportsRightBoundedReads() const override { return true; }
    void setReadUntilPosition(size_t p) override { read_until = p; log.read_until[idx] = p; }

    off_t seek(off_t off, int whence) override
    {
        if (whence == SEEK_SET)
            file_offset = static_cast<size_t>(off);
        else if (whence == SEEK_CUR)
            file_offset += static_cast<size_t>(off);
        log.start_offset[idx] = file_offset;
        resetWorkingBuffer();
        return static_cast<off_t>(file_offset);
    }

    off_t getPosition() override { return static_cast<off_t>(file_offset); }
    size_t getFileOffsetOfBufferEnd() const override { return file_offset; }

private:
    bool nextImpl() override
    {
        const size_t end = read_until ? std::min(*read_until, data.size()) : data.size();
        if (file_offset >= end)
            return false;
        const size_t n = std::min(end - file_offset, internal_buffer.size());
        memcpy(internal_buffer.begin(), data.data() + file_offset, n);
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + n);
        file_offset += n;
        return true;
    }

    String data;
    BoundLog & log;
    size_t idx;
    size_t file_offset = 0;
    std::optional<size_t> read_until;
};

class BoundRecordingSource : public IFileBasedSourceReader
{
public:
    BoundRecordingSource(std::unordered_map<String, String> data_, BoundLog & log_)
        : data(std::move(data_)), log(log_) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override
    {
        auto it = data.find(object.remote_path);
        if (it == data.end())
            return nullptr;
        const size_t idx = log.read_until.size();
        log.read_until.emplace_back(std::nullopt);
        log.start_offset.emplace_back(0);
        return std::make_unique<BoundRecordingBuffer>(it->second, log, idx);
    }

    String name() const override { return "BoundRecordingSource"; }

private:
    std::unordered_map<String, String> data;
    BoundLog & log;
};

}

TEST(ReaderExecutor, ReadBigAtBoundsConnectionToRequest)
{
    /// `readBigAt` drives a `makeTransientForReadAt` transient over a bounded extent. A
    /// transient never takes a live-connection lease (it is a one-shot, not a wide
    /// sequential scan), so it opens a STATELESS connection - but that one-shot must
    /// still be bounded to the request [offset, offset+want) (object-local), so the
    /// borrowed HTTP connection is fully drained and returned to the pool reusable
    /// rather than abandoned open-ended after the request's bytes.
    const size_t offset = 4096;
    const size_t want = 8192;

    BoundLog log;
    auto source = std::make_shared<BoundRecordingSource>(
        std::unordered_map<String, String>{{"obj", String(1u << 20, 'x')}}, log);   // 1 MiB
    StoredObjects objects;
    objects.emplace_back("obj", "", 1u << 20);

    auto limit = std::make_shared<LongConnectionLimit>(10);   // a unit is free, but a transient never takes one
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 64u << 10;
    executor_options.min_bytes_for_seek = 0;
    executor_options.long_connection_limit = limit;
    ReaderExecutor executor(source, objects, {}, executor_options);

    auto transient = executor.makeTransientForReadAt(offset, want);

    size_t total = 0;
    while (total < want)
    {
        auto chain = transient->readNextWindow();
        if (chain.empty())
            break;
        total += chain.range().size;
    }

    EXPECT_EQ(total, want) << "the transient reads exactly the requested extent";
    ASSERT_FALSE(log.read_until.empty());
    EXPECT_EQ(log.start_offset[0], offset);
    ASSERT_TRUE(log.read_until[0].has_value()) << "the one-shot connection must be right-bounded, not open-ended";
    EXPECT_EQ(*log.read_until[0], offset + want) << "bounded to the request extent (object-local coordinates)";
}

TEST(ReaderExecutor, ReadBigAtBoundsLongConnectionOnEncryptedFile)
{
    /// Encrypted readBigAt over the live path. Inside readFromSource the
    /// `file_pos` parameter is a physical (header-inclusive) offset, so the
    /// live-connection bound must be in object-local physical coordinates and
    /// include data_start_offset. A bound short by data_start_offset truncates the
    /// read and throws CANNOT_READ_ALL_DATA - this is the regression test for that
    /// coordinate-space bug (the unencrypted test cannot catch it).
    String key(16, 'k');
    FileEncryption::InitVector iv(UInt128{0x0123456789abcdefULL});
    const size_t header_size = 64;   // one AES_128_CTR header == data_start_offset

    String plaintext(64u << 10, '\0');
    for (size_t i = 0; i < plaintext.size(); ++i)
        plaintext[i] = static_cast<char>((i * 31 + 7) & 0xFF);
    String file_bytes = makeEncryptedFile(key, iv, plaintext);   // header(64) + ciphertext

    BoundLog log;
    auto source = std::make_shared<BoundRecordingSource>(
        std::unordered_map<String, String>{{"obj", file_bytes}}, log);
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    auto limit = std::make_shared<LongConnectionLimit>(10);   // slot available -> live path
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 256u << 10;
    executor_options.min_bytes_for_seek = 0;
    executor_options.long_connection_limit = limit;
    ReaderExecutor executor(source, objects, {}, executor_options);
    executor.addDecryptionLayer("/test", [&](UInt128, const String &) { return key; });
    executor.initDecryption();   // parses the header -> data_start_offset = 64

    const size_t offset = 4096;   // logical
    const size_t want = 8192;     // logical bytes
    const size_t open_index = log.read_until.size();   // first transient open is the next one

    auto transient = executor.makeTransientForReadAt(offset, want);

    size_t total = 0;
    String got;
    while (total < want)
    {
        auto chain = transient->readNextWindow();
        if (chain.empty())
            break;
        for (const auto & n : chain.getNodes())
        {
            got.append(n.data(), n.size);
            total += n.size;
        }
    }

    EXPECT_EQ(total, want) << "the encrypted transient reads the full extent (no short read / CANNOT_READ_ALL_DATA)";
    EXPECT_EQ(got, plaintext.substr(offset, want)) << "decrypted bytes match the plaintext slice";
    ASSERT_GT(log.read_until.size(), open_index);
    EXPECT_EQ(log.start_offset[open_index], offset + header_size);
    ASSERT_TRUE(log.read_until[open_index].has_value()) << "the live connection must be right-bounded";
    EXPECT_EQ(*log.read_until[open_index], offset + header_size + want)
        << "object-local physical bound includes data_start_offset (the encryption header)";
}

TEST(ReaderExecutor, ReadBigAtBoundsLongConnectionToObjectEndAcrossBoundary)
{
    /// A readBigAt extent that straddles two objects on the live path: each
    /// connection is per object, so the non-tail object's connection must be
    /// bounded to its own end (not past it, which would leave it abandoned),
    /// while the tail object is bounded to the extent end.
    const size_t s0 = 100u << 10;   // obj0 = 100 KiB
    const size_t s1 = 100u << 10;   // obj1 = 100 KiB
    BoundLog log;
    auto source = std::make_shared<BoundRecordingSource>(
        std::unordered_map<String, String>{{"o0", String(s0, 'a')}, {"o1", String(s1, 'b')}}, log);
    StoredObjects objects;
    objects.emplace_back("o0", "", s0);
    objects.emplace_back("o1", "", s1);

    auto limit = std::make_shared<LongConnectionLimit>(10);   // slot available -> live path on the first object
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 1u << 20;
    executor_options.min_bytes_for_seek = 0;
    executor_options.long_connection_limit = limit;
    ReaderExecutor executor(source, objects, {}, executor_options);

    const size_t offset = 90u << 10;   // 90 KiB into o0
    const size_t want = 50u << 10;     // ends at 140 KiB -> 40 KiB into o1
    auto transient = executor.makeTransientForReadAt(offset, want);

    size_t total = 0;
    while (total < want)
    {
        auto chain = transient->readNextWindow();
        if (chain.empty())
            break;
        total += chain.range().size;
    }

    EXPECT_EQ(total, want);
    ASSERT_GE(log.read_until.size(), 2u) << "one open per object piece";
    ASSERT_TRUE(log.read_until[0].has_value());
    EXPECT_EQ(*log.read_until[0], s0) << "o0 connection bounded to its own end, not past it to the extent end";
    ASSERT_TRUE(log.read_until[1].has_value());
    EXPECT_EQ(*log.read_until[1], want - (s0 - offset))
        << "o1 connection bounded to exactly its piece of the request (object-local): a "
           "transient never streams past what was asked, on any object piece";
}

TEST(ReaderExecutor, UnknownSizeStatelessReaderBoundsOneShotToExtent)
{
    /// The no-slot one-shot branch: an unknown-size source with a finite advertised
    /// extent but no available LongConnectionLimit slot must still bound each one-shot
    /// connection (to what it reads), so it drains and is returned to the pool
    /// reusable instead of an open-ended GET abandoned after the clamped read.
    /// Before the fix the bound was skipped whenever the size was unknown, even with
    /// a concrete extent, leaving these connections open-ended under slot pressure.
    const size_t data_size = 1u << 20;   // 1 MiB available at the source
    const size_t extent = 200u << 10;    // consumer reads only 200 KiB

    BoundLog log;
    auto source = std::make_shared<BoundRecordingSource>(
        std::unordered_map<String, String>{{"obj", String(data_size, 'z')}}, log);
    StoredObjects objects;
    objects.emplace_back("obj", "", StoredObject::UnknownSize);

    /// No `long_connection_limit` in Options -> no slot -> the stateless one-shot path.
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 64u << 10;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(source, objects, {}, executor_options);
    executor.setReadExtent(extent);

    size_t total = 0;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        total += chain.range().size;
    }

    EXPECT_EQ(total, extent) << "the unknown-size stateless reader stops at the advertised extent";
    ASSERT_FALSE(log.read_until.empty());
    for (size_t i = 0; i < log.read_until.size(); ++i)
        EXPECT_TRUE(log.read_until[i].has_value())
            << "one-shot open #" << i << " must be right-bounded (finite extent advertised), not open-ended";
}

TEST(ReaderExecutor, ReadBigAtTransientStatsRollUpToParent)
{
    /// ProfileEvents are emitted at the read site (instant), so a `readBigAt` transient's
    /// source reads show up immediately in the current thread group - even before the
    /// transient is destroyed. `mergeTransientStats` then rolls its stats into the parent's
    /// REPORT aggregate (the `reader_executor_log` row), which is a separate sink and does
    /// NOT re-emit - so random-access reads are visible AND never double-counted.
    TestThreadGroup tg;
    String content(4000, 'B');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 4000);

    ReaderExecutor::Options parent_options;
    parent_options.window_size = 1000;
    parent_options.min_bytes_for_seek = 0;
    auto parent = std::make_unique<ReaderExecutor>(source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, parent_options);

    {
        auto transient = parent->makeTransientForReadAt(0, /*read_size=*/4000);
        while (true)
        {
            auto chain = transient->readNextWindow();
            if (chain.empty())
                break;
        }
        /// The transient already emitted its source reads to ProfileEvents at the read
        /// site - observable now, while it is still alive.
        EXPECT_GT(tg.get(ProfileEvents::ReaderExecutorSourceRequests), 0u)
            << "a transient emits its ProfileEvents at the read site (instant)";
        EXPECT_GT(tg.get(ProfileEvents::ReaderExecutorBytesFromSource), 0)
            << "a transient emits its ProfileEvents at the read site (instant)";
        parent->mergeTransientStats(*transient);
        /// `transient` is destroyed here.
    }

    const auto src_after_transient = tg.get(ProfileEvents::ReaderExecutorSourceRequests);
    const auto bytes_after_transient = tg.get(ProfileEvents::ReaderExecutorBytesFromSource);

    parent.reset();   /// parent destruction writes its report aggregate; it must NOT re-emit
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorSourceRequests), src_after_transient)
        << "mergeTransientStats feeds the parent's report aggregate, not ProfileEvents - no double-count";
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorBytesFromSource), bytes_after_transient)
        << "the parent's destruction does not re-emit the rolled-up source bytes";
}

TEST(LongConnectionLimit, MoveAssignReleasesPreviousSlot)
{
    /// Move-assignment must release the currently-held unit BEFORE taking `other`'s,
    /// else a unit of capacity leaks permanently.
    auto limit = std::make_shared<LongConnectionLimit>(2);

    auto a = limit->tryAcquire(limit);
    auto b = limit->tryAcquire(limit);
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    EXPECT_EQ(limit->getActiveCount(), 2u);

    /// Move-assign `b` into `a` — a's unit must come back to the counter.
    a = std::move(b);
    EXPECT_EQ(limit->getActiveCount(), 1u);

    a = {};
    EXPECT_EQ(limit->getActiveCount(), 0u);
}

TEST(LongConnectionLimit, MoveAssignFromEmptyLeaseReleasesCurrent)
{
    /// Assigning an empty lease into a holding one must still release the current unit.
    auto limit = std::make_shared<LongConnectionLimit>(2);

    auto a = limit->tryAcquire(limit);
    ASSERT_TRUE(a);
    EXPECT_EQ(limit->getActiveCount(), 1u);

    LongConnectionSlot empty;          // holds no unit
    a = std::move(empty);            // must drop a's unit
    EXPECT_FALSE(a);
    EXPECT_EQ(limit->getActiveCount(), 0u);
}

TEST(LongConnectionLimit, SelfMoveAssignIsNoOp)
{
    /// Self-assignment must not double-release.
    auto limit = std::make_shared<LongConnectionLimit>(1);
    auto s = limit->tryAcquire(limit);
    ASSERT_TRUE(s);
    EXPECT_EQ(limit->getActiveCount(), 1u);

    auto * self = &s;                // via pointer so the compiler doesn't flag self-move
    s = std::move(*self);
    EXPECT_EQ(limit->getActiveCount(), 1u);
}

TEST(ReaderExecutor, UnknownSizeStreamsToEof)
{
    /// When `StoredObject::bytes_size == UnknownSize`,
    /// `OffsetMap::hasUnknownSize` is true and the executor switches to
    /// streaming-until-EOF: it reads `window_size` bytes at a time from
    /// the source and detects EOF when the source returns short. The
    /// source itself (`MemorySourceReader` backed by a temp file) knows
    /// the real size; only the executor's view is unknown.
    String content(1500, 'U');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", StoredObject::UnknownSize);

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 500;
    ReaderExecutor executor(source, objects, {}, executor_options);

    String collected;
    while (true)
    {
        ChainedBuffers w = executor.readNextWindow();
        if (w.empty())
            break;
        for (const auto & node : w.getNodes())
            collected.append(node.data(), node.size);
    }
    EXPECT_EQ(collected.size(), content.size());
    EXPECT_EQ(collected, content);
}

TEST(ReaderExecutor, UnknownSizeEofIsLatchedUntilSeek)
{
    /// After the source returns short and EOF is latched, subsequent
    /// `readNextWindow` calls keep returning empty without re-hitting
    /// the source. A backward `seek` clears the latch so reads resume.
    String content(600, 'L');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", StoredObject::UnknownSize);

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 1000;
    ReaderExecutor executor(source, objects, {}, executor_options);

    auto r1 = executor.readNextWindow();   /// reads all 600 bytes, latches EOF
    EXPECT_EQ(r1.range().size, 600u);

    /// Latched: stays empty without re-reading.
    EXPECT_TRUE(executor.readNextWindow().empty());
    EXPECT_TRUE(executor.readNextWindow().empty());

    /// Seek back to position 0 — latch cleared, reads resume.
    executor.seek(0);
    auto r2 = executor.readNextWindow();
    EXPECT_EQ(r2.range().size, 600u);
    EXPECT_EQ(r2.range().offset, 0u);
}

TEST(ReaderExecutor, UnknownSizeZeroByteTerminalRead)
{
    /// Unknown-size source whose size is an exact multiple of the window size,
    /// so the terminal read returns 0 bytes: `readNextWindow` returns an empty
    /// chain and the caller stops, never making the follow-up call that would hit
    /// the pre-read EOF gate. All bytes up to that zero-byte terminal must still
    /// be served correctly.
    String content(1000, 'E');   /// exactly 2 * window
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", StoredObject::UnknownSize);

    auto limit = std::make_shared<LongConnectionLimit>(10);
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 500;
    executor_options.long_connection_limit = limit;
    ReaderExecutor executor(source, objects, {}, executor_options);

    String collected;
    while (true)
    {
        ChainedBuffers w = executor.readNextWindow();
        if (w.empty())
            break;
        for (const auto & node : w.getNodes())
            collected.append(node.data(), node.size);
    }
    EXPECT_EQ(collected, content);
}

TEST(ReaderExecutor, UnknownSizeMultiObjectRejected)
{
    /// Multi-object pipelines need each object's `bytes_size` to compute
    /// the cumulative `file_offset`. With an unknown size we can't.
    /// `OffsetMap::build` rejects the combination outright.
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"a", "AA"},
            {"b", "BB"},
        });

    StoredObjects objects;
    objects.emplace_back("a", "", StoredObject::UnknownSize);
    objects.emplace_back("b", "", 2);

    EXPECT_ANY_THROW({
        ReaderExecutor::Options executor_options;
        executor_options.window_size = 100;
        ReaderExecutor executor(source, objects, {}, executor_options);
    });
}

namespace
{

/// Mock cache that reports hits/misses at FULL block granularity, matching the
/// production behaviour of `PageCacheProvider` and `DiskCacheProvider`. A clipped
/// per-request view would hide the cache-vs-cache overlap problem these tests target.

/// Held read buffer over a run of resident FULL blocks. `read(sub)` clamps to its own
/// range (shared-storage safety) and assembles the overlapping stored blocks, so it
/// covers `sub` exactly.
class WideGranularityReadBuffer : public CacheReader
{
public:
    WideGranularityReadBuffer(ByteRange range_in_file, std::unordered_map<size_t, String> & storage_, size_t block_size_)
        : range_member(range_in_file), storage(storage_), block_size(block_size_) {}

    ByteRange range() const override { return range_member; }
    ChainedBuffers read(ByteRange sub) override
    {
        ChainedBuffers result;
        const size_t lo = std::max(sub.offset, range_member.offset);
        const size_t hi = std::min(sub.end(), range_member.end());
        if (lo >= hi)
            return result;

        ChainedBuffers assembled;
        const size_t first_block = lo / block_size;
        const size_t last_block = (hi - 1) / block_size;
        for (size_t b = first_block; b <= last_block; ++b)
        {
            auto it = storage.find(b);
            if (it == storage.end())
                continue;
            const auto & data = it->second;
            auto buf = std::make_shared<OwnedChainedBuffer>(data.size());
            std::memcpy(buf->data(), data.data(), data.size());
            assembled.append(ChainedBufferNode{buf, 0, data.size(), b * block_size});
        }
        return assembled.slice(ByteRange{lo, hi - lo});
    }

private:
    ByteRange range_member;
    std::unordered_map<size_t, String> & storage;
    size_t block_size;
};

/// Held write buffer over a block-aligned miss range for `WideGranularityMockCache`.
/// `write` stores FULL blocks (first-writer-wins), logs each stored block to `put_log`
/// with `(block_range, slice.totalBytes())` (always disjoint, so total == range.size),
/// and advances `committed` per block (even on a first-writer-wins loss, so `complete`
/// converges). A block `data` does not fully cover is left for a later window.
class WideGranularityWriteBuffer : public CacheWriter
{
public:
    WideGranularityWriteBuffer(
        ByteRange aligned_range,
        std::unordered_map<size_t, String> & storage_,
        std::vector<std::pair<ByteRange, size_t>> & put_log_,
        size_t block_size_)
        : range_member(aligned_range), storage(storage_), put_log(put_log_), block_size(block_size_) {}

    ByteRange range() const override { return range_member; }
    IntervalSet committed() const override { return committed_ranges; }

    size_t write(ChainedBuffers data) override
    {
        if (data.empty())
            return 0;
        size_t bytes_written = 0;
        const ByteRange dr = data.range();
        for (size_t offset = range_member.offset; offset < range_member.end(); offset += block_size)
        {
            const size_t b = offset / block_size;
            const ByteRange block_range{offset, block_size};

            if (committed_ranges.subtract(block_range).empty())
                continue;  /// block already fully written

            /// The part of this block `data` carries.
            const size_t lo = std::max(block_range.offset, dr.offset);
            const size_t hi = std::min(block_range.end(), dr.end());
            if (lo >= hi)
                continue;

            /// Accumulate incrementally, like a real `FileSegment` filled across several
            /// writes: stage the covered bytes in a per-block buffer and commit the block
            /// to `storage` only once every byte has arrived. A whole-block cell split
            /// across machine windows still completes.
            String & buf = pending.try_emplace(b, String(block_size, '\0')).first->second;
            for (const auto & sub : committed_ranges.subtract(ByteRange{lo, hi - lo}))
            {
                if (!data.covers(sub))
                    continue;
                data.slice(sub).copyTo(buf.data() + (sub.offset - block_range.offset), sub);
                committed_ranges.add(sub);
                bytes_written += sub.size;
            }
            if (committed_ranges.subtract(block_range).empty())
            {
                put_log.emplace_back(block_range, block_size);
                storage[b] = std::move(buf);
                pending.erase(b);
            }
        }
        return bytes_written;
    }

    ChainedBuffers read(ByteRange sub) override
    {
        ChainedBuffers result;
        const size_t lo = std::max(sub.offset, range_member.offset);
        const size_t hi = std::min(sub.end(), range_member.end());
        if (lo >= hi)
            return result;

        ChainedBuffers assembled;
        const size_t first_block = lo / block_size;
        const size_t last_block = (hi - 1) / block_size;
        for (size_t b = first_block; b <= last_block; ++b)
        {
            auto it = storage.find(b);
            if (it == storage.end())
                continue;
            const auto & data = it->second;
            auto buf = std::make_shared<OwnedChainedBuffer>(data.size());
            std::memcpy(buf->data(), data.data(), data.size());
            assembled.append(ChainedBufferNode{buf, 0, data.size(), b * block_size});
        }
        return assembled.slice(ByteRange{lo, hi - lo});
    }

private:
    ByteRange range_member;
    std::unordered_map<size_t, String> & storage;
    std::vector<std::pair<ByteRange, size_t>> & put_log;
    size_t block_size;
    IntervalSet committed_ranges;
    /// Per-block staging buffers for bytes that arrived but have not yet completed the
    /// whole block (incremental fill across multiple `write` calls).
    std::unordered_map<size_t, String> pending;
};

class WideGranularityMockCache : public ICacheProvider
{
public:
    WideGranularityMockCache(size_t block_size_, String name_)
        : block_size(block_size_), provider_name(std::move(name_)) {}

    String name() const override { return provider_name; }
    CacheTier tier() const override { return CacheTier::FilesystemCache; }

    /// Read-only residency probe at FULL block granularity: a block IS the write
    /// unit (`seedBlock`/`hasBlock` operate on whole blocks), so each miss range
    /// is ONE block - a touch fetches the whole block it lands in (cell-edge
    /// shaping). Hits coalesce adjacent blocks into one entry; never mutates the
    /// store.
    CacheViewPtr planResidencyView(const StoredObject &, size_t, ByteRange range_in_file) override
    {
        auto view = std::make_unique<CacheView>();
        if (range_in_file.size == 0)
            return view;

        const size_t start_block = range_in_file.offset / block_size;
        const size_t end_block = (range_in_file.end() + block_size - 1) / block_size;

        bool run_active = false;
        ByteRange run_range{0, 0};
        auto flush_hit_run = [&]()
        {
            if (!run_active)
                return;
            view->hit_entries.push_back(HitEntry{
                run_range, std::make_unique<WideGranularityReadBuffer>(run_range, storage, block_size)});
            run_active = false;
        };

        for (size_t b = start_block; b < end_block; ++b)
        {
            const ByteRange block_range{b * block_size, block_size};
            if (storage.contains(b))
            {
                if (!run_active)
                {
                    run_active = true;
                    run_range = block_range;
                }
                else
                    run_range.size = block_range.end() - run_range.offset;
            }
            else
            {
                flush_hit_run();
                view->miss_entries.push_back(MissEntry{block_range, /*writer=*/nullptr});
            }
        }
        flush_hit_run();
        return view;
    }

    void openWriteBuffers(const StoredObject &, size_t, CacheView & view) override
    {
        for (auto & entry : view.miss_entries)
            entry.writer = std::make_unique<WideGranularityWriteBuffer>(entry.range, storage, put_log, block_size);
    }

    bool hasBlock(size_t block_index) const { return storage.contains(block_index); }

    /// (range argument to put, totalBytes of chain argument to put)
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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 128 * 1024;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(src, objects, {page_cache, disk_cache}, executor_options);

    auto chain = executor.readNextWindow();
    EXPECT_EQ(chain.range().size, 128u * 1024u);
    EXPECT_EQ(chain.totalBytes(), 128u * 1024u);
}

/// THE BUG: PageCache (64K blocks) hits block 0, misses block 1. DiskCache
/// (4M blocks) holds the whole 4M segment, so for the chain's lookup of
/// [64K, 128K) it reports a hit at [0, 4M) — covering bytes already in
/// PageCache. Returned chain today has duplicate coverage.
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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 128 * 1024;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(src, objects, {page_cache, disk_cache}, executor_options);

    auto chain = executor.readNextWindow();
    EXPECT_EQ(chain.range().size, 128u * 1024u);
    EXPECT_EQ(chain.totalBytes(), 128u * 1024u)
        << "Returned chain must not contain duplicate coverage from cache-vs-cache overlap";
}

/// Three caches with 64K / 1M / 4M granularities. PageCache hits, MidCache
/// hit covers PageCache, DiskCache not reached. Returned chain must be
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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 128 * 1024;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(src, objects, {page_cache, mid_cache, disk_cache}, executor_options);

    auto chain = executor.readNextWindow();
    EXPECT_EQ(chain.range().size, 128u * 1024u);
    EXPECT_EQ(chain.totalBytes(), 128u * 1024u);
}

/// PageCache hits the prefix, DiskCache is cold. The prefix is streamed from
/// PageCache (not promoted into DiskCache); the cold tail is a gap whose
/// block-aligned miss over-reads and fills the whole DiskCache segment. The
/// plan-centric path serves the resident prefix and the gap in separate
/// `readNextWindow` calls, so drain the scan.
TEST(ReaderExecutor, ChainLowerCacheFilledFullyAfterRead)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(4 * 1024 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 4 * 1024 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(4 * 1024 * 1024, "DiskMock");
    page_cache->seedBlock(0, 'P');

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 128 * 1024;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(src, objects, {page_cache, disk_cache}, executor_options);

    size_t total = 0;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.range().size == 0)
            break;
        total += chain.range().size;
    }
    EXPECT_EQ(total, 4u * 1024 * 1024);
    EXPECT_TRUE(disk_cache->hasBlock(0))
        << "DiskCache must be filled with the full segment after the read (gap over-read)";
}

/// Every write across the chain must receive a chain with disjoint coverage —
/// totalBytes == range.size. A chain with duplicate nodes would overflow
/// the write buffer's flat copy.
TEST(ReaderExecutor, ChainPutReceivesDisjointChain)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(4 * 1024 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 4 * 1024 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(4 * 1024 * 1024, "DiskMock");
    page_cache->seedBlock(0, 'P');

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 128 * 1024;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(src, objects, {page_cache, disk_cache}, executor_options);

    /// Drain: the resident prefix and the cold gap are served in separate calls;
    /// the gap's backfill is the put we are checking.
    while (executor.readNextWindow().range().size != 0)
        ;

    ASSERT_FALSE(disk_cache->putLog().empty()) << "DiskCache put must be called";
    for (const auto & [range, total] : disk_cache->putLog())
        EXPECT_EQ(total, range.size)
            << "put received non-disjoint chain: range=[" << range.offset
            << ", " << range.end() << "), totalBytes=" << total;
}

/// Cache block extends past the window's tail. Returned chain must end at
/// the window boundary, not at the block boundary.
TEST(ReaderExecutor, ChainHitExtendsBeyondWindowEnd)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(128 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 128 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    page_cache->seedBlock(0, 'P');

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 50 * 1024;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(src, objects, {page_cache}, executor_options);

    auto chain = executor.readNextWindow();
    EXPECT_EQ(chain.range().offset, 0u);
    EXPECT_EQ(chain.range().size, 50u * 1024u);
    EXPECT_EQ(chain.totalBytes(), 50u * 1024u);
}

/// Window starts inside a cache block. Bytes before window.offset must not
/// appear in the returned chain.
TEST(ReaderExecutor, ChainHitExtendsBeforeWindowStart)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(128 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 128 * 1024);

    auto page_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "PageMock");
    page_cache->seedBlock(0, 'P');

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 40 * 1024;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(src, objects, {page_cache}, executor_options);

    executor.seek(10 * 1024);
    auto chain = executor.readNextWindow();
    EXPECT_EQ(chain.range().offset, 10u * 1024u);
    EXPECT_EQ(chain.range().size, 40u * 1024u);
    EXPECT_EQ(chain.totalBytes(), 40u * 1024u);
}

/// Cache miss range extends past the window end (cache block is larger than
/// the window's tail). Source fetch goes past the window to fill the block;
/// cache stores the full block; user chain is exactly window size.
TEST(ReaderExecutor, ChainWindowEndCacheMissExtendsPast)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(128 * 1024, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 128 * 1024);

    auto disk_cache = std::make_shared<WideGranularityMockCache>(64 * 1024, "DiskMock");

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 50 * 1024;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(src, objects, {disk_cache}, executor_options);

    auto chain = executor.readNextWindow();
    EXPECT_EQ(chain.range().size, 50u * 1024u);
    EXPECT_EQ(chain.totalBytes(), 50u * 1024u);
    EXPECT_TRUE(disk_cache->hasBlock(0))
        << "Cache block must be filled past the window end (intentional read-ahead)";
}

/// Cache-range pruning: a lower-tier miss cell that a faster tier already holds in FULL is
/// dropped at plan build - no writer is opened, so the lower tier is not filled for it (the
/// read is served from the faster tier). Page block == disk block (64K), so the page hit
/// covers the whole disk cell `[0,64K)`. Without pruning the page bytes would be promoted
/// down and `hasBlock(0)` would be true; with pruning the disk cell stays empty.
TEST(ReaderExecutor, PrunesLowerTierMissCoveredByFasterTier)
{
    const size_t block = 64 * 1024;
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(block, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", block);

    auto page_cache = std::make_shared<WideGranularityMockCache>(block, "PageMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(block, "DiskMock");
    page_cache->seedBlock(0, 'P');  // page holds the whole disk cell [0,64K)

    ReaderExecutor::Options executor_options;
    executor_options.window_size = block;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(src, objects, {page_cache, disk_cache}, executor_options);

    size_t total = 0;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.range().size == 0)
            break;
        total += chain.range().size;
    }
    EXPECT_EQ(total, block) << "the page hit must still serve the data";
    EXPECT_FALSE(disk_cache->hasBlock(0))
        << "the disk cell fully covered by the page hit must be pruned, not filled";
}

/// Proactive fill down to the deepest tier, across an EMBEDDED faster-tier hit. Disk cell
/// is 4K; page block is 1K and holds [1K,2K) inside that cell; disk is cold. Read [1K,4K)
/// (seek to 1K). The [2K,4K) no-tier gap's fetch aligns to the disk cell [0,4K), reaching
/// LEFT past the seek position to the cell floor, so the disk cell fills WHOLE: [0,1K)
/// over-read from source (left of the request), [1K,2K) READ THROUGH from the source (the
/// page hit serves the user but is never written down), [2K,4K) from source. The client
/// still gets only [1K,4K).
TEST(ReaderExecutor, ProactivelyFillsLowerCellAcrossEmbeddedFasterHit)
{
    const size_t page_block = 1024;
    const size_t disk_block = 4096;
    const size_t file = 4096;
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(file, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file);

    auto page_cache = std::make_shared<WideGranularityMockCache>(page_block, "PageMock");
    auto disk_cache = std::make_shared<WideGranularityMockCache>(disk_block, "DiskMock");
    page_cache->seedBlock(1, 'P');  // page holds [1K,2K), embedded in the disk cell [0,4K)

    ReaderExecutor::Options executor_options;
    executor_options.window_size = file;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(src, objects, {page_cache, disk_cache}, executor_options);
    executor.seek(page_block);  // request starts at 1K

    size_t delivered = 0;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.range().size == 0)
            break;
        delivered += chain.range().size;
    }
    EXPECT_EQ(delivered, file - page_block) << "client gets only the requested [1K,4K)";
    EXPECT_TRUE(disk_cache->hasBlock(0))
        << "the whole disk cell [0,4K) must fill - incl. the [0,1K) prefix left of the seek";
}

namespace
{
    /// Records every cache access (each `planResidencyView` probe) so tests can assert
    /// which `StoredObject` and `object_file_offset` the cache provider received per
    /// piece. The view always reports the whole range as a single MISS so the executor
    /// falls through to the source — keeps the data path simple.
    struct TrackedLookup
    {
        String remote_path;
        size_t object_file_offset;
        ByteRange range_in_file;
    };

    /// No-op write buffer: never commits (the source bytes are not cached).
    /// `write`/`read` are no-ops.
    class TrackingWriteBuffer : public CacheWriter
    {
    public:
        explicit TrackingWriteBuffer(ByteRange aligned_range_) : aligned_range(aligned_range_) {}
        ByteRange range() const override { return aligned_range; }
        IntervalSet committed() const override { return committed_ranges; }
        size_t write(ChainedBuffers) override { return 0; }
        ChainedBuffers read(ByteRange) override { return {}; }
    private:
        ByteRange aligned_range;
        IntervalSet committed_ranges;
    };

    class TrackingCacheProvider : public ICacheProvider
    {
    public:
        String name() const override { return "Tracking"; }
        CacheTier tier() const override { return CacheTier::FilesystemCache; }

        /// Read-only probe: record the access (the executor calls this both at plan
        /// build, so a cold window logs per-object passes)
        /// and report the whole range as one writer-null miss.
        CacheViewPtr planResidencyView(
            const StoredObject & object, size_t object_file_offset, ByteRange range_in_file) override
        {
            log.push_back(TrackedLookup{object.remote_path, object_file_offset, range_in_file});
            auto view = std::make_unique<CacheView>();
            view->miss_entries.push_back(MissEntry{range_in_file, /*writer=*/nullptr});
            return view;
        }

        void openWriteBuffers(const StoredObject &, size_t, CacheView & view) override
        {
            for (auto & entry : view.miss_entries)
                entry.writer = std::make_unique<TrackingWriteBuffer>(entry.range);
        }

        std::vector<TrackedLookup> log;
    };
}

TEST(ReaderExecutor, CacheLookupSplitByObjectBoundary)
{
    /// A single physical request that spans two objects must be issued to
    /// the cache as TWO `lookup` calls — one per object — each carrying
    /// the right `StoredObject` and `object_file_offset`. Previously the
    /// executor handed the cache a single file-level range with one
    /// (executor-wide) cache key, so caches that key per object (the new
    /// `DiskCacheProvider`) couldn't tell the bytes apart.
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"blob_A", String(300, 'A')},
            {"blob_B", String(200, 'B')},
        });

    StoredObjects objects;
    objects.emplace_back("blob_A", "", 300);
    objects.emplace_back("blob_B", "", 200);

    auto tracker = std::make_shared<TrackingCacheProvider>();

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 500;
    ReaderExecutor executor(source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{tracker}, executor_options);

    auto chain = executor.readNextWindow();
    EXPECT_EQ(chain.range().size, 500u);

    /// With plan-then-stream the window is probed for residency per object ONCE, at plan
    /// build (the plan is reused across the serve; the engine's inline pieces consult the
    /// DISPLAY, never the provider - late-populated cells are served via the writers' wait,
    /// not a fresh probe). The point of the test is that the probe splits at the object
    /// boundary, each call carrying the right `StoredObject` and `object_file_offset`.
    ASSERT_EQ(tracker->log.size(), 2u);

    for (size_t pass = 0; pass < 1; ++pass)
    {
        const auto & a = tracker->log[pass * 2];
        const auto & b = tracker->log[pass * 2 + 1];

        EXPECT_EQ(a.remote_path, "blob_A");
        EXPECT_EQ(a.object_file_offset, 0u);
        EXPECT_EQ(a.range_in_file.offset, 0u);
        EXPECT_EQ(a.range_in_file.size, 300u);

        EXPECT_EQ(b.remote_path, "blob_B");
        EXPECT_EQ(b.object_file_offset, 300u);
        EXPECT_EQ(b.range_in_file.offset, 300u);
        EXPECT_EQ(b.range_in_file.size, 200u);
    }
}

/// For an unknown-size source, the worker can latch `reached_eof` mid-flight
/// while still producing a partial chain with the real final bytes.
/// `readNextWindow`'s `atEnd()` branch must drain the in-flight machine before
/// reporting EOF, so those final bytes are collected and served; only the
/// nothing-in-flight case short-circuits to empty.
TEST(ReaderExecutor, UnknownSizePrefetchedFinalBytesAreServed)
{
    /// 30 bytes "ABAB...". The source has the real bytes; the executor is
    /// told the size is unknown, so it discovers EOF only via a short
    /// return from the source.
    constexpr size_t total = 30;
    String content(total, 0);
    for (size_t i = 0; i < total; ++i)
        content[i] = static_cast<char>('A' + (i % 2));

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", StoredObject::UnknownSize);

    auto pool = std::make_shared<SyncPrefetchPool>();

    constexpr size_t window = 16;
    ReaderExecutor::Options executor_options;
    executor_options.window_size = window;
    executor_options.min_bytes_for_seek = 0;
    executor_options.prefetch_pool = pool;
    ReaderExecutor executor(source, objects, {}, executor_options);

    /// First call: sync-read [0, 16). At the end of the call,
    /// `maybeTriggerPrefetch` submits P1 for [16, 32). The synchronous
    /// pool runs P1 inline: the source short-returns 14 bytes (EOF at 30),
    /// the worker sets `reached_eof = true`, and the future ends up
    /// holding the 14-byte chain.
    auto r1 = executor.readNextWindow();
    EXPECT_EQ(r1.range().offset, 0u);
    EXPECT_EQ(r1.range().size, window);

    /// Pre-fix: returns {} (EOF gate fires; prefetch dropped). Post-fix:
    /// the prefetched final bytes are served.
    auto r2 = executor.readNextWindow();
    EXPECT_EQ(r2.range().offset, window) << "prefetched final bytes lost";
    EXPECT_EQ(r2.range().size, total - window);

    /// Third call: no pending prefetch, `reached_eof` still set → real EOF.
    auto r3 = executor.readNextWindow();
    EXPECT_TRUE(r3.empty());
}

TEST(ReaderExecutor, ResidentRunOverlapsDownstreamGapPrefetch)
{
    /// The resident/prefetch OVERLAP: `maybeTriggerPrefetch` targets the FIRST GAP in the
    /// plan (`nextGapStart`), not the cursor's residency, so a downstream gap prefetches in
    /// the background WHILE the resident run before it streams from cache. `serveCacheBlock`
    /// runs with that prefetch in flight (the connection cluster is in the job, so the
    /// foreground touches nothing), and the gap is consumed when the cursor reaches it.
    ///
    /// Layout: cold [0,100), CACHED [100,200), cold [200,300), window 300. After the first
    /// gap read lands the cursor on the resident run at 100, the [200,300) gap is prefetched
    /// while [100,200) is served from cache. `SyncPrefetchPool` runs the prefetch inline,
    /// making it deterministic.
    constexpr size_t total = 300;
    String content(total, 0);
    for (size_t i = 0; i < total; ++i)
        content[i] = static_cast<char>('A' + (i % 26));

    StoredObjects objects;
    objects.emplace_back("obj", "", total);

    auto cache = std::make_shared<MockCacheProvider>(100);

    /// Warm the middle block [100, 200) so the cursor crosses cold -> resident -> cold.
    {
        auto warm_source = std::make_shared<MemorySourceReader>(
            std::unordered_map<String, String>{{"obj", content}});
        ReaderExecutor::Options warmup_options;
        warmup_options.window_size = 100;
        ReaderExecutor warmup(warm_source, objects, {cache}, warmup_options);
        warmup.seek(100);
        warmup.readNextWindow();
        ASSERT_TRUE(cache->hasBlock(1));
    }

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    auto pool = std::make_shared<SyncPrefetchPool>();
    ReaderExecutor::Options executor_options;
    executor_options.window_size = total;
    executor_options.min_bytes_for_seek = 0;
    executor_options.prefetch_pool = pool;
    ReaderExecutor executor(source, objects, {cache}, executor_options);

    /// Cold gap [0,100); advances the cursor to the resident run at 100.
    auto r1 = executor.readNextWindow();
    EXPECT_EQ(r1.range().offset, 0u);
    EXPECT_EQ(r1.range().size, 100u);
    /// The crux: the cursor is resident at 100, but the first gap ahead [200,300) is now
    /// prefetched - overlapping the resident run that precedes it.
    EXPECT_TRUE(inspect(executor).hasInflightPrefetch())
        << "the first gap ahead must prefetch during the resident run (overlap)";

    /// Resident run [100,200) from cache, served while the [200,300) prefetch runs. A
    /// RELEASED machine may already be collected behind the cursor (the lead top-up
    /// policy), so "still in flight" is not the invariant - the prefetch not being LOST
    /// is: its block is committed to the cache and serves the next window without a
    /// re-fetch.
    auto r2 = executor.readNextWindow();
    EXPECT_EQ(r2.range().offset, 100u);
    EXPECT_EQ(r2.range().size, 100u);
    EXPECT_TRUE(cache->hasBlock(2)) << "the overlapped prefetch's bytes must be committed, not cancelled";

    /// Cold gap [200,300) - the cursor reaches it and consumes the overlapped prefetch.
    auto r3 = executor.readNextWindow();
    EXPECT_EQ(r3.range().offset, 200u);
    EXPECT_EQ(r3.range().size, 100u);

    EXPECT_TRUE(executor.readNextWindow().empty());
}

/// Every cache populate is synchronous (inline on the read thread): both the
/// foreground gap fill and the prefetch-collect fill credit
/// `ReaderExecutorBytesPushedToCacheSync`, with or without a prefetch pool (the
/// deferred async populate path was retired with the put lane). `stats` are flushed
/// to `ProfileEvents` in `~ReaderExecutor`, so each delta is read only after the
/// executor scope closes.
TEST(ReaderExecutor, PopulatesInlineWithOrWithoutPool)
{
    constexpr size_t file_size = 2048;
    constexpr size_t window = 512;
    String content(file_size, 'P');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto & pe = CurrentThread::getProfileEvents();

    /// No prefetch pool: every populate runs on the foreground path.
    {
        const auto sync_before = pe[ProfileEvents::ReaderExecutorBytesPushedToCacheSync];
        {
            auto cache = std::make_shared<MockCacheProvider>(window);
            ReaderExecutor::Options executor_options;
            executor_options.window_size = window;
            ReaderExecutor executor(source, objects, {cache}, executor_options);
            while (!executor.readNextWindow().empty()) {}
        }
        EXPECT_EQ(pe[ProfileEvents::ReaderExecutorBytesPushedToCacheSync] - sync_before, file_size)
            << "without a prefetch pool every populate is synchronous";
    }

    /// With a prefetch pool the worker fetches the gap bytes, but the collect writes
    /// them INLINE on the read thread - so the populate is synchronous too.
    {
        const auto sync_before = pe[ProfileEvents::ReaderExecutorBytesPushedToCacheSync];
        {
            auto cache = std::make_shared<MockCacheProvider>(window);
            auto pool = std::make_shared<SyncPrefetchPool>();
            ReaderExecutor::Options executor_options;
            executor_options.window_size = window;
            executor_options.prefetch_pool = pool;
            ReaderExecutor executor(source, objects, {cache}, executor_options);
            while (!executor.readNextWindow().empty()) {}
        }
        const auto sync_delta = pe[ProfileEvents::ReaderExecutorBytesPushedToCacheSync] - sync_before;
        EXPECT_EQ(sync_delta, file_size) << "the prefetch-collect fill writes inline on the read thread";
    }
}

/// `ReadBufferFromOwnMemoryFile` (used by `BackupInMemory::readFile`, and
/// anywhere a fully-buffered in-memory blob is exposed as a file-shaped
/// buffer) pre-loads its content into `working_buffer` at construction;
/// its `nextImpl` returns false at first call. With the default
/// `supportsExternalBufferMode() = true` from `ReadBuffer`, the executor's
/// `readIntoBlock` would call `set(dest, chunk)` + `next()`, observe
/// `next() == false`, return 0 — and the executor would treat the source
/// as truncated (throw `CANNOT_READ_ALL_DATA` for known-size, latch
/// `reached_eof` for unknown-size), silently dropping the in-memory bytes.
///
/// `ReadBufferFromMemoryFileBase` overrides `supportsExternalBufferMode()`
/// to `false` so `readIntoBlock` falls back to `buf.read(dest, n)`, which
/// copies from `working_buffer`. This test exercises the full path:
/// `BufferSourceReader` whose factory hands back a `ReadBufferFromOwnMemoryFile`,
/// driven through `ReaderExecutor`, expects every byte through.
TEST(ReaderExecutor, MemoryBackedFileBufferIsReadFully)
{
    constexpr size_t total = 128;
    String content(total, 0);
    for (size_t i = 0; i < total; ++i)
        content[i] = static_cast<char>('A' + (i % 26));

    auto source = std::make_shared<BufferSourceReader>(
        [content](const StoredObject &) -> std::unique_ptr<ReadBufferFromFileBase>
        {
            return std::make_unique<ReadBufferFromOwnMemoryFile>("memfile", content);
        },
        "MemorySource");

    StoredObjects objects;
    objects.emplace_back("memfile", "", total);

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 32;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(source, objects, {}, executor_options);

    /// Drive multiple windows to make sure subsequent reads also work,
    /// not just the first. Pre-fix the very first read would throw
    /// `CANNOT_READ_ALL_DATA` because `total_read == 0 < pr.size`.
    String collected;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        size_t base = collected.size();
        collected.resize(base + chain.range().size);
        chain.copyTo(collected.data() + base, chain.range());
    }
    EXPECT_EQ(collected, content) << "memory-backed file buffer must deliver all bytes through the executor";
}

/// End-to-end: drive the REAL `ReaderExecutor` over a REAL `DiskCacheProvider`
/// backed by a REAL `FileCache`, force real eviction between windows, and
/// assert the source connection is opened exactly once (no reset, no re-read).
///
/// The other pin tests use a MOCK cache (`EvictableSegmentMockCache`). This test
/// closes the gap: it proves the executor's in-flight pin keeps the
/// partially-downloaded segment non-releasable through an eviction flood that
/// targets the REAL FileCache LRU/reserve machinery — exercising the real
/// `DiskCacheWriter`/`CacheWriter::pin` path the mock can only approximate.
TEST(ReaderExecutor, RealDiskCacheSequentialEvictionKeepsPinnedSegment)
{
    DB::ServerUUID::setRandomForUnitTests();

    /// `FileCache::reserve` charges the per-query budget via
    /// `CurrentThread::getQueryId()`, so a real `ThreadStatus` + `QueryScope`
    /// (with a query context) must be in scope.
    ///
    /// Another test in the binary may have instantiated the `MainThreadStatus`
    /// singleton (e.g. via `MainThreadStatus::getInstance()`), leaving
    /// `current_thread` set for the rest of the process; `ThreadStatus`'s ctor
    /// asserts `!current_thread`. Clear it for our own status and restore the
    /// previous pointer on exit (the singleton's process-exit dtor asserts it
    /// is still `current_thread`). Mirrors `FileCacheTest`'s SetUp/TearDown —
    /// without it this test aborts under shuffled / sanitizer CI runs.
    auto * saved_thread = DB::current_thread;
    DB::current_thread = nullptr;
    SCOPE_EXIT({ DB::current_thread = saved_thread; });

    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("reader_exec_real_disk_cache");
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    namespace fs = std::filesystem;
    auto cache_path = fs::temp_directory_path() / "reader_exec_pin_it_cache";
    fs::remove_all(cache_path);
    fs::create_directories(cache_path);
    SCOPE_EXIT({ fs::remove_all(cache_path); });

    DB::FileCacheSettings settings;
    settings[DB::FileCacheSetting::path] = cache_path.string();
    /// Sized so the streamed object's single segment plus a little headroom
    /// fits, and a flood of other keys forces eviction of anything releasable.
    settings[DB::FileCacheSetting::max_size] = 24 * 1024;
    settings[DB::FileCacheSetting::max_elements] = 4;
    settings[DB::FileCacheSetting::max_file_segment_size] = 8 * 1024;
    /// Alignment == segment size keeps the streamed segment PARTIALLY_DOWNLOADED
    /// across windows (a smaller alignment would shrink it to DOWNLOADED on
    /// complete, removing the state the pin protects).
    settings[DB::FileCacheSetting::boundary_alignment] = 8 * 1024;
    settings[DB::FileCacheSetting::load_metadata_asynchronously] = false;
    settings[DB::FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("reader_exec_pin_it", settings);
    cache->initialize();
    const auto & origin = DB::FileCache::getCommonOrigin();

    DB::FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
    auto provider = std::make_shared<DB::DiskCacheProvider>(cache, cache_settings, /*query_id_=*/String{});

    /// One object that is a single 8 KiB-aligned segment, streamed in 2 KiB
    /// windows (4 windows) so the segment is PARTIALLY_DOWNLOADED between
    /// windows.
    String content(8000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"stream_obj", content}});
    StoredObjects objects;
    objects.emplace_back("stream_obj", "stream_obj", 8000);

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(provider);

    auto limit = std::make_shared<LongConnectionLimit>(10);
    /// NOTE: no prefetch pool — keep reads synchronous so the flood between
    /// windows is deterministic.
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 2000;
    executor_options.min_bytes_for_seek = 0;
    executor_options.long_connection_limit = limit;
    auto executor = std::make_unique<ReaderExecutor>(source, objects, caches, executor_options);

    /// Flood the cache with unrelated keys to force eviction of any releasable
    /// segment. The streamed segment must survive because it is pinned.
    auto flood = [&](int round)
    {
        for (int i = 0; i < 6; ++i)
        {
            auto key = DB::FileCacheKey::fromPath("flood_" + std::to_string(round) + "_" + std::to_string(i));
            auto h = cache->getOrSet(key, 0, 8 * 1024, 8 * 1024, DB::CreateFileSegmentSettings{}, 0, origin);
            for (auto & seg : *h)
            {
                if (seg->state() != DB::FileSegment::State::EMPTY)
                    continue;
                if (seg->getOrSetDownloader() != DB::FileSegment::getCallerId())
                    continue;
                std::string failure_reason;
                if (!seg->reserve(8 * 1024, 1000, failure_reason))
                {
                    seg->completePartAndResetDownloader();
                    continue;
                }
                /// `FileSegment::write` requires the key's on-disk directory.
                auto key_str = key.toString();
                auto subdir = fs::path(cache_path) / key_str.substr(0, 3) / key_str;
                if (!fs::exists(subdir))
                    fs::create_directories(subdir);
                std::string payload(8 * 1024, 'Z');
                seg->write(payload.data(), payload.size(), seg->getCurrentWriteOffset());
                seg->completePartAndResetDownloader();
            }
        }
    };

    String result;
    int round = 0;
    while (true)
    {
        auto chain = executor->readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            result.append(node.data(), node.size);
        flood(round++);   // eviction pressure before the next window
    }

    /// The streamed segment stayed pinned through every flood, so its bytes were
    /// served intact rather than re-read or lost to eviction.
    EXPECT_EQ(result, content);
}

TEST(ReaderExecutor, DoneMachineCollectedAndLeadToppedUpBehindCursor)
{
    /// The fill-ahead lead is a consumer-anchored HIGH-WATER, not a fetch-then-drain
    /// sawtooth: when the worker finishes its piece early, the collect does not wait for
    /// the cursor to consume the whole window - the released machine is joined at the next
    /// serve and the NEXT piece launches for `[cursor, cursor + lead)`. Pinned here: the
    /// cursor strictly INSIDE the first machine's window while the second machine is
    /// already in flight beyond it, bounded by the horizon.
    DB::ServerUUID::setRandomForUnitTests();

    auto * saved_thread = DB::current_thread;
    DB::current_thread = nullptr;
    SCOPE_EXIT({ DB::current_thread = saved_thread; });

    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("reader_exec_lead_topup");
    auto query_scope_holder = DB::QueryScope::create(query_context);

    namespace fs = std::filesystem;
    auto cache_path = fs::temp_directory_path() / "reader_exec_lead_topup_cache";
    fs::remove_all(cache_path);
    fs::create_directories(cache_path);
    SCOPE_EXIT({ fs::remove_all(cache_path); });

    DB::FileCacheSettings settings;
    settings[DB::FileCacheSetting::path] = cache_path.string();
    settings[DB::FileCacheSetting::max_size] = 1024 * 1024;   /// no eviction pressure
    settings[DB::FileCacheSetting::max_file_segment_size] = 2000;
    settings[DB::FileCacheSetting::boundary_alignment] = 2000;
    settings[DB::FileCacheSetting::load_metadata_asynchronously] = false;
    settings[DB::FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("reader_exec_lead_topup", settings);
    cache->initialize();

    DB::FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
    auto provider = std::make_shared<DB::DiskCacheProvider>(cache, cache_settings, /*query_id_=*/String{});

    String content(16000, '\0');
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('a' + (i % 23));
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"lead_obj", content}});
    StoredObjects objects;
    objects.emplace_back("lead_obj", "lead_obj", content.size());

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(provider);

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 2000;
    executor_options.fill_ahead_lead = 6000;
    executor_options.min_bytes_for_seek = 0;
    executor_options.prefetch_pool = pool;
    ReaderExecutor executor(source, objects, caches, executor_options);

    /// Window 1 [0, 2000) is a cold inline read; its finishWindow launches the background
    /// machine at the frontier, horizon-clamped: [2000, 2000 + 6000).
    auto w1 = executor.readNextWindow();
    ASSERT_EQ(w1.range().offset, 0u);
    ASSERT_EQ(w1.range().size, 2000u);
    ASSERT_TRUE(inspect(executor).hasInflightPrefetch());
    EXPECT_EQ(inspect(executor).inflightPrefetchOffset(), 2000u);
    const size_t first_lead_end
        = inspect(executor).inflightPrefetchOffset() + inspect(executor).inflightPrefetchSize();
    EXPECT_EQ(first_lead_end, 8000u);

    /// Let the worker finish the whole lead (a memory source is fast; bounded wait).
    for (int i = 0; i < 5000 && !inspect(executor).inflightPrefetchReleased(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_TRUE(inspect(executor).inflightPrefetchReleased());

    /// Window 2 [2000, 4000) serves from the machine's committed cells. Its finishWindow
    /// must collect the RELEASED machine although the cursor (4000) is still strictly
    /// inside its window (end 8000) - the empty slot proves the collect - but the top-up
    /// is HELD: only 2000 of the horizon is open, below the half-lead refill threshold.
    auto w2 = executor.readNextWindow();
    ASSERT_EQ(w2.range().offset, 2000u);
    ASSERT_EQ(w2.range().size, 2000u);
    ASSERT_FALSE(inspect(executor).hasInflightPrefetch()) << "released machine not collected behind the cursor";

    /// Window 3 [4000, 6000) opens half the horizon (4000 of 6000): the top-up launches
    /// at the old fetch frontier, bounded by the new horizon [6000, 6000 + 6000).
    auto w3 = executor.readNextWindow();
    ASSERT_EQ(w3.range().offset, 4000u);
    ASSERT_EQ(w3.range().size, 2000u);
    ASSERT_TRUE(inspect(executor).hasInflightPrefetch()) << "half-open horizon must top the lead up";
    EXPECT_EQ(inspect(executor).inflightPrefetchOffset(), 8000u);
    EXPECT_LE(inspect(executor).inflightPrefetchOffset() + inspect(executor).inflightPrefetchSize(), 6000u + 6000u);

    /// Drain and verify the whole payload.
    String result;
    for (const auto & node : w1.getNodes())
        result.append(node.data(), node.size);
    for (const auto & node : w2.getNodes())
        result.append(node.data(), node.size);
    for (const auto & node : w3.getNodes())
        result.append(node.data(), node.size);
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            result.append(node.data(), node.size);
    }
    EXPECT_EQ(result, content);
}

TEST(ReaderExecutor, PrefetchRunsPastTheAdvancingExtent)
{
    /// The read extent (`setReadUntilPosition`) bounds the CONSUMER, not the producer:
    /// where the run is predicted to continue (`prefetchAllowance` = max(extent, reach)),
    /// the fill-ahead machine crosses the extent instead of stopping and restarting at
    /// every per-mark-range advance. The serve still EOFs at the extent; once the extent
    /// advances, the already-fetched bytes serve with ZERO new source requests.
    DB::ServerUUID::setRandomForUnitTests();

    auto * saved_thread = DB::current_thread;
    DB::current_thread = nullptr;
    SCOPE_EXIT({ DB::current_thread = saved_thread; });

    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("reader_exec_past_extent");
    auto query_scope_holder = DB::QueryScope::create(query_context);

    namespace fs = std::filesystem;
    auto cache_path = fs::temp_directory_path() / "reader_exec_past_extent_cache";
    fs::remove_all(cache_path);
    fs::create_directories(cache_path);
    SCOPE_EXIT({ fs::remove_all(cache_path); });

    DB::FileCacheSettings settings;
    settings[DB::FileCacheSetting::path] = cache_path.string();
    settings[DB::FileCacheSetting::max_size] = 1024 * 1024;
    settings[DB::FileCacheSetting::max_file_segment_size] = 2000;
    settings[DB::FileCacheSetting::boundary_alignment] = 2000;
    settings[DB::FileCacheSetting::load_metadata_asynchronously] = false;
    settings[DB::FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("reader_exec_past_extent", settings);
    cache->initialize();

    DB::FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
    auto provider = std::make_shared<DB::DiskCacheProvider>(cache, cache_settings, /*query_id_=*/String{});

    String content(8000, '\0');
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('a' + (i % 23));
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"extent_obj", content}});
    StoredObjects objects;
    objects.emplace_back("extent_obj", "extent_obj", content.size());

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(provider);

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 2000;
    executor_options.fill_ahead_lead = 6000;
    executor_options.min_bytes_for_seek = 0;
    executor_options.prefetch_pool = pool;
    ReaderExecutor executor(source, objects, caches, executor_options);

    /// The first mark range ends at 6000; the file continues to 8000.
    executor.setReadExtent(6000);

    /// Window 1 [0, 2000): the cold inline read; its finishWindow launches the machine at
    /// the frontier. No consumed run is confirmed yet, so the first launch stays
    /// extent-bounded: [2000, 6000), not past it - the declared extent is a BOUND,
    /// not a consumption commitment; beyond-extent reach must be earned.
    auto w1 = executor.readNextWindow();
    ASSERT_EQ(w1.range().offset, 0u);
    ASSERT_EQ(w1.range().size, 2000u);
    ASSERT_TRUE(inspect(executor).hasInflightPrefetch());
    EXPECT_EQ(inspect(executor).inflightPrefetchOffset(), 2000u);
    EXPECT_EQ(inspect(executor).inflightPrefetchOffset() + inspect(executor).inflightPrefetchSize(), 6000u)
        << "no consumed run yet - the first launch must stop at the extent";

    /// Window 2 [2000, 4000): the checkpointed run (est 0.7*4000 = 2800; end
    /// 4000 + 0.7*(4000+2800) = 8760, clamped to the 8000 file end) passes the
    /// extent, so its finishWindow launches the crossing top-up [6000, 8000).
    for (int i = 0; i < 5000 && !inspect(executor).inflightPrefetchReleased(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_TRUE(inspect(executor).inflightPrefetchReleased());
    auto w2 = executor.readNextWindow();
    ASSERT_EQ(w2.range().offset, 2000u);
    ASSERT_EQ(w2.range().size, 2000u);
    ASSERT_TRUE(inspect(executor).hasInflightPrefetch()) << "the consumed run must earn a crossing top-up";
    EXPECT_EQ(inspect(executor).inflightPrefetchOffset(), 6000u);
    EXPECT_EQ(inspect(executor).inflightPrefetchOffset() + inspect(executor).inflightPrefetchSize(), 8000u)
        << "the crossing machine covers the tail past the extent";

    /// Window 3 [4000, 6000) serves from committed cells; let the crossing machine
    /// finish - the at-extent serve below collects it, folding the worker's stats in.
    auto w3 = executor.readNextWindow();
    ASSERT_EQ(w3.range().offset, 4000u);
    ASSERT_EQ(w3.range().size, 2000u);
    for (int i = 0; i < 5000 && !inspect(executor).inflightPrefetchReleased(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_TRUE(inspect(executor).inflightPrefetchReleased());

    /// The cursor sits at the extent; the serve EOFs there (empty window) - the consumer
    /// bound is untouched by the producer's crossing. This read also collects the
    /// released crossing machine, folding the worker's stats in.
    String result;
    for (const auto & node : w1.getNodes())
        result.append(node.data(), node.size);
    for (const auto & node : w2.getNodes())
        result.append(node.data(), node.size);
    for (const auto & node : w3.getNodes())
        result.append(node.data(), node.size);
    ASSERT_EQ(executor.getPosition(), 6000u);
    auto at_extent = executor.readNextWindow();
    EXPECT_TRUE(at_extent.empty()) << "the consumer still EOFs at the extent";
    ASSERT_FALSE(inspect(executor).hasInflightPrefetch()) << "the at-extent serve collects the machine";
    const size_t requests_before_advance = inspect(executor).sourceRequests();

    /// The next mark range advances the extent; the tail was already fetched by the
    /// crossing machine, so it serves with ZERO new source requests.
    executor.setReadExtent(8000);
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            result.append(node.data(), node.size);
    }
    EXPECT_EQ(result, content);
    EXPECT_EQ(inspect(executor).sourceRequests(), requests_before_advance)
        << "advancing the extent must not pay a new source request for already-fetched bytes";
}

TEST(ReaderExecutor, FullCacheColdReadServesRefusedBytesFromBank)
{
    /// A cache too small for the scan: once the plan's held segments fill the budget every
    /// further write is refused. The worker retains only that refused residue (capped at one
    /// window) and stops the lead early; the collect banks it; the serve delivers it from the
    /// bank - the read must produce every byte regardless of the cache being full. The
    /// launcher still ASKS the full lead: the residue cap, not a tier gate, limits the fetch.
    DB::ServerUUID::setRandomForUnitTests();

    auto * saved_thread = DB::current_thread;
    DB::current_thread = nullptr;
    SCOPE_EXIT({ DB::current_thread = saved_thread; });

    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("reader_exec_full_cache");
    auto query_scope_holder = DB::QueryScope::create(query_context);

    namespace fs = std::filesystem;
    auto cache_path = fs::temp_directory_path() / "reader_exec_full_cache";
    fs::remove_all(cache_path);
    fs::create_directories(cache_path);
    SCOPE_EXIT({ fs::remove_all(cache_path); });

    DB::FileCacheSettings settings;
    settings[DB::FileCacheSetting::path] = cache_path.string();
    /// Room for TWO segments; the plan holds its segments (non-releasable), so from the
    /// third segment on every reserve fails - a deterministic full cache.
    settings[DB::FileCacheSetting::max_size] = 4000;
    settings[DB::FileCacheSetting::max_file_segment_size] = 2000;
    settings[DB::FileCacheSetting::boundary_alignment] = 2000;
    settings[DB::FileCacheSetting::load_metadata_asynchronously] = false;
    settings[DB::FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("reader_exec_full_cache", settings);
    cache->initialize();

    DB::FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 100;
    auto provider = std::make_shared<DB::DiskCacheProvider>(cache, cache_settings, /*query_id_=*/String{});

    String content(16000, '\0');
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('0' + (i % 61));
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"full_obj", content}});
    StoredObjects objects;
    objects.emplace_back("full_obj", "full_obj", content.size());

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(provider);

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 2000;
    executor_options.fill_ahead_lead = 6000;
    executor_options.min_bytes_for_seek = 0;
    executor_options.prefetch_pool = pool;
    ReaderExecutor executor(source, objects, caches, executor_options);

    auto w1 = executor.readNextWindow();
    ASSERT_EQ(w1.range().offset, 0u);
    ASSERT_TRUE(inspect(executor).hasInflightPrefetch());
    EXPECT_EQ(inspect(executor).inflightPrefetchSize(), 6000u)
        << "the full lead is asked even with the cache full - the residue cap limits the fetch, not a tier gate";

    String result;
    for (const auto & node : w1.getNodes())
        result.append(node.data(), node.size);
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            result.append(node.data(), node.size);
    }
    EXPECT_EQ(result, content) << "refused bytes must reach the consumer through the bank";
}

TEST(ReaderExecutor, EncryptedColdCellWindowStartsBelowHeader)
{
    /// On an encrypted file the first cache cell spans the header: when that cell is not
    /// committed (populate refused here - the cache cannot hold even the header), fetch
    /// runs and machine windows legitimately start at PHYSICAL 0, below the header, where
    /// no logical coordinate exists. Every comparison and log along the collect/seek path
    /// must stay on the physical side - a logical conversion of such a window aborts
    /// debug builds (the `toLogical` underflow guard).
    DB::ServerUUID::setRandomForUnitTests();

    auto * saved_thread = DB::current_thread;
    DB::current_thread = nullptr;
    SCOPE_EXIT({ DB::current_thread = saved_thread; });

    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("reader_exec_enc_subheader");
    auto query_scope_holder = DB::QueryScope::create(query_context);

    namespace fs = std::filesystem;
    auto cache_path = fs::temp_directory_path() / "reader_exec_enc_subheader_cache";
    fs::remove_all(cache_path);
    fs::create_directories(cache_path);
    SCOPE_EXIT({ fs::remove_all(cache_path); });

    DB::FileCacheSettings settings;
    settings[DB::FileCacheSetting::path] = cache_path.string();
    /// Smaller than the 64-byte encryption header: every populate is refused, so the
    /// first cell (which spans the header) never commits and the cold fetch runs from 0.
    settings[DB::FileCacheSetting::max_size] = 48;
    settings[DB::FileCacheSetting::max_file_segment_size] = 2000;
    settings[DB::FileCacheSetting::boundary_alignment] = 2000;
    settings[DB::FileCacheSetting::load_metadata_asynchronously] = false;
    settings[DB::FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("reader_exec_enc_subheader", settings);
    cache->initialize();

    DB::FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 100;
    auto provider = std::make_shared<DB::DiskCacheProvider>(cache, cache_settings, /*query_id_=*/String{});

    String key(16, 'e');
    FileEncryption::InitVector iv(UInt128{0xC0FFEEu});
    String plaintext(4000, '\0');
    for (size_t i = 0; i < plaintext.size(); ++i)
        plaintext[i] = static_cast<char>('A' + (i % 29));
    String file_bytes = makeEncryptedFile(key, iv, plaintext);

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"enc_obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("enc_obj", "enc_obj", file_bytes.size());

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(provider);

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 1000;
    executor_options.fill_ahead_lead = 3000;
    executor_options.min_bytes_for_seek = 0;
    executor_options.prefetch_pool = pool;
    ReaderExecutor executor(source, objects, caches, executor_options);
    executor.addDecryptionLayer("/enc",
        [&](UInt128, const String &) { return key; });
    executor.initDecryption();

    /// A seek right after init launches the ahead lead at the un-attempted job start -
    /// PHYSICAL 0 here (the refused header keeps the first cell uncommitted) - and the
    /// second seek's keep-prefetch compare must evaluate that window on the physical
    /// side (a logical conversion of offset 0 aborts debug builds).
    executor.seek(0);
    ASSERT_TRUE(inspect(executor).hasInflightPrefetch());
    ASSERT_EQ(inspect(executor).inflightPrefetchOffset(), 0u);
    executor.seek(10);
    executor.seek(0);

    String result;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            result.append(node.data(), node.size);
    }
    EXPECT_EQ(result, plaintext);
}

TEST(ReaderExecutor, PlanGrowsInWindowStepsToTheTarget)
{
    /// The plan is probed in `window_size` steps until the enriched span reaches the
    /// `plan_look_ahead_max_window` target: with no cache (nothing to enrich) the steps
    /// tile exactly and the plan ends AT the target - not at one window, not at EOF.
    String content(64000, 'g');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", content.size());

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 8000;
    executor_options.plan_look_ahead_max_window = 32000;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(source, objects, {}, executor_options);

    auto w1 = executor.readNextWindow();
    ASSERT_EQ(w1.range().offset, 0u);
    EXPECT_EQ(inspect(executor).planEnd(), 32000u) << "four window probes tile the plan to the target";
}

TEST(ReaderExecutor, PlanKeepsSpanWhenCellOverhangsIt)
{
    /// One cold 24000-byte cell overhangs the 16000 plan span. The plan keeps its
    /// span (`plan_end` = the probed target - the overhang was never probed for
    /// residency), while the geometry carries the WHOLE cell: its overhang is
    /// fill-only work via the schedule's cell closure, so the fetch still fills
    /// the touched cell to its boundary and the next plan finds it resident.
    String content(64000, 'h');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", content.size());

    auto cache = std::make_shared<MockCacheProvider>(24000);

    ReaderExecutor::Options executor_options;
    executor_options.window_size = 8000;
    executor_options.plan_look_ahead_max_window = 16000;
    executor_options.min_bytes_for_seek = 0;
    ReaderExecutor executor(source, objects, {cache}, executor_options);

    auto w1 = executor.readNextWindow();
    ASSERT_EQ(w1.range().offset, 0u);
    EXPECT_EQ(inspect(executor).planEnd(), 16000u) << "the plan span is the probed target";
    auto geom = inspect(executor).planGeometry();
    ASSERT_EQ(geom->entries.size(), 1u);
    ASSERT_FALSE(geom->entries[0].aligned_miss.empty());
    EXPECT_EQ(geom->entries[0].aligned_miss.front().end(), 24000u)
        << "the geometry carries the whole overhanging cell as fill work";
}

/// Stage-5 "stop at the first loss" under REAL FileCache contention (not the downloader-blind
/// `MockCacheWriter`, which masked an earlier dead-work bug). A sibling on another thread holds
/// the SECOND segment's downloader (DOWNLOADING, no committed bytes) over the same key+origin the
/// inline executor uses. The first window therefore straddles an own-led segment (S0) and the
/// sibling-led one (S1). The unified-foreground inline machine must fetch only the contiguous LED
/// PREFIX [0, 64) - serving it as a SHORT window and stopping at the sibling boundary - rather
/// than blocking to fetch past the sibling. The test then commits the sibling and drains the rest:
/// S1 is read through the sync fallback (the cursor-at-sibling case) and S2 is fetched by the
/// executor, so the full file is served correctly. (If the executor blocked on the sibling inside
/// window 1, this test would deadlock: the sibling commits only AFTER window 1 returns.)
TEST(ReaderExecutor, UnifiedForegroundStopsAtFirstSiblingLedSegment)
{
    DB::ServerUUID::setRandomForUnitTests();

    auto * saved_thread = DB::current_thread;
    DB::current_thread = nullptr;
    SCOPE_EXIT({ DB::current_thread = saved_thread; });

    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("reader_exec_first_loss_main");
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    namespace fs = std::filesystem;
    auto cache_path = fs::temp_directory_path() / "reader_exec_first_loss_cache";
    fs::remove_all(cache_path);
    fs::create_directories(cache_path);
    SCOPE_EXIT({ fs::remove_all(cache_path); });

    constexpr size_t segment_size = 64;
    constexpr size_t file_size = 3 * segment_size;   /// S0 [0,64) S1 [64,128) S2 [128,192)

    DB::FileCacheSettings settings;
    settings[DB::FileCacheSetting::path] = cache_path.string();
    settings[DB::FileCacheSetting::max_size] = 1ull << 20;
    settings[DB::FileCacheSetting::max_elements] = 64;
    settings[DB::FileCacheSetting::max_file_segment_size] = segment_size;
    settings[DB::FileCacheSetting::boundary_alignment] = segment_size;
    settings[DB::FileCacheSetting::load_metadata_asynchronously] = false;
    settings[DB::FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("reader_exec_first_loss", settings);
    cache->initialize();

    DB::FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;

    /// Fixed key + origin shared by the inline executor's provider and the sibling, so the manual
    /// download lands on exactly the segment the executor elects.
    auto key = DB::FileCacheKey::fromPath("obj");
    auto origin = DB::FileCache::getCommonOrigin();
    auto provider = std::make_shared<DB::DiskCacheProvider>(
        cache, cache_settings, /*query_id_=*/String{}, /*local_throttler_=*/nullptr,
        std::optional<DB::FileCacheKey>(key), std::optional<DB::FileCacheOriginInfo>(origin));

    /// Distinct bytes per segment so a misplaced serve is obvious.
    const String content = String(segment_size, 'A') + String(segment_size, 'B') + String(segment_size, 'C');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    ReaderExecutor::Options opts;
    opts.window_size = 2 * segment_size;   /// one window spans S0 + S1, so it straddles the boundary
    opts.min_bytes_for_seek = 0;
    opts.long_connection_limit = std::make_shared<LongConnectionLimit>(10);

    std::latch holding{1};      /// sibling holds S1's downloader (DOWNLOADING)
    std::latch go_commit{1};    /// main lets the sibling commit S1 (after window 1 returns)
    std::latch committed{1};    /// sibling has committed S1 (DOWNLOADED)
    std::atomic<bool> sib_won_downloader{false};

    std::thread sibling([&]
    {
        DB::current_thread = nullptr;
        DB::ThreadStatus sib_status;
        auto sib_context = DB::Context::createCopy(getContext().context);
        sib_context->makeQueryContext();
        sib_context->setCurrentQueryId("reader_exec_first_loss_sibling");
        auto sib_scope = DB::QueryScope::create(sib_context);

        /// Win S1's downloader and keep it DOWNLOADING with no committed bytes: a different thread
        /// id makes our caller id differ from the executor's, so the executor loses the election.
        auto holder = cache->getOrSet(key, segment_size, segment_size, file_size,
            DB::CreateFileSegmentSettings{}, 0, origin);
        auto & seg = holder->front();
        sib_won_downloader.store(seg.getOrSetDownloader() == DB::FileSegment::getCallerId());
        holding.count_down();

        go_commit.wait();
        std::string failure_reason;
        if (seg.reserve(segment_size, 1000, failure_reason))
        {
            String s1 = content.substr(segment_size, segment_size);
            seg.write(s1.data(), s1.size(), seg.getCurrentWriteOffset());
            seg.completePartAndResetDownloader();   /// -> DOWNLOADED
        }
        committed.count_down();
    });

    ReaderExecutor executor(source, objects, {provider}, opts);

    /// Wait until the sibling owns S1 before the first serve elects it.
    holding.wait();
    EXPECT_TRUE(sib_won_downloader.load()) << "the sibling must win S1's downloader on its own thread";

    /// Window 1: must NOT block on the sibling - it serves the led prefix and returns.
    String got1;
    {
        auto chain = executor.readNextWindow();
        for (const auto & node : chain.getNodes())
            got1.append(node.data(), node.size);
    }
    /// Now the sibling may commit S1, and the rest can be drained.
    go_commit.count_down();
    committed.wait();
    sibling.join();

    String rest;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            rest.append(node.data(), node.size);
    }

    EXPECT_LE(got1.size(), segment_size)
        << "window 1 must stop at the sibling boundary (serve the led prefix), not read through S1";
    EXPECT_EQ(content.substr(0, got1.size()), got1) << "the prefix bytes must be correct";
    EXPECT_EQ(got1 + rest, content) << "the whole file is served correctly under real contention";
}

/// KT2b of the two-cursors model (see `ReaderExecutor`'s class comment): display truth
/// can REGRESS behind the launch high-water. A sibling wins a segment's downloader, our serve
/// stops at its boundary (short window) - then the sibling's query DIES without committing a
/// byte, resetting the segment to EMPTY. The hole sits behind everything already attempted,
/// so only the window-anchored pump can heal it: the next serve must re-elect the segment,
/// fetch it from the source, POPULATE the cell (not just bank cache-blind bytes), and serve -
/// no livelock, no false EOF. Same real-`FileCache` choreography as the test above.
TEST(ReaderExecutor, PumpHealsAbandonedSiblingSegment)
{
    DB::ServerUUID::setRandomForUnitTests();

    auto * saved_thread = DB::current_thread;
    DB::current_thread = nullptr;
    SCOPE_EXIT({ DB::current_thread = saved_thread; });

    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("reader_exec_sib_death_main");
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    namespace fs = std::filesystem;
    auto cache_path = fs::temp_directory_path() / "reader_exec_sib_death_cache";
    fs::remove_all(cache_path);
    fs::create_directories(cache_path);
    SCOPE_EXIT({ fs::remove_all(cache_path); });

    constexpr size_t segment_size = 64;
    constexpr size_t file_size = 3 * segment_size;   /// S0 [0,64) S1 [64,128) S2 [128,192)

    DB::FileCacheSettings settings;
    settings[DB::FileCacheSetting::path] = cache_path.string();
    settings[DB::FileCacheSetting::max_size] = 1ull << 20;
    settings[DB::FileCacheSetting::max_elements] = 64;
    settings[DB::FileCacheSetting::max_file_segment_size] = segment_size;
    settings[DB::FileCacheSetting::boundary_alignment] = segment_size;
    settings[DB::FileCacheSetting::load_metadata_asynchronously] = false;
    settings[DB::FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("reader_exec_sib_death", settings);
    cache->initialize();

    DB::FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;

    auto key = DB::FileCacheKey::fromPath("obj");
    auto origin = DB::FileCache::getCommonOrigin();
    auto provider = std::make_shared<DB::DiskCacheProvider>(
        cache, cache_settings, /*query_id_=*/String{}, /*local_throttler_=*/nullptr,
        std::optional<DB::FileCacheKey>(key), std::optional<DB::FileCacheOriginInfo>(origin));

    const String content = String(segment_size, 'A') + String(segment_size, 'B') + String(segment_size, 'C');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    ReaderExecutor::Options opts;
    opts.window_size = 2 * segment_size;   /// window 1 straddles S0 + the sibling-led S1
    opts.min_bytes_for_seek = 0;
    opts.long_connection_limit = std::make_shared<LongConnectionLimit>(10);

    std::latch holding{1};      /// sibling holds S1's downloader (DOWNLOADING, zero bytes)
    std::latch go_die{1};       /// main lets the sibling ABANDON (after window 1 returns)
    std::latch died{1};         /// the segment is back to EMPTY, holder gone
    std::atomic<bool> sib_won_downloader{false};

    std::thread sibling([&]
    {
        DB::current_thread = nullptr;
        DB::ThreadStatus sib_status;
        auto sib_context = DB::Context::createCopy(getContext().context);
        sib_context->makeQueryContext();
        sib_context->setCurrentQueryId("reader_exec_sib_death_sibling");
        auto sib_scope = DB::QueryScope::create(sib_context);

        auto holder = cache->getOrSet(key, segment_size, segment_size, file_size,
            DB::CreateFileSegmentSettings{}, 0, origin);
        auto & seg = holder->front();
        sib_won_downloader.store(seg.getOrSetDownloader() == DB::FileSegment::getCallerId());
        holding.count_down();

        go_die.wait();
        /// The death: give up the downloader with ZERO bytes written and drop the holder -
        /// the killed-query path. The segment resets to EMPTY; nobody will ever commit it.
        seg.resetDownloader();
        holder = nullptr;
        died.count_down();
    });

    ReaderExecutor executor(source, objects, {provider}, opts);

    holding.wait();
    EXPECT_TRUE(sib_won_downloader.load()) << "the sibling must win S1's downloader on its own thread";

    /// Window 1: serves the led prefix [0, 64) short, stopping at the sibling boundary.
    String got1;
    {
        auto chain = executor.readNextWindow();
        for (const auto & node : chain.getNodes())
            got1.append(node.data(), node.size);
    }
    EXPECT_LE(got1.size(), segment_size) << "window 1 must stop at the sibling boundary";
    EXPECT_EQ(content.substr(0, got1.size()), got1);

    /// The sibling dies; the hole at S1 now has no live writer and no committed byte.
    go_die.count_down();
    died.wait();
    sibling.join();

    /// The pump must heal the hole: re-elect S1, fetch it, populate the cell, and serve the
    /// rest of the file. A livelock hangs here; a false EOF truncates `rest`.
    String rest;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            rest.append(node.data(), node.size);
    }
    EXPECT_EQ(got1 + rest, content) << "the abandoned segment's bytes must be served, not skipped";

    /// The heal must POPULATE the cache cell, not just serve cache-blind banked bytes.
    auto verify = cache->getOrSet(key, segment_size, segment_size, file_size,
        DB::CreateFileSegmentSettings{}, 0, origin);
    EXPECT_EQ(verify->front().state(), DB::FileSegment::State::DOWNLOADED)
        << "the pump's piece runs the same fill flow as any machine - the cell completes";
}

/// Reproduces the `chassert(!is_last_holder)` abort in `FileSegment::complete`'s
/// DOWNLOADING branch. `planResidencyView` credits a concurrently-DOWNLOADING
/// segment's committed prefix as a HIT, so its `read_holder` passively pins the
/// in-flight segment for the plan's lifetime, with no writer of its own. When a
/// query is interrupted, that read view can outlive the segment's downloader and
/// become its LAST holder while it is still DOWNLOADING; the holder dtor then runs
/// `complete()` as a NON-downloader last holder and trips the assert. The drops run
/// on a helper thread so `getCallerId()` (thread-id based) differs from the
/// downloader -- exactly the cross-thread split a `max_threads` read produces
/// (download on a worker thread, teardown on the query thread). Legacy never hits
/// this: it downloads and resets the DOWNLOADING state on the same thread, so a
/// non-downloader is never the last holder of a DOWNLOADING segment.
TEST(ReaderExecutor, RealDiskCacheSiblingReadViewOutlivesDownloaderOfDownloadingSegment)
{
    DB::ServerUUID::setRandomForUnitTests();

    auto * saved_thread = DB::current_thread;
    DB::current_thread = nullptr;
    SCOPE_EXIT({ DB::current_thread = saved_thread; });

    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("reader_exec_sibling_downloading");
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    namespace fs = std::filesystem;
    auto cache_path = fs::temp_directory_path() / "reader_exec_sibling_dl_cache";
    fs::remove_all(cache_path);
    fs::create_directories(cache_path);
    SCOPE_EXIT({ fs::remove_all(cache_path); });

    DB::FileCacheSettings settings;
    settings[DB::FileCacheSetting::path] = cache_path.string();
    settings[DB::FileCacheSetting::max_size] = 64 * 1024;
    settings[DB::FileCacheSetting::max_elements] = 8;
    settings[DB::FileCacheSetting::max_file_segment_size] = 8 * 1024;
    settings[DB::FileCacheSetting::boundary_alignment] = 8 * 1024;
    settings[DB::FileCacheSetting::load_metadata_asynchronously] = false;
    settings[DB::FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("reader_exec_sibling_dl", settings);
    cache->initialize();

    DB::FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;

    /// Fix the key + origin so the manual download lands on exactly the segment
    /// `planResidencyView` will probe.
    auto key = DB::FileCacheKey::fromPath("sib_obj");
    auto origin = DB::FileCache::getCommonOrigin();
    auto provider = std::make_shared<DB::DiskCacheProvider>(
        cache, cache_settings, /*query_id_=*/String{}, /*local_throttler_=*/nullptr,
        std::optional<DB::FileCacheKey>(key), std::optional<DB::FileCacheOriginInfo>(origin));

    DB::StoredObject object("sib_obj", "sib_obj", 8192);

    /// Download a committed prefix and leave the segment DOWNLOADING (the downloader
    /// never completes -- modelling a download interrupted mid-flight). This thread
    /// holds the downloader role.
    auto dl_holder = cache->getOrSet(key, 0, 8192, 8192, DB::CreateFileSegmentSettings{}, 0, origin);
    ASSERT_EQ(dl_holder->size(), 1u);
    {
        auto & seg = dl_holder->front();
        ASSERT_EQ(seg.getOrSetDownloader(), DB::FileSegment::getCallerId());
        std::string failure_reason;
        ASSERT_TRUE(seg.reserve(2048, 1000, failure_reason)) << failure_reason;
        auto key_str = key.toString();
        auto subdir = fs::path(cache_path) / key_str.substr(0, 3) / key_str;
        if (!fs::exists(subdir))
            fs::create_directories(subdir);
        std::string payload(2048, 'Q');
        seg.write(payload.data(), payload.size(), seg.getCurrentWriteOffset());
        /// NB: no completePartAndResetDownloader -> stays DOWNLOADING, we remain the downloader.
        ASSERT_EQ(seg.state(), DB::FileSegment::State::DOWNLOADING);
    }

    /// A residency read view over the same range: its `read_holder` now passively
    /// pins the DOWNLOADING segment (committed prefix credited as a hit).
    auto view = provider->planResidencyView(object, /*object_file_offset=*/0, DB::ByteRange{0, 8192});
    ASSERT_TRUE(view != nullptr);

    /// Drop both holders on a HELPER thread: its thread-id makes `getCallerId()`
    /// differ from the downloader, so `complete()` does NOT reset the DOWNLOADING
    /// state. Dropping the downloader holder first leaves the segment DOWNLOADING
    /// held only by the read view; dropping the read view then completes a
    /// DOWNLOADING segment as a NON-downloader LAST holder -> the abort (pre-fix).
    std::thread dropper([&]
    {
        dl_holder = nullptr;   // non-downloader, not last -> segment stays DOWNLOADING
        view.reset();        // non-downloader, LAST holder of DOWNLOADING -> abort (pre-fix)
    });
    dropper.join();

    /// With the fix the abandoned download is recovered to PARTIALLY_DOWNLOADED (keeping the
    /// committed prefix) rather than completed-as-DOWNLOADING, so a fresh probe sees no stuck
    /// DOWNLOADING segment and the committed bytes survive for reuse.
    auto after = cache->get(key, 0, 8192, /*file_segments_limit=*/0, origin.user_id);
    bool has_live_downloading = false;
    bool has_committed_prefix = false;
    if (after)
        for (const auto & seg : *after)
        {
            if (seg->state() == DB::FileSegment::State::DOWNLOADING)
                has_live_downloading = true;
            if (seg->state() == DB::FileSegment::State::PARTIALLY_DOWNLOADED
                || seg->state() == DB::FileSegment::State::DOWNLOADED)
                has_committed_prefix = true;
        }
    EXPECT_FALSE(has_live_downloading);
    EXPECT_TRUE(has_committed_prefix);
}

namespace
{

/// A source whose buffer opens fine but throws on the first read. It drives the
/// executor's foreground inline-write path past `claim` (-> the miss segment
/// becomes DOWNLOADING) and then throws inside `fetch_into`, before any write completes
/// the segment -- the exact window the L1 guard must cover.
class ThrowOnReadBuffer : public ReadBufferFromFileBase
{
public:
    explicit ThrowOnReadBuffer(size_t file_size_)
        : ReadBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0, file_size_) {}

    String getFileName() const override { return "ThrowOnReadBuffer"; }

    off_t seek(off_t off, int whence) override
    {
        if (whence == SEEK_SET)
            file_offset = static_cast<size_t>(off);
        else if (whence == SEEK_CUR)
            file_offset += static_cast<size_t>(off);
        resetWorkingBuffer();
        return static_cast<off_t>(file_offset);
    }

    off_t getPosition() override { return static_cast<off_t>(file_offset); }
    size_t getFileOffsetOfBufferEnd() const override { return file_offset; }

private:
    bool nextImpl() override
    {
        throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_FILE, "ThrowOnReadBuffer: injected source read failure");
    }

    size_t file_offset = 0;
};

class ThrowOnReadSource : public IFileBasedSourceReader
{
public:
    explicit ThrowOnReadSource(size_t file_size_) : file_size(file_size_) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject &) override
    {
        return std::make_unique<ThrowOnReadBuffer>(file_size);
    }

    String name() const override { return "ThrowOnReadSource"; }

private:
    size_t file_size;
};

}

/// Executor-level repro of the abort via the FOREGROUND inline-write path
/// (`fetchAndBackfillGaps`): a cold read elects the FileCache downloader of a miss
/// segment (-> DOWNLOADING), then the source throws before the write completes it. The
/// foreground (unlike the worker's `coordinatedPrefetch`, which has a `releaseElected
/// Downloaders` scope guard) does NOT reset the elected downloader on the exception, so
/// the segment is left DOWNLOADING in the retained plan. Destroying the executor on
/// another thread (the teardown thread is not the foreground/downloader thread, as in a
/// `max_threads` read) then completes a DOWNLOADING segment as a non-downloader last
/// holder -> the abort. The L1 foreground guard resets it on the foreground thread, so
/// teardown is clean.
TEST(ReaderExecutor, ForegroundElectThenSourceThrowLeavesNoDownloadingSegment)
{
    DB::ServerUUID::setRandomForUnitTests();

    auto * saved_thread = DB::current_thread;
    DB::current_thread = nullptr;
    SCOPE_EXIT({ DB::current_thread = saved_thread; });

    DB::ThreadStatus thread_status;

    Poco::XML::DOMParser dom_parser;
    std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    getMutableContext().context->setConfig(config);

    auto query_context = DB::Context::createCopy(getContext().context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("reader_exec_fg_elect_throw");
    chassert(&DB::CurrentThread::get() == &thread_status);
    auto query_scope_holder = DB::QueryScope::create(query_context);

    namespace fs = std::filesystem;
    auto cache_path = fs::temp_directory_path() / "reader_exec_fg_elect_throw_cache";
    fs::remove_all(cache_path);
    fs::create_directories(cache_path);
    SCOPE_EXIT({ fs::remove_all(cache_path); });

    DB::FileCacheSettings settings;
    settings[DB::FileCacheSetting::path] = cache_path.string();
    settings[DB::FileCacheSetting::max_size] = 64 * 1024;
    settings[DB::FileCacheSetting::max_elements] = 8;
    settings[DB::FileCacheSetting::max_file_segment_size] = 8 * 1024;
    settings[DB::FileCacheSetting::boundary_alignment] = 8 * 1024;
    settings[DB::FileCacheSetting::load_metadata_asynchronously] = false;
    settings[DB::FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

    auto cache = std::make_shared<DB::FileCache>("reader_exec_fg_elect_throw", settings);
    cache->initialize();

    DB::FilesystemCacheSettings cache_settings;
    cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
    auto provider = std::make_shared<DB::DiskCacheProvider>(cache, cache_settings, /*query_id_=*/String{});

    auto source = std::make_shared<ThrowOnReadSource>(8000);
    StoredObjects objects;
    objects.emplace_back("fg_obj", "fg_obj", 8000);

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(provider);

    auto limit = std::make_shared<LongConnectionLimit>(10);
    /// No prefetch pool -> the foreground does the fetch synchronously on THIS thread.
    ReaderExecutor::Options executor_options;
    executor_options.window_size = 2000;
    executor_options.min_bytes_for_seek = 0;
    executor_options.long_connection_limit = limit;
    auto executor = std::make_unique<ReaderExecutor>(source, objects, caches, executor_options);

    /// Foreground read on THIS thread: elects the miss segment (-> DOWNLOADING) then the
    /// source throws. The elected downloader's role is bound to this thread.
    EXPECT_ANY_THROW(executor->readNextWindow());

    /// Tear the executor down on a DIFFERENT thread (not the downloader thread). Without
    /// the L1 guard the retained plan still pins the DOWNLOADING segment, and its read
    /// holder completes it as a non-downloader last holder -> abort. With the guard the
    /// segment was reset on the foreground thread, so teardown is clean.
    std::thread destroyer([&] { executor.reset(); });
    destroyer.join();

    SUCCEED();
}

/// The metrics tests read the executor's ProfileEvents from a fresh per-test ThreadGroup
/// (starts at zero) -- the same path that feeds `system.events`.
TEST(ReaderExecutor, ProfileEventsCountSourceReadsAndBytes)
{
    TestThreadGroup tg;

    /// 1 MiB file read in 256 KiB windows -> 4 stateless source opens, all bytes served.
    constexpr size_t size = 1024 * 1024;
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(size, 'M')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", size);
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = 256 * 1024;
        ReaderExecutor executor(source, objects, {}, executor_options);
        while (!executor.readNextWindow().empty()) {}
    }

    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorSourceRequests), 4u);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorBytesFromSource), size);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorRequestedBytes), size);
    /// No cache tiers configured: the cache counters stay 0.
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorCacheGetRequests), 0u);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorCachePopulateRequests), 0u);
    /// Local readers have no right-bounded support and no connection to abandon, so
    /// the one-shots count 0 incomplete - as a bounded remote one-shot would (a known
    /// size bounds each stateless read to exactly its window).
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorIncompleteConnections), 0u);
}

TEST(ReaderExecutor, ModeledCostMatchesFormula)
{
    TestThreadGroup tg;

    /// Modeled cost = 30ms/source request + 20ms/MiB from source (cache and
    /// incomplete-connection terms 0): 4 window-sized requests + 1 MiB transferred.
    /// The local one-shots count no incomplete connections (nothing to abandon).
    constexpr size_t size = 1024 * 1024;
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(size, 'C')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", size);
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = 256 * 1024;
        ReaderExecutor executor(source, objects, {}, executor_options);
        while (!executor.readNextWindow().empty()) {}
    }

    const auto cost = tg.get(ProfileEvents::ReaderExecutorModeledCostMicroseconds);
    const auto requested = tg.get(ProfileEvents::ReaderExecutorRequestedBytes);
    EXPECT_EQ(cost, 30000u * 4 + 20000u);  // 4 reads + 1 MiB
    EXPECT_EQ(requested, size);

    /// The KPI: modeled ms per requested MiB.
    const double ms_per_mib = (static_cast<double>(cost) / 1000.0)
        / (static_cast<double>(requested) / (1024.0 * 1024.0));
    EXPECT_DOUBLE_EQ(ms_per_mib, 140.0);
}

TEST(ReaderExecutor, ModeledCostScalesWithSourceRequests)
{
    TestThreadGroup tg;

    /// Smaller windows over the same data -> more source requests -> higher modeled cost,
    /// so the KPI (cost per requested MiB) rises even though the bytes are unchanged.
    constexpr size_t size = 1024 * 1024;
    {
        auto source = std::make_shared<MemorySourceReader>(
            std::unordered_map<String, String>{{"a.bin", String(size, 'a')}});
        StoredObjects objects;
        objects.emplace_back("a.bin", "", size);
        ReaderExecutor::Options coarse_options;
        coarse_options.window_size = 1024 * 1024;
        ReaderExecutor coarse(source, objects, {}, coarse_options);
        while (!coarse.readNextWindow().empty()) {}
    }
    const auto cost_after_coarse = tg.get(ProfileEvents::ReaderExecutorModeledCostMicroseconds);
    const auto requests_after_coarse = tg.get(ProfileEvents::ReaderExecutorSourceRequests);
    {
        auto source = std::make_shared<MemorySourceReader>(
            std::unordered_map<String, String>{{"b.bin", String(size, 'b')}});
        StoredObjects objects;
        objects.emplace_back("b.bin", "", size);
        ReaderExecutor::Options fine_options;
        fine_options.window_size = 64 * 1024;
        ReaderExecutor fine(source, objects, {}, fine_options);
        while (!fine.readNextWindow().empty()) {}
    }
    const auto cost_after_fine = tg.get(ProfileEvents::ReaderExecutorModeledCostMicroseconds);
    const auto requests_after_fine = tg.get(ProfileEvents::ReaderExecutorSourceRequests);

    EXPECT_EQ(requests_after_coarse, 1u);
    EXPECT_EQ(requests_after_fine - requests_after_coarse, 16u);
    EXPECT_GT(cost_after_fine - cost_after_coarse, cost_after_coarse);
}

namespace
{

/// In-memory right-bounded buffer whose `nextImpl` consumes one `gate` token
/// per call once `free_calls` are spent, signalling `entered` at its first
/// gated call. Holds a machine's fetch step at a deterministic mid-window
/// block so a test can interrupt it between blocks.
class GatedBuffer : public ReadBufferFromFileBase
{
public:
    GatedBuffer(const String & data_, size_t buf_size, size_t free_calls_,
                std::latch & entered_, std::counting_semaphore<> & gate_)
        : ReadBufferFromFileBase(buf_size, nullptr, 0)
        , data(data_), free_calls(free_calls_), entered(entered_), gate(gate_) {}

    String getFileName() const override { return "GatedBuffer"; }
    bool supportsRightBoundedReads() const override { return true; }
    void setReadUntilPosition(size_t p) override { read_until = p; }

    off_t seek(off_t off, int whence) override
    {
        file_offset = whence == SEEK_SET ? static_cast<size_t>(off) : file_offset + static_cast<size_t>(off);
        resetWorkingBuffer();
        return static_cast<off_t>(file_offset);
    }
    off_t getPosition() override { return static_cast<off_t>(file_offset); }
    size_t getFileOffsetOfBufferEnd() const override { return file_offset; }

private:
    bool nextImpl() override
    {
        if (calls++ >= free_calls)
        {
            if (!entered_signalled)
            {
                entered_signalled = true;
                entered.count_down();
            }
            gate.acquire();
        }
        const size_t end = read_until ? std::min(*read_until, data.size()) : data.size();
        if (file_offset >= end)
            return false;
        const size_t n = std::min(end - file_offset, internal_buffer.size());
        memcpy(internal_buffer.begin(), data.data() + file_offset, n);
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + n);
        file_offset += n;
        return true;
    }

    String data;
    size_t free_calls;
    std::latch & entered;
    std::counting_semaphore<> & gate;
    size_t calls = 0;
    bool entered_signalled = false;
    size_t file_offset = 0;
    std::optional<size_t> read_until;
};

/// Source whose `gated_open_index`-th open (0-based) returns a gated buffer;
/// every other open is ungated (`free_calls = SIZE_MAX`) over the same content.
class GatedSource : public IFileBasedSourceReader
{
public:
    GatedSource(String content_, size_t gated_open_index_, size_t buf_size_, size_t free_calls_,
                std::latch & entered_, std::counting_semaphore<> & gate_)
        : content(std::move(content_)), gated_open_index(gated_open_index_), buf_size(buf_size_)
        , free_calls(free_calls_), entered(entered_), gate(gate_) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject &) override
    {
        const size_t idx = opens.fetch_add(1);
        const size_t free = idx == gated_open_index ? free_calls : std::numeric_limits<size_t>::max();
        return std::make_unique<GatedBuffer>(content, buf_size, free, entered, gate);
    }

    String name() const override { return "GatedSource"; }

    std::atomic<size_t> opens{0};

private:
    String content;
    size_t gated_open_index;
    size_t buf_size;
    size_t free_calls;
    std::latch & entered;
    std::counting_semaphore<> & gate;
};

}

TEST(ReaderExecutor, TakeoverServesPartialPrefixWithoutDataLoss)
{
    /// A collect that catches the machine mid-window interrupts it instead of
    /// blocking for the full window: the fetched prefix is served and the
    /// remainder re-covered by the normal dispatch. Scheduling decides whether
    /// the interrupt lands mid-fetch (partial collect) or the worker finishes
    /// first (plain hit) - BOTH must deliver byte-identical data. Whenever the
    /// partial path fires it also pins the interrupt-short guard: without the
    /// flag-first check in `fetchGapsFromSource`, a size-known short read would
    /// throw CANNOT_READ_ALL_DATA and fail this test.
    constexpr size_t FILE_SIZE = 16000;
    constexpr size_t WINDOW = 4000;
    constexpr size_t BLOCK = 250;
    String content(FILE_SIZE, 0);
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('a' + (i % 23));

    std::latch entered{1};
    std::counting_semaphore<> gate{0};
    /// Open #0 = window 1's synchronous one-shot (ungated). Open #1 = the
    /// machine's fetch for window 2: one free block, then one token per block.
    auto source = std::make_shared<GatedSource>(
        content, /*gated_open_index=*/1, BLOCK, /*free_calls=*/1, entered, gate);

    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    auto pool = std::make_shared<PrefetchThreadPool>(1);
    TestThreadGroup tg;
    String result;
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = WINDOW;
        executor_options.min_bytes_for_seek = 0;
        executor_options.block_size = BLOCK;
        executor_options.prefetch_pool = pool;
        ReaderExecutor executor(source, objects, {}, executor_options);

        auto w1 = executor.readNextWindow();
        ASSERT_EQ(w1.range().size, WINDOW);
        for (const auto & node : w1.getNodes())
            result.append(node.data(), node.size);

        /// The machine for window 2 is mid-fetch at the gate. Feed it one block
        /// per token from a helper while the collect below interrupts and waits.
        entered.wait();
        std::thread feeder([&]
        {
            for (int i = 0; i < 1000; ++i)
                gate.release();
        });

        while (true)
        {
            auto chain = executor.readNextWindow();
            if (chain.empty())
                break;
            for (const auto & node : chain.getNodes())
                result.append(node.data(), node.size);
        }
        feeder.join();
    }

    EXPECT_EQ(result, content) << "interrupted/partial collects must not lose or duplicate bytes";
    const auto partials = tg.get(ProfileEvents::ReaderExecutorPartialCollects);
    const auto interrupted = tg.get(ProfileEvents::ReaderExecutorMachineInterrupted);
    EXPECT_LE(partials, interrupted) << "a partial collect implies an interrupted machine";
    /// Which outcome the scheduling produced (both are valid; see the header
    /// comment) - recorded so CI history shows the partial path is exercised.
    RecordProperty("partial_collects", static_cast<int>(partials));
    RecordProperty("machine_interrupted", static_cast<int>(interrupted));
}

namespace
{

/// Page-cache fixture for the deferred-fill tests: a real in-process PageCache
/// + provider over the single object "obj".
struct PageCacheFixture
{
    static constexpr size_t CAP = 64ull << 20;

    std::shared_ptr<PageCache> cache = std::make_shared<PageCache>(
        std::chrono::milliseconds(2000), "LRU", 0.5,
        /*min_size_in_bytes=*/CAP, /*max_size_in_bytes=*/CAP,
        /*free_memory_ratio=*/0.0, /*num_shards=*/1);

    std::shared_ptr<PageCacheProvider> provider(size_t block, size_t file_size) const
    {
        PageCacheFile file;
        file.path = "obj";
        return std::make_shared<PageCacheProvider>(
            cache, std::move(file), block, /*inject_eviction=*/false,
            /*bypass_if_missing=*/false, /*file_size_in_bytes=*/file_size);
    }
};

}

TEST(ReaderExecutor, MachineCollectFillsCacheInline)
{
    /// A machine-collected window's cache fill runs INLINE on the read thread
    /// (`BytesPushedToCacheSync`), exactly like the first sync-path window; the
    /// async fill path is retired with the put lane. The warm pass is
    /// deterministic: `observeAndSchedule` settles every fill before rebuilding,
    /// so after seek(0) the whole file is page-cache resident.
    constexpr size_t FILE_SIZE = 16000;
    constexpr size_t WINDOW = 2000;
    constexpr size_t BLOCK = 500;
    String content(FILE_SIZE, 0);
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('A' + (i % 29));

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    PageCacheFixture pc;
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(pc.provider(BLOCK, FILE_SIZE));

    /// Inline pool: machine fetches complete at launch, so every collect is a
    /// full one and deterministically writes its fill inline. (A real pool in a
    /// zero-think-time drain loop revokes/interrupts most machines before their
    /// first block - covered by the gated takeover test.)
    auto pool = std::make_shared<SyncPrefetchPool>();
    TestThreadGroup tg;
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = WINDOW;
        executor_options.min_bytes_for_seek = 0;
        executor_options.block_size = BLOCK;
        executor_options.prefetch_pool = pool;
        ReaderExecutor executor(source, objects, caches, executor_options);

        String cold;
        while (true)
        {
            auto chain = executor.readNextWindow();
            if (chain.empty())
                break;
            for (const auto & node : chain.getNodes())
                cold.append(node.data(), node.size);
        }
        EXPECT_EQ(cold, content);
        EXPECT_GT(tg.get(ProfileEvents::ReaderExecutorBytesPushedToCacheSync), 0u)
            << "machine-collected windows fill the cache inline on the read thread";
    }
    /// Cold executor destroyed: its fills already landed inline, so the page cache
    /// now holds the whole file.

    /// Warm pass with a FRESH executor over the same cache: its residency plan
    /// sees the inline fills as plain hits - nothing from the source. (A seek
    /// within the cold executor would NOT show this: its plan geometry is an
    /// immutable all-miss snapshot, so its machines re-fetch and only the
    /// collect prefers the cache copies.)
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = WINDOW;
        executor_options.min_bytes_for_seek = 0;
        executor_options.block_size = BLOCK;
        executor_options.prefetch_pool = pool;
        ReaderExecutor executor(source, objects, caches, executor_options);
        const auto src_before = tg.get(ProfileEvents::ReaderExecutorBytesFromSource);
        String warm;
        while (true)
        {
            auto chain = executor.readNextWindow();
            if (chain.empty())
                break;
            for (const auto & node : chain.getNodes())
                warm.append(node.data(), node.size);
        }
        EXPECT_EQ(warm, content);
        EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorBytesFromSource), src_before)
            << "warm pass must be served entirely from the page cache";
        EXPECT_GT(tg.get(ProfileEvents::ReaderExecutorBytesFromPageCache), 0u);
    }
}

TEST(ReaderExecutor, WarmServePromotesInline)
{
    /// The warm-path lever: an fs-resident scan serves from the slower tier and
    /// promotes the served runs into the faster (page) tier - inline on the serve
    /// thread (the put lane is gone). The fs mock stores 'D' bytes (the source is 'X'),
    /// so an all-'D' result proves the serve came from the fs tier; the source must
    /// never be read.
    constexpr size_t FILE_SIZE = 8000;
    constexpr size_t SEG = 2000;
    constexpr size_t WINDOW = 1000;
    constexpr size_t BLOCK = 500;

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(FILE_SIZE, 'X')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    auto fs = std::make_shared<EvictableSegmentMockCache>(SEG);
    for (size_t i = 0; i < FILE_SIZE / SEG; ++i)
        fs->seedSegment(i, 'D');   /// every segment fully resident with genuine 'D' bytes
    PageCacheFixture pcfix;
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(pcfix.provider(BLOCK, FILE_SIZE));   /// faster tier, cold
    caches.push_back(fs);                                 /// slower tier, warm

    auto pool = std::make_shared<SyncPrefetchPool>();
    TestThreadGroup tg;
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = WINDOW;
        executor_options.min_bytes_for_seek = 0;
        executor_options.block_size = BLOCK;
        executor_options.prefetch_pool = pool;
        ReaderExecutor executor(source, objects, caches, executor_options);

        String result;
        while (true)
        {
            auto chain = executor.readNextWindow();
            if (chain.empty())
                break;
            for (const auto & node : chain.getNodes())
                result.append(node.data(), node.size);
        }
        EXPECT_EQ(result, String(FILE_SIZE, 'D')) << "warm serve must come from the fs tier";
        EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorBytesFromSource), 0u);
        EXPECT_GT(tg.get(ProfileEvents::ReaderExecutorBytesPromoted), 0u)
            << "served runs must be promoted into the faster tier";
    }
}

namespace
{

/// Stage 3 spine: drive a REAL executor over a seeded residency and assert its
/// readNextWindow outputs equal buildSchedule's predicted steps. Window/block/
/// look-ahead are set >= the file so runs are never chunked and the plan never
/// rebuilds - the schedule's maximal-run steps then map 1:1 to the live calls.
///
/// Byte-KPIs computed analytically from the schedule (the cost oracle): every
/// byte's origin is in the schedule, so R / served need no run.
struct PredictedKpi { size_t from_source = 0; size_t served_from_cache = 0; };

PredictedKpi predictKpi(const PlanSchedule & s)
{
    PredictedKpi k;
    for (const auto & r : s.retrieves)
        if (r.source == PlanSchedule::Source::Remote)
            k.from_source += r.range.size;
    /// The cache is the buffer: EVERY delivered User byte is read out of a cell (a resident hit, or
    /// a miss filled from the source then read back), so it all counts as a cache read - a cold miss
    /// shows both `from_source` (filling the cell) and `served_from_cache` (serving it out). (These
    /// plans have no bypass gap, the one path that serves from a bank instead of a cell.)
    for (const auto & tr : s.ranges)
        if (tr.purpose == PlanSchedule::Purpose::User)
            k.served_from_cache += tr.range.size;
    return k;
}

void validateScheduleMatchesReality(
    std::shared_ptr<IFileBasedSourceReader> src,
    const StoredObjects & objects,
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches,
    size_t file_size,
    size_t min_bytes_for_seek)
{
    ReaderExecutor::Options opts;
    opts.window_size = file_size * 2 + 1;
    opts.block_size = file_size * 2 + 1;
    opts.plan_look_ahead_max_window = file_size * 2 + 1;
    opts.min_bytes_for_seek = min_bytes_for_seek;

    TestThreadGroup tg;  /// per-call ProfileEvents context for the KPI deltas
    auto & pe = CurrentThread::getProfileEvents();
    const auto src0 = pe[ProfileEvents::ReaderExecutorBytesFromSource];
    const auto page0 = pe[ProfileEvents::ReaderExecutorBytesFromPageCache];
    const auto fs0 = pe[ProfileEvents::ReaderExecutorBytesFromFilesystemCache];

    ReaderExecutor executor(src, objects, std::move(caches), opts);

    std::vector<ByteRange> outputs;
    std::shared_ptr<const CoverageMap> geom;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (!geom)
            geom = inspect(executor).planGeometry();  /// the initial (and only) plan
        if (chain.empty())
            break;
        outputs.push_back(chain.range());
    }

    ASSERT_NE(geom, nullptr);
    /// The oracle must predict with the HARNESS executor's serve sizes (window/block span the
    /// file, so runs are never chunked) - mismatched constants would silently diverge if this
    /// oracle ever predicts per-call windows.
    auto sched = buildSchedule(*geom,
        /*serve_window_bytes=*/file_size * 2 + 1, /*serve_block_bytes=*/file_size * 2 + 1);

    ASSERT_EQ(outputs.size(), sched.serve_runs.size()) << "serve-run count vs live windows";
    for (size_t i = 0; i < outputs.size(); ++i)
    {
        EXPECT_EQ(outputs[i].offset, sched.serve_runs[i].output.offset) << "window " << i << " offset";
        EXPECT_EQ(outputs[i].size, sched.serve_runs[i].output.size) << "window " << i << " size";
    }

    /// The schedule's predicted byte-KPIs equal the executor's actual
    /// ProfileEvents - for non-bridged plans. With bridging (`min_bytes_for_seek
    /// > 0`) the schedule's `connections()` predicts a resident hole is
    /// over-read from remote, but whether the executor actually bridges it is a
    /// runtime live-connection decision the static schedule cannot predict
    /// exactly (and the >= file window distorts it further), so the R
    /// prediction is only an upper bound there. The no-bridge case is exact.
    if (min_bytes_for_seek == 0)
    {
        const auto k = predictKpi(sched);
        EXPECT_EQ(pe[ProfileEvents::ReaderExecutorBytesFromSource] - src0, k.from_source) << "R";
        EXPECT_EQ((pe[ProfileEvents::ReaderExecutorBytesFromPageCache] - page0)
                + (pe[ProfileEvents::ReaderExecutorBytesFromFilesystemCache] - fs0),
            k.served_from_cache) << "served from cache";
    }
}

std::pair<std::shared_ptr<MemorySourceReader>, StoredObjects> srcOf(size_t file_size)
{
    auto src = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(file_size, 'S')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);
    return {src, objects};
}

}

TEST(PlanScheduleValidation, ColdAllMiss)
{
    auto [src, objects] = srcOf(256 * 1024);
    auto fs = std::make_shared<WideGranularityMockCache>(64 * 1024, "fs");
    validateScheduleMatchesReality(src, objects, {fs}, 256 * 1024, /*min_bytes_for_seek=*/0);
}

TEST(PlanScheduleValidation, TwoTierDisjointHits)
{
    auto [src, objects] = srcOf(256 * 1024);
    auto page = std::make_shared<WideGranularityMockCache>(64 * 1024, "page");
    auto fs = std::make_shared<WideGranularityMockCache>(64 * 1024, "fs");
    page->seedBlock(0, 'P');   // [0,64K)
    fs->seedBlock(2, 'D');     // [128K,192K)
    validateScheduleMatchesReality(src, objects, {page, fs}, 256 * 1024, /*min_bytes_for_seek=*/0);
}

TEST(PlanScheduleValidation, ResidentIsland)
{
    auto [src, objects] = srcOf(256 * 1024);
    auto page = std::make_shared<WideGranularityMockCache>(64 * 1024, "page");
    page->seedBlock(1, 'P');   // resident island [64K,128K), gaps either side
    auto fs = std::make_shared<WideGranularityMockCache>(64 * 1024, "fs");
    validateScheduleMatchesReality(src, objects, {page, fs}, 256 * 1024, /*min_bytes_for_seek=*/0);
}

TEST(PlanScheduleValidation, MixedGranularitiesWithBridge)
{
    auto [src, objects] = srcOf(512 * 1024);
    auto page = std::make_shared<WideGranularityMockCache>(64 * 1024, "page");
    auto fs = std::make_shared<WideGranularityMockCache>(256 * 1024, "fs");
    page->seedBlock(3, 'P');   // resident [192K,256K) inside the first fs segment
    validateScheduleMatchesReality(src, objects, {page, fs}, 512 * 1024, /*min_bytes_for_seek=*/64 * 1024);
}

/// Stage 2 assert spine: the shadow cursor tracks the live walk. With one block per
/// window and one plan over the whole file, the cursor must - after every window -
/// index the step whose output contains the current position (the invariant the
/// schedule-driven serve will rely on). A resident island gives mixed hit/gap steps,
/// and a prefetched gap must reach the Ready phase, exercising the lifecycle shadow.
/// The per-window chasserts inside readNextWindow are the broader burn-in; this is the
/// focused external check. The shadow is only maintained under DEBUG_OR_SANITIZER_BUILD,
/// so its assertions are guarded (the read still runs in release, exercising the path).
TEST(ReaderExecutor, ScheduleShadowsLiveWalk)
{
    const size_t block = 4096;
    const size_t file = block * 16;  // 64K
    auto [src, objects] = srcOf(file);
    auto cache = std::make_shared<WideGranularityMockCache>(block, "Mock");
    cache->seedBlock(4, 'R');  // resident island [16K,24K) -> hit steps amid gaps
    cache->seedBlock(5, 'R');

    auto pool = std::make_shared<SyncPrefetchPool>();  // machine lifecycle resolves inline
    ReaderExecutor::Options opts;
    opts.window_size = block;            // one block per window -> the cursor walks many windows
    opts.block_size = block;
    opts.plan_look_ahead_max_window = file;  // ONE plan over the file -> the cursor spans it
    opts.min_bytes_for_seek = 0;         // no bridging -> 1:1 gap:retrieve, clean phases
    opts.prefetch_pool = pool;
    ReaderExecutor executor(src, objects, {cache}, opts);

    size_t windows = 0;
    [[maybe_unused]] bool saw_progress = false;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        ++windows;
#if defined(DEBUG_OR_SANITIZER_BUILD)
        const size_t pos = executor.getPosition();  // physical == logical (no encryption)
        const size_t c = inspect(executor).serveRunAt(pos);
        ASSERT_LT(c, inspect(executor).serveRunCount());
        const auto run = inspect(executor).serveRunOutput(c);
        EXPECT_TRUE(run.offset <= pos && pos <= run.offset + run.size)
            << "the derived serve run [" << run.offset << "," << run.offset + run.size
            << ") must contain position " << pos;
        for (size_t i = 0; i < inspect(executor).retrieveCount(); ++i)
            if (inspect(executor).retrieveLaunchProgress(i) > 0)
                saw_progress = true;
#endif
    }
    EXPECT_GT(windows, 4u) << "the file must take several windows so the cursor walks";
#if defined(DEBUG_OR_SANITIZER_BUILD)
    EXPECT_TRUE(saw_progress) << "a prefetched gap must show launch progress / completion";
#endif
}

/// The schedule-driven interpreter serves a coalesced job spanning a bridged hole and
/// multi-window jobs byte-correctly: it changes the fetch coordination, not the served
/// windows. Two resident islands (one wide, kept separate; one narrow, bridged into the
/// coalesced source job) over an all-'S' source - the served bytes are the cache-seeded
/// chars where resident, 'S' from the source elsewhere. (In a debug build this also
/// exercises the assert spine.)
TEST(ReaderExecutor, ScheduleDrivenCoalescedReadContent)
{
    const size_t block = 4096;
    const size_t file = block * 20;  // 80K
    auto [src, objects] = srcOf(file);
    auto page = std::make_shared<WideGranularityMockCache>(block, "page");
    auto fs = std::make_shared<WideGranularityMockCache>(block, "fs");
    page->seedBlock(2, 'P'); page->seedBlock(3, 'P');  // wide island [8K,16K), not bridged
    fs->seedBlock(10, 'F');                            // narrow island [40K,44K), bridged
    auto pool = std::make_shared<SyncPrefetchPool>();
    ReaderExecutor::Options opts;
    opts.window_size = block * 3;
    opts.block_size = block;
    opts.plan_look_ahead_max_window = file;
    opts.min_bytes_for_seek = block;   // bridges the 4K island into one coalesced job
    opts.prefetch_pool = pool;
    ReaderExecutor executor(src, objects, {page, fs}, opts);
    String out;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            out.append(node.data(), node.size);
    }
    String expected(file, 'S');
    for (size_t i = 2 * block; i < 4 * block; ++i) expected[i] = 'P';   // wide island served from cache
    for (size_t i = 10 * block; i < 11 * block; ++i) expected[i] = 'F'; // narrow island served from cache
    EXPECT_EQ(out, expected) << "schedule-driven read must serve the resident islands from cache and the rest from source";
}

/// A backward seek INSIDE the look-ahead plan must not serve a discontiguous chain: the
/// jump invalidates the ahead-banked ready_bytes, so the interpreter must re-plan from the
/// new position. Read forward (banking ahead), seek back inside the plan, read to the end,
/// and check the post-seek tail is the contiguous, byte-correct [4K, end).
TEST(ReaderExecutor, ScheduleDrivenBackwardSeekTail)
{
    const size_t block = 4096;
    const size_t file = block * 20;  // 80K
    auto [src, objects] = srcOf(file);
    auto cache = std::make_shared<WideGranularityMockCache>(block, "c");
    cache->seedBlock(5, 'R');  // an island so steps mix hit/gap
    auto pool = std::make_shared<SyncPrefetchPool>();
    ReaderExecutor::Options opts;
    opts.window_size = block * 2;
    opts.block_size = block;
    opts.plan_look_ahead_max_window = file;   // ONE plan -> the seek stays within it
    opts.min_bytes_for_seek = block;
    opts.prefetch_pool = pool;
    ReaderExecutor executor(src, objects, {cache}, opts);
    for (int i = 0; i < 4; ++i)           // read forward ~32K, banking ahead
        executor.readNextWindow();
    executor.seek(block);                 // backward, into the surviving plan
    String out;
    while (true)
    {
        auto r = executor.readNextWindow();
        if (r.empty())
            break;
        for (const auto & node : r.getNodes())
            out.append(node.data(), node.size);
    }
    String expected(file - block, 'S');                                 // tail [4K, end)
    for (size_t i = 5 * block; i < 6 * block; ++i) expected[i - block] = 'R'; // island block 5 served from cache
    EXPECT_EQ(out, expected) << "post-backward-seek tail must be the contiguous, byte-correct [4K, end)";
}

/// The per-mark-range setReadExtent cadence (advance the extent one window ahead before
/// each read) must not strand a stale machine handle: setReadExtent cancels the in-flight
/// prefetch, and a later serve must not collect the cancelled machine. A full byte-correct
/// read (and no crash) confirms the cancelled job's machine handle is cleared.
TEST(ReaderExecutor, ScheduleDrivenSetReadExtentCadence)
{
    const size_t block = 4096;
    const size_t file = block * 20;  // 80K
    auto [src, objects] = srcOf(file);
    auto cache = std::make_shared<WideGranularityMockCache>(block, "c");
    auto pool = std::make_shared<SyncPrefetchPool>();
    ReaderExecutor::Options opts;
    opts.window_size = block * 2;
    opts.block_size = block;
    opts.plan_look_ahead_max_window = file;
    opts.min_bytes_for_seek = 0;
    opts.prefetch_pool = pool;
    ReaderExecutor executor(src, objects, {cache}, opts);
    String out;
    while (true)
    {
        executor.setReadExtent(std::min(file, executor.getPosition() + block * 2));  // advance ahead of the cursor
        auto r = executor.readNextWindow();
        if (r.empty())
            break;
        for (const auto & node : r.getNodes())
            out.append(node.data(), node.size);
    }
    EXPECT_EQ(out, String(file, 'S')) << "per-window setReadExtent cadence must read the file byte-correctly";
}

/// Schedule-driven fill, seek mid-coarse-segment: seeking to 64K lands inside
/// the slow tier's 256K segment [0,256K), whose head [0,64K) is before-slack.
/// The schedule-driven borrow must still fill that whole segment (it owns its
/// slack), pulling the [0,64K) head from the fetch. The fast 32K tier owns no
/// slack here - its residency is probed only over the plan span (from 64K up),
/// so it has no cell below 64K (the slack-not-promoted rule is a model
/// invariant the executor geometry never exercises - the meaningful check is
/// the hand-built `PlanScheduleRetrieves.SlackNotPromotedToFasterTier`).
TEST(ReaderExecutor, SchedulesFillOfSeekStraddledLowerSegment)
{
    const size_t file = 256 * 1024;
    auto [src, objects] = srcOf(file);
    auto fast = std::make_shared<WideGranularityMockCache>(32 * 1024, "FastMock");
    auto slow = std::make_shared<WideGranularityMockCache>(256 * 1024, "SlowMock");

    auto pool = std::make_shared<SyncPrefetchPool>();  // deferred fill runs inline, deterministic
    ReaderExecutor::Options opts;
    opts.window_size = file;
    opts.block_size = file;
    opts.plan_look_ahead_max_window = file;
    opts.min_bytes_for_seek = 0;
    opts.prefetch_pool = pool;
    ReaderExecutor executor(src, objects, {fast, slow}, opts);
    executor.seek(64 * 1024);  // request [64K,256K); slow segment head [0,64K) is before-slack

    while (!executor.readNextWindow().empty()) {}
    executor.seek(0);  // tear the plan down so all deferred fills reap

    /// Slow tier owns the segment: it fills from 0 (incl. the [0,64K) slack head).
    EXPECT_TRUE(slow->hasBlock(0)) << "slow segment fills whole, incl. the before-slack head";
    /// Fast tier has no cell below the plan start (64K), so it never receives the
    /// slack - automatic from the plan-bounded geometry, not the borrow filter.
    for (const auto & [range, total] : fast->putLog())
        EXPECT_GE(range.offset, 64u * 1024u) << "fast tier has no sub-plan-start cell";
    EXPECT_TRUE(fast->hasBlock(2)) << "fast tier still fills the requested [64K,...) blocks";
}

/// Several fetches fill several gaps within ONE plan: resident islands split the
/// file into gaps, and a one-block window makes each multi-block gap take
/// several fetches - all driven by the single schedule computed once for the
/// plan. Every gap block must end up cached.
TEST(ReaderExecutor, SeveralFetchesFillAllGaps)
{
    const size_t block = 4096;
    const size_t nblocks = 8;
    const size_t file = block * nblocks;  // 32K
    auto [src, objects] = srcOf(file);
    auto cache = std::make_shared<WideGranularityMockCache>(block, "Mock");
    cache->seedBlock(2, 'R');  // resident islands at blocks 2 and 5 ...
    cache->seedBlock(5, 'R');  // ... so the gaps are blocks {0,1}, {3,4}, {6,7}

    auto pool = std::make_shared<SyncPrefetchPool>();  // deferred fill runs inline, deterministic
    ReaderExecutor::Options opts;
    opts.window_size = block;             // one block per window -> a 2-block gap = two fetches
    opts.block_size = block;
    opts.plan_look_ahead_max_window = file;   // ONE plan over the whole file -> one schedule, many fetches
    opts.min_bytes_for_seek = 0;
    opts.prefetch_pool = pool;
    ReaderExecutor executor(src, objects, {cache}, opts);

    size_t delivered = 0;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        delivered += chain.range().size;
    }
    executor.seek(0);  // tear the plan down so any pending fills reap

    EXPECT_EQ(delivered, file) << "the whole file is delivered across the windowed reads";
    for (size_t b = 0; b < nblocks; ++b)
        EXPECT_TRUE(cache->hasBlock(b))
            << "gap block " << b << " must be cached after the multi-fetch fill";
}

/// The schedule predicts the executor's byte-KPIs exactly. Gap + resident island
/// + before-slack (seek mid-block) so R and served are both non-zero.
TEST(ReaderExecutor, SchedulePredictsByteKpis)
{
    const size_t block = 64 * 1024;
    const size_t file = 256 * 1024;
    auto [src, objects] = srcOf(file);
    auto fs = std::make_shared<WideGranularityMockCache>(block, "fs");
    fs->seedBlock(1, 'R');  // resident island [64K,128K)

    TestThreadGroup tg;
    auto & pe = CurrentThread::getProfileEvents();
    const auto src0 = pe[ProfileEvents::ReaderExecutorBytesFromSource];
    const auto page0 = pe[ProfileEvents::ReaderExecutorBytesFromPageCache];
    const auto fs0 = pe[ProfileEvents::ReaderExecutorBytesFromFilesystemCache];

    ReaderExecutor::Options opts;
    opts.window_size = file * 2;
    opts.block_size = file * 2;
    opts.plan_look_ahead_max_window = file * 2;
    opts.min_bytes_for_seek = 0;
    ReaderExecutor executor(src, objects, {fs}, opts);
    executor.seek(32 * 1024);  // mid-block-0 -> before-slack [0,32K) when filling block 0

    std::shared_ptr<const CoverageMap> geom;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (!geom)
            geom = inspect(executor).planGeometry();
        if (chain.empty())
            break;
    }
    ASSERT_NE(geom, nullptr);

    /// The oracle schedules over the geometry's own span - the live plan was built
    /// at the post-seek cursor, so `plan_start == 32 KiB` already.
    auto sched = buildSchedule(*geom,
        /*serve_window_bytes=*/file * 2, /*serve_block_bytes=*/file * 2);
    auto k = predictKpi(sched);

    EXPECT_EQ(pe[ProfileEvents::ReaderExecutorBytesFromSource] - src0, k.from_source) << "R";
    EXPECT_EQ((pe[ProfileEvents::ReaderExecutorBytesFromPageCache] - page0)
            + (pe[ProfileEvents::ReaderExecutorBytesFromFilesystemCache] - fs0),
        k.served_from_cache) << "served from cache";
}

/// Cache-chain policy: an embedded upper-tier hit at the CELL TAIL is served from
/// the upper tier and never re-fetched - the fast 64K tier holds [192K,256K) at the
/// tail of the slow 256K segment [0,256K); reading [0,512K) fetches only the true
/// gaps ([0,192K)+[256K,512K) = 448K). Nothing writes the hit down into the slow
/// cell (no cross-tier down-fill), so the slow segment keeps a tail hole - it heals
/// as a plain miss once the upper tier evicts.
TEST(ReaderExecutor, EmbeddedUpperHitAtCellTailIsNotRefetched)
{
    const size_t file = 512 * 1024;
    auto [src, objects] = srcOf(file);
    auto fast = std::make_shared<WideGranularityMockCache>(64 * 1024, "fast");
    auto slow = std::make_shared<WideGranularityMockCache>(256 * 1024, "slow");
    fast->seedBlock(3, 'F');  // resident [192K,256K), embedded in the slow segment [0,256K)

    TestThreadGroup tg;
    auto & pe = CurrentThread::getProfileEvents();
    const auto src0 = pe[ProfileEvents::ReaderExecutorBytesFromSource];

    ReaderExecutor::Options opts;
    opts.window_size = file * 2 + 1;       // window >= segment: the production case
    opts.block_size = file * 2 + 1;
    opts.plan_look_ahead_max_window = file * 2 + 1;
    opts.min_bytes_for_seek = 64 * 1024;
    ReaderExecutor executor(src, objects, {fast, slow}, opts);

    while (!executor.readNextWindow().empty()) {}
    executor.seek(0);  // reap any deferred fills

    /// Only the true gaps reach the source; the embedded hit is NOT re-fetched.
    EXPECT_EQ(pe[ProfileEvents::ReaderExecutorBytesFromSource] - src0, 448u * 1024u)
        << "only the gaps [0,192K)+[256K,512K) are fetched";
    /// No down-fill: the slow segment keeps its tail hole behind the upper hit.
    EXPECT_FALSE(slow->hasBlock(0)) << "slow segment [0,256K) keeps the tail hole (no down-fill)";
    EXPECT_TRUE(slow->hasBlock(1)) << "slow segment [256K,512K) completes";
}

/// Same rule, wider hit: the faster-tier hit [128K,256K) spans the tail half of the
/// slow segment [0,256K). The schedule splits gap [0,128K) / hit / gap [256K,512K):
/// the two gaps are fetched, the hit serves from the fast tier and is never
/// re-fetched, and the slow segment keeps the hole behind it (no down-fill).
TEST(ReaderExecutor, WideEmbeddedUpperHitIsNotRefetched)
{
    const size_t file = 512 * 1024;
    auto [src, objects] = srcOf(file);
    auto fast = std::make_shared<WideGranularityMockCache>(64 * 1024, "fast");
    auto slow = std::make_shared<WideGranularityMockCache>(256 * 1024, "slow");
    fast->seedBlock(2, 'F');  // resident [128K,256K): 128K, wider than min_bytes_for_seek,
    fast->seedBlock(3, 'F');  // embedded in the slow segment [0,256K)

    TestThreadGroup tg;
    auto & pe = CurrentThread::getProfileEvents();
    const auto src0 = pe[ProfileEvents::ReaderExecutorBytesFromSource];

    ReaderExecutor::Options opts;
    opts.window_size = file * 2 + 1;       // window >= segment: the production case
    opts.block_size = file * 2 + 1;
    opts.plan_look_ahead_max_window = file * 2 + 1;
    opts.min_bytes_for_seek = 64 * 1024;
    ReaderExecutor executor(src, objects, {fast, slow}, opts);

    while (!executor.readNextWindow().empty()) {}
    executor.seek(0);  // reap any deferred fills

    /// Only the two gaps reach the source; the hit is never re-fetched.
    EXPECT_EQ(pe[ProfileEvents::ReaderExecutorBytesFromSource] - src0, 384u * 1024u)
        << "only the gaps [0,128K)+[256K,512K) are fetched";
    /// No down-fill: the slow segment keeps the hole behind the upper hit.
    EXPECT_FALSE(slow->hasBlock(0)) << "slow segment [0,256K) keeps the hole (no down-fill)";
    EXPECT_TRUE(slow->hasBlock(1)) << "slow segment [256K,512K) completes";
}

/// Case 3 of the embedded-hit rule: when an upper tier fully covers a lower
/// tier's miss, the read needs ZERO remote and ZERO over-read (the cost the
/// rule promises). The lower tier is NOT written down today - a fully-covered
/// region has no gap, so no source fetch and no assemble-push reach the lower
/// writer, and a write-down to a slower tier is a separate unimplemented
/// policy. Fast 64K tier holds all of [0,256K); the slow 256K segment is a
/// miss but wholly covered.
TEST(ReaderExecutor, LowerSegmentFullyCoveredByUpperHitNeedsNoRemote)
{
    const size_t file = 256 * 1024;
    auto [src, objects] = srcOf(file);
    auto fast = std::make_shared<WideGranularityMockCache>(64 * 1024, "fast");
    auto slow = std::make_shared<WideGranularityMockCache>(256 * 1024, "slow");
    for (size_t b = 0; b < 4; ++b)
        fast->seedBlock(b, 'F');  // fast resident over the whole slow segment [0,256K)

    TestThreadGroup tg;
    auto & pe = CurrentThread::getProfileEvents();
    const auto src0 = pe[ProfileEvents::ReaderExecutorBytesFromSource];

    ReaderExecutor::Options opts;
    opts.window_size = file * 2 + 1;
    opts.block_size = file * 2 + 1;
    opts.plan_look_ahead_max_window = file * 2 + 1;
    opts.min_bytes_for_seek = 64 * 1024;
    ReaderExecutor executor(src, objects, {fast, slow}, opts);

    while (!executor.readNextWindow().empty()) {}
    executor.seek(0);  // reap deferred fills

    EXPECT_EQ(pe[ProfileEvents::ReaderExecutorBytesFromSource] - src0, 0u)
        << "fully upper-resident request: no remote fetch";
    /// The slower tier is NOT written down from the fully-covering upper hit
    /// today (no gap -> no fetch -> no assemble-push); a page->fs write-down is
    /// a separate unimplemented policy. Pins the current behavior.
    EXPECT_FALSE(slow->hasBlock(0)) << "lower tier not written down from a fully-covering upper hit";
}

/// Stage 0 of the connection-decision unification: the plan-geometry lookahead that will size the
/// long connection. From a start offset it streams across cold gaps and bridges resident runs
/// STRICTLY below `min_bytes_for_seek`, stopping at the first run at/above the bound (a wide cached
/// run is where the connection reopens, not bridges) or the plan end. Behavior-neutral here - not
/// yet consulted by any open decision.
TEST(ReaderExecutor, ScheduleLookaheadReachBridgesSmallCachedRuns)
{
    const size_t file = 512 * 1024;
    auto [src, objects] = srcOf(file);
    auto cache = std::make_shared<WideGranularityMockCache>(32 * 1024, "c");
    cache->seedBlock(4, 'C');             // [128K,160K): a 32K cached run (< 64K -> bridged)
    cache->seedBlock(8, 'C');             // [256K,320K): a 64K cached run (== 64K -> NOT bridged)
    cache->seedBlock(9, 'C');

    TestThreadGroup tg;
    ReaderExecutor::Options opts;
    opts.window_size = file * 2 + 1;      // one plan over the whole object
    opts.block_size = file * 2 + 1;
    opts.plan_look_ahead_max_window = file * 2 + 1;
    opts.min_bytes_for_seek = 64 * 1024;
    ReaderExecutor executor(src, objects, {cache}, opts);

    executor.readNextWindow();            // build the plan (geometry)

    /// From 0: stream the cold [0,128K), bridge the 32K run [128K,160K), stream the cold
    /// [160K,256K), then STOP at the 64K run [256K,320K) (== min_bytes_for_seek, not bridged).
    EXPECT_EQ(inspect(executor).scheduleLookaheadReach(0), 256u * 1024u);
    /// From just after the bridged run: same stop at the wide run.
    EXPECT_EQ(inspect(executor).scheduleLookaheadReach(160 * 1024), 256u * 1024u);
    /// From past the wide run: only cold remains, so it streams to the plan end (the file).
    EXPECT_EQ(inspect(executor).scheduleLookaheadReach(320 * 1024), file);
}

TEST(ReaderExecutor, ReadContinuityTrackerCapturesFullSequentialRead)
{
    /// Wiring check: a cold sequential read spanning MANY plans must accumulate the
    /// full contiguous read into the continuity estimator. The look-ahead is shrunk
    /// to one segment so every window forces a fresh plan; the watermark must feed
    /// each re-plan's source reads exactly once - no double-feed (which would fold
    /// the run and collapse the estimate) and no skipped region. So `predicted_reach`
    /// grows by one segment per window and equals the file size at EOF.
    const size_t seg = 16 * 1024;
    const size_t file = 5 * seg;          /// 80 KiB -> 5 plans / 5 windows
    String content(file, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file);

    auto cache = std::make_shared<EvictableSegmentMockCache>(seg);   /// cold -> all miss
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    ReaderExecutor::Options opts;
    opts.window_size = seg;               /// one segment per window
    opts.plan_look_ahead_max_window = seg;    /// one segment per plan -> a re-plan every window
    opts.min_bytes_for_seek = 4 * 1024;
    ReaderExecutor executor(source, objects, caches, opts);

    String result;
    size_t reach_after_1 = 0;
    size_t reach_after_3 = 0;
    int n = 0;
    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            result.append(node.data(), node.size);
        ++n;
        if (n == 1)
            reach_after_1 = inspect(executor).predictedEnd();
        if (n == 3)
            reach_after_3 = inspect(executor).predictedEnd();
    }

    EXPECT_EQ(result, content);                          /// full read, no corruption
    /// Replicate the tracker's arithmetic over k contiguous one-segment plan feeds:
    /// the first feed starts the run, each later feed is an exact continuation that
    /// checkpoints the grown run into the estimate. The run length inside these
    /// values is what detects a double-feed/gap.
    const double alpha = ReadContinuityTracker::Options{}.ewma_alpha;
    const auto predicted = [&](size_t feeds)
    {
        double est = 0;
        size_t frontier = 0;
        for (size_t k = 1; k <= feeds; ++k)
        {
            frontier += seg;
            if (k > 1)
                est = alpha * static_cast<double>(frontier) + (1 - alpha) * est;
        }
        return frontier + std::max<size_t>(
            static_cast<size_t>(alpha * static_cast<double>(frontier) + (1 - alpha) * est),
            static_cast<size_t>(est));
    };
    EXPECT_EQ(reach_after_1, predicted(1)) << "first plan fed exactly one segment";
    EXPECT_EQ(reach_after_3, predicted(3))
        << "with a one-segment plan the run tracks the consumed span exactly, so after "
           "three windows it spans three segments, no double-feed/gap";
    EXPECT_EQ(inspect(executor).predictedEnd(), predicted(file / seg))
        << "the estimator captured the full contiguous read across all plans";
}

namespace
{

String chainBytes(const ChainedBuffers & chain)
{
    String s;
    for (const auto & node : chain.getNodes())
        s.append(node.data(), node.size);
    return s;
}

/// A plain (unencrypted) single-object executor for exercising the long-connection
/// mechanics in isolation - the tests drive `openLong*` / `serveFromLongConnection*` /
/// `dropLongConnection*` directly via the inspector, independent of the read funnel.
struct LongConnRig
{
    String content;
    std::shared_ptr<MemorySourceReader> source;
    StoredObjects objects;
    std::shared_ptr<LongConnectionLimit> limit;
    std::unique_ptr<ReaderExecutor> executor;

    LongConnRig(size_t size, size_t min_bytes_for_seek, size_t block, size_t max_tail_for_drain)
    {
        content.resize(size);
        for (size_t i = 0; i < size; ++i)
            content[i] = static_cast<char>('A' + (i % 26));
        source = std::make_shared<MemorySourceReader>(
            std::unordered_map<String, String>{{"obj", content}});
        objects.emplace_back("obj", "", size);
        limit = std::make_shared<LongConnectionLimit>(4);

        ReaderExecutor::Options opts;
        opts.window_size = block;
        opts.block_size = block;
        opts.min_bytes_for_seek = min_bytes_for_seek;
        opts.max_tail_for_drain = max_tail_for_drain;
        opts.long_connection_limit = limit;
        executor = std::make_unique<ReaderExecutor>(
            source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, opts);
    }
};

}

TEST(ReaderExecutor, LongConnectionContiguousServeReleasesAtBound)
{
    const size_t size = 64 * 1024;
    const size_t block = 4096;
    LongConnRig rig(size, /*min_bytes_for_seek=*/4096, block, /*max_tail_for_drain=*/1024);
    auto & ex = *rig.executor;

    inspect(ex).openLongConnection(/*phys_offset=*/0, /*reach=*/size);
    EXPECT_TRUE(inspect(ex).hasLongConn());
    EXPECT_EQ(inspect(ex).longConnPosition(), 0u);
    EXPECT_EQ(inspect(ex).longConnBound(), size);
    EXPECT_TRUE(inspect(ex).longConnServes("obj"));
    EXPECT_EQ(rig.limit->getActiveCount(), 1u);

    String got;
    size_t pos = 0;
    while (inspect(ex).hasLongConn() && pos < size)
    {
        ChainedBuffers w = inspect(ex).serveFromLongConnection(pos, block);
        ASSERT_GT(w.totalBytes(), 0u);
        got += chainBytes(w);
        pos += w.totalBytes();
    }

    EXPECT_EQ(got, rig.content);                         /// byte-exact contiguous read
    EXPECT_FALSE(inspect(ex).hasLongConn());               /// released at bound
    EXPECT_EQ(rig.limit->getActiveCount(), 0u);          /// slot freed
    EXPECT_EQ(inspect(ex).incompleteConnections(), 0u);    /// clean exhaust, no I
}

TEST(ReaderExecutor, LongConnectionBridgesSmallForwardGap)
{
    const size_t size = 64 * 1024;
    const size_t block = 4096;
    const size_t min_seek = 8192;
    LongConnRig rig(size, min_seek, block, /*max_tail_for_drain=*/1024);
    auto & ex = *rig.executor;
    inspect(ex).openLongConnection(0, size);

    ChainedBuffers w0 = inspect(ex).serveFromLongConnection(0, block);
    EXPECT_EQ(chainBytes(w0), rig.content.substr(0, block));
    EXPECT_EQ(inspect(ex).longConnPosition(), block);

    /// canContinue truth table at frontier `block`:
    EXPECT_TRUE(inspect(ex).longConnCanContinue(block + 2048, block));         /// forward, gap <= min_seek
    EXPECT_FALSE(inspect(ex).longConnCanContinue(block + min_seek + 1, block)); /// gap > min_seek
    EXPECT_FALSE(inspect(ex).longConnCanContinue(0, block));                   /// backward
    EXPECT_FALSE(inspect(ex).longConnCanContinue(size - 100, block));          /// off+want past bound

    /// Serve across a 2048-byte forward gap -> bridged (frontier jumps past the gap).
    const size_t gap_off = block + 2048;
    ChainedBuffers w1 = inspect(ex).serveFromLongConnection(gap_off, block);
    EXPECT_EQ(chainBytes(w1), rig.content.substr(gap_off, block));
    EXPECT_EQ(inspect(ex).longConnPosition(), gap_off + block);                /// gap discarded, not re-served
}

TEST(ReaderExecutor, LongConnectionDropBeforeBoundCountsIncomplete)
{
    const size_t size = 64 * 1024;
    const size_t block = 4096;
    LongConnRig rig(size, 4096, block, /*max_tail_for_drain=*/1024);
    auto & ex = *rig.executor;
    inspect(ex).openLongConnection(0, size);                         /// bound = 64 KiB
    inspect(ex).serveFromLongConnection(0, block);                   /// transferred 4 KiB; tail to bound >> drain
    EXPECT_TRUE(inspect(ex).hasLongConn());

    inspect(ex).dropLongConnection();                                /// tail too big to drain -> incomplete
    EXPECT_FALSE(inspect(ex).hasLongConn());
    EXPECT_EQ(inspect(ex).incompleteConnections(), 1u);
    EXPECT_EQ(rig.limit->getActiveCount(), 0u);
}

/// A piece that straddles the connection bound MID-BLOCK is split at the bound: the
/// straddling block is re-cut into an exact-span piece served from the connection
/// (which drains exactly to `read_until` and releases clean) and a remainder read on a
/// fresh GET. Reach-predicted bounds are arbitrary byte values, so this is the common
/// straddle shape. Neither failure mode of a drop may appear: no incomplete connection
/// (abort) and no drained-and-refetched bytes - every byte crosses the wire once.
TEST(ReaderExecutor, LongConnectionSplitsAtMidBlockBound)
{
    const size_t size = 64 * 1024;
    const size_t block = 4096;
    LongConnRig rig(size, /*min_bytes_for_seek=*/4096, block, /*max_tail_for_drain=*/1024);
    auto & ex = *rig.executor;
    /// Plant a connection whose bound falls inside the third block: two whole blocks
    /// fit, then 1808 bytes of block [8192,12288) - wider than the drain allowance,
    /// so the old drop path would abort the connection as incomplete.
    inspect(ex).openLongConnection(0, 10000);

    String got;
    while (true)
    {
        auto chain = ex.readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            got.append(node.data(), node.size);
    }
    EXPECT_EQ(got, rig.content);
    EXPECT_EQ(inspect(ex).incompleteConnections(), 0u)
        << "the straddling piece splits at the bound - the connection is never abandoned";
    EXPECT_EQ(inspect(ex).bytesFromSource(), size)
        << "every byte crosses the wire exactly once - no drained-and-refetched tail";
}

/// Same straddle with a tail INSIDE the drain allowance (bound 512 bytes past a block
/// edge, drain allowance 1024): the old path would drain-and-discard those 512 bytes
/// and refetch them on the fallback GET; the split serves them instead.
TEST(ReaderExecutor, LongConnectionSplitServesWouldBeDrainedTail)
{
    const size_t size = 64 * 1024;
    const size_t block = 4096;
    LongConnRig rig(size, /*min_bytes_for_seek=*/4096, block, /*max_tail_for_drain=*/1024);
    auto & ex = *rig.executor;
    inspect(ex).openLongConnection(0, 2 * block + 512);

    String got;
    while (true)
    {
        auto chain = ex.readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            got.append(node.data(), node.size);
    }
    EXPECT_EQ(got, rig.content);
    EXPECT_EQ(inspect(ex).incompleteConnections(), 0u);
    EXPECT_EQ(inspect(ex).bytesFromSource(), size)
        << "the would-be-drained 512-byte tail is served, not discarded and refetched";
}

TEST(ReaderExecutor, LongConnectionDropDrainsSmallTail)
{
    const size_t size = 64 * 1024;
    const size_t block = 4096;
    const size_t drain = 2048;
    LongConnRig rig(size, 4096, block, drain);
    auto & ex = *rig.executor;
    inspect(ex).openLongConnection(0, /*reach=*/block + 1024);       /// bound = 5120
    EXPECT_EQ(inspect(ex).longConnBound(), block + 1024);
    inspect(ex).serveFromLongConnection(0, block);                   /// position 4096; tail 1024 <= drain
    EXPECT_TRUE(inspect(ex).hasLongConn());

    inspect(ex).dropLongConnection();                                /// drains the 1 KiB tail to the bound
    EXPECT_FALSE(inspect(ex).hasLongConn());
    EXPECT_EQ(inspect(ex).incompleteConnections(), 0u);    /// completed -> not incomplete
    EXPECT_EQ(rig.limit->getActiveCount(), 0u);
}

namespace
{

/// A source buffer that mimics object storage opened with `use_external_buffer = true`: it owns
/// no read memory, and `nextImpl` fills the caller's externally `set()` buffer
/// (`internal_buffer`). This is the path where a raw `read` would refill a stale external
/// pointer; a local file buffer cannot reproduce it because it falls back to its own memory.
class ExternalBufferReader : public ReadBufferFromFileBase
{
public:
    explicit ExternalBufferReader(std::shared_ptr<const String> data_)
        : ReadBufferFromFileBase(/*buf_size=*/0, /*existing_memory=*/nullptr, /*alignment=*/0, data_->size())
        , data(std::move(data_))
    {
    }

    bool nextImpl() override
    {
        const size_t n = producible();
        if (n == 0)
            return false;
        memcpy(internal_buffer.begin(), data->data() + file_pos, n);   /// into the external set() buffer
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + n);
        pos = working_buffer.begin();
        file_pos += n;
        return true;
    }

    off_t seek(off_t off, int) override { file_pos = static_cast<size_t>(off); resetWorkingBuffer(); return off; }
    off_t getPosition() override { return static_cast<off_t>(file_pos) - static_cast<off_t>(available()); }
    String getFileName() const override { return "external_mock"; }
    void setReadUntilPosition(size_t position) override { read_until = position; }
    void setReadUntilEnd() override { read_until.reset(); }
    bool supportsRightBoundedReads() const override { return true; }
    bool supportsExternalBufferMode() const override { return true; }

protected:
    /// Bytes the next `nextImpl` would produce into the currently `set()` buffer.
    size_t producible() const
    {
        const size_t cap = read_until ? std::min(*read_until, data->size()) : data->size();
        if (file_pos >= cap || internal_buffer.empty())
            return 0;
        return std::min(internal_buffer.size(), cap - file_pos);
    }

    std::shared_ptr<const String> data;
    size_t file_pos = 0;
    std::optional<size_t> read_until;
};

/// An external-buffer source that throws once it has delivered more than `budget` bytes,
/// simulating a transient failure / closed stream mid-response. The budget is per buffer
/// instance, so a freshly opened connection starts over — letting a test fail the drain on a
/// held connection while a subsequently opened connection reads cleanly.
class FaultBudgetReader : public ExternalBufferReader
{
public:
    FaultBudgetReader(std::shared_ptr<const String> data_, size_t budget_)
        : ExternalBufferReader(std::move(data_)), budget(budget_)
    {
    }

    bool nextImpl() override
    {
        const size_t n = producible();
        if (delivered + n > budget)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "FaultBudgetReader: injected read failure past budget");
        delivered += n;
        return ExternalBufferReader::nextImpl();
    }

private:
    size_t budget;
    size_t delivered = 0;
};

class ExternalBufferSourceReader : public IFileBasedSourceReader
{
public:
    explicit ExternalBufferSourceReader(std::shared_ptr<const String> data_) : data(std::move(data_)) {}
    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject &) override
    {
        return std::make_unique<ExternalBufferReader>(data);
    }
    String name() const override { return "ExternalBufferSourceReader"; }

private:
    std::shared_ptr<const String> data;
};

class FaultBudgetSourceReader : public IFileBasedSourceReader
{
public:
    FaultBudgetSourceReader(std::shared_ptr<const String> data_, size_t budget_)
        : data(std::move(data_)), budget(budget_) {}
    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject &) override
    {
        return std::make_unique<FaultBudgetReader>(data, budget);
    }
    String name() const override { return "FaultBudgetSourceReader"; }

private:
    std::shared_ptr<const String> data;
    size_t budget;
};

std::shared_ptr<String> makePatternContent(size_t size)
{
    auto content = std::make_shared<String>(size, '\0');
    for (size_t i = 0; i < size; ++i)
        (*content)[i] = static_cast<char>('A' + (i % 26));
    return content;
}

}

TEST(ReaderExecutor, LongConnectionBridgeDoesNotClobberServedWindow)
{
    /// Regression for the external-buffer discard path: with a source opened in external-buffer
    /// mode (object storage), the gap bridge (`skipForward`) must read the discarded bytes into
    /// its own scratch, not through the source's stale external pointer — the last served
    /// window's block.
    const size_t size = 64 * 1024;
    const size_t block = 4096;
    auto content = makePatternContent(size);

    StoredObjects objects;
    objects.emplace_back("obj", "", size);
    auto limit = std::make_shared<LongConnectionLimit>(4);
    ReaderExecutor::Options opts;
    opts.window_size = block;
    opts.block_size = block;
    opts.min_bytes_for_seek = 8192;
    opts.max_tail_for_drain = 1024;
    opts.long_connection_limit = limit;
    ReaderExecutor ex(std::make_shared<ExternalBufferSourceReader>(content), objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, opts);

    /// Serve one window and hold it; the source's external pointer now dangles into its block.
    inspect(ex).openLongConnection(0, size);
    ChainedBuffers held = inspect(ex).serveFromLongConnection(0, block);
    EXPECT_EQ(chainBytes(held), content->substr(0, block));

    /// A small forward gap on the open connection -> bridged via `skipForward`.
    const size_t gap_off = block + 2048;
    ChainedBuffers next = inspect(ex).serveFromLongConnection(gap_off, block);
    EXPECT_EQ(chainBytes(next), content->substr(gap_off, block));

    /// The held window must be intact — the bridge must not write into its block.
    EXPECT_EQ(chainBytes(held), content->substr(0, block));
}

TEST(ReaderExecutor, LongConnectionDrainFailureDoesNotAbortQuery)
{
    /// The drain in `dropLongConnection` is best-effort: it completes the held GET on discarded
    /// tail bytes so the connection returns to the keep-alive pool. If the held response throws
    /// while draining, the query must NOT fail — the connection is released as incomplete and a
    /// subsequent read succeeds on a fresh connection. Fails before the fix, when the drain
    /// exception escaped `dropLongConnection`.
    const size_t size = 64 * 1024;
    const size_t block = 4096;
    auto content = makePatternContent(size);

    StoredObjects objects;
    objects.emplace_back("obj", "", size);
    auto limit = std::make_shared<LongConnectionLimit>(4);
    ReaderExecutor::Options opts;
    opts.window_size = block;
    opts.block_size = block;
    opts.min_bytes_for_seek = 8192;
    /// The whole tail is within the drain limit, so the drop ATTEMPTS the drain.
    opts.max_tail_for_drain = size;
    opts.long_connection_limit = limit;
    /// The budget covers the one served window (`block`); the drain — which reads the remaining
    /// tail up to the bound — crosses it and throws. A fresh connection starts over.
    ReaderExecutor ex(std::make_shared<FaultBudgetSourceReader>(content, /*budget=*/block + block / 2), objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, opts);

    inspect(ex).openLongConnection(0, size);       /// bound = file end -> a large undrained tail remains
    ChainedBuffers served = inspect(ex).serveFromLongConnection(0, block);
    EXPECT_EQ(chainBytes(served), content->substr(0, block));

    /// The drain throws past the budget; the failure is swallowed, the connection is released
    /// and counted as incomplete.
    EXPECT_NO_THROW(inspect(ex).dropLongConnection());
    EXPECT_FALSE(inspect(ex).hasLongConn());
    EXPECT_EQ(inspect(ex).incompleteConnections(), 1u);
    EXPECT_EQ(limit->getActiveCount(), 0u);

    /// The required read still succeeds on a freshly opened connection (per-instance budget).
    inspect(ex).openLongConnection(0, size);
    ChainedBuffers reread = inspect(ex).serveFromLongConnection(0, block);
    EXPECT_EQ(chainBytes(reread), content->substr(0, block));
}

TEST(ReaderExecutor, LongConnectionCapacityZeroAlwaysFallsBack)
{
    /// A zero-capacity limit never grants a slot: the structural open rule still fires on a
    /// sequential scan, and every warranted open must fall back to a one-shot read - counted
    /// as a fallback, never opening a long connection - while serving every byte correctly.
    TestThreadGroup tg;
    const size_t size = 64 * 1024;
    const size_t window = 4096;
    auto content = makePatternContent(size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", *content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", size);

    ReaderExecutor::Options opts;
    opts.window_size = window;
    opts.block_size = window;
    opts.min_bytes_for_seek = 8192;
    opts.long_connection_limit = std::make_shared<LongConnectionLimit>(0);
    ReaderExecutor ex(source, objects, {}, opts);

    String got;
    while (true)
    {
        auto w = ex.readNextWindow();
        if (w.empty())
            break;
        got += chainBytes(w);
    }

    EXPECT_EQ(got, *content);                                                       /// one-shots serve everything
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorLongConnectionOpened), 0u);
    EXPECT_GE(tg.get(ProfileEvents::ReaderExecutorLongConnectionFallbacks), 1u);    /// wanted long, no slot
}

TEST(ReaderExecutor, EmptyFileIsImmediateEOF)
{
    /// A zero-size known-size object: no source request, no window, immediate EOF.
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", ""}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 0);
    ReaderExecutor::Options opts;
    opts.block_size = 256;
    ReaderExecutor ex(source, objects, {}, opts);

    EXPECT_EQ(ex.totalSize(), 0u);
    EXPECT_TRUE(ex.readNextWindow().empty());
}

TEST(ReaderExecutor, MissingFileWithUnknownSizeThrows)
{
    /// `DiskLocal::prepareRead` marks an unstatable file `UnknownSize`; the executor must
    /// then open it and surface the real error (file does not exist) instead of latching
    /// EOF and treating it as an empty read.
    StoredObject missing;
    missing.remote_path = (std::filesystem::temp_directory_path() / "reader_executor_does_not_exist.bin").string();
    missing.bytes_size = StoredObject::UnknownSize;
    auto source = std::make_shared<BufferSourceReader>(
        [](const StoredObject & obj) -> std::unique_ptr<ReadBufferFromFileBase>
        {
            return createReadBufferFromFileBase(obj.remote_path, ReadSettings{});
        },
        "LocalSource");
    ReaderExecutor::Options opts;
    opts.block_size = 256;
    ReaderExecutor ex(source, StoredObjects{missing}, {}, opts);

    EXPECT_ANY_THROW(ex.readNextWindow());
}

TEST(ReaderExecutor, LongConnectionClampReachAndShouldOpen)
{
    const size_t size = 64 * 1024;
    LongConnRig rig(size, 4096, 4096, 1024);
    auto & ex = *rig.executor;

    EXPECT_EQ(inspect(ex).clampReach(/*predicted_end=*/size * 4, /*phys_off=*/1000), size); /// clamped to file end
    EXPECT_EQ(inspect(ex).clampReach(/*predicted_end=*/3000, /*phys_off=*/1000), 3000u);    /// within file, unchanged
    EXPECT_EQ(inspect(ex).clampReach(/*predicted_end=*/500, /*phys_off=*/1000), 1000u);     /// behind the ask: floored, no reach
    EXPECT_FALSE(inspect(ex).shouldOpenLongConnection(0));            /// no continuity feed yet -> predicted reach 0, not "long"
}

TEST(ReaderExecutor, LongConnectionForegroundDrainsWholeFile)
{
    /// W2 foreground drain: with a held long connection, the synchronous gap reads
    /// drain it window-by-window from a SINGLE open GET (`SourceRequests` stays 1), and
    /// it is released at its bound after the last window.
    const size_t window = 100;
    const size_t size = 4 * window;
    String content(size, 0);
    for (size_t i = 0; i < size; ++i)
        content[i] = static_cast<char>('A' + (i % 26));

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", size);
    auto limit = std::make_shared<LongConnectionLimit>(4);

    ReaderExecutor::Options opts;
    opts.window_size = window;
    opts.min_bytes_for_seek = 4096;
    opts.long_connection_limit = limit;          /// no prefetch pool: pure synchronous reads
    ReaderExecutor ex(source, objects, {}, opts);

    inspect(ex).openLongConnection(0, size);                 /// foreground holds [0, size); one open GET
    EXPECT_EQ(inspect(ex).sourceRequests(), 1u);   /// the open

    String got;
    while (true)
    {
        auto w = ex.readNextWindow();
        if (w.empty())
            break;
        got += chainBytes(w);
    }

    EXPECT_EQ(got, content);                     /// byte-exact
    EXPECT_EQ(inspect(ex).sourceRequests(), 1u);   /// one GET served the whole file (drained, not re-opened)
    EXPECT_FALSE(inspect(ex).hasLongConn());       /// drained to its bound + released
    EXPECT_EQ(limit->getActiveCount(), 0u);
}

TEST(ReaderExecutor, LongConnectionDrainedAcrossPrefetchWindows)
{
    /// W2: the long connection is drained by the foreground sync read AND, once carried
    /// into the prefetch machine, by the worker - one open GET serves the whole file
    /// across foreground + prefetch windows (`SourceRequests` stays 1).
    const size_t window = 100;
    const size_t size = 4 * window;
    String content(size, 0);
    for (size_t i = 0; i < size; ++i)
        content[i] = static_cast<char>('A' + (i % 26));

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", size);
    auto limit = std::make_shared<LongConnectionLimit>(4);

    ReaderExecutor::Options opts;
    opts.window_size = window;
    opts.min_bytes_for_seek = 4096;
    opts.prefetch_pool = std::make_shared<SyncPrefetchPool>();
    opts.long_connection_limit = limit;
    ReaderExecutor ex(source, objects, {}, opts);

    inspect(ex).openLongConnection(0, size);
    EXPECT_EQ(inspect(ex).sourceRequests(), 1u);

    /// Window 1: foreground drains [0,100), then carries the connection into the prefetch
    /// machine, whose worker drains [100,200) (advancing it, not yet at bound).
    auto r1 = ex.readNextWindow();
    EXPECT_EQ(r1.range().size, window);
    EXPECT_TRUE(inspect(ex).hasInflightPrefetch());
    EXPECT_TRUE(inspect(ex).machineHasLongConn());   /// carried + worker-advanced
    EXPECT_FALSE(inspect(ex).hasLongConn());

    String got = chainBytes(r1);
    while (true)
    {
        auto w = ex.readNextWindow();
        if (w.empty())
            break;
        got += chainBytes(w);
    }

    EXPECT_EQ(got, content);                       /// byte-exact across fg + worker drains
    EXPECT_EQ(inspect(ex).sourceRequests(), 1u);     /// ONE GET served the whole file
    EXPECT_FALSE(inspect(ex).hasLongConn());         /// exhausted at bound
    EXPECT_EQ(limit->getActiveCount(), 0u);
}

TEST(ReaderExecutor, LongConnectionSpansAdvancingExtent)
{
    /// A long connection opens when the predicted forward reach runs past the read extent and
    /// is bounded by that reach, so it spans the reader's advancing right boundary
    /// (200 -> 300 -> 400 -> 500, each a `setReadUntilPosition` per mark range) instead of
    /// reopening at every one. The reach-bounded channel reopens only as it exhausts its bound,
    /// so reading [0,500) across the advances costs FEWER GETs than one-per-window - the
    /// coalescing the long connection exists to provide.
    const size_t window = 100;
    const size_t size = 6 * window;                /// 600; the scan reads only [0,500)
    String content(size, 0);
    for (size_t i = 0; i < size; ++i)
        content[i] = static_cast<char>('A' + (i % 26));

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", size);
    auto limit = std::make_shared<LongConnectionLimit>(4);

    ReaderExecutor::Options opts;
    opts.window_size = window;
    opts.min_bytes_for_seek = 4096;
    opts.long_connection_limit = limit;
    ReaderExecutor ex(source, objects, {}, opts);

    String got;
    auto read_to = [&](size_t extent)
    {
        ex.setReadExtent(extent);                  /// the executor side of setReadUntilPosition
        while (ex.getPosition() < extent)
        {
            auto w = ex.readNextWindow();
            if (w.empty())
                break;
            got += chainBytes(w);
        }
    };

    /// Advance the extent one window at a time, mirroring MergeTree's per-mark-range
    /// `setReadUntilPosition`. The reach-bounded long connection spans several advances
    /// per open and reopens only as it exhausts its bound.
    read_to(2 * window);
    read_to(3 * window);
    read_to(4 * window);
    read_to(5 * window);
    EXPECT_TRUE(inspect(ex).hasLongConn()) << "a long connection is engaged for the forward run";

    /// The long connection coalesces the five windows into far fewer GETs than the
    /// one-per-window a short connection would pay, spanning the advancing extent.
    EXPECT_LT(inspect(ex).sourceRequests(), 5u) << "coalesced across windows, not one GET per window";
    EXPECT_GE(inspect(ex).sourceRequests(), 1u);
    EXPECT_EQ(got, content.substr(0, 5 * window));      /// [0,500) served byte-exact
}

/// Byte value at logical offset `i` within a file: deterministic pattern.
static unsigned char patternByte(size_t i)
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

    /// ProfileEvents gathered from one `overReadScan` run.
    struct ScanCounts
    {
        ProfileEvents::Count source_requests = 0;
        ProfileEvents::Count opened = 0;
        ProfileEvents::Count hits = 0;
    };

    /// Drive a PipelineReadBuffer over `objects` in the compressed reader's access pattern: read a
    /// full `block` from each mark, with marks advancing by `mark_step < block` so each read seeks
    /// back into the previous (over-read) window. PipelineReadBuffer absorbs those in-buffer seeks,
    /// so the executor sees a forward-only scan and a held connection stays reusable. Returns the
    /// executor's ProfileEvents for the run (collected in an isolated ThreadGroup).
    ScanCounts overReadScan(
        const StoredObjects & objects, size_t total, size_t block, size_t mark_step,
        std::shared_ptr<LongConnectionLimit> limit)
    {
        TestThreadGroup tg;
        auto ex = std::make_unique<ReaderExecutor>(
            std::make_shared<LocalSourceReader>(), objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, ReaderExecutor::Options{
                .min_bytes_for_seek = 64 * 1024, .block_size = block,
                .max_tail_for_drain = 64 * 1024, .long_connection_limit = std::move(limit)});
        PipelineReadBuffer buf(std::move(ex));

        std::vector<char> window(block);
        for (size_t mark = 0; mark + block <= total; mark += mark_step)
        {
            buf.seek(static_cast<off_t>(mark), SEEK_SET);
            buf.readStrict(window.data(), block);
            bool ok = true;
            for (size_t i = 0; i < block && ok; ++i)
                ok = static_cast<unsigned char>(window[i]) == patternByte(mark + i);
            EXPECT_TRUE(ok) << "data mismatch in window at mark " << mark;
        }

        return {tg.get(ProfileEvents::ReaderExecutorSourceRequests),
                tg.get(ProfileEvents::ReaderExecutorLongConnectionOpened),
                tg.get(ProfileEvents::ReaderExecutorLongConnectionHits)};
    }
};


#if USE_SSL


/// Write raw `bytes` to `dir/name` and return the matching StoredObject (physical size).
static StoredObject writeBytesObject(const std::filesystem::path & dir, const std::string & name, const String & bytes)
{
    auto path = dir / name;
    std::ofstream f(path, std::ios::binary);
    f.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    f.close();

    StoredObject obj;
    obj.remote_path = path.string();
    obj.bytes_size = bytes.size();
    return obj;
}

TEST_F(ReaderExecutorTest, DecryptsSmallPayload)
{
    /// Single layer, payload smaller than one block -- the executor serves plaintext.
    String key(16, 'q');
    const FileEncryption::InitVector iv(UInt128{42});
    const String plaintext = "Hello, encrypted world!";
    StoredObjects objects{writeBytesObject(tmp_dir, "small.enc", makeEncryptedFile(key, iv, plaintext))};

    ReaderExecutor executor(std::make_shared<LocalSourceReader>(), objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, ReaderExecutor::Options{});
    executor.addDecryptionLayer("/t", [&](UInt128, const String &) { return key; });
    executor.initDecryption();

    auto out = drain(executor);
    ASSERT_EQ(out.size(), plaintext.size());
    EXPECT_EQ(String(out.begin(), out.end()), plaintext);
}

TEST_F(ReaderExecutorTest, DecryptsAcrossManyWindows)
{
    /// Plaintext far larger than the block, so the executor decrypts many successive windows,
    /// each at its own increasing logical offset -- the CTR keystream offset must advance per
    /// window or the tail windows come back garbage.
    String key(16, 'k');
    const FileEncryption::InitVector iv(UInt128{0x0123456789abcdefULL});

    const size_t plaintext_size = 4096 * 3 + 777;
    String plaintext(plaintext_size, '\0');
    for (size_t i = 0; i < plaintext_size; ++i)
        plaintext[i] = static_cast<char>((i * 31 + 7) & 0xFF);

    StoredObjects objects{writeBytesObject(tmp_dir, "multi.enc", makeEncryptedFile(key, iv, plaintext))};

    /// A small block forces several windows (3 full + a partial tail).
    ReaderExecutor executor(std::make_shared<LocalSourceReader>(), objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, ReaderExecutor::Options{.block_size = 4096});
    executor.addDecryptionLayer("/t",
        [&](UInt128 got_fp, const String &)
        {
            EXPECT_EQ(got_fp, FileEncryption::calculateKeyFingerprint(key));
            return key;
        });
    executor.initDecryption();

    auto out = drain(executor);
    ASSERT_EQ(out.size(), plaintext.size());
    EXPECT_EQ(String(out.begin(), out.end()), plaintext);
}

TEST_F(ReaderExecutorTest, DecryptsAcrossBlobBoundary)
{
    /// A single encrypted file (header + ciphertext) split across two objects. The header lives in
    /// the first object and the payload spans both, so this exercises the physical shift
    /// `position + data_start_offset` and `findObjectAt` crossing an object boundary while decrypting.
    String key(16, 'm');
    const FileEncryption::InitVector iv(UInt128{0x55});
    const size_t plaintext_size = 5000;
    String plaintext(plaintext_size, '\0');
    for (size_t i = 0; i < plaintext_size; ++i)
        plaintext[i] = static_cast<char>((i * 17 + 3) & 0xFF);

    const String file_bytes = makeEncryptedFile(key, iv, plaintext);  // 64-byte header + ciphertext
    const size_t split = FileEncryption::Header::kSize + 2000;        // header + prefix in the first object
    ASSERT_GT(file_bytes.size(), split);

    StoredObjects objects{
        writeBytesObject(tmp_dir, "part_a.enc", file_bytes.substr(0, split)),
        writeBytesObject(tmp_dir, "part_b.enc", file_bytes.substr(split))};

    /// A small block forces windows to reach and cross the object boundary.
    ReaderExecutor executor(std::make_shared<LocalSourceReader>(), objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, ReaderExecutor::Options{.block_size = 1024});
    executor.addDecryptionLayer("/m", [&](UInt128, const String &) { return key; });
    executor.initDecryption();

    auto out = drain(executor);
    ASSERT_EQ(out.size(), plaintext.size());
    EXPECT_EQ(String(out.begin(), out.end()), plaintext);
}

TEST_F(ReaderExecutorTest, DecryptsMultiLayer)
{
    /// Two encryption layers stacked, in the layout a legacy `DiskEncrypted`-over-`DiskEncrypted`
    /// configuration produces on write:
    ///   [outer_h_plain]                        -- 64 bytes, in clear
    ///   [outer.encrypt(inner_h)]               -- 64 bytes, ciphertext
    ///   [outer.encrypt(inner.encrypt(text))]
    /// The outer keystream covers the inner header AND payload -- outer's keystream offset for
    /// user-byte P is `P + 64`, inner's is `P`. `initDecryption` peels the outer layer off the
    /// inner header before parsing it.
    String key_inner(16, 'i');
    String key_outer(16, 'o');
    const FileEncryption::InitVector iv_inner(UInt128{1});
    const FileEncryption::InitVector iv_outer(UInt128{2});

    const String plaintext(4096 + 500, 'X');

    String inner_h_bytes;
    {
        WriteBufferFromString wb(inner_h_bytes);
        FileEncryption::Header inner_h;
        inner_h.algorithm = FileEncryption::Algorithm::AES_128_CTR;
        inner_h.key_fingerprint = FileEncryption::calculateKeyFingerprint(key_inner);
        inner_h.init_vector = iv_inner;
        inner_h.write(wb);
        wb.finalize();
    }
    ASSERT_EQ(inner_h_bytes.size(), FileEncryption::Header::kSize);

    const String inner_ciphertext = aesCtrEncrypt(key_inner, iv_inner, plaintext);

    /// `inner_h_bytes` at outer offset 0, `inner_ciphertext` at outer offset 64.
    const String outer_h_ciphertext = aesCtrEncryptAt(
        key_outer, iv_outer, /*stream_offset=*/0, inner_h_bytes.data(), inner_h_bytes.size());
    const String outer_payload_ciphertext = aesCtrEncryptAt(
        key_outer, iv_outer, /*stream_offset=*/FileEncryption::Header::kSize,
        inner_ciphertext.data(), inner_ciphertext.size());

    String file_bytes;
    {
        WriteBufferFromString wb(file_bytes);
        FileEncryption::Header outer_h;
        outer_h.algorithm = FileEncryption::Algorithm::AES_128_CTR;
        outer_h.key_fingerprint = FileEncryption::calculateKeyFingerprint(key_outer);
        outer_h.init_vector = iv_outer;
        outer_h.write(wb);
        wb.finalize();
    }
    file_bytes += outer_h_ciphertext;
    file_bytes += outer_payload_ciphertext;

    StoredObjects objects{writeBytesObject(tmp_dir, "layered.enc", file_bytes)};

    ReaderExecutor executor(std::make_shared<LocalSourceReader>(), objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, ReaderExecutor::Options{.block_size = 4096});
    /// Layers are added outermost-first, innermost-last -- the order the stacked-disk prepareRead
    /// chain produces (each layer recurses into its delegate before appending its `needDecryption`).
    executor.addDecryptionLayer("/outer", [&](UInt128, const String &) { return key_outer; });
    executor.addDecryptionLayer("/inner", [&](UInt128, const String &) { return key_inner; });
    executor.initDecryption();

    auto out = drain(executor);
    ASSERT_EQ(out.size(), plaintext.size());
    EXPECT_EQ(String(out.begin(), out.end()), plaintext);
}

TEST_F(ReaderExecutorTest, TotalSizeIsZeroForEmptyEncryptedSource)
{
    /// An empty encrypted source has no header. `initDecryption` skips it (no throw) and leaves
    /// `data_start_offset` set, so `totalSize()` must report 0, not underflow `physical - data_start_offset`.
    StoredObjects objects{makeFile("empty.bin", 0)};

    ReaderExecutor executor(std::make_shared<LocalSourceReader>(), objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, ReaderExecutor::Options{});
    executor.addDecryptionLayer("layer0", [](UInt128, const String &) { return String{}; });
    executor.addDecryptionLayer("layer1", [](UInt128, const String &) { return String{}; });
    executor.initDecryption();

    EXPECT_EQ(executor.totalSize(), 0u);
}

TEST_F(ReaderExecutorTest, UndersizedEncryptedSourceThrowsOnInit)
{
    /// A non-empty file smaller than the declared headers is corrupt: `initDecryption` surfaces it
    /// as CANNOT_READ_ALL_DATA (so `totalSize()` is never reached with 0 < physical < data_start_offset).
    StoredObjects objects{makeFile("tiny.bin", 10)};   // 10 bytes < 128-byte two-layer header

    ReaderExecutor executor(std::make_shared<LocalSourceReader>(), objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, ReaderExecutor::Options{});
    executor.addDecryptionLayer("layer0", [](UInt128, const String &) { return String{}; });
    executor.addDecryptionLayer("layer1", [](UInt128, const String &) { return String{}; });

    EXPECT_THROW(executor.initDecryption(), DB::Exception);
}

TEST_F(ReaderExecutorTest, EncryptedEofReleasesLongConnectionSlot)
{
    /// Regression: `atEnd` compared the logical `position` against the physical
    /// `offset_map.totalSize()`. For an encrypted file the physical size is larger by
    /// `data_start_offset`, so after the last plaintext byte `position` stayed below the physical
    /// size, `atEnd` stayed false, the EOF branch was skipped and the `LongConnectionLimit` slot was
    /// pinned past EOF. With the logical `totalSize()` the slot is released.
    String key(16, 'k');
    const FileEncryption::InitVector iv(UInt128{0xfeedfaceULL});
    const String plaintext(2048, 'E');
    StoredObjects objects{writeBytesObject(tmp_dir, "eof.enc", makeEncryptedFile(key, iv, plaintext))};

    auto limit = std::make_shared<LongConnectionLimit>(4);
    ReaderExecutor executor(std::make_shared<LocalSourceReader>(), objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, ReaderExecutor::Options{.min_bytes_for_seek = 64, .block_size = 512, .long_connection_limit = limit});
    executor.addDecryptionLayer("/t", [&](UInt128, const String &) { return key; });
    executor.initDecryption();

    auto out = drain(executor);
    ASSERT_EQ(out.size(), plaintext.size());
    EXPECT_EQ(String(out.begin(), out.end()), plaintext);
    EXPECT_EQ(limit->getActiveCount(), 0u);
}

TEST_F(ReaderExecutorTest, EncryptionHeaderCacheServesRepeatedOpens)
{
    /// With a shared header cache, the first open populates it and the second serves the header
    /// from the cache (skipping the source read); both must decrypt to the same plaintext.
    String key(16, 'c');
    FileEncryption::InitVector iv(UInt128{0x1234abcdULL});
    const String plaintext(5000, 'Z');
    StoredObjects objects{writeBytesObject(tmp_dir, "cached.enc", makeEncryptedFile(key, iv, plaintext))};

    auto cache = std::make_shared<EncryptionHeaderCache>("SLRU", 1 << 20, 0.5);
    auto key_finder = [&](UInt128, const String &) { return key; };

    auto read_once = [&]
    {
        ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects,
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{}, ReaderExecutor::Options{.block_size = 4096, .encryption_header_cache = cache});
        ex.addDecryptionLayer("/c", key_finder);
        ex.initDecryption();
        auto out = drain(ex);
        return String(out.begin(), out.end());
    };

    /// First open: cache miss -> reads, parses, populates.
    EXPECT_EQ(read_once(), plaintext);
    /// The header bytes are now cached under the object's storage path.
    EXPECT_TRUE(cache->read(objects.front().remote_path).has_value());
    /// Second open: cache hit -> header served from cache, still decrypts correctly.
    EXPECT_EQ(read_once(), plaintext);
}

#endif
