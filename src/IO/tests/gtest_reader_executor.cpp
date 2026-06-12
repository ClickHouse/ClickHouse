#include <IO/ReaderExecutor.h>
#include <IO/BufferSourceReader.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/ICacheProvider.h>
#include <IO/IntervalSet.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/LiveConnectionLimit.h>
#include <IO/ReadSettings.h>
#include <IO/Rope.h>
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

namespace ProfileEvents
{
    extern const Event LiveSourceBufferCreated;
    extern const Event LiveSourceBufferHits;
    extern const Event LiveSourceBufferFallbacks;
    extern const Event LiveSourceBufferBytes;
    extern const Event ReaderExecutorBufferSlotAcquired;
    extern const Event ReaderExecutorBytesPushedToCacheSync;
    extern const Event ReaderExecutorBytesPushedToCacheAsync;
    extern const Event ReaderExecutorBytesFromSource;
    extern const Event ReaderExecutorBytesFromPageCache;
    extern const Event ReaderExecutorSourceRequests;
    extern const Event ReaderExecutorRequestedBytes;
    extern const Event ReaderExecutorModeledCostMicroseconds;
    extern const Event ReaderExecutorCacheGetRequests;
    extern const Event ReaderExecutorCachePopulateRequests;
    extern const Event ReaderExecutorIncompleteConnections;
    extern const Event ReaderExecutorMachineInterrupted;
    extern const Event ReaderExecutorPartialCollects;
    extern const Event ReaderExecutorPutScheduled;
    extern const Event ReaderExecutorPutPoolFull;
    extern const Event ReaderExecutorPutAbandoned;
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
        return thread_group->performance_counters[event].load(std::memory_order_relaxed);
    }
};

}

namespace
{

/// Mock prefetch pool whose `submitJob` always returns nullptr ("queue full"
/// fallback). The executor still calls `ensurePreAcquiredSlot` before the
/// submit attempt, so the pre-acquired slot DOES get set — exposing the
/// leak path we want to exercise without needing real worker threads.
class FakePrefetchPool : public PrefetchThreadPool
{
public:
    FakePrefetchPool() : PrefetchThreadPool(NoWorkers{}) {}
    std::shared_ptr<JobHandle> submitJob(std::function<void()> /*task*/) override
    {
        return nullptr;
    }
};

/// Mock pool that runs every submitted job synchronously on the calling
/// thread and returns a `Done`-state handle (the machine then holds the
/// produced rope). Eliminates worker-thread timing from prefetch-related tests.
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

/// Shared, minimal `CacheView` for the in-memory mocks: it just owns the hit/miss
/// entry vectors the executor reads back. The per-range read/write buffers do the
/// real work over the mock's storage. No deferred-LRU bump (the mocks have no LRU),
/// so the default destructor is fine.
class MockCacheView : public CacheView
{
public:
    const VectorWithMemoryTracking<HitEntry> & hits() const override { return hit_entries; }
    const VectorWithMemoryTracking<MissEntry> & misses() const override { return miss_entries; }

    VectorWithMemoryTracking<HitEntry> hit_entries;
    VectorWithMemoryTracking<MissEntry> miss_entries;
};

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
    size_t readable() const override { return range_member.end(); }

    Rope read(ByteRange sub) override
    {
        Rope result;
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
            auto buf = std::make_shared<OwnedRopeBuffer>(data.size());
            std::memcpy(buf->data(), data.data(), data.size());
            Rope block_rope;
            block_rope.append(RopeNode{buf, 0, data.size(), b * block_size});
            result.append(block_rope.slice(ByteRange{lo, hi - lo}));
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
    const IntervalSet & committed() const override { return committed_ranges; }
    bool complete() const override { return committed_ranges.subtract(range_member).empty(); }

    size_t write(Rope data) override
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
                Rope slice = data.slice(block_range);
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

    Rope read(ByteRange sub) override
    {
        Rope result;
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
            auto buf = std::make_shared<OwnedRopeBuffer>(data.size());
            std::memcpy(buf->data(), data.data(), data.size());
            Rope block_rope;
            block_rope.append(RopeNode{buf, 0, data.size(), b * block_size});
            result.append(block_rope.slice(ByteRange{lo, hi - lo}));
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
    /// store (never mutating it), coalescing adjacent same-kind blocks into one entry.
    /// Hits carry a held read buffer; misses are whole-block-aligned with no writer.
    CacheViewPtr planResidencyView(const StoredObject &, size_t, ByteRange range_in_file) override
    {
        auto view = std::make_unique<MockCacheView>();
        if (range_in_file.size == 0)
            return view;

        const size_t start_block = range_in_file.offset / block_size;
        const size_t end_block = (range_in_file.end() + block_size - 1) / block_size;

        bool run_active = false;
        bool run_is_hit = false;
        ByteRange run_range{0, 0};
        auto flush_run = [&]()
        {
            if (!run_active)
                return;
            if (run_is_hit)
                view->hit_entries.push_back(HitEntry{
                    run_range, std::make_unique<MockCacheReader>(run_range, storage, block_size)});
            else
                view->miss_entries.push_back(MissEntry{run_range, /*writer=*/nullptr});
            run_active = false;
        };

        for (size_t b = start_block; b < end_block; ++b)
        {
            const bool is_hit = storage.contains(b);
            const ByteRange block_range{b * block_size, block_size};
            if (run_active && run_is_hit != is_hit)
                flush_run();
            if (!run_active)
            {
                run_active = true;
                run_is_hit = is_hit;
                run_range = block_range;
            }
            else
                run_range.size = block_range.end() - run_range.offset;
        }
        flush_run();
        return view;
    }

    VectorWithMemoryTracking<MissEntry> openWriteBuffers(
        const StoredObject &, size_t, const VectorWithMemoryTracking<ByteRange> & aligned_miss_ranges) override
    {
        VectorWithMemoryTracking<MissEntry> result;
        result.reserve(aligned_miss_ranges.size());
        for (const auto & aligned : aligned_miss_ranges)
            result.push_back(MissEntry{
                aligned, std::make_unique<MockCacheWriter>(aligned, storage, block_size)});
        return result;
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

TEST(ReaderExecutor, PrefetchBoxRoundTripKeepsSingleConnection)
{
    /// Cold sequential scan driven by a REAL prefetch pool. The source-connection
    /// cluster is moved foreground -> job at submit and reclaimed back at consume /
    /// cancel-queued every window. Assert the round-trip is faithful: the source
    /// connection is opened EXACTLY once and reused across all windows (no churn of
    /// the server-shared slot), with no data corruption. The no-pool R=1 tests
    /// (`MultipleEvictionsKeepSingleConnection` etc.) never exercise the box move, so
    /// this is the regression guard for the Step-2 connection-ownership transfer.
    String content(8000, 0);
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('A' + (i % 26));
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", 8000);

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    auto limit = std::make_shared<LiveConnectionLimit>(10);
    TestThreadGroup tg;
    String result;
    {
        /// min_bytes_for_seek=0: contiguous windows continue the open connection.
        ReaderExecutor executor(source, objects, {}, /*window_size=*/1000, /*min_bytes_for_seek=*/0);
        executor.setPrefetchPool(pool);
        executor.setBufferLimit(limit);

        while (true)
        {
            auto rope = executor.readNextWindow();
            if (rope.empty())
                break;
            for (const auto & node : rope.getNodes())
                result.append(node.data(), node.size);
            /// The cluster lives in exactly one place (foreground or the in-flight job),
            /// so at most one connection slot is ever active - never two.
            EXPECT_LE(limit->getActiveCount(), 1u);
        }
    }

    EXPECT_EQ(result, content);   /// no corruption across the box move
    /// The executor flushed `stats` into the thread group's ProfileEvents on destruction.
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorSourceRequests), 1u); /// one connection, round-tripped, never reopened
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
    ///     sliced to [750, 1000) - size 250, or less when the takeover interrupted
    ///     the worker mid-window and a shorter prefix past 750 was served.
    ///   - Cancel branch (worker hadn't started): a fresh window from position 750
    ///     of size min(window_size, file_size - 750), so the rope spans [750, 1250).
    /// All are valid outcomes and the test accepts any.

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

TEST(ReaderExecutor, PrefetchWindowRespondsToMemoryPressure)
{
    /// Read-ahead is speculative, so the prefetch window tracks memory pressure: it
    /// uses the full synchronous window at Normal/Elevated and is suppressed at
    /// High/Critical. Uses the stateless path (a present-but-zero-capacity
    /// buffer_limit, so no slot is acquired) so the window read is observable.
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
        auto limit = std::make_shared<LiveConnectionLimit>(0);   // present but no slots -> stateless window reads
        ReaderExecutor executor(source, objects, {},
            /*window_size=*/256u << 10, /*min_bytes_for_seek=*/0, /*block_size=*/32u << 10);
        executor.setPrefetchPool(pool);
        executor.setBufferLimit(limit);

        Rope rope = executor.readNextWindow();   // synchronous full-window read, then schedules a prefetch
        return {rope.range().size, executor.hasInflightPrefetch(), executor.inflightPrefetchSize()};
    };

    const Reading normal = measure(0.50);
    EXPECT_TRUE(normal.scheduled);
    EXPECT_EQ(normal.prefetch_window, normal.sync_window) << "Normal: prefetch uses the full synchronous window";

    const Reading elevated = measure(0.80);
    EXPECT_TRUE(elevated.scheduled);
    EXPECT_EQ(elevated.prefetch_window, elevated.sync_window) << "Elevated: prefetch uses the full synchronous window";

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
    auto merged = ReaderExecutor::mergeRanges(ranges, 50);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 300);
}

TEST(ReaderExecutor, MergeRangesSmallGap)
{
    /// Small gap (10 bytes) < min_gap (100) — merge
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {110, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 100);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0);
    EXPECT_EQ(merged[0].size, 210);
}

TEST(ReaderExecutor, MergeRangesLargeGap)
{
    /// Large gap (500 bytes) > min_gap (100) — don't merge
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {600, 100}};
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
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {120, 100}, {1000, 100}};
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
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {100, 100}};
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

#include <IO/FileEncryptionCommon.h>
#include <IO/WriteBufferFromString.h>

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

    /// Read the whole file through the executor and decrypt each served node via
    /// decryptInPlace - mirrors how PipelineReadBuffer consumes encrypted reads
    /// now that the executor returns ciphertext (logical offsets) and decryption
    /// is deferred to the consumer.
    String readAllDecrypted(ReaderExecutor & executor)
    {
        String result;
        while (true)
        {
            auto w = executor.readNextWindow();
            if (w.empty())
                break;
            for (const auto & n : w.getNodes())
            {
                String chunk(n.data(), n.size);
                if (executor.needsDecryption())
                    executor.decryptInPlace(chunk.data(), chunk.size(), n.logical_offset);
                result += chunk;
            }
        }
        return result;
    }
}

TEST(ReaderExecutor, DecryptInPlaceAcrossMultipleNodes)
{
    /// End-to-end exercise of `decryptInPlace` over a multi-node window: the
    /// executor returns ciphertext (logical offsets) and the consumer decrypts
    /// each served node. Plaintext is larger than ROPE_BLOCK_SIZE (and a
    /// non-multiple total) so several nodes, including a partial tail, are decrypted.

    String key(16, 'k');
    FileEncryption::InitVector iv(UInt128{0x0123456789abcdefULL});

    const size_t plaintext_size = ReaderExecutor::ROPE_BLOCK_SIZE * 3 + 12345;
    String plaintext(plaintext_size, '\0');
    for (size_t i = 0; i < plaintext_size; ++i)
        plaintext[i] = static_cast<char>((i * 31 + 7) & 0xFF);  /// distinguishable

    String file_bytes = makeEncryptedFile(key, iv, plaintext);

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    /// Window larger than the plaintext so the entire file is read in one
    /// readNextWindow call — the >3 MiB ciphertext is served as several nodes,
    /// each decrypted by decryptInPlace (3 full blocks + 1 partial).
    ReaderExecutor executor(source, objects, {},
        /*window_size=*/plaintext_size + ReaderExecutor::ROPE_BLOCK_SIZE);
    executor.addDecryptionLayer(
        "/test", 0,
        [&](UInt128 got_fp, const String &)
        {
            EXPECT_EQ(got_fp, FileEncryption::calculateKeyFingerprint(key));
            return key;
        });
    executor.initDecryption();

    String result = readAllDecrypted(executor);

    ASSERT_EQ(result.size(), plaintext.size());
    EXPECT_EQ(result, plaintext);
}

TEST(ReaderExecutor, EncryptedEofReleasesBufferLimitSlot)
{
    /// Regression: `atEnd` used to compare the logical `position` against
    /// the physical `offset_map.totalSize()`. For an encrypted file the
    /// physical size is larger by `data_start_offset` bytes, so after the
    /// last plaintext byte `position` is strictly less than
    /// `offset_map.totalSize()` and `atEnd` stays false. That skipped the
    /// EOF branch in `readNextWindow` and left the `LiveConnectionLimit`
    /// slot pinned past EOF.
    String key(16, 'k');
    FileEncryption::InitVector iv(UInt128{0xfeedfacecafeULL});
    String plaintext(2048, 'E');
    String file_bytes = makeEncryptedFile(key, iv, plaintext);

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    auto buffer_limit = std::make_shared<LiveConnectionLimit>(4);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/512);
    executor.setBufferLimit(buffer_limit);
    executor.addDecryptionLayer(
        "/test", 0,
        [&](UInt128, const String &) { return key; });
    executor.initDecryption();

    while (true)
    {
        auto w = executor.readNextWindow();
        if (w.empty())
            break;
    }

    EXPECT_EQ(buffer_limit->getActiveCount(), 0u);
}

TEST(ReaderExecutor, DecryptInPlaceSmallPayload)
{
    /// Same path but payload smaller than ROPE_BLOCK_SIZE — exercises the
    /// single-iteration loop.

    String key(16, 'q');
    FileEncryption::InitVector iv(UInt128{42});
    const String plaintext = "Hello, encrypted world!";
    String file_bytes = makeEncryptedFile(key, iv, plaintext);

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    ReaderExecutor executor(source, objects, {}, /*window_size=*/4096);
    executor.addDecryptionLayer("/t", 0,
        [&](UInt128, const String &) { return key; });
    executor.initDecryption();

    String result = readAllDecrypted(executor);
    EXPECT_EQ(result, plaintext);
}

TEST(ReaderExecutor, DecryptInPlaceMultiLayer)
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
    /// inner header bytes before parsing them; `decryptInPlace` must apply
    /// per-layer keystream offsets to recover the plaintext.

    String key_inner(16, 'i');
    String key_outer(16, 'o');
    FileEncryption::InitVector iv_inner(UInt128{1});
    FileEncryption::InitVector iv_outer(UInt128{2});

    const String plaintext(ReaderExecutor::ROPE_BLOCK_SIZE + 500, 'X');

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

    ReaderExecutor executor(source, objects, {},
        /*window_size=*/plaintext.size() + 2048);
    /// Layers are added outermost-first, innermost-last — same order the
    /// stacked-disk prepareRead chain produces (each layer recurses into
    /// its delegate before appending its own `needDecryption`).
    executor.addDecryptionLayer("/outer", 0,
        [&](UInt128, const String &) { return key_outer; });
    executor.addDecryptionLayer("/inner", 0,
        [&](UInt128, const String &) { return key_inner; });
    executor.initDecryption();

    String result = readAllDecrypted(executor);
    ASSERT_EQ(result.size(), plaintext.size());
    EXPECT_EQ(result, plaintext);
}

#endif

TEST(ReaderExecutor, MergeRangesOverlapping)
{
    /// Overlapping ranges merge into their union regardless of min_gap > 0.
    /// Without the saturating-subtraction fix, gap = sorted[i].offset - prev.end()
    /// underflows on overlap and the merge branch is skipped, leaving overlapping
    /// ranges in the output.
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 100}, {50, 100}};
    auto merged = ReaderExecutor::mergeRanges(ranges, 10);
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged[0].offset, 0u);
    EXPECT_EQ(merged[0].size, 150u);  /// [0, 100) ∪ [50, 150) = [0, 150)
}

TEST(ReaderExecutor, MergeRangesContained)
{
    /// One range fully contained in another. The union is the wider range;
    /// without the fix the underflow path emits both ranges.
    VectorWithMemoryTracking<ByteRange> ranges = {{0, 200}, {50, 100}};
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
        ReaderExecutor warmup(warm_source, objects, {cache}, /*window_size=*/100);
        warmup.seek(100);
        warmup.readNextWindow();
        ASSERT_TRUE(cache->hasBlock(1));
    }

    String real_content(300, 'S');
    auto real_source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", real_content}});

    ReaderExecutor executor(
        real_source, objects, {cache},
        /*window_size=*/300,
        /*min_bytes_for_seek=*/8 * 1024 * 1024);

    /// Drain to EOF: the plan serves the run at the cursor (gap or resident) one
    /// call at a time.
    Rope assembled;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.empty())
            break;
        assembled.append(std::move(rope));
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

        /// Now let the executor go out of scope: the destructor must finish
        /// cleanly because `machine` was already cleared inside
        /// `readNextWindow` before the throw.
    }
    SUCCEED();
}

TEST(ReaderExecutor, LocalReadUsesFullWindow)
{
    /// Local reads have no LiveConnectionLimit / live buffer, so they take the
    /// stateless path. Like stateless remote reads, they keep the full window
    /// (not a single block) so one open amortises its setup over a window
    /// instead of reopening the source per block.
    constexpr size_t file_size = 2 * ReaderExecutor::ROPE_BLOCK_SIZE;
    String content(file_size, 'L');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    /// No buffer_limit, no caches -> stateless local path; default 8 MiB window.
    ReaderExecutor executor(source, objects, {});

    auto w = executor.readNextWindow();
    ASSERT_FALSE(w.empty());
    EXPECT_EQ(w.range().size, file_size)
        << "local read must keep the full window (whole 2 MiB file), not shrink to one block";
    EXPECT_EQ(w.getNodes().size(), file_size / ReaderExecutor::ROPE_BLOCK_SIZE)
        << "the window is split into block-sized rope nodes";
}

TEST(ReaderExecutor, ConfiguredBlockSizeControlsNodeSize)
{
    /// A stateless (local) read keeps the full window but splits it into rope
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

    /// No buffer_limit, no caches -> stateless local path.
    ReaderExecutor executor(
        source, objects, {},
        /*window_size=*/4 * 1024 * 1024,
        /*min_bytes_for_seek=*/0,
        /*block_size=*/configured_block);

    auto w = executor.readNextWindow();
    ASSERT_FALSE(w.empty());
    EXPECT_EQ(w.range().size, file_size)
        << "window must span the whole file, not be capped at the 256 KiB block";
    EXPECT_EQ(w.getNodes().size(), file_size / configured_block)
        << "the window is split into configured-block-sized rope nodes";
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

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setPrefetchPool(pool);

    /// Window 1: synchronous read, then maybeTriggerPrefetch submits a prefetch
    /// for window 2 that queues behind the blocked worker.
    auto w1 = executor.readNextWindow();
    ASSERT_FALSE(w1.empty());
    ASSERT_TRUE(executor.hasInflightPrefetch());

    /// Window 2: the prefetch is still Queued, so tryCancel succeeds on the
    /// consume path and the cancelled handle must be stashed for the drain.
    auto w2 = executor.readNextWindow();
    ASSERT_FALSE(w2.empty());
    EXPECT_EQ(executor.abandonedPrefetchCount(), 1u)
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
    /// no live buffer is kept without a buffer_limit) throws.
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

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setPrefetchPool(pool);

    auto w1 = executor.readNextWindow();
    ASSERT_FALSE(w1.empty());
    ASSERT_TRUE(executor.hasInflightPrefetch());

    /// Window 2: tryCancel succeeds, then the fallback read throws.
    EXPECT_THROW(executor.readNextWindow(), DB::Exception);
    EXPECT_EQ(executor.abandonedPrefetchCount(), 1u)
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

    EXPECT_THROW(
        ReaderExecutor(source, objects, {}, /*window_size=*/0),
        DB::Exception);
    EXPECT_THROW(
        ReaderExecutor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0, /*block_size=*/0),
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

TEST(ReaderExecutor, LiveBufferReusesConnection)
{
    TestThreadGroup tg;

    /// 2000 bytes, window=500 → 4 sequential readNextWindow calls.
    String content(2000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 2000);

    auto limit = std::make_shared<LiveConnectionLimit>(10);

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

    auto limit = std::make_shared<LiveConnectionLimit>(0);

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
    VectorWithMemoryTracking<MissEntry> openWriteBuffers(
        const StoredObject &, size_t, const VectorWithMemoryTracking<ByteRange> & aligned_miss_ranges) override;

    String name() const override { return "EvictableSegmentMock"; }
    CacheTier tier() const override { return CacheTier::FilesystemCache; }

    /// Evict every segment not currently pinned by a caller.
    void evictUnpinned()
    {
        for (auto & [idx, live] : liveness)
            if (live.use_count() == 1)
                downloaded[idx] = 0;
    }

    size_t segmentSize() const { return segment_size; }

    std::shared_ptr<int> & livenessFor(size_t idx)
    {
        auto & p = liveness[idx];
        if (!p)
            p = std::make_shared<int>(0);
        return p;
    }

    /// idx -> bytes downloaded from segment start (0 == empty).
    std::unordered_map<size_t, size_t> downloaded;
    /// idx -> liveness token; an extra ref (held by the executor's pin) makes
    /// the segment non-evictable.
    std::unordered_map<size_t, std::shared_ptr<int>> liveness;
    std::vector<std::pair<ByteRange, size_t>> put_log;
    /// idx -> true means put() returns 0 (simulates a cache that refuses writes).
    std::unordered_map<size_t, bool> reject_put;

private:
    size_t segment_size;
};

/// Held read buffer over ONE segment's committed prefix `[seg_start, seg_start+dl)`.
/// `readable()` re-reads the LIVE `downloaded[idx]` each call (clamped to the hit
/// range), so a partial segment grown across windows by a concurrent writer becomes
/// visible - like the real `DiskCacheReader`. `read(sub)` fabricates 'D' bytes
/// for the committed sub-range.
class EvictableSegmentReadBuffer : public CacheReader
{
public:
    EvictableSegmentReadBuffer(ByteRange hit_range_, size_t seg_idx_, EvictableSegmentMockCache & cache_)
        : hit_range(hit_range_), seg_idx(seg_idx_), cache(cache_) {}

    ByteRange range() const override { return hit_range; }

    size_t readable() const override
    {
        const size_t seg = cache.segmentSize();
        const size_t seg_start = seg_idx * seg;
        const size_t dl = cache.downloaded.contains(seg_idx) ? cache.downloaded[seg_idx] : 0;
        const size_t committed_end = seg_start + std::min(dl, seg);
        return std::min(committed_end, hit_range.end());
    }

    Rope read(ByteRange sub) override
    {
        Rope result;
        const size_t lo = std::max(sub.offset, hit_range.offset);
        const size_t hi = std::min({sub.end(), hit_range.end(), readable()});
        if (lo >= hi)
            return result;
        auto buf = std::make_shared<OwnedRopeBuffer>(hi - lo);
        std::memset(buf->data(), 'D', hi - lo);
        result.append(RopeNode{buf, 0, hi - lo, lo});
        return result;
    }

private:
    ByteRange hit_range;
    size_t seg_idx;
    EvictableSegmentMockCache & cache;
};

/// Held write buffer over ONE aligned (segment) miss range. `write` appends into the
/// segment append-only at the live `cwo` (with `reject_put` and the `livenessFor`
/// token), advancing `committed`. `read` serves the committed prefix as 'D' bytes.
/// `pin(frontier)`: a partially-downloaded segment returns its liveness token (so it
/// survives `evictUnpinned` while the executor holds it).
class EvictableSegmentWriteBuffer : public CacheWriter
{
public:
    EvictableSegmentWriteBuffer(ByteRange aligned_range_, size_t seg_idx_, EvictableSegmentMockCache & cache_)
        : aligned_range(aligned_range_), seg_idx(seg_idx_), cache(cache_) {}

    ByteRange range() const override { return aligned_range; }
    const IntervalSet & committed() const override { return committed_ranges; }
    bool complete() const override { return committed_ranges.subtract(aligned_range).empty(); }

    size_t write(Rope data) override
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

        cache.downloaded[seg_idx] = std::min(seg, (cwo + contiguous) - seg_start);
        cache.livenessFor(seg_idx);
        committed_ranges.add(ByteRange{cwo, contiguous});
        return contiguous;
    }

    Rope read(ByteRange sub) override
    {
        Rope result;
        const size_t seg = cache.segmentSize();
        const size_t seg_start = seg_idx * seg;
        const size_t dl = cache.downloaded.contains(seg_idx) ? cache.downloaded[seg_idx] : 0;
        const size_t committed_end = seg_start + std::min(dl, seg);
        const size_t lo = std::max({sub.offset, aligned_range.offset, seg_start});
        const size_t hi = std::min({sub.end(), aligned_range.end(), committed_end});
        if (lo >= hi)
            return result;
        auto buf = std::make_shared<OwnedRopeBuffer>(hi - lo);
        std::memset(buf->data(), 'D', hi - lo);
        result.append(RopeNode{buf, 0, hi - lo, lo});
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
    auto view = std::make_unique<MockCacheView>();
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
            /// Miss tail past the frontier, segment-aligned head (so the source
            /// over-read fills the segment prefix), tail clamped to the request.
            const size_t miss_head = seg_start + dl;
            const size_t miss_end = std::min(seg_end, range_in_file.end());
            if (miss_head < miss_end)
                view->miss_entries.push_back(MissEntry{ByteRange{miss_head, miss_end - miss_head}, /*writer=*/nullptr});
        }
        else
        {
            const size_t miss_end = std::min(seg_end, range_in_file.end());
            if (seg_start < miss_end)
                view->miss_entries.push_back(MissEntry{ByteRange{seg_start, miss_end - seg_start}, /*writer=*/nullptr});
        }
    }
    return view;
}

inline VectorWithMemoryTracking<MissEntry> EvictableSegmentMockCache::openWriteBuffers(
    const StoredObject &, size_t, const VectorWithMemoryTracking<ByteRange> & aligned_miss_ranges)
{
    VectorWithMemoryTracking<MissEntry> result;
    result.reserve(aligned_miss_ranges.size());
    for (const auto & aligned : aligned_miss_ranges)
    {
        /// Each aligned miss lies within a single segment (one miss entry per segment
        /// in `planResidencyView`); derive its index from the offset.
        const size_t seg_idx = aligned.offset / segment_size;
        result.push_back(MissEntry{
            aligned, std::make_unique<EvictableSegmentWriteBuffer>(aligned, seg_idx, *this)});
    }
    return result;
}

} // anonymous namespace

TEST(ReaderExecutor, SequentialMidReadEvictionDoesNotResetConnection)
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

    auto limit = std::make_shared<LiveConnectionLimit>(10);

    auto executor = std::make_unique<ReaderExecutor>(source, objects, caches, /*window_size=*/1000, /*min_bytes_for_seek=*/0);
    executor->setBufferLimit(limit);

    String result;
    auto consume = [&](Rope rope)
    {
        for (const auto & node : rope.getNodes())
            result.append(node.data(), node.size);
    };

    /// Window 1: [0,1000). Fills the segment to cwo=1000; the executor pins it.
    auto w1 = executor->readNextWindow();
    ASSERT_FALSE(w1.empty());
    consume(std::move(w1));

    /// Eviction pressure. The pinned segment must survive (use_count()==2).
    cache->evictUnpinned();
    ASSERT_EQ(cache->downloaded[0], 1000u) << "pinned in-flight segment was evicted";

    /// Drain the rest sequentially.
    while (true)
    {
        auto rope = executor->readNextWindow();
        if (rope.empty())
            break;
        consume(std::move(rope));
    }

    EXPECT_EQ(result, content);   /// no corruption / no missing bytes
    /// Destroy the executor so it flushes `stats` into the thread group's ProfileEvents.
    executor.reset();
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorSourceRequests), 1u); /// connection opened exactly once
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferCreated), 1);
}

TEST(ReaderExecutor, MultipleEvictionsKeepSingleConnection)
{
    TestThreadGroup tg;
    String content(4000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});
    StoredObjects objects;
    objects.emplace_back("file", "", 4000);

    auto cache = std::make_shared<EvictableSegmentMockCache>(4000);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    auto limit = std::make_shared<LiveConnectionLimit>(10);
    auto executor = std::make_unique<ReaderExecutor>(source, objects, caches, /*window_size=*/1000, /*min_bytes_for_seek=*/0);
    executor->setBufferLimit(limit);

    String result;
    while (true)
    {
        auto rope = executor->readNextWindow();
        if (rope.empty())
            break;
        for (const auto & node : rope.getNodes())
            result.append(node.data(), node.size);
        EXPECT_LE(limit->getActiveCount(), 1u);   /// no slot churn
        cache->evictUnpinned();                      /// eviction pressure before next window
    }

    EXPECT_EQ(result, content);
    /// Destroy the executor so it flushes `stats` into the thread group's ProfileEvents.
    executor.reset();
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorSourceRequests), 1u);
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferCreated), 1);
}

TEST(ReaderExecutor, PrefetchConsumeRebuildsPinAcrossSegmentBoundary)
{
    TestThreadGroup tg;

    /// Windows >= 2 arrive via the machine COLLECT path, where the foreground
    /// rebuilds the Strategy-A pin under the new frontier. Two 2000-byte
    /// segments, window 1000: W3 is the first window of segment 1 - the collect
    /// must re-pin that fresh partial segment, or an eviction sweep right after
    /// drops it. The INLINE pool keeps the deferred fills synchronous in this
    /// test, so `downloaded[1]` is deterministic at the eviction point (with a
    /// real pool the put may not have landed yet - the segment would be
    /// pinned-but-empty, which the eviction also survives, but the byte assert
    /// would race).
    String content(4000, 'Q');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});
    StoredObjects objects;
    objects.emplace_back("file", "", 4000);

    auto cache = std::make_shared<EvictableSegmentMockCache>(2000);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    auto pool = std::make_shared<SyncPrefetchPool>();
    auto limit = std::make_shared<LiveConnectionLimit>(10);
    auto executor = std::make_unique<ReaderExecutor>(source, objects, caches, /*window_size=*/1000, /*min_bytes_for_seek=*/0);
    executor->setPrefetchPool(pool);
    executor->setBufferLimit(limit);

    String result;
    auto consume = [&](Rope rope)
    {
        for (const auto & node : rope.getNodes())
            result.append(node.data(), node.size);
    };

    /// W1 [0,1000) sync (no machine in flight yet) -> launches the machine for [1000,2000).
    consume(executor->readNextWindow());
    /// W2 [1000,2000) collect -> fills segment 0 to 2000 (full).
    consume(executor->readNextWindow());
    /// W3 [2000,3000) collect -> first window of segment 1: fills it to cwo=1000 (partial)
    /// and must RE-PIN it at collect; launches the machine for [3000,4000).
    consume(executor->readNextWindow());

    /// Evict everything unpinned. Segment 1 is the partial in-flight segment whose
    /// fill just landed; the PUT-side `fill_pin` (held until the machine's reap)
    /// protects it - the foreground finalize pinned before the deferred fill landed
    /// and so could not.
    cache->evictUnpinned();
    EXPECT_EQ(cache->downloaded[1], 1000u) << "consume-path pin did not protect the in-flight segment";

    /// Finish and verify no corruption.
    while (true)
    {
        auto rope = executor->readNextWindow();
        if (rope.empty())
            break;
        consume(std::move(rope));
    }
    EXPECT_EQ(result, content);
    /// Destroy the executor so it flushes `stats` into the thread group's ProfileEvents.
    executor.reset();
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorSourceRequests), 1u);
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

    ReaderExecutor executor(source, objects, caches, /*window_size=*/1000, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(std::make_shared<LiveConnectionLimit>(10));

    ASSERT_FALSE(executor.readNextWindow().empty());      /// [0,1000) fills + pins segment 0
    ASSERT_EQ(cache->downloaded[0], 1000u);
    cache->evictUnpinned();
    ASSERT_EQ(cache->downloaded[0], 1000u) << "segment 0 should be pinned before the seek";

    executor.seek(5000);                                  /// continuity breaks -> pin released
    cache->evictUnpinned();
    EXPECT_EQ((cache->downloaded.contains(0) ? cache->downloaded[0] : 0u), 0u)
        << "pin should be released on seek, allowing eviction of segment 0";

    auto rope = executor.readNextWindow();                /// [5000,6000)
    ASSERT_FALSE(rope.empty());
    String got;
    for (const auto & node : rope.getNodes())
        got.append(node.data(), node.size);
    EXPECT_EQ(got, content.substr(5000, got.size()));
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

    ReaderExecutor executor(source, objects, caches, /*window_size=*/1000, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(std::make_shared<LiveConnectionLimit>(10));

    auto rope = executor.readNextWindow();   /// [0,1000)
    ASSERT_FALSE(rope.empty());
    String got;
    for (const auto & node : rope.getNodes())
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

    ReaderExecutor executor(source, objects, caches, /*window_size=*/1000, /*min_bytes_for_seek=*/0);
    auto transient = executor.makeTransientForReadAt(0, /*read_size=*/4000);
    ASSERT_TRUE(transient != nullptr);
    auto rope = transient->readNextWindow();
    ASSERT_FALSE(rope.empty());

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

    auto limit = std::make_shared<LiveConnectionLimit>(10);   // a unit is free, but a transient never takes one
    ReaderExecutor executor(source, objects, {}, /*window_size=*/64u << 10, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    TestThreadGroup tg;
    auto transient = executor.makeTransientForReadAt(offset, want);

    size_t total = 0;
    while (total < want)
    {
        auto rope = transient->readNextWindow();
        if (rope.empty())
            break;
        total += rope.range().size;
    }

    EXPECT_EQ(total, want) << "the transient reads exactly the requested extent";
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferCreated), 0)
        << "a transient never opens a live connection - it takes a one-shot";
    EXPECT_GT(tg.get(ProfileEvents::LiveSourceBufferFallbacks), 0u)
        << "the transient read goes the stateless one-shot path";
    ASSERT_FALSE(log.read_until.empty());
    EXPECT_EQ(log.start_offset[0], offset);
    ASSERT_TRUE(log.read_until[0].has_value()) << "the one-shot connection must be right-bounded, not open-ended";
    EXPECT_EQ(*log.read_until[0], offset + want) << "bounded to the request extent (object-local coordinates)";
}

TEST(ReaderExecutor, ReadBigAtBoundsLiveConnectionOnEncryptedFile)
{
    /// Encrypted readBigAt over the live path. Inside readFromSource the
    /// `logical_offset` parameter is a physical (header-inclusive) offset, so the
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

    auto limit = std::make_shared<LiveConnectionLimit>(10);   // slot available -> live path
    ReaderExecutor executor(source, objects, {}, /*window_size=*/256u << 10, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);
    executor.addDecryptionLayer("/test", 0, [&](UInt128, const String &) { return key; });
    executor.initDecryption();   // parses the header -> data_start_offset = 64

    const size_t offset = 4096;   // logical
    const size_t want = 8192;     // logical bytes
    const size_t open_index = log.read_until.size();   // first transient open is the next one

    auto transient = executor.makeTransientForReadAt(offset, want);

    size_t total = 0;
    String got;
    while (total < want)
    {
        auto rope = transient->readNextWindow();
        if (rope.empty())
            break;
        for (const auto & n : rope.getNodes())
        {
            String chunk(n.data(), n.size);
            if (transient->needsDecryption())
                transient->decryptInPlace(chunk.data(), chunk.size(), n.logical_offset);
            got += chunk;
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

TEST(ReaderExecutor, ReadBigAtBoundsLiveConnectionToObjectEndAcrossBoundary)
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

    auto limit = std::make_shared<LiveConnectionLimit>(10);   // slot available -> live path on the first object
    ReaderExecutor executor(source, objects, {}, /*window_size=*/1u << 20, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    const size_t offset = 90u << 10;   // 90 KiB into o0
    const size_t want = 50u << 10;     // ends at 140 KiB -> 40 KiB into o1
    auto transient = executor.makeTransientForReadAt(offset, want);

    size_t total = 0;
    while (total < want)
    {
        auto rope = transient->readNextWindow();
        if (rope.empty())
            break;
        total += rope.range().size;
    }

    EXPECT_EQ(total, want);
    ASSERT_GE(log.read_until.size(), 2u) << "one open per object piece";
    ASSERT_TRUE(log.read_until[0].has_value());
    EXPECT_EQ(*log.read_until[0], s0) << "o0 connection bounded to its own end, not past it to the extent end";
    ASSERT_TRUE(log.read_until[1].has_value());
    EXPECT_EQ(*log.read_until[1], want - (s0 - offset)) << "o1 connection bounded to the extent end (object-local)";
}

TEST(ReaderExecutor, SequentialReaderBoundsConnectionToAdvertisedExtent)
{
    /// A sequential (non-transient) reader given an advertised extent via
    /// setReadExtent - the standard setReadUntilPosition contract that
    /// MergeTreeReaderStream::adjustRightMark drives per mark range - bounds its
    /// live connection to that extent and streams within it on ONE connection,
    /// drained at the extent and reusable, instead of an open-ended connection
    /// abandoned mid-response when the read stops short of the file end. Updating
    /// the extent (the next mark range) resumes the read on a fresh bounded
    /// connection.
    const size_t file_size = 1u << 20;   // 1 MiB
    const size_t extent1 = 300u << 10;   // first advertised range
    const size_t extent2 = 600u << 10;   // extended range (next mark range)

    BoundLog log;
    auto source = std::make_shared<BoundRecordingSource>(
        std::unordered_map<String, String>{{"obj", String(file_size, 'x')}}, log);
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto limit = std::make_shared<LiveConnectionLimit>(10);
    ReaderExecutor executor(source, objects, {}, /*window_size=*/64u << 10, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    TestThreadGroup tg;

    auto drain_to_extent = [&]()
    {
        size_t read = 0;
        while (true)
        {
            auto rope = executor.readNextWindow();
            if (rope.empty())
                break;
            read += rope.range().size;
        }
        return read;
    };

    executor.setReadExtent(extent1);
    EXPECT_EQ(drain_to_extent(), extent1) << "the reader stops at the advertised extent (empty window past it)";

    executor.setReadExtent(extent2);
    EXPECT_EQ(drain_to_extent(), extent2 - extent1) << "extending the advertised extent resumes the read";

    ASSERT_GE(log.read_until.size(), 2u) << "one connection per advertised extent (streamed within each)";
    ASSERT_TRUE(log.read_until[0].has_value()) << "the live connection must be right-bounded, not open-ended";
    EXPECT_EQ(*log.read_until[0], extent1) << "bounded to the first advertised extent (object-local)";
    ASSERT_TRUE(log.read_until[1].has_value());
    EXPECT_EQ(*log.read_until[1], extent2) << "bounded to the extended extent";
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferCreated), 2)
        << "exactly one streamed connection per extent (reused across the windows within each)";
}

TEST(ReaderExecutor, UnknownSizeReaderWithExtentBoundsAndReleasesConnection)
{
    /// An unknown-size source given a finite advertised extent (via setReadExtent)
    /// must still bound its live connection to the extent - so the connection
    /// drains and its LiveConnectionLimit slot is released when the consumer stops
    /// at the extent - instead of leaving an open-ended connection + slot pinned.
    const size_t data_size = 1u << 20;   // 1 MiB available at the source
    const size_t extent = 200u << 10;    // consumer advertises reading only 200 KiB

    BoundLog log;
    auto source = std::make_shared<BoundRecordingSource>(
        std::unordered_map<String, String>{{"obj", String(data_size, 'y')}}, log);
    StoredObjects objects;
    objects.emplace_back("obj", "", StoredObject::UnknownSize);

    auto limit = std::make_shared<LiveConnectionLimit>(10);
    ReaderExecutor executor(source, objects, {}, /*window_size=*/64u << 10, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);
    executor.setReadExtent(extent);

    size_t total = 0;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.empty())
            break;
        total += rope.range().size;
    }

    EXPECT_EQ(total, extent) << "the unknown-size reader stops at the advertised extent";
    ASSERT_FALSE(log.read_until.empty());
    ASSERT_TRUE(log.read_until[0].has_value())
        << "the connection must be bounded to the extent even for unknown size, not open-ended";
    EXPECT_EQ(*log.read_until[0], extent) << "bounded to the advertised extent (object-local)";
    EXPECT_EQ(limit->getActiveCount(), 0u)
        << "the live buffer + slot must be released once the extent is reached, not pinned";
}

TEST(ReaderExecutor, UnknownSizeStatelessReaderBoundsOneShotToExtent)
{
    /// The no-slot one-shot branch: an unknown-size source with a finite advertised
    /// extent but no available LiveConnectionLimit slot must still bound each one-shot
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

    /// No setBufferLimit -> no slot -> the stateless one-shot path.
    ReaderExecutor executor(source, objects, {}, /*window_size=*/64u << 10, /*min_bytes_for_seek=*/0);
    executor.setReadExtent(extent);

    size_t total = 0;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.empty())
            break;
        total += rope.range().size;
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

    auto parent = std::make_unique<ReaderExecutor>(
        source, objects, VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{},
        /*window_size=*/1000, /*min_bytes_for_seek=*/0);

    {
        auto transient = parent->makeTransientForReadAt(0, /*read_size=*/4000);
        while (true)
        {
            auto rope = transient->readNextWindow();
            if (rope.empty())
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

TEST(LiveConnectionLimit, MoveAssignReleasesPreviousSlot)
{
    /// Move-assignment must release the currently-held unit BEFORE taking `other`'s,
    /// else a unit of capacity leaks permanently.
    auto limit = std::make_shared<LiveConnectionLimit>(2);

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

TEST(LiveConnectionLimit, MoveAssignFromEmptyLeaseReleasesCurrent)
{
    /// Assigning an empty lease into a holding one must still release the current unit.
    auto limit = std::make_shared<LiveConnectionLimit>(2);

    auto a = limit->tryAcquire(limit);
    ASSERT_TRUE(a);
    EXPECT_EQ(limit->getActiveCount(), 1u);

    LiveConnectionSlot empty;          // holds no unit
    a = std::move(empty);            // must drop a's unit
    EXPECT_FALSE(a);
    EXPECT_EQ(limit->getActiveCount(), 0u);
}

TEST(LiveConnectionLimit, SelfMoveAssignIsNoOp)
{
    /// Self-assignment must not double-release.
    auto limit = std::make_shared<LiveConnectionLimit>(1);
    auto s = limit->tryAcquire(limit);
    ASSERT_TRUE(s);
    EXPECT_EQ(limit->getActiveCount(), 1u);

    auto * self = &s;                // via pointer so the compiler doesn't flag self-move
    s = std::move(*self);
    EXPECT_EQ(limit->getActiveCount(), 1u);
}

TEST(ReaderExecutor, LiveBufferReleasedAtEof)
{
    /// Once the caller reads to EOF, the per-stream `LiveConnectionLimit`
    /// slot (and the associated open connection) must be returned even if
    /// the `ReaderExecutor` itself is not yet destroyed. Otherwise a
    /// finished-but-still-held reader pins capacity from the global
    /// budget.
    String content(2000, 'E');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"file", content}});

    StoredObjects objects;
    objects.emplace_back("file", "", 2000);

    auto limit = std::make_shared<LiveConnectionLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Read every window — last call returns an empty rope (EOF).
    auto r1 = executor.readNextWindow();
    EXPECT_EQ(r1.range().size, 500u);
    EXPECT_EQ(limit->getActiveCount(), 1u) << "expected one open slot during streaming";

    auto r2 = executor.readNextWindow();
    auto r3 = executor.readNextWindow();
    auto r4 = executor.readNextWindow();
    EXPECT_EQ(r2.range().size, 500u);
    EXPECT_EQ(r3.range().size, 500u);
    EXPECT_EQ(r4.range().size, 500u);
    EXPECT_EQ(limit->getActiveCount(), 1u);

    /// EOF — slot must be released by readNextWindow itself.
    auto r5 = executor.readNextWindow();
    EXPECT_TRUE(r5.empty());
    EXPECT_EQ(limit->getActiveCount(), 0u)
        << "live buffer slot must be released when EOF is reached";

    /// Idempotent: calling readNextWindow again at EOF is still EOF and
    /// keeps the slot count at zero.
    auto r6 = executor.readNextWindow();
    EXPECT_TRUE(r6.empty());
    EXPECT_EQ(limit->getActiveCount(), 0u);
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

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500);

    String collected;
    while (true)
    {
        Rope w = executor.readNextWindow();
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

    ReaderExecutor executor(source, objects, {}, /*window_size=*/1000);

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

TEST(ReaderExecutor, UnknownSizeZeroByteTerminalReleasesLiveSlot)
{
    /// Unknown-size source whose content is an exact multiple of the window
    /// size, so the terminal live read returns 0 bytes: readNextWindow returns
    /// an empty rope and the caller stops, never making the follow-up call that
    /// would hit the pre-read EOF gate. The live buffer + its LiveConnectionLimit
    /// slot must still be released as soon as EOF is latched, not leaked until
    /// the executor is destroyed.
    String content(1000, 'E');   /// exactly 2 * window
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", StoredObject::UnknownSize);

    auto limit = std::make_shared<LiveConnectionLimit>(10);
    ReaderExecutor executor(source, objects, {}, /*window_size=*/500);
    executor.setBufferLimit(limit);

    String collected;
    while (true)
    {
        Rope w = executor.readNextWindow();
        if (w.empty())
            break;
        for (const auto & node : w.getNodes())
            collected.append(node.data(), node.size);
    }
    EXPECT_EQ(collected, content);
    EXPECT_EQ(limit->getActiveCount(), 0u)
        << "live buffer + slot must be released when EOF is latched on a zero-byte terminal read";
}

TEST(ReaderExecutor, UnknownSizeMultiObjectRejected)
{
    /// Multi-object pipelines need each object's `bytes_size` to compute
    /// the cumulative `logical_offset`. With an unknown size we can't.
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
        ReaderExecutor executor(source, objects, {}, /*window_size=*/100);
    });
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

    auto limit = std::make_shared<LiveConnectionLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Read to EOF.
    while (!executor.readNextWindow().empty()) {}
    EXPECT_EQ(limit->getActiveCount(), 0u);

    /// Seek back to the start and read again — slot must come back.
    executor.seek(0);
    auto r = executor.readNextWindow();
    EXPECT_EQ(r.range().offset, 0u);
    EXPECT_EQ(r.range().size, 500u);
    EXPECT_EQ(limit->getActiveCount(), 1u)
        << "slot must be re-acquired after backward seek + read";
}

TEST(ReaderExecutor, CacheOnlyWindowClosesStaleLiveBuffer)
{
    /// A wide cold gap (two segments, > a window) opens a live connection that holds the
    /// lease. A later window served entirely from cache leaves that connection parked
    /// behind the cursor where it can no longer continue the stream; it must be closed and
    /// its lease released at the cache-only window, not held idle until the next miss/EOF.
    constexpr size_t window_bytes = 1000;
    String content(4 * window_bytes, 'X');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", 4 * window_bytes);

    /// Segments 0,1 ([0, 2*window)) empty -> a 2-window cold gap, so the read opens a
    /// kept-live connection (reach > a window). Segments 2,3 ([2*window, 4*window))
    /// pre-downloaded so the windows over them are pure cache hits.
    auto cache = std::make_shared<EvictableSegmentMockCache>(window_bytes);
    cache->downloaded[2] = window_bytes;
    cache->downloaded[3] = window_bytes;
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(cache);

    auto limit = std::make_shared<LiveConnectionLimit>(10);
    ReaderExecutor executor(source, objects, caches, /*window_size=*/window_bytes, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Window 1 [0, window): cold gap (reach 2 windows) -> live connection + lease.
    ASSERT_FALSE(executor.readNextWindow().empty());
    EXPECT_EQ(limit->getActiveCount(), 1u) << "a wide cold gap opens a live connection";

    /// Window 2 [window, 2*window): still the cold gap -> the live connection continues.
    ASSERT_FALSE(executor.readNextWindow().empty());
    EXPECT_EQ(limit->getActiveCount(), 1u);

    /// Window 3 [2*window, 3*window): full cache hit, no source read -> the now-stale
    /// live connection is closed and its lease released.
    ASSERT_FALSE(executor.readNextWindow().empty());
    EXPECT_EQ(limit->getActiveCount(), 0u)
        << "cache-only window must close the now-stale live connection and release its lease";
}

TEST(ReaderExecutor, SeekClosesStaleLiveBufferEvenWithoutReadFromSource)
{
    /// Regression: `seek` used to defer closing a stale `live_buffer` to
    /// `readFromSource`. If the next window was fully cache-served (or just
    /// nothing was read after seek), the old connection — and its
    /// `LiveConnectionLimit` slot — stayed open until EOF or executor
    /// destruction, burning `max_remote_read_connections` capacity.
    String content_a(2000, 'A');
    String content_b(2000, 'B');
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"a", content_a}, {"b", content_b}});

    StoredObjects objects;
    objects.emplace_back("a", "", 2000);
    objects.emplace_back("b", "", 2000);

    auto limit = std::make_shared<LiveConnectionLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/500, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Read from object "a" — opens a live buffer + acquires a slot.
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 500u);
    EXPECT_EQ(limit->getActiveCount(), 1u);

    /// Seek into object "b". No read afterwards — the stale connection to
    /// "a" must be closed by `seek` itself, not by a future `readFromSource`.
    executor.seek(2500);
    EXPECT_EQ(limit->getActiveCount(), 0u)
        << "stale live buffer + slot must be released by seek when the "
           "target is in a different object";
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

    auto limit = std::make_shared<LiveConnectionLimit>(10);

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
    size_t readable() const override { return range_member.end(); }

    Rope read(ByteRange sub) override
    {
        Rope result;
        const size_t lo = std::max(sub.offset, range_member.offset);
        const size_t hi = std::min(sub.end(), range_member.end());
        if (lo >= hi)
            return result;

        Rope assembled;
        const size_t first_block = lo / block_size;
        const size_t last_block = (hi - 1) / block_size;
        for (size_t b = first_block; b <= last_block; ++b)
        {
            auto it = storage.find(b);
            if (it == storage.end())
                continue;
            const auto & data = it->second;
            auto buf = std::make_shared<OwnedRopeBuffer>(data.size());
            std::memcpy(buf->data(), data.data(), data.size());
            assembled.append(RopeNode{buf, 0, data.size(), b * block_size});
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
    const IntervalSet & committed() const override { return committed_ranges; }
    bool complete() const override { return committed_ranges.subtract(range_member).empty(); }

    size_t write(Rope data) override
    {
        size_t bytes_written = 0;
        for (size_t offset = range_member.offset; offset < range_member.end(); offset += block_size)
        {
            const size_t b = offset / block_size;
            const ByteRange block_range{offset, block_size};

            if (committed_ranges.subtract(block_range).empty())
                continue;
            if (!data.covers(block_range))
                continue;

            Rope slice = data.slice(block_range);
            put_log.emplace_back(block_range, slice.totalBytes());

            if (!storage.contains(b))
            {
                String content;
                content.resize(slice.totalBytes());
                slice.copyTo(content.data(), block_range);
                bytes_written += content.size();
                storage[b] = std::move(content);
            }
            committed_ranges.add(block_range);
        }
        return bytes_written;
    }

    Rope read(ByteRange sub) override
    {
        Rope result;
        const size_t lo = std::max(sub.offset, range_member.offset);
        const size_t hi = std::min(sub.end(), range_member.end());
        if (lo >= hi)
            return result;

        Rope assembled;
        const size_t first_block = lo / block_size;
        const size_t last_block = (hi - 1) / block_size;
        for (size_t b = first_block; b <= last_block; ++b)
        {
            auto it = storage.find(b);
            if (it == storage.end())
                continue;
            const auto & data = it->second;
            auto buf = std::make_shared<OwnedRopeBuffer>(data.size());
            std::memcpy(buf->data(), data.data(), data.size());
            assembled.append(RopeNode{buf, 0, data.size(), b * block_size});
        }
        return assembled.slice(ByteRange{lo, hi - lo});
    }

private:
    ByteRange range_member;
    std::unordered_map<size_t, String> & storage;
    std::vector<std::pair<ByteRange, size_t>> & put_log;
    size_t block_size;
    IntervalSet committed_ranges;
};

class WideGranularityMockCache : public ICacheProvider
{
public:
    WideGranularityMockCache(size_t block_size_, String name_)
        : block_size(block_size_), provider_name(std::move(name_)) {}

    String name() const override { return provider_name; }
    CacheTier tier() const override { return CacheTier::FilesystemCache; }

    /// A block is the write unit (`seedBlock`/`hasBlock` operate on whole blocks), so
    /// both fetch edges round to `block_size`: a touch fills the whole block it lands in.
    size_t fetchHeadAlignment() const override { return block_size; }
    size_t fetchTailAlignment() const override { return block_size; }

    /// Read-only residency probe at FULL block granularity, coalescing adjacent
    /// same-kind blocks. Never mutates the store. Hits carry a held read buffer;
    /// misses are whole-block-aligned with no writer.
    CacheViewPtr planResidencyView(const StoredObject &, size_t, ByteRange range_in_file) override
    {
        auto view = std::make_unique<MockCacheView>();
        if (range_in_file.size == 0)
            return view;

        const size_t start_block = range_in_file.offset / block_size;
        const size_t end_block = (range_in_file.end() + block_size - 1) / block_size;

        bool run_active = false;
        bool run_is_hit = false;
        ByteRange run_range{0, 0};
        auto flush_run = [&]()
        {
            if (!run_active)
                return;
            if (run_is_hit)
                view->hit_entries.push_back(HitEntry{
                    run_range, std::make_unique<WideGranularityReadBuffer>(run_range, storage, block_size)});
            else
                view->miss_entries.push_back(MissEntry{run_range, /*writer=*/nullptr});
            run_active = false;
        };

        for (size_t b = start_block; b < end_block; ++b)
        {
            const bool is_hit = storage.contains(b);
            const ByteRange block_range{b * block_size, block_size};
            if (run_active && run_is_hit != is_hit)
                flush_run();
            if (!run_active)
            {
                run_active = true;
                run_is_hit = is_hit;
                run_range = block_range;
            }
            else
                run_range.size = block_range.end() - run_range.offset;
        }
        flush_run();
        return view;
    }

    VectorWithMemoryTracking<MissEntry> openWriteBuffers(
        const StoredObject &, size_t, const VectorWithMemoryTracking<ByteRange> & aligned_miss_ranges) override
    {
        VectorWithMemoryTracking<MissEntry> result;
        result.reserve(aligned_miss_ranges.size());
        for (const auto & aligned : aligned_miss_ranges)
            result.push_back(MissEntry{
                aligned, std::make_unique<WideGranularityWriteBuffer>(aligned, storage, put_log, block_size)});
        return result;
    }

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

    ReaderExecutor executor(src, objects, {page_cache, disk_cache},
                             /*window_size=*/128 * 1024,
                             /*min_bytes_for_seek=*/0);

    size_t total = 0;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.range().size == 0)
            break;
        total += rope.range().size;
    }
    EXPECT_EQ(total, 4u * 1024 * 1024);
    EXPECT_TRUE(disk_cache->hasBlock(0))
        << "DiskCache must be filled with the full segment after the read (gap over-read)";
}

/// Every write across the chain must receive a rope with disjoint coverage —
/// totalBytes == range.size. A rope with duplicate nodes would overflow
/// the write buffer's flat copy.
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

    /// Drain: the resident prefix and the cold gap are served in separate calls;
    /// the gap's backfill is the put we are checking.
    while (executor.readNextWindow().range().size != 0)
        ;

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

    ReaderExecutor executor(src, objects, {page_cache, disk_cache},
                             /*window_size=*/block, /*min_bytes_for_seek=*/0);

    size_t total = 0;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.range().size == 0)
            break;
        total += rope.range().size;
    }
    EXPECT_EQ(total, block) << "the page hit must still serve the data";
    EXPECT_FALSE(disk_cache->hasBlock(0))
        << "the disk cell fully covered by the page hit must be pruned, not filled";
}

/// Proactive fill down to the deepest tier, across an EMBEDDED faster-tier hit. Disk cell
/// is 4K; page block is 1K and holds [1K,2K) inside that cell; disk is cold. Read [1K,4K)
/// (seek to 1K). The [2K,4K) no-tier gap's fetch aligns to the disk cell [0,4K), reaching
/// LEFT past the seek position and across the page hit to the cell floor, so the disk cell
/// fills WHOLE: [0,1K) over-read from source (left of the request), [1K,2K) promoted from
/// page, [2K,4K) from source. The client still gets only [1K,4K).
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

    ReaderExecutor executor(src, objects, {page_cache, disk_cache},
                             /*window_size=*/file, /*min_bytes_for_seek=*/0);
    executor.seek(page_block);  // request starts at 1K

    size_t delivered = 0;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.range().size == 0)
            break;
        delivered += rope.range().size;
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
        const IntervalSet & committed() const override { return committed_ranges; }
        bool complete() const override { return false; }
        size_t write(Rope) override { return 0; }
        Rope read(ByteRange) override { return {}; }
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
        /// build and in `serveLateHits`, so a cold window logs two per-object passes)
        /// and report the whole range as one writer-null miss.
        CacheViewPtr planResidencyView(
            const StoredObject & object, size_t object_file_offset, ByteRange range_in_file) override
        {
            log.push_back(TrackedLookup{object.remote_path, object_file_offset, range_in_file});
            auto view = std::make_unique<MockCacheView>();
            view->miss_entries.push_back(MissEntry{range_in_file, /*writer=*/nullptr});
            return view;
        }

        VectorWithMemoryTracking<MissEntry> openWriteBuffers(
            const StoredObject &, size_t, const VectorWithMemoryTracking<ByteRange> & aligned_miss_ranges) override
        {
            VectorWithMemoryTracking<MissEntry> result;
            result.reserve(aligned_miss_ranges.size());
            for (const auto & aligned : aligned_miss_ranges)
                result.push_back(MissEntry{aligned, std::make_unique<TrackingWriteBuffer>(aligned)});
            return result;
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

    ReaderExecutor executor(
        source, objects,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>>{tracker},
        /*window_size=*/500);

    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().size, 500u);

    /// With plan-then-stream (single-tier, the default), the window is probed for
    /// residency once per object via `planResidency` and then - because the data
    /// is cold - the gap is filled with a per-object `lookup`. The mock counts
    /// both as accesses, so the cache sees four per-object calls: [blob_A, blob_B]
    /// from planning, then [blob_A, blob_B] from the gap fill. The point of the
    /// test is that BOTH passes split at the object boundary, each call carrying
    /// the right `StoredObject` and `object_file_offset`.
    ASSERT_EQ(tracker->log.size(), 4u);

    for (size_t pass = 0; pass < 2; ++pass)
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

TEST(ReaderExecutor, MultiObjectWindowReusesOneLeaseAcrossObjects)
{
    /// A gather window spanning two objects takes exactly ONE lease (it is
    /// object-agnostic now). "a" opens a live connection with it; then "b" reuses the
    /// SAME lease - `readFromSource` closes "a"'s connection and reopens for "b" keeping
    /// the lease, so "b" also goes live (sequential, never two connections at once)
    /// instead of falling back to a stateless one-shot read.
    TestThreadGroup tg;
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"a", String(500, 'A')}, {"b", String(500, 'B')}});
    StoredObjects objects;
    objects.emplace_back("a", "", 500);
    objects.emplace_back("b", "", 500);

    auto limit = std::make_shared<LiveConnectionLimit>(10);
    /// window 600 < the 1000-byte file, so the plan spans more than a window -> a wide
    /// plan that takes the lease; the [0, 600) window still straddles "a" (500) and "b".
    ReaderExecutor executor(source, objects, {}, /*window_size=*/600, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    auto rope = executor.readNextWindow();
    ASSERT_EQ(rope.range().size, 600u);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorBufferSlotAcquired), 1)
        << "one object-agnostic lease covers the whole wide plan";
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferCreated), 2)
        << "both objects open a live connection in turn, reusing the one lease";
    EXPECT_EQ(tg.get(ProfileEvents::LiveSourceBufferFallbacks), 0)
        << "no stateless fallback - the lease is reused across the object boundary";
}

TEST(ReaderExecutor, PreAcquiredSlotMatchesObjectAtCursor)
{
    /// Previously `ensurePreAcquiredSlot` blindly pre-acquired for the
    /// FIRST object's path. A `seek` into a later object would acquire a
    /// slot for object A that the next source read (against object B)
    /// couldn't consume, then re-acquire another slot — and the first
    /// slot stayed pinned. The fix: pre-acquire for the object that
    /// covers the cursor's current `position`.
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"obj_A", String(500, 'A')},
            {"obj_B", String(500, 'B')},
        });

    StoredObjects objects;
    objects.emplace_back("obj_A", "", 500);
    objects.emplace_back("obj_B", "", 500);

    auto limit = std::make_shared<LiveConnectionLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/200, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// Seek past the first object before any reads.
    executor.seek(700);
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 700u);

    /// Exactly one lease held. The lease is object-agnostic now, so seeking past
    /// obj_A no longer reserves (and leaks) a per-object slot - the read against
    /// obj_B simply uses the one lease.
    EXPECT_EQ(limit->getActiveCount(), 1u)
        << "a seek past the first object must not leave an extra leased unit";
}

TEST(ReaderExecutor, SlotReleasedOnSeekToDifferentObject)
{
    /// After reading from object A, a seek into object B must release
    /// A's slot (live buffer for A is now stale) so total active slots
    /// stay at 1 — not grow with each cross-object seek.
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"obj_A", String(500, 'A')},
            {"obj_B", String(500, 'B')},
        });

    StoredObjects objects;
    objects.emplace_back("obj_A", "", 500);
    objects.emplace_back("obj_B", "", 500);

    auto limit = std::make_shared<LiveConnectionLimit>(10);

    ReaderExecutor executor(source, objects, {}, /*window_size=*/200, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);

    /// First read from object A.
    auto r1 = executor.readNextWindow();
    EXPECT_EQ(r1.range().offset, 0u);
    EXPECT_EQ(limit->getActiveCount(), 1u);

    /// Seek into object B and read.
    executor.seek(750);
    auto r2 = executor.readNextWindow();
    EXPECT_EQ(r2.range().offset, 750u);

    /// The object-agnostic lease is reused across the cross-object seek, so the count
    /// stays at one - it never accumulates a unit per object.
    EXPECT_EQ(limit->getActiveCount(), 1u) << "must not accumulate leases across cross-object seeks";
}

TEST(ReaderExecutor, PreAcquiredSlotReleasedWhenPrefetchNotSubmitted)
{
    /// `maybeTriggerPrefetch` pre-acquires a slot before it submits the prefetch.
    /// When no prefetch task ends up running - the pool's `submit` returns nullptr
    /// (queue full) - the slot must be released immediately, not held idle, or a
    /// full prefetch queue pins `max_remote_read_connections` slots across readers
    /// (and a later cross-object seek would compound it with a second stale slot).
    ///
    /// The fake prefetch pool returns nullptr from `submit`, so `ensurePreAcquiredSlot`
    /// fires but no prefetch handle is produced - exactly that path.
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{
            {"obj_A", String(500, 'A')},
            {"obj_B", String(500, 'B')},
        });

    StoredObjects objects;
    objects.emplace_back("obj_A", "", 500);
    objects.emplace_back("obj_B", "", 500);

    auto limit = std::make_shared<LiveConnectionLimit>(10);
    auto pool = std::make_shared<FakePrefetchPool>();

    ReaderExecutor executor(source, objects, {}, /*window_size=*/200, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(limit);
    executor.setPrefetchPool(pool);

    /// `seek(0)`'s tail `maybeTriggerPrefetch` pre-acquires an obj_A slot, then
    /// `submit` returns nullptr -> the slot is released right away.
    executor.seek(0);
    EXPECT_EQ(limit->getActiveCount(), 0u)
        << "a prefetch that was never submitted must not leave its slot reserved";

    /// Same on a cross-object seek into obj_B: no stale obj_A slot accumulates.
    executor.seek(700);
    EXPECT_EQ(limit->getActiveCount(), 0u);

    /// The read itself takes exactly one lease.
    auto rope = executor.readNextWindow();
    EXPECT_EQ(rope.range().offset, 700u);
    EXPECT_EQ(limit->getActiveCount(), 1u) << "the read holds exactly one lease";
}

/// For an unknown-size source, the worker can latch `reached_eof` mid-flight
/// while still producing a partial rope with the real final bytes.
/// `readNextWindow`'s EOF gate must defer to the in-flight machine (`atEnd()
/// && !machine`), so those bytes are collected and served; only the
/// nothing-in-flight case short-circuits to EOF.
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
    ReaderExecutor executor(source, objects, {}, window, /*min_bytes_for_seek=*/0);
    executor.setPrefetchPool(pool);

    /// First call: sync-read [0, 16). At the end of the call,
    /// `maybeTriggerPrefetch` submits P1 for [16, 32). The synchronous
    /// pool runs P1 inline: the source short-returns 14 bytes (EOF at 30),
    /// the worker sets `reached_eof = true`, and the future ends up
    /// holding the 14-byte rope.
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
        ReaderExecutor warmup(warm_source, objects, {cache}, /*window_size=*/100);
        warmup.seek(100);
        warmup.readNextWindow();
        ASSERT_TRUE(cache->hasBlock(1));
    }

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    auto pool = std::make_shared<SyncPrefetchPool>();
    ReaderExecutor executor(source, objects, {cache}, /*window_size=*/total, /*min_bytes_for_seek=*/0);
    executor.setPrefetchPool(pool);

    /// Cold gap [0,100); advances the cursor to the resident run at 100.
    auto r1 = executor.readNextWindow();
    EXPECT_EQ(r1.range().offset, 0u);
    EXPECT_EQ(r1.range().size, 100u);
    /// The crux: the cursor is resident at 100, but the first gap ahead [200,300) is now
    /// prefetched - overlapping the resident run that precedes it.
    EXPECT_TRUE(executor.hasInflightPrefetch())
        << "the first gap ahead must prefetch during the resident run (overlap)";

    /// Resident run [100,200) from cache, served WHILE the [200,300) prefetch is in flight
    /// (the relaxed `serveCacheBlock` path). The re-trigger at the tail is a no-op while one
    /// is already pending.
    auto r2 = executor.readNextWindow();
    EXPECT_EQ(r2.range().offset, 100u);
    EXPECT_EQ(r2.range().size, 100u);
    EXPECT_TRUE(executor.hasInflightPrefetch());

    /// Cold gap [200,300) - the cursor reaches it and consumes the overlapped prefetch.
    auto r3 = executor.readNextWindow();
    EXPECT_EQ(r3.range().offset, 200u);
    EXPECT_EQ(r3.range().size, 100u);

    EXPECT_TRUE(executor.readNextWindow().empty());
}

/// Cache populates are split by the context that produced them: a foreground
/// (synchronous) read credits `ReaderExecutorBytesPushedToCacheSync`, while a
/// prefetch worker credits `ReaderExecutorBytesPushedToCacheAsync`. `stats` are
/// flushed to `ProfileEvents` in `~ReaderExecutor`, so each delta is read only
/// after the executor scope closes.
TEST(ReaderExecutor, PopulateSplitsSyncAndDeferredByPath)
{
    /// The write-side split: a sync-path window populates synchronously on the
    /// foreground (`BytesPushedToCacheSync`); a machine-collected window defers
    /// its fill to a put step (`BytesPushedToCacheAsync`). Every byte lands in
    /// exactly one of the two.
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
        const auto sync_before = pe[ProfileEvents::ReaderExecutorBytesPushedToCacheSync].load(std::memory_order_relaxed);
        const auto async_before = pe[ProfileEvents::ReaderExecutorBytesPushedToCacheAsync].load(std::memory_order_relaxed);
        {
            auto cache = std::make_shared<MockCacheProvider>(window);
            ReaderExecutor executor(source, objects, {cache}, window);
            while (!executor.readNextWindow().empty()) {}
        }
        EXPECT_EQ(pe[ProfileEvents::ReaderExecutorBytesPushedToCacheSync].load(std::memory_order_relaxed) - sync_before, file_size)
            << "without a prefetch pool every populate is synchronous";
        EXPECT_EQ(pe[ProfileEvents::ReaderExecutorBytesPushedToCacheAsync].load(std::memory_order_relaxed) - async_before, 0u);
    }

    /// `SyncPrefetchPool` runs each submitted job inline: machine-collected
    /// windows defer their fill to put steps, and with pools present the
    /// sync-path windows defer through put-only machines too - so EVERY
    /// populate is deferred (async) and together they still cover the file.
    {
        const auto sync_before = pe[ProfileEvents::ReaderExecutorBytesPushedToCacheSync].load(std::memory_order_relaxed);
        const auto async_before = pe[ProfileEvents::ReaderExecutorBytesPushedToCacheAsync].load(std::memory_order_relaxed);
        {
            auto cache = std::make_shared<MockCacheProvider>(window);
            auto pool = std::make_shared<SyncPrefetchPool>();
            ReaderExecutor executor(source, objects, {cache}, window);
            executor.setPrefetchPool(pool);
            while (!executor.readNextWindow().empty()) {}
        }
        const auto sync_delta = pe[ProfileEvents::ReaderExecutorBytesPushedToCacheSync].load(std::memory_order_relaxed) - sync_before;
        const auto async_delta = pe[ProfileEvents::ReaderExecutorBytesPushedToCacheAsync].load(std::memory_order_relaxed) - async_before;
        EXPECT_EQ(sync_delta, 0u) << "with pools present no populate runs on the client thread";
        EXPECT_EQ(async_delta, file_size) << "every window defers its fill to a put step";
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

    ReaderExecutor executor(source, objects, {}, /*window_size=*/32, /*min_bytes_for_seek=*/0);

    /// Drive multiple windows to make sure subsequent reads also work,
    /// not just the first. Pre-fix the very first read would throw
    /// `CANNOT_READ_ALL_DATA` because `total_read == 0 < pr.size`.
    String collected;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.empty())
            break;
        size_t base = collected.size();
        collected.resize(base + rope.range().size);
        rope.copyTo(collected.data() + base, rope.range());
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
TEST(ReaderExecutor, RealDiskCacheSequentialEvictionKeepsConnection)
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

    auto limit = std::make_shared<LiveConnectionLimit>(10);
    /// NOTE: no prefetch pool — keep reads synchronous so the flood between
    /// windows is deterministic.
    auto executor = std::make_unique<ReaderExecutor>(source, objects, caches, /*window_size=*/2000, /*min_bytes_for_seek=*/0);
    executor->setBufferLimit(limit);

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

    auto & profile_events = DB::CurrentThread::getProfileEvents();
    const auto created_before = profile_events[ProfileEvents::LiveSourceBufferCreated].load();
    const auto src_before = profile_events[ProfileEvents::ReaderExecutorSourceRequests].load();

    String result;
    int round = 0;
    while (true)
    {
        auto rope = executor->readNextWindow();
        if (rope.empty())
            break;
        for (const auto & node : rope.getNodes())
            result.append(node.data(), node.size);
        flood(round++);   // eviction pressure before the next window
    }

    EXPECT_EQ(result, content);
    /// The streamed segment stayed pinned through every flood, so the live
    /// connection was never reset and the left bytes were never re-read. Destroy the
    /// executor so it flushes `stats` into the thread's ProfileEvents.
    executor.reset();
    EXPECT_EQ(profile_events[ProfileEvents::ReaderExecutorSourceRequests].load() - src_before, 1u);
    EXPECT_EQ(profile_events[ProfileEvents::LiveSourceBufferCreated].load() - created_before, 1);
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
        ReaderExecutor executor(source, objects, {}, /*window_size=*/256 * 1024);
        while (!executor.readNextWindow().empty()) {}
    }

    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorSourceRequests), 4u);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorBytesFromSource), size);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorRequestedBytes), size);
    /// No cache tiers configured: the cache counters stay 0.
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorCacheGetRequests), 0u);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorCachePopulateRequests), 0u);
    /// No read extent is set, so each one-shot opens the whole object and is dropped
    /// at its window's end, before the object bound: counted as incomplete. (Extent-
    /// bounded reads - e.g. MergeTree mark ranges - drain to the bound and count 0.)
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorIncompleteConnections), 4u);
}

TEST(ReaderExecutor, ModeledCostMatchesFormula)
{
    TestThreadGroup tg;

    /// Modeled cost = 30ms/source request + 20ms/MiB from source + 5ms/incomplete
    /// connection (cache terms 0): 4 window-sized requests + 1 MiB transferred + 4
    /// one-shot connections dropped before the object bound (no read extent is set).
    constexpr size_t size = 1024 * 1024;
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", String(size, 'C')}});
    StoredObjects objects;
    objects.emplace_back("obj", "", size);
    {
        ReaderExecutor executor(source, objects, {}, /*window_size=*/256 * 1024);
        while (!executor.readNextWindow().empty()) {}
    }

    const auto cost = tg.get(ProfileEvents::ReaderExecutorModeledCostMicroseconds);
    const auto requested = tg.get(ProfileEvents::ReaderExecutorRequestedBytes);
    EXPECT_EQ(cost, 30000u * 4 + 20000u + 5000u * 4);  // 4 reads + 1 MiB + 4 incomplete
    EXPECT_EQ(requested, size);

    /// The KPI: modeled ms per requested MiB.
    const double ms_per_mib = (static_cast<double>(cost) / 1000.0)
        / (static_cast<double>(requested) / (1024.0 * 1024.0));
    EXPECT_DOUBLE_EQ(ms_per_mib, 160.0);
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
        ReaderExecutor coarse(source, objects, {}, /*window_size=*/1024 * 1024);
        while (!coarse.readNextWindow().empty()) {}
    }
    const auto cost_after_coarse = tg.get(ProfileEvents::ReaderExecutorModeledCostMicroseconds);
    const auto requests_after_coarse = tg.get(ProfileEvents::ReaderExecutorSourceRequests);
    {
        auto source = std::make_shared<MemorySourceReader>(
            std::unordered_map<String, String>{{"b.bin", String(size, 'b')}});
        StoredObjects objects;
        objects.emplace_back("b.bin", "", size);
        ReaderExecutor fine(source, objects, {}, /*window_size=*/64 * 1024);
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
        ReaderExecutor executor(source, objects, {}, WINDOW, /*min_bytes_for_seek=*/0, BLOCK);
        executor.setPrefetchPool(pool);

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
            auto rope = executor.readNextWindow();
            if (rope.empty())
                break;
            for (const auto & node : rope.getNodes())
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

/// Inline pool whose submitJob can be toggled to reject ("queue full"): runs
/// accepted jobs synchronously on the calling thread - deterministic put-step
/// scheduling outcomes with zero worker-thread timing.
class TogglePool : public PrefetchThreadPool
{
public:
    TogglePool() : PrefetchThreadPool(NoWorkers{}) {}
    std::shared_ptr<JobHandle> submitJob(std::function<void()> task) override
    {
        if (fail.load())
            return nullptr;
        task();
        return makeCompletedJobHandleForTest();
    }
    std::atomic<bool> fail{false};
};

}

TEST(ReaderExecutor, MachineCollectDefersCacheFillToPutStep)
{
    /// Stage-3 contract: a machine-collected window's cache fill runs as a
    /// background put step (`BytesPushedToCacheAsync`); the first window (sync
    /// path, no machine yet) still writes synchronously. The warm pass is
    /// deterministic: `planResidencyWindow` joins every deferred fill before
    /// rebuilding, so after seek(0) the whole file is page-cache resident.
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
    /// full one and deterministically defers its fill to a put step. (A real
    /// pool in a zero-think-time drain loop revokes/interrupts most machines
    /// before their first block - covered by the gated takeover test.)
    auto pool = std::make_shared<SyncPrefetchPool>();
    TestThreadGroup tg;
    {
        ReaderExecutor executor(source, objects, caches, WINDOW, /*min_bytes_for_seek=*/0, BLOCK);
        executor.setPrefetchPool(pool);

        String cold;
        while (true)
        {
            auto rope = executor.readNextWindow();
            if (rope.empty())
                break;
            for (const auto & node : rope.getNodes())
                cold.append(node.data(), node.size);
        }
        EXPECT_EQ(cold, content);
        EXPECT_GT(tg.get(ProfileEvents::ReaderExecutorBytesPushedToCacheAsync), 0u)
            << "machine-collected windows must defer their fill to put steps";
        EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorBytesPushedToCacheSync), 0u)
            << "with pools present even the sync-path window defers (put-only machine)";
        EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorPutAbandoned), 0u);
    }
    /// Cold executor destroyed: its teardown joined every deferred fill, so the
    /// page cache now holds the whole file.

    /// Warm pass with a FRESH executor over the same cache: its residency plan
    /// sees the deferred fills as plain hits - nothing from the source. (A seek
    /// within the cold executor would NOT show this: its plan geometry is an
    /// immutable all-miss snapshot, so its machines re-fetch and only the
    /// collect prefers the cache copies.)
    {
        ReaderExecutor executor(source, objects, caches, WINDOW, /*min_bytes_for_seek=*/0, BLOCK);
        executor.setPrefetchPool(pool);
        const auto src_before = tg.get(ProfileEvents::ReaderExecutorBytesFromSource);
        String warm;
        while (true)
        {
            auto rope = executor.readNextWindow();
            if (rope.empty())
                break;
            for (const auto & node : rope.getNodes())
                warm.append(node.data(), node.size);
        }
        EXPECT_EQ(warm, content);
        EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorBytesFromSource), src_before)
            << "warm pass must be served entirely from the page cache";
        EXPECT_GT(tg.get(ProfileEvents::ReaderExecutorBytesFromPageCache), 0u);
    }
}

TEST(ReaderExecutor, PutStepParkReschedAbandonLadder)
{
    /// Pool-full handling for deferred fills: rejected at schedule -> parked
    /// (`PutPoolFull`), one reschedule at the next executor touch, abandoned if
    /// still rejected (`PutAbandoned`) - and reads stay byte-correct either way
    /// (a dropped fill only loses cache residency).
    constexpr size_t FILE_SIZE = 8000;
    constexpr size_t WINDOW = 2000;
    constexpr size_t BLOCK = 500;
    String content(FILE_SIZE, 0);
    for (size_t i = 0; i < content.size(); ++i)
        content[i] = static_cast<char>('0' + (i % 10));

    auto make_objects = [&]
    {
        StoredObjects objects;
        objects.emplace_back("obj", "", FILE_SIZE);
        return objects;
    };

    /// Reschedule case: park the put at window 2's collect, let the next
    /// window's sweep reschedule it successfully.
    {
        auto source = std::make_shared<MemorySourceReader>(
            std::unordered_map<String, String>{{"obj", content}});
        PageCacheFixture pc;
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
        caches.push_back(pc.provider(BLOCK, FILE_SIZE));
        auto pool = std::make_shared<TogglePool>();
        TestThreadGroup tg;
        {
            ReaderExecutor executor(source, make_objects(), caches, WINDOW, 0, BLOCK);
            executor.setPrefetchPool(pool);

            auto w1 = executor.readNextWindow();   /// sync; launches machine for w2 (inline fetch)
            ASSERT_FALSE(w1.empty());
            pool->fail.store(true);                /// w2's collect: put schedule rejected -> parked
            auto w2 = executor.readNextWindow();
            ASSERT_FALSE(w2.empty());
            EXPECT_GT(tg.get(ProfileEvents::ReaderExecutorPutPoolFull), 0u);
            pool->fail.store(false);               /// next sweep grants the reschedule
            String rest;
            while (true)
            {
                auto rope = executor.readNextWindow();
                if (rope.empty())
                    break;
                for (const auto & node : rope.getNodes())
                    rest.append(node.data(), node.size);
            }
            EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorPutAbandoned), 0u)
                << "a granted reschedule must not abandon the fill";
            EXPECT_GT(tg.get(ProfileEvents::ReaderExecutorBytesPushedToCacheAsync), 0u);
        }
    }

    /// Abandon case: the pool stays full through the reschedule.
    {
        auto source = std::make_shared<MemorySourceReader>(
            std::unordered_map<String, String>{{"obj", content}});
        PageCacheFixture pc;
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
        caches.push_back(pc.provider(BLOCK, FILE_SIZE));
        auto pool = std::make_shared<TogglePool>();
        TestThreadGroup tg;
        String all;
        {
            ReaderExecutor executor(source, make_objects(), caches, WINDOW, 0, BLOCK);
            executor.setPrefetchPool(pool);

            auto w1 = executor.readNextWindow();
            ASSERT_FALSE(w1.empty());
            for (const auto & node : w1.getNodes())
                all.append(node.data(), node.size);
            pool->fail.store(true);                /// parks w2's put AND blocks new launches
            while (true)
            {
                auto rope = executor.readNextWindow();
                if (rope.empty())
                    break;
                for (const auto & node : rope.getNodes())
                    all.append(node.data(), node.size);
            }
        }
        EXPECT_EQ(all, content) << "an abandoned fill must not affect served bytes";
        EXPECT_GT(tg.get(ProfileEvents::ReaderExecutorPutAbandoned), 0u)
            << "a put parked twice must be abandoned";
    }
}

TEST(ReaderExecutor, WarmServeDefersPromoteToPutStep)
{
    /// The warm-path lever: an fs-resident scan serves from the slower tier and
    /// promotes the served runs into the faster (page) tier - through put-only
    /// machines when pools are present, so the serve loop itself never writes a
    /// cache. The fs mock fabricates 'D' bytes; the source must never be read.
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
        fs->downloaded[i] = SEG;   /// every segment fully resident
    PageCacheFixture pcfix;
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(pcfix.provider(BLOCK, FILE_SIZE));   /// faster tier, cold
    caches.push_back(fs);                                 /// slower tier, warm

    auto pool = std::make_shared<SyncPrefetchPool>();
    TestThreadGroup tg;
    {
        ReaderExecutor executor(source, objects, caches, WINDOW, /*min_bytes_for_seek=*/0, BLOCK);
        executor.setPrefetchPool(pool);

        String result;
        while (true)
        {
            auto rope = executor.readNextWindow();
            if (rope.empty())
                break;
            for (const auto & node : rope.getNodes())
                result.append(node.data(), node.size);
        }
        EXPECT_EQ(result, String(FILE_SIZE, 'D')) << "warm serve must come from the fs tier";
        EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorBytesFromSource), 0u);
        EXPECT_GT(tg.get(ProfileEvents::ReaderExecutorBytesPromoted), 0u)
            << "served runs must be promoted into the faster tier";
        EXPECT_GT(tg.get(ProfileEvents::ReaderExecutorPutScheduled), 0u)
            << "promotes must go through put-only machines, not the serve loop";
    }
}
