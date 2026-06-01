/// Integration tests for `ReaderExecutor` driving a REAL cache chain:
///   `PageCacheProvider` (over a real `PageCache`)
///   + one or two `DiskCacheProvider`s (over real `FileCache`s),
/// backed by a file-backed source. No mocks — every component is the real one
/// used in production. The chain order mirrors the production wiring
/// (`PageCache` first, then filesystem cache(s)).
///
/// Attribution strategy: each scenario uses SEPARATE executors that SHARE the
/// same providers. `executor.getSourceRequestsCount()` is the source-read
/// signal — a fresh executor whose count stays 0 was served entirely from the
/// warmed cache chain. Cache state (which layer is warm) is controlled via
/// setup: a fresh `PageCacheFile` makes the page layer cold, a fresh
/// `FileCache` (or `removeAllReleasable`) makes a filesystem layer cold.

#include <IO/ReaderExecutor.h>
#include <IO/ISourceReader.h>
#include <IO/ICacheProvider.h>
#include <IO/PageCacheProvider.h>
#include <IO/DiskCacheProvider.h>
#include <IO/SourceBufferLimit.h>
#include <IO/ReadSettings.h>
#include <IO/Rope.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/PageCache.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/QueryScope.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/IO/createReadBufferFromFileBase.h>

#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheSettings.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Interpreters/FileCache/FileCacheKey.h>
#include <Interpreters/Context.h>
#include <Core/ServerUUID.h>

#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>

#include <Core/Defines.h>

#include <gtest/gtest.h>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

namespace fs = std::filesystem;

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

namespace ProfileEvents
{
    extern const Event ReaderExecutorBytesFromPageCache;
    extern const Event ReaderExecutorBytesFromFilesystemCache;
    extern const Event ReaderExecutorBytesFromSource;
    extern const Event ReaderExecutorBytesPushedToCacheSync;
    extern const Event ReaderExecutorBytesPushedToCacheAsync;
}

using namespace DB;

namespace
{

/// In-memory source reader. `open` materializes the requested object into a
/// temp file and returns a file-backed `ReadBufferFromFileBase`. Temp files
/// are cleaned up on destruction. (Copied from `gtest_reader_executor.cpp`.)
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
        auto path = fs::temp_directory_path() / ("test_chain_source_" + std::to_string(file_counter++));
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
            fs::remove(p);
    }

private:
    std::unordered_map<String, String> data;
    size_t file_counter = 0;
    std::vector<fs::path> temp_files;
};

/// In-memory source whose buffer honors `setReadUntilPosition` and advertises
/// `supportsRightBoundedReads`, mirroring `ReadBufferFromS3`. The file-backed
/// source above does not (local file descriptors return `false`), so it cannot
/// exercise the executor's connection-draining right bound. Needed to reproduce
/// the cache-expanded `readBigAt` short read.
class BoundedMemorySource : public ISourceReader
{
public:
    explicit BoundedMemorySource(std::unordered_map<String, String> data_) : data(std::move(data_)) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override
    {
        auto it = data.find(object.remote_path);
        if (it == data.end())
            return nullptr;
        return std::make_unique<BoundedBuffer>(it->second);
    }

    String name() const override { return "BoundedMemorySource"; }

private:
    class BoundedBuffer : public ReadBufferFromFileBase
    {
    public:
        explicit BoundedBuffer(String data_)
            : ReadBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0), data(std::move(data_)) {}

        String getFileName() const override { return "BoundedBuffer"; }
        bool supportsRightBoundedReads() const override { return true; }
        void setReadUntilPosition(size_t p) override { read_until = p; }

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
        size_t file_offset = 0;
        std::optional<size_t> read_until;
    };

    std::unordered_map<String, String> data;
};

/// Drive a `readBigAt`-style transient over `[offset, offset + want)`, mirroring
/// `PipelineReadBuffer::readBigAt`, and roll its stats into the parent.
String readBigAtViaTransient(ReaderExecutor & parent, size_t offset, size_t want)
{
    auto t = parent.makeTransientForReadAt(offset, want);
    String out;
    size_t total = 0;
    while (total < want)
    {
        auto rope = t->readNextWindow();
        if (rope.empty())
            break;
        for (const auto & node : rope.getNodes())
        {
            if (total >= want)
                break;
            const size_t copy = std::min(node.size, want - total);
            out.append(node.data(), copy);
            total += copy;
        }
    }
    parent.mergeTransientStats(*t);
    return out;
}

/// Distinct per-offset byte pattern so a short or mis-served read is detectable
/// (a uniform fill would hide off-by-one / wrong-block bugs).
String makePattern(size_t size)
{
    String s;
    s.resize(size);
    for (size_t i = 0; i < size; ++i)
        s[i] = static_cast<char>('A' + (i * 7 + (i / 251)) % 26);
    return s;
}

} // anonymous namespace


/// Provides the query-context preamble that `FileCache::reserve` needs
/// (`DiskCacheHandle::put` reserves space against the current query context).
/// Replicates the `DiskCacheHandlePinSurvivesEviction` setup. Each test gets a
/// fresh cache directory.
class ReaderExecutorCacheChain : public ::testing::Test
{
public:
    ReaderExecutorCacheChain()
    {
        /// Reset current_thread to avoid conflicts of ThreadStatus with MainThreadStatus.
        current_thread = nullptr;
        getContext();
    }

    ~ReaderExecutorCacheChain() override
    {
        current_thread = MainThreadStatus::get();
    }

    void SetUp() override
    {
        ServerUUID::setRandomForUnitTests();

        thread_status.emplace();

        Poco::XML::DOMParser dom_parser;
        std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
        Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
        Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
        getMutableContext().context->setConfig(config);

        query_context = Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId("reader_executor_cache_chain");
        chassert(&CurrentThread::get() == &*thread_status);
        query_scope_holder.emplace(QueryScope::create(query_context));

        cache_root = fs::current_path() / "reader_executor_chain_cache";
        if (fs::exists(cache_root))
            fs::remove_all(cache_root);
        fs::create_directories(cache_root);
    }

    void TearDown() override
    {
        query_scope_holder.reset();
        query_context.reset();
        thread_status.reset();
        if (fs::exists(cache_root))
            fs::remove_all(cache_root);
    }

    /// Construct + initialize a real `FileCache` rooted in a fresh subdir.
    /// `boundary_alignment == max_file_segment_size` keeps partially-filled
    /// segments at their full range (the state pins protect).
    std::shared_ptr<FileCache> makeFileCache(const String & name, size_t segment_size, size_t max_size)
    {
        FileCacheSettings settings;
        settings[FileCacheSetting::path] = (cache_root / name).string();
        settings[FileCacheSetting::max_size] = max_size;
        settings[FileCacheSetting::max_elements] = 1000;
        settings[FileCacheSetting::max_file_segment_size] = segment_size;
        settings[FileCacheSetting::boundary_alignment] = segment_size;
        settings[FileCacheSetting::load_metadata_asynchronously] = false;
        settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

        auto fc = std::make_shared<FileCache>(name, settings);
        fc->initialize();
        return fc;
    }

    static std::shared_ptr<PageCache> makePageCache()
    {
        return std::make_shared<PageCache>(
            std::chrono::milliseconds(2000), "LRU", 0.5,
            /*min_size_in_bytes=*/1ull << 24,
            /*max_size_in_bytes=*/1ull << 24,
            /*free_memory_ratio=*/0.0,
            /*num_shards=*/1);
    }

    static std::shared_ptr<PageCacheProvider> makePageProvider(
        const std::shared_ptr<PageCache> & page_cache,
        const String & file_path,
        size_t block_size,
        size_t file_size)
    {
        PageCacheFile page_file;
        page_file.path = file_path;
        page_file.file_version = "v1";
        return std::make_shared<PageCacheProvider>(
            page_cache, page_file, block_size,
            /*inject_eviction=*/false, /*bypass_if_missing=*/false,
            /*file_size_in_bytes=*/file_size);
    }

    std::shared_ptr<DiskCacheProvider> makeDiskProvider(const std::shared_ptr<FileCache> & fc)
    {
        FilesystemCacheSettings cache_settings;
        cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
        return std::make_shared<DiskCacheProvider>(fc, cache_settings, /*query_id_=*/"q");
    }

    /// Drive an executor to EOF and return all bytes read.
    static String drainAll(ReaderExecutor & executor)
    {
        String result;
        while (true)
        {
            auto rope = executor.readNextWindow();
            if (rope.empty())
                break;
            for (const auto & node : rope.getNodes())
                result.append(node.data(), node.size);
        }
        return result;
    }

protected:
    std::optional<ThreadStatus> thread_status;
    ContextMutablePtr query_context;
    std::optional<QueryScope> query_scope_holder;
    fs::path cache_root;
};


/// Scenario 1: both caches empty. Executor#1 reads the whole file from source
/// and warms the chain; executor#2 (same providers) serves the whole file from
/// the warmed chain (`getSourceRequestsCount() == 0`).
TEST_F(ReaderExecutorCacheChain, ColdPopulatesAllLayers)
{
    constexpr size_t segment_size = 64;
    constexpr size_t block_size = 16;
    constexpr size_t file_size = 5 * segment_size; /// 320 bytes

    const String content = makePattern(file_size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto page_cache = makePageCache();
    auto fc = makeFileCache("fc1", segment_size, /*max_size=*/1ull << 20);

    auto page_provider = makePageProvider(page_cache, "obj", block_size, file_size);
    auto disk_provider = makeDiskProvider(fc);

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(page_provider);
    caches.push_back(disk_provider);

    /// Executor #1: cold chain → reads from source, warms page + fs.
    {
        ReaderExecutor executor(source, objects, caches, /*window_size=*/block_size, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        EXPECT_EQ(drainAll(executor), content);
        EXPECT_GT(executor.getSourceRequestsCount(), 0u) << "cold chain must hit the source";
    }

    /// Executor #2: same providers, now warm → served entirely from the chain.
    {
        ReaderExecutor executor(source, objects, caches, /*window_size=*/block_size, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        EXPECT_EQ(drainAll(executor), content);
        EXPECT_EQ(executor.getSourceRequestsCount(), 0u)
            << "warm chain must serve everything without touching the source";
    }
}

/// Regression: a cold `readBigAt` of a small range strictly inside a page-cache
/// block. The page-cache miss legitimately expands to the whole block (larger
/// than the requested extent), so the transient must read the full block from
/// the source - bounding the connection to what it actually reads (the block),
/// drained and reusable - and populate the block. The earlier code bounded the
/// connection to the smaller requested extent, so the size-known source read
/// came up short and threw `CANNOT_READ_ALL_DATA`. A bound-honoring source is
/// required: the local-file source cannot trigger the truncating bound.
TEST_F(ReaderExecutorCacheChain, ReadBigAtInsidePageCacheBlock)
{
    constexpr size_t block_size = 64;
    constexpr size_t file_size = 4 * block_size;   // 256 bytes, four page-cache blocks
    const String content = makePattern(file_size);

    auto source = std::make_shared<BoundedMemorySource>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto page_cache = makePageCache();
    auto page_provider = makePageProvider(page_cache, "obj", block_size, file_size);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(page_provider);

    ReaderExecutor executor(source, objects, caches, /*window_size=*/4 * block_size, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));   // slot available -> live path

    /// [70, 80): a 10-byte slice strictly inside page-cache block [64, 128).
    const size_t off = block_size + 6;
    const size_t want = 10;
    const String got = readBigAtViaTransient(executor, off, want);
    EXPECT_EQ(got, content.substr(off, want)) << "cold readBigAt inside a block returns the exact slice";
    const size_t src_after_first = executor.getSourceRequestsCount();
    EXPECT_GT(src_after_first, 0u) << "a cold readBigAt must hit the source";

    /// The over-read populated the whole block: a second readBigAt elsewhere in
    /// the same block [64, 128) is served from the page cache with no new source
    /// request.
    const String got2 = readBigAtViaTransient(executor, block_size + 40, 8);   // [104, 112)
    EXPECT_EQ(got2, content.substr(block_size + 40, 8));
    EXPECT_EQ(executor.getSourceRequestsCount(), src_after_first)
        << "the cold readBigAt over-read and populated the full page-cache block";
}


/// Scenario 2: page layer warm, fs layer emptied. The page cache serves
/// everything; because the fs cache is empty it could not have served, and a
/// page hit means the executor never even consults / populates the fs layer.
TEST_F(ReaderExecutorCacheChain, PageHitSkipsSourceAndFs)
{
    constexpr size_t segment_size = 64;
    constexpr size_t block_size = 16;
    constexpr size_t file_size = 5 * segment_size;

    const String content = makePattern(file_size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto page_cache = makePageCache();
    auto fc = makeFileCache("fc2", segment_size, /*max_size=*/1ull << 20);
    const auto & origin = FileCache::getCommonOrigin();

    auto page_provider = makePageProvider(page_cache, "obj", block_size, file_size);
    auto disk_provider = makeDiskProvider(fc);

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(page_provider);
    caches.push_back(disk_provider);

    /// Warm both layers.
    {
        ReaderExecutor executor(source, objects, caches, /*window_size=*/block_size, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        EXPECT_EQ(drainAll(executor), content);
    }

    /// Empty the fs cache; page stays warm.
    fc->removeAllReleasable(origin.user_id);

    /// Executor #2: page serves everything. fs is empty so it could not have.
    {
        ReaderExecutor executor(source, objects, caches, /*window_size=*/block_size, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        EXPECT_EQ(drainAll(executor), content);
        EXPECT_EQ(executor.getSourceRequestsCount(), 0u)
            << "page layer must serve everything; fs was emptied and source untouched";
    }

    /// Page hits => executor never populates fs. A fresh fs lookup must still
    /// report only misses across the file.
    {
        StoredObject object{"obj", "", file_size};
        auto handle = disk_provider->lookup(object, /*object_file_offset=*/0, ByteRange{0, file_size});
        auto status = handle->status();
        EXPECT_TRUE(status.hit_ranges.empty())
            << "page hits must not back-fill the fs cache";
        EXPECT_FALSE(status.miss_ranges.empty());
    }
}


/// Scenario 3: page cold, fs warm. A fresh `PageCacheFile` makes the page layer
/// cold; the SAME warm fs serves the whole file. A third executor reusing the
/// fresh page provider (now populated by #2) serves entirely from page.
TEST_F(ReaderExecutorCacheChain, FsHitWhenPageColdRepopulatesPage)
{
    constexpr size_t segment_size = 64;
    constexpr size_t block_size = 16;
    constexpr size_t file_size = 5 * segment_size;

    const String content = makePattern(file_size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto page_cache = makePageCache();
    auto fc = makeFileCache("fc3", segment_size, /*max_size=*/1ull << 20);
    auto disk_provider = makeDiskProvider(fc);

    /// Warm both via a provider on PageCacheFile "warm".
    {
        auto warm_page_provider = makePageProvider(page_cache, "warm", block_size, file_size);
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
        caches.push_back(warm_page_provider);
        caches.push_back(disk_provider);

        ReaderExecutor executor(source, objects, caches, /*window_size=*/block_size, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        EXPECT_EQ(drainAll(executor), content);
    }

    /// Executor #2: FRESH PageCacheFile "cold" (page cold) + same warm fs.
    /// fs serves the file while page is cold.
    auto cold_page_provider = makePageProvider(page_cache, "cold", block_size, file_size);
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
        caches.push_back(cold_page_provider);
        caches.push_back(disk_provider);

        ReaderExecutor executor(source, objects, caches, /*window_size=*/block_size, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        EXPECT_EQ(drainAll(executor), content);
        EXPECT_EQ(executor.getSourceRequestsCount(), 0u)
            << "fs layer must serve everything while page is cold";
    }

    /// Executor #3: reuses #2's fresh page provider (now populated). Drop the
    /// fs from the chain entirely to prove page alone serves it.
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
        caches.push_back(cold_page_provider);

        ReaderExecutor executor(source, objects, caches, /*window_size=*/block_size, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        EXPECT_EQ(drainAll(executor), content);
        EXPECT_EQ(executor.getSourceRequestsCount(), 0u)
            << "fs hit in #2 must have repopulated the page layer";
    }
}


/// Scenario 4: partial fs hit. Executor#1 reads only the prefix `[0, half)`
/// (one window) and warms the fs prefix. Executor#2 reads the whole file: the
/// prefix is served from fs, the tail from source. We attribute via
/// `ProfileEvents`: `ReaderExecutorBytesFromFilesystemCache` ~ prefix and
/// `ReaderExecutorBytesFromSource` ~ tail.
///
/// The executor increments these thread-local counters (propagated through the
/// fixture's `QueryScope` thread group); we measure the delta ACROSS executor#2
/// only, so executor#1's own hit/miss accounting is excluded.
TEST_F(ReaderExecutorCacheChain, PartialFsHitTailFromSource)
{
    constexpr size_t segment_size = 64;
    constexpr size_t file_size = 6 * segment_size; /// 384 bytes
    constexpr size_t half = file_size / 2;         /// 192, a segment boundary

    const String content = makePattern(file_size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto fc = makeFileCache("fc4", segment_size, /*max_size=*/1ull << 20);
    auto disk_provider = makeDiskProvider(fc);

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(disk_provider);

    /// Executor #1: read only the prefix `[0, half)` (window == half, one read).
    {
        ReaderExecutor executor(source, objects, caches, /*window_size=*/half, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        auto rope = executor.readNextWindow();
        ASSERT_EQ(rope.range().size, half);
        String prefix;
        for (const auto & node : rope.getNodes())
            prefix.append(node.data(), node.size);
        EXPECT_EQ(prefix, content.substr(0, half));
    }

    auto & counters = CurrentThread::getProfileEvents();
    const auto hit_before = counters[ProfileEvents::ReaderExecutorBytesFromFilesystemCache].load(std::memory_order_relaxed);
    const auto miss_before = counters[ProfileEvents::ReaderExecutorBytesFromSource].load(std::memory_order_relaxed);

    /// Executor #2: read the whole file. Prefix from fs, tail from source.
    {
        ReaderExecutor executor(source, objects, caches, /*window_size=*/file_size, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        EXPECT_EQ(drainAll(executor), content);
        EXPECT_GT(executor.getSourceRequestsCount(), 0u) << "the tail must be fetched from source";
    }

    const auto hit_delta = counters[ProfileEvents::ReaderExecutorBytesFromFilesystemCache].load(std::memory_order_relaxed) - hit_before;
    const auto miss_delta = counters[ProfileEvents::ReaderExecutorBytesFromSource].load(std::memory_order_relaxed) - miss_before;

    /// Prefix served from the filesystem cache, tail from source.
    EXPECT_EQ(hit_delta, half)
        << "the warmed prefix must be a filesystem-cache hit (not re-read from source)";
    EXPECT_EQ(miss_delta, file_size - half)
        << "only the tail must miss";
}


/// Per-tier attribution: a hit served by the page cache lands in
/// `ReaderExecutorBytesFromPageCache`, leaving the filesystem-cache and source
/// counters untouched. `PartialFsHitTailFromSource` covers the
/// filesystem-cache side, so together they pin down the page/fs split.
TEST_F(ReaderExecutorCacheChain, PageCacheHitAttributedToPageTier)
{
    constexpr size_t block_size = 16;
    constexpr size_t file_size = 320;

    const String content = makePattern(file_size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto page_cache = makePageCache();
    auto page_provider = makePageProvider(page_cache, "obj", block_size, file_size);

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(page_provider);

    /// Warm the page cache.
    {
        ReaderExecutor executor(source, objects, caches, /*window_size=*/block_size, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        EXPECT_EQ(drainAll(executor), content);
    }

    auto & counters = CurrentThread::getProfileEvents();
    const auto page_before = counters[ProfileEvents::ReaderExecutorBytesFromPageCache].load(std::memory_order_relaxed);
    const auto fs_before = counters[ProfileEvents::ReaderExecutorBytesFromFilesystemCache].load(std::memory_order_relaxed);
    const auto src_before = counters[ProfileEvents::ReaderExecutorBytesFromSource].load(std::memory_order_relaxed);

    /// Warm read: the page cache serves the whole file.
    {
        ReaderExecutor executor(source, objects, caches, /*window_size=*/block_size, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        EXPECT_EQ(drainAll(executor), content);
        EXPECT_EQ(executor.getSourceRequestsCount(), 0u)
            << "the warm read must be served entirely from the page cache";
    }

    EXPECT_EQ(counters[ProfileEvents::ReaderExecutorBytesFromPageCache].load(std::memory_order_relaxed) - page_before, file_size)
        << "the warm read must be attributed to the page-cache tier";
    EXPECT_EQ(counters[ProfileEvents::ReaderExecutorBytesFromFilesystemCache].load(std::memory_order_relaxed) - fs_before, 0u)
        << "no filesystem cache is in the chain";
    EXPECT_EQ(counters[ProfileEvents::ReaderExecutorBytesFromSource].load(std::memory_order_relaxed) - src_before, 0u)
        << "the source must not be touched on a page hit";
}


/// Scenario 5: two fs layers. Chain [page(cold), fastFs(cold), slowFs(warm)].
/// Only slowFs is warmed. The chain reads from slowFs, and the populate path
/// back-fills the faster upstream layers (page + fastFs). We verify fastFs got
/// populated by reading through fastFs alone with source count 0.
TEST_F(ReaderExecutorCacheChain, TwoFsLayersFastMissSlowHit)
{
    constexpr size_t segment_size = 64;
    constexpr size_t block_size = 16;
    constexpr size_t file_size = 5 * segment_size;

    const String content = makePattern(file_size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto page_cache = makePageCache();
    auto fast_fc = makeFileCache("fast_fc", segment_size, /*max_size=*/1ull << 20);
    auto slow_fc = makeFileCache("slow_fc", segment_size, /*max_size=*/1ull << 20);

    auto fast_provider = makeDiskProvider(fast_fc);
    auto slow_provider = makeDiskProvider(slow_fc);

    /// Warm ONLY slowFs (read through a chain of just [slowFs]).
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
        caches.push_back(slow_provider);

        ReaderExecutor executor(source, objects, caches, /*window_size=*/block_size, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        EXPECT_EQ(drainAll(executor), content);
        EXPECT_GT(executor.getSourceRequestsCount(), 0u);
    }

    auto page_provider = makePageProvider(page_cache, "obj", block_size, file_size);

    /// Executor #2: full chain [page(cold), fastFs(cold), slowFs(warm)].
    /// slowFs serves; page + fastFs are back-filled.
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
        caches.push_back(page_provider);
        caches.push_back(fast_provider);
        caches.push_back(slow_provider);

        ReaderExecutor executor(source, objects, caches, /*window_size=*/block_size, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        EXPECT_EQ(drainAll(executor), content);
        EXPECT_EQ(executor.getSourceRequestsCount(), 0u)
            << "slowFs must serve everything; source untouched";
    }

    /// Executor #3: chain of just [fastFs]. If fastFs was populated, source
    /// count stays 0.
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
        caches.push_back(fast_provider);

        ReaderExecutor executor(source, objects, caches, /*window_size=*/block_size, /*min_bytes_for_seek=*/0);
        executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
        EXPECT_EQ(drainAll(executor), content);
        EXPECT_EQ(executor.getSourceRequestsCount(), 0u)
            << "fastFs must have been back-filled while serving from slowFs";
    }
}


/// Scenario 6: eviction in the chain keeps a single source connection. Chain
/// [page(cold), fs(small, evicting)], cold sequential scan in small windows.
/// Between windows we flood the fs cache with unrelated keys to force eviction.
/// The pinned in-flight fs segment under the live frontier must survive, so the
/// source connection is opened exactly once.
TEST_F(ReaderExecutorCacheChain, EvictionInChainKeepsSingleConnection)
{
    constexpr size_t segment_size = 64;
    constexpr size_t block_size = 16;
    constexpr size_t file_size = 4 * segment_size; /// 256 bytes
    constexpr size_t window = block_size;

    const String content = makePattern(file_size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});

    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto page_cache = makePageCache();
    /// Tiny fs cache: a single object's worth of segments won't all fit, and
    /// the unrelated flood forces eviction of everything releasable.
    auto fc = makeFileCache("fc6", segment_size, /*max_size=*/2 * segment_size);
    const auto & user = FileCache::getCommonOrigin();

    auto page_provider = makePageProvider(page_cache, "obj", block_size, file_size);
    auto disk_provider = makeDiskProvider(fc);

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(page_provider);
    caches.push_back(disk_provider);

    ReaderExecutor executor(source, objects, caches, /*window_size=*/window, /*min_bytes_for_seek=*/0);
    executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));

    auto flood_fs = [&](size_t round)
    {
        auto flood_key = FileCacheKey::fromPath("flood_key_" + std::to_string(round));
        for (size_t off = 0; off < 4 * segment_size; off += segment_size)
        {
            auto h = fc->getOrSet(flood_key, off, segment_size, /*file_size=*/4 * segment_size, {}, 0, user);
            for (auto & seg : *h)
            {
                if (seg->state() != FileSegment::State::EMPTY)
                    continue;
                if (seg->getOrSetDownloader() != FileSegment::getCallerId())
                    continue;
                std::string failure_reason;
                if (!seg->reserve(seg->range().size(), 1000, failure_reason))
                {
                    seg->completePartAndResetDownloader();
                    continue;
                }
                std::string data(seg->range().size(), 'F');
                seg->write(data.data(), data.size(), seg->getCurrentWriteOffset());
                FileSegment::complete(FileSegmentPtr(seg), /*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
            }
        }
    };

    String result;
    size_t round = 0;
    while (true)
    {
        auto rope = executor.readNextWindow();
        if (rope.empty())
            break;
        for (const auto & node : rope.getNodes())
            result.append(node.data(), node.size);
        /// Eviction pressure before the next window.
        flood_fs(round++);
    }

    EXPECT_EQ(result, content) << "no corruption / no missing bytes under eviction pressure";
    EXPECT_EQ(executor.getSourceRequestsCount(), 1u)
        << "the pinned in-flight fs segment must survive eviction; source opened exactly once";
}
