/// Integration tests for `ReaderExecutor` driving a REAL cache chain:
///   `PageCacheProvider` (over a real `PageCache`)
///   + one or two `DiskCacheProvider`s (over real `FileCache`s),
/// backed by a file-backed source. No mocks — every component is the real one
/// used in production. The chain order mirrors the production wiring
/// (`PageCache` first, then filesystem cache(s)).
///
/// Attribution strategy: each scenario uses SEPARATE executors that SHARE the
/// same providers. The `ReaderExecutorSourceRequests` ProfileEvent (read via
/// `sourceRequestsSoFar`, flushed when each executor is destroyed) is the
/// source-read signal — an executor that adds 0 to it was served entirely from
/// the warmed cache chain. Cache state (which layer is warm) is controlled via
/// setup: a fresh `PageCacheFile` makes the page layer cold, a fresh
/// `FileCache` (or `removeAllReleasable`) makes a filesystem layer cold.

#include <IO/ReaderExecutor.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/ICacheProvider.h>
#include <IO/ResidencyIterator.h>
#include <IO/PageCacheProvider.h>
#include <IO/DiskCacheProvider.h>
#include <IO/LongConnectionLimit.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/ReadSettings.h>
#include <IO/ChainedBuffers.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/tests/ReaderExecutorInspector.h>
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
    extern const Event ReaderExecutorSourceRequests;
}

using namespace DB;

namespace
{

/// The executor flushes its `stats` into the thread's `ProfileEvents` only in its
/// destructor (transients never emit - they roll up into the parent). So these read
/// the CUMULATIVE counter across every executor destroyed so far in the current test;
/// scope each executor in its own block and take a before/after delta to attribute a
/// metric to one executor. The fixture installs a `ThreadStatus`, so the counters live
/// on this thread.
size_t sourceRequestsSoFar()
{
    return CurrentThread::getProfileEvents()[ProfileEvents::ReaderExecutorSourceRequests];
}

size_t bytesFromSourceSoFar()
{
    return CurrentThread::getProfileEvents()[ProfileEvents::ReaderExecutorBytesFromSource];
}

/// In-memory source reader. `open` materializes the requested object into a
/// temp file and returns a file-backed `ReadBufferFromFileBase`. Temp files
/// are cleaned up on destruction. (Copied from `gtest_reader_executor.cpp`.)
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
class BoundedMemorySource : public IFileBasedSourceReader
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
        auto chain = t->readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
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
/// (`CacheWriter::write` reserves space against the current query context).
/// Each test gets a fresh cache directory.
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
    /// `boundary_alignment == max_file_segment_size` (the default) keeps partially-filled
    /// segments at their full range (the state pins protect); pass a finer `alignment` for
    /// the spanning-segment shapes (fetch cells smaller than the write segment).
    std::shared_ptr<FileCache> makeFileCache(
        const String & name, size_t segment_size, size_t max_size, size_t alignment = 0)
    {
        FileCacheSettings settings;
        settings[FileCacheSetting::path] = (cache_root / name).string();
        settings[FileCacheSetting::max_size] = max_size;
        settings[FileCacheSetting::max_elements] = 1000;
        settings[FileCacheSetting::max_file_segment_size] = segment_size;
        settings[FileCacheSetting::boundary_alignment] = alignment ? alignment : segment_size;
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
        size_t file_size,
        bool bypass_if_missing = false)
    {
        PageCacheFile page_file;
        page_file.path = file_path;
        page_file.file_version = "v1";
        return std::make_shared<PageCacheProvider>(
            page_cache, page_file, block_size,
            /*inject_eviction=*/false, bypass_if_missing,
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
            auto chain = executor.readNextWindow();
            if (chain.empty())
                break;
            for (const auto & node : chain.getNodes())
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
/// the warmed chain (adds 0 to `ReaderExecutorSourceRequests`).
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
        ReaderExecutor::Options executor_options;
        executor_options.window_size = block_size;
        executor_options.min_bytes_for_seek = 0;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        EXPECT_EQ(drainAll(executor), content);
    }
    EXPECT_GT(sourceRequestsSoFar(), 0u) << "cold chain must hit the source";

    /// Executor #2: same providers, now warm → served entirely from the chain.
    const size_t src_before_warm = sourceRequestsSoFar();
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = block_size;
        executor_options.min_bytes_for_seek = 0;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        EXPECT_EQ(drainAll(executor), content);
    }
    EXPECT_EQ(sourceRequestsSoFar(), src_before_warm)
        << "warm chain must serve everything without touching the source";
}

/// A request-map demand run whose start is NOT grid-aligned: the head cell floors at the
/// alignment grid BELOW the demand start, and the fill must drop to that cell floor - an
/// append-only segment writer starting above its head would refuse every write and the
/// head cell would never populate (silently: the bank serves the reads). At most one grid
/// quantum of hole bytes completes the intersecting cell - the accepted edge cost. A
/// second executor over the same cache proves the head cell is genuinely resident.
TEST_F(ReaderExecutorCacheChain, RequestMapWideHoleIsNotObservedOrPopulated)
{
    /// CA3 join: a WIDE request-map hole is never observed - the plan ends at
    /// the hole, so no `getOrSet` runs there and no cache segment is created
    /// for hole bytes. A seek to the next covered range re-plans and populates
    /// only that range. (RM3 already proved the hole is not FETCHED; CA3 adds
    /// that it is not even ALLOCATED.)
    constexpr size_t segment_size = 64;
    constexpr size_t file_size = 16 * segment_size;

    const String content = makePattern(file_size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto fc = makeFileCache("fc_hole", segment_size, /*max_size=*/1ull << 20);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(makeDiskProvider(fc));

    /// Demand: [0, 4*seg) and [12*seg, 16*seg); the 8-segment hole in the
    /// middle dwarfs min_bytes_for_seek.
    const size_t r2_start = 12 * segment_size;
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = segment_size;
        executor_options.min_bytes_for_seek = segment_size;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        executor.setRequestMap({{0, 4 * segment_size}, {r2_start, 4 * segment_size}});
        executor.setReadBound(file_size);

        /// Read the first covered range only.
        String got;
        while (got.size() < 4 * segment_size)
        {
            auto chain = executor.readNextWindow();
            if (chain.empty())
                break;
            for (const auto & node : chain.getNodes())
                got.append(node.data(), node.size);
        }
        EXPECT_EQ(got, content.substr(0, 4 * segment_size));

        /// Seek over the hole to the second covered range.
        executor.seek(r2_start);
        String tail;
        while (tail.size() < 4 * segment_size)
        {
            auto chain = executor.readNextWindow();
            if (chain.empty())
                break;
            for (const auto & node : chain.getNodes())
                tail.append(node.data(), node.size);
        }
        EXPECT_EQ(tail, content.substr(r2_start, 4 * segment_size));
    }

    /// Only the two covered ranges were populated; the wide hole [4*seg,12*seg)
    /// has no cache segment. A fresh warm probe over the hole is all-miss.
    {
        // `resolve`'s 2nd arg is the ask's OBJECT-LOCAL start; objects.front() is at file
        // base 0, so it equals the file offset (4 * segment_size).
        auto view = probeView(*makeDiskProvider(fc), objects.front(),
            4 * segment_size, ByteRange{4 * segment_size, 8 * segment_size});
        EXPECT_TRUE(view->allMiss()) << "the wide hole was never observed or populated";
    }
    /// The covered ranges DID populate.
    {
        auto view = probeView(*makeDiskProvider(fc), objects.front(),
            /*object_file_offset=*/0, ByteRange{0, 4 * segment_size});
        EXPECT_FALSE(view->hits().empty()) << "the first covered range populated";
    }
}

TEST_F(ReaderExecutorCacheChain, RequestMapUnalignedStartPopulatesHeadCell)
{
    constexpr size_t segment_size = 64;
    constexpr size_t file_size = 8 * segment_size;

    const String content = makePattern(file_size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto fc = makeFileCache("fc_headcell", segment_size, /*max_size=*/1ull << 20);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(makeDiskProvider(fc));

    /// Demand starts mid-cell at 3.5 segments; the wide lead-in hole (224 bytes
    /// >> min_bytes_for_seek) must not be walked back to, but the head cell
    /// [3*segment, 4*segment) must fill from its floor.
    const size_t demand_start = 3 * segment_size + segment_size / 2;
    const size_t demand_size = file_size - demand_start;
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = segment_size;
        executor_options.min_bytes_for_seek = 16;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        executor.setRequestMap({{demand_start, demand_size}});
        executor.setReadBound(file_size);
        executor.seek(demand_start);

        String got;
        while (got.size() < demand_size)
        {
            auto chain = executor.readNextWindow();
            if (chain.empty())
                break;
            for (const auto & node : chain.getNodes())
                got.append(node.data(), node.size);
        }
        EXPECT_EQ(got, content.substr(demand_start, demand_size));
        EXPECT_LT(inspect(executor).bytesFromSource(), file_size)
            << "the wide lead-in hole is not fetched wholesale";
    }

    /// Warm re-read of the SAME demand: everything - including the unaligned
    /// head - must come from the cache.
    const size_t src_before_warm = sourceRequestsSoFar();
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = segment_size;
        executor_options.min_bytes_for_seek = 16;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        executor.setRequestMap({{demand_start, demand_size}});
        executor.setReadBound(file_size);
        executor.seek(demand_start);

        String got;
        while (got.size() < demand_size)
        {
            auto chain = executor.readNextWindow();
            if (chain.empty())
                break;
            for (const auto & node : chain.getNodes())
                got.append(node.data(), node.size);
        }
        EXPECT_EQ(got, content.substr(demand_start, demand_size));
    }
    EXPECT_EQ(sourceRequestsSoFar(), src_before_warm)
        << "the head cell populated on the cold pass - the warm pass never hits the source";
}

/// The plan span is independent of `read_extent_end`, so advancing the extent per "mark
/// range" does NOT rebuild the plan (the cursor stays inside the one plan that already
/// reaches the file end) -- one observation for the whole scan, serving identical bytes.
TEST_F(ReaderExecutorCacheChain, PlanReusedAcrossExtentAdvances)
{
    constexpr size_t block_size = 16;
    constexpr size_t file_size = 16 * block_size; /// 256
    constexpr size_t mark = 32;                   /// per-"mark-range" extent step

    const String content = makePattern(file_size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto page_cache = makePageCache();
    auto page_provider = makePageProvider(page_cache, "obj", block_size, file_size);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(page_provider);

    /// Warm the page cache over the whole file.
    {
        ReaderExecutor::Options o;
        o.window_size = block_size;
        o.min_bytes_for_seek = 0;
        o.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor warm(source, objects, caches, o);
        EXPECT_EQ(drainAll(warm), content);
    }

    /// Scan the file in `mark`-sized "mark ranges": advance the extent, then read up to
    /// it, repeat. Returns the bytes read and the number of plan (re)builds.
    auto scan = [&]() -> std::pair<String, UInt64>
    {
        ReaderExecutor::Options o;
        o.window_size = file_size; /// base request covers the whole (small) file
        o.min_bytes_for_seek = 0;
        o.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        o.plan_look_ahead_max_window = file_size;
        ReaderExecutor ex(source, objects, caches, o);

        String out;
        for (size_t extent = mark;; extent = std::min(extent + mark, file_size))
        {
            ex.setReadBound(extent);
            while (out.size() < extent)
            {
                auto chain = ex.readNextWindow();
                if (chain.empty())
                    break;
                for (const auto & node : chain.getNodes())
                    out.append(node.data(), node.size);
            }
            if (extent >= file_size)
                break;
        }
        return {out, inspect(ex).observationCount()};
    };

    const auto [gen_out, gen_obs] = scan();

    EXPECT_EQ(gen_out, content)
        << "serves identical bytes across extent advances";
    EXPECT_EQ(gen_obs, 1u)
        << "plans the whole file once and reuses it across every extent advance";
}

/// With a prefetch pool the gap fills and promotes run INLINE on the read thread (the put
/// lane is gone). This asserts the cache is populated CORRECTLY (cold and warm): the warm
/// re-read is fully served from the chain (cheaper than cold), i.e. the inline writes
/// populate without corrupting the read path.
TEST_F(ReaderExecutorCacheChain, InlineFillPopulatesCacheFully)
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
    auto fc = makeFileCache("lane_fc", segment_size, /*max_size=*/1ull << 20);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(makePageProvider(page_cache, "obj", block_size, file_size));
    caches.push_back(makeDiskProvider(fc));

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    ReaderExecutor::Options opts;
    opts.window_size = block_size;
    opts.min_bytes_for_seek = 0;
    opts.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
    opts.prefetch_pool = pool;

    const size_t src_before_cold = sourceRequestsSoFar();
    {
        ReaderExecutor cold(source, objects, caches, opts);
        EXPECT_EQ(drainAll(cold), content) << "cold scan serves all bytes";
    }   /// inline fills already landed on the read thread
    const size_t cold_source = sourceRequestsSoFar() - src_before_cold;

    const size_t src_before_warm = sourceRequestsSoFar();
    {
        ReaderExecutor warm(source, objects, caches, opts);
        EXPECT_EQ(drainAll(warm), content) << "warm scan serves all bytes (cache + source)";
    }
    const size_t warm_source = sourceRequestsSoFar() - src_before_warm;

    EXPECT_GT(cold_source, 0u) << "cold scan must hit the source";
    EXPECT_EQ(warm_source, 0u)
        << "the inline fills populate the whole chain (same-thread writes, in window order), "
           "so the warm re-read touches the source 0 times";
}

/// NO prefetch pool: every foreground window is served by a one-window FetchMachine run INLINE on
/// the read thread (LocalRunner). Asserts the cold scan (a) serves every byte, (b) counts NO
/// prefetch hits (an inline collect is a synchronous fetch, not a prefetch - the counter must
/// stay meaningful with `remote_filesystem_read_prefetch = 0`), and (c) populated the cache
/// fully, so the warm re-read hits the source 0 times.
TEST_F(ReaderExecutorCacheChain, UnifiedForegroundServesAndPopulatesViaInlineMachine)
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
    auto fc = makeFileCache("unified_fg_fc", segment_size, /*max_size=*/1ull << 20);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(makePageProvider(page_cache, "obj", block_size, file_size));
    caches.push_back(makeDiskProvider(fc));

    ReaderExecutor::Options opts;
    opts.window_size = block_size;
    opts.min_bytes_for_seek = 0;
    opts.long_connection_limit = std::make_shared<LongConnectionLimit>(10);

    const size_t src_before_cold = sourceRequestsSoFar();
    UInt64 cold_prefetch_hits = 0;
    {
        ReaderExecutor cold(source, objects, caches, opts);
        EXPECT_EQ(drainAll(cold), content) << "cold inline scan serves all bytes";
        cold_prefetch_hits = inspect(cold).prefetchHits();
    }
    const size_t cold_source = sourceRequestsSoFar() - src_before_cold;

    const size_t src_before_warm = sourceRequestsSoFar();
    {
        ReaderExecutor warm(source, objects, caches, opts);
        EXPECT_EQ(drainAll(warm), content) << "warm inline scan serves all bytes";
    }
    const size_t warm_source = sourceRequestsSoFar() - src_before_warm;

    EXPECT_GT(cold_source, 0u) << "cold inline scan must hit the source";
    EXPECT_EQ(cold_prefetch_hits, 0u)
        << "an inline collect is a synchronous fetch, not a prefetch hit";
    EXPECT_EQ(warm_source, 0u)
        << "the inline FetchMachine fills the cache fully, so the warm re-read touches source 0 times";
}

/// The inline foreground serve *with* a prefetch pool: it must coexist with the read-ahead machine
/// without clobbering the single machine slot (the inline launch is guarded by `!machine`, not
/// `!machineFor(ri)`). Cold scan serves every byte; warm re-read hits the source 0 times - neither
/// path corrupts the other's fill.
TEST_F(ReaderExecutorCacheChain, UnifiedForegroundCoexistsWithReadAheadPool)
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
    auto fc = makeFileCache("unified_fg_pool_fc", segment_size, /*max_size=*/1ull << 20);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(makePageProvider(page_cache, "obj", block_size, file_size));
    caches.push_back(makeDiskProvider(fc));

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    ReaderExecutor::Options opts;
    opts.window_size = block_size;
    opts.min_bytes_for_seek = 0;
    opts.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
    opts.prefetch_pool = pool;

    const size_t src_before_cold = sourceRequestsSoFar();
    {
        ReaderExecutor cold(source, objects, caches, opts);
        EXPECT_EQ(drainAll(cold), content) << "cold scan (read-ahead + inline) serves all bytes";
    }
    const size_t cold_source = sourceRequestsSoFar() - src_before_cold;

    const size_t src_before_warm = sourceRequestsSoFar();
    {
        ReaderExecutor warm(source, objects, caches, opts);
        EXPECT_EQ(drainAll(warm), content) << "warm scan serves all bytes";
    }
    const size_t warm_source = sourceRequestsSoFar() - src_before_warm;

    EXPECT_GT(cold_source, 0u) << "cold scan must hit the source";
    EXPECT_EQ(warm_source, 0u)
        << "read-ahead + inline foreground populate the cache fully; warm re-read touches source 0 times";
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

    /// [70, 80): a 10-byte slice strictly inside page-cache block [64, 128).
    const size_t off = block_size + 6;
    const size_t want = 10;
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = 4 * block_size;
        executor_options.min_bytes_for_seek = 0;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        const String got = readBigAtViaTransient(executor, off, want);
        EXPECT_EQ(got, content.substr(off, want)) << "cold readBigAt inside a block returns the exact slice";
    }
    const size_t src_after_first = sourceRequestsSoFar();
    EXPECT_GT(src_after_first, 0u) << "a cold readBigAt must hit the source";

    /// The over-read populated the whole block: a second readBigAt elsewhere in the
    /// same block [64, 128) is served from the (now-warm) page cache with no new source
    /// request - a second executor over the same providers sees no source delta.
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = 4 * block_size;
        executor_options.min_bytes_for_seek = 0;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        const String got2 = readBigAtViaTransient(executor, block_size + 40, 8);   // [104, 112)
        EXPECT_EQ(got2, content.substr(block_size + 40, 8));
    }
    EXPECT_EQ(sourceRequestsSoFar(), src_after_first)
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
        ReaderExecutor::Options executor_options;
        executor_options.window_size = block_size;
        executor_options.min_bytes_for_seek = 0;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        EXPECT_EQ(drainAll(executor), content);
    }

    /// Empty the fs cache; page stays warm.
    fc->removeAllReleasable(origin.user_id);

    /// Executor #2: page serves everything. fs is empty so it could not have.
    const size_t src_before_page = sourceRequestsSoFar();
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = block_size;
        executor_options.min_bytes_for_seek = 0;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        EXPECT_EQ(drainAll(executor), content);
    }
    EXPECT_EQ(sourceRequestsSoFar(), src_before_page)
        << "page layer must serve everything; fs was emptied and source untouched";

    /// Page hits => executor never populates fs. A fresh fs lookup must still
    /// report only misses across the file.
    {
        StoredObject object{"obj", "", file_size};
        auto view = probeView(*disk_provider, object, /*object_file_offset=*/0, ByteRange{0, file_size});
        EXPECT_TRUE(view->hits().empty())
            << "page hits must not back-fill the fs cache";
        EXPECT_FALSE(view->misses().empty());
    }
}


/// Cache-chain policy, interior hole (example A): the page holds the PREFIX `[0,16)`
/// and the SUFFIX `[32,64)` of an fs segment; the interior hole is `[16,32)`; fs is
/// empty. The page ranges SERVE directly (the upper tier is a serve bonus) but nothing
/// writes them down into the fs cell, so the demand fetch for the hole starts at the
/// cell's append-only floor and READS THROUGH the page-held prefix from the source:
/// `[0,32)` crosses the wire, the fs segment fills to 32, and the page-held suffix
/// costs nothing (no window pumps past it - the fs tail stays a hole until the page
/// tier evicts).
TEST_F(ReaderExecutorCacheChain, PageHeldRangesServeWhileFsFillsByReadThrough)
{
    constexpr size_t segment_size = 64;
    constexpr size_t block_size = 16;
    constexpr size_t file_size = 2 * segment_size;

    const String content = makePattern(file_size);
    auto source = std::make_shared<BoundedMemorySource>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto page_cache = makePageCache();
    auto fc = makeFileCache("fcA", segment_size, /*max_size=*/1ull << 20);
    auto page_provider = makePageProvider(page_cache, "obj", block_size, file_size);
    auto disk_provider = makeDiskProvider(fc);

    /// Warm ONLY page blocks [0,16) and [32,64) via a page-only chain; a page read fills
    /// whole blocks, so two reads leave exactly the [16,32) hole cold.
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> page_only;
        page_only.push_back(page_provider);
        ReaderExecutor::Options o;
        o.window_size = block_size;
        o.min_bytes_for_seek = 0;
        o.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor warmer(source, objects, page_only, o);
        EXPECT_EQ(readBigAtViaTransient(warmer, 0, block_size), content.substr(0, block_size));
        EXPECT_EQ(readBigAtViaTransient(warmer, 2 * block_size, 2 * block_size),
            content.substr(2 * block_size, 2 * block_size));
    }

    /// page + fs (fs empty). Read the segment [0,64): only the hole is fetched.
    const size_t src_bytes_before = bytesFromSourceSoFar();
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
        caches.push_back(page_provider);
        caches.push_back(disk_provider);
        ReaderExecutor::Options o;
        o.window_size = block_size;
        o.min_bytes_for_seek = 0;
        o.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, o);
        EXPECT_EQ(readBigAtViaTransient(executor, 0, segment_size), content.substr(0, segment_size));
    }
    EXPECT_EQ(bytesFromSourceSoFar() - src_bytes_before, 2 * block_size)
        << "the hole's fetch reads through the page-held prefix ([0,32) from the source); "
           "the page-held suffix is never pumped";

    /// The fs segment holds the read-through prefix + the hole; its tail stays a hole.
    {
        StoredObject object{"obj", "", file_size};
        auto view = probeView(*disk_provider, object, /*object_file_offset=*/0, ByteRange{0, segment_size});
        EXPECT_FALSE(view->hits().empty()) << "the fs segment must hold the read-through prefix + the hole";
    }
}


/// Contrast to example A: the page holds ONLY the prefix `[0,16)` of the fs segment; the
/// suffix `[16,64)` is covered by no tier. The demand fetch starts at the cell's
/// append-only floor and takes the whole cell in one piece - the page-held prefix is
/// read through and the uncovered tail fetched: `[0,64)` from the source.
TEST_F(ReaderExecutorCacheChain, CrossCacheFetchesWholeCellForUncoveredTail)
{
    constexpr size_t segment_size = 64;
    constexpr size_t block_size = 16;
    constexpr size_t file_size = 2 * segment_size;

    const String content = makePattern(file_size);
    auto source = std::make_shared<BoundedMemorySource>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto page_cache = makePageCache();
    auto fc = makeFileCache("fcB", segment_size, /*max_size=*/1ull << 20);
    auto page_provider = makePageProvider(page_cache, "obj", block_size, file_size);
    auto disk_provider = makeDiskProvider(fc);

    /// Warm ONLY page block [0,16); leave [16,64) cold.
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> page_only;
        page_only.push_back(page_provider);
        ReaderExecutor::Options o;
        o.window_size = block_size;
        o.min_bytes_for_seek = 0;
        o.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor warmer(source, objects, page_only, o);
        EXPECT_EQ(readBigAtViaTransient(warmer, 0, block_size), content.substr(0, block_size));
    }

    const size_t src_bytes_before = bytesFromSourceSoFar();
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
        caches.push_back(page_provider);
        caches.push_back(disk_provider);
        ReaderExecutor::Options o;
        o.window_size = block_size;
        o.min_bytes_for_seek = 0;
        o.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, o);
        EXPECT_EQ(readBigAtViaTransient(executor, 0, segment_size), content.substr(0, segment_size));
    }
    EXPECT_EQ(bytesFromSourceSoFar() - src_bytes_before, segment_size)
        << "the whole cell is fetched from its floor: the page-held prefix is read through, "
           "the uncovered tail follows";
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
        ReaderExecutor::Options executor_options;
        executor_options.window_size = half;
        executor_options.min_bytes_for_seek = 0;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        auto chain = executor.readNextWindow();
        ASSERT_EQ(chain.range().size, half);
        String prefix;
        for (const auto & node : chain.getNodes())
            prefix.append(node.data(), node.size);
        EXPECT_EQ(prefix, content.substr(0, half));
    }

    auto & counters = CurrentThread::getProfileEvents();
    const auto hit_before = counters[ProfileEvents::ReaderExecutorBytesFromFilesystemCache];
    const auto miss_before = counters[ProfileEvents::ReaderExecutorBytesFromSource];
    const size_t src_before = sourceRequestsSoFar();

    /// Executor #2: read the whole file. Prefix from fs, tail from source.
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = file_size;
        executor_options.min_bytes_for_seek = 0;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        EXPECT_EQ(drainAll(executor), content);
    }
    EXPECT_GT(sourceRequestsSoFar() - src_before, 0u) << "the tail must be fetched from source";

    const auto hit_delta = counters[ProfileEvents::ReaderExecutorBytesFromFilesystemCache] - hit_before;
    const auto miss_delta = counters[ProfileEvents::ReaderExecutorBytesFromSource] - miss_before;

    /// The cache is the buffer, so every delivered byte is read out of a cell and counts as a
    /// filesystem-cache read: the warmed prefix (a hit) AND the tail (fetched from the source, then
    /// read back out of the filled cell). The tail therefore shows in BOTH counters - fetched from
    /// the source once, served from the cache once - so `hit_delta` is the whole file.
    EXPECT_EQ(hit_delta, file_size)
        << "every delivered byte is read out of a cell (prefix hit + tail served from the filled cell)";
    EXPECT_EQ(miss_delta, file_size - half)
        << "only the tail is fetched from the source";
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
        ReaderExecutor::Options executor_options;
        executor_options.window_size = block_size;
        executor_options.min_bytes_for_seek = 0;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        EXPECT_EQ(drainAll(executor), content);
    }

    auto & counters = CurrentThread::getProfileEvents();
    const auto page_before = counters[ProfileEvents::ReaderExecutorBytesFromPageCache];
    const auto fs_before = counters[ProfileEvents::ReaderExecutorBytesFromFilesystemCache];
    const auto src_before = counters[ProfileEvents::ReaderExecutorBytesFromSource];
    const size_t src_req_before = sourceRequestsSoFar();

    /// Warm read: the page cache serves the whole file.
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = block_size;
        executor_options.min_bytes_for_seek = 0;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        EXPECT_EQ(drainAll(executor), content);
    }

    EXPECT_EQ(sourceRequestsSoFar(), src_req_before)
        << "the warm read must be served entirely from the page cache";
    EXPECT_EQ(counters[ProfileEvents::ReaderExecutorBytesFromPageCache] - page_before, file_size)
        << "the warm read must be attributed to the page-cache tier";
    EXPECT_EQ(counters[ProfileEvents::ReaderExecutorBytesFromFilesystemCache] - fs_before, 0u)
        << "no filesystem cache is in the chain";
    EXPECT_EQ(counters[ProfileEvents::ReaderExecutorBytesFromSource] - src_before, 0u)
        << "the source must not be touched on a page hit";
}


/// Bypass mode (`read_from_page_cache_if_exists_otherwise_bypass_cache`): a page
/// miss must serve correct bytes from the source but never populate the cache -
/// `put` returns 0, nothing is counted as pushed, and a second reader on the same
/// provider still misses entirely.
TEST_F(ReaderExecutorCacheChain, PageCacheBypassModeDoesNotPopulate)
{
    constexpr size_t block_size = 64;
    constexpr size_t file_size = 4 * block_size;   // 256 bytes
    const String content = makePattern(file_size);

    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto page_cache = makePageCache();
    auto page_provider = makePageProvider(page_cache, "obj", block_size, file_size, /*bypass_if_missing=*/true);
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(page_provider);

    auto & counters = CurrentThread::getProfileEvents();
    const auto pushed_before = counters[ProfileEvents::ReaderExecutorBytesPushedToCacheSync];

    /// Cold read in bypass mode: serves the file from the source, populates nothing.
    const size_t src_before_cold = sourceRequestsSoFar();
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = block_size;
        executor_options.min_bytes_for_seek = 0;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        EXPECT_EQ(drainAll(executor), content);
    }
    EXPECT_GT(sourceRequestsSoFar() - src_before_cold, 0u) << "a cold bypass read hits the source";
    EXPECT_EQ(counters[ProfileEvents::ReaderExecutorBytesPushedToCacheSync] - pushed_before, 0u)
        << "bypass mode must not push (or count) any bytes to the cache";

    /// A second reader on the same provider still misses - bypass populated nothing.
    const size_t src_before_second = sourceRequestsSoFar();
    {
        ReaderExecutor::Options executor_options;
        executor_options.window_size = block_size;
        executor_options.min_bytes_for_seek = 0;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, executor_options);
        EXPECT_EQ(drainAll(executor), content);
    }
    EXPECT_GT(sourceRequestsSoFar() - src_before_second, 0u)
        << "bypass populated nothing, so the second read still misses";
}


/// Scenario 6: eviction in the chain keeps a single source connection. Chain
/// [page(cold), fs(small, evicting)], cold sequential scan in small windows.
/// Between windows we flood the fs cache with unrelated keys to force eviction. The in-flight
/// segment under the live frontier stays pinned, so the data is served correctly (no
/// corruption), but with the small fixed plan window the executor does not pin far ahead: cells
/// the cursor has not yet reached are evicted by the flood and re-fetched -- one GET per
/// re-fetched cold cell, not a single spanning connection. The aggressive flood is a worst
/// case; realistic fragmented load is cost-neutral (the integration A/B).
TEST_F(ReaderExecutorCacheChain, EvictionInChainRefetchesEvictedCells)
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

    ReaderExecutor::Options executor_options;
    executor_options.window_size = window;
    executor_options.min_bytes_for_seek = 0;
    executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
    auto executor = std::make_unique<ReaderExecutor>(source, objects, caches, executor_options);

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
        auto chain = executor->readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            result.append(node.data(), node.size);
        /// Eviction pressure before the next window.
        flood_fs(round++);
    }

    EXPECT_EQ(result, content) << "no corruption / no missing bytes under eviction pressure";
    /// Destroy the executor so it flushes its `stats` into the thread's ProfileEvents.
    executor.reset();
    /// The engine's piece walk keys on the FILL frontier (committed cells PLUS the bank), so a
    /// segment the tiny cache refuses (the plan's held writers make the resident segments
    /// non-releasable) is fetched ONCE, overflow-banked, and served from the bank - the old
    /// drain loop was bank-blind (cells-only frontier) and re-fetched the refused range every
    /// window (7 GETs: one initial + three per refused segment). Pieces now run strictly
    /// forward, so the held long connection streams the whole scan: ONE request.
    EXPECT_EQ(sourceRequestsSoFar(), 1u)
        << "a cache-refused segment is fetched once and served from the bank while it stays banked";
}


/// THE CACHE-CHAIN POLICY's degradation path: correctness with several populating tiers,
/// performance for one. A page-resident middle [16,48) splits the fs segment's gaps into
/// a head and a tail Remote. Nothing orders the tail behind the middle (no down-fill is
/// scheduled, no launch clamp exists): the ahead launch runs immediately, its write is
/// refused at the append-only segment frontier, the bytes are BANKED at collect, and the
/// serve delivers the head from the cell, the middle from the page hit, and the tail from
/// the bank - every byte correct, the fs segment simply stays partial (it heals as a plain
/// miss once the page tier evicts the middle).
TEST_F(ReaderExecutorCacheChain, ForeignMiddleBanksTailAndServesCorrectly)
{
    constexpr size_t segment_size = 64;
    constexpr size_t alignment = 16;
    constexpr size_t block_size = 16;
    constexpr size_t file_size = 64;
    constexpr size_t window = block_size;

    const String content = makePattern(file_size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto page_cache = makePageCache();
    auto fc = makeFileCache("t3_clamp", segment_size, /*max_size=*/1ull << 20, alignment);

    /// Warm ONLY the middle [16,48) into the page cache (a page-only pass).
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> warm_caches;
        warm_caches.push_back(makePageProvider(page_cache, "obj", block_size, file_size));
        ReaderExecutor::Options warm_opts;
        warm_opts.window_size = window;
        warm_opts.min_bytes_for_seek = 8;
        ReaderExecutor warmer(source, objects, warm_caches, warm_opts);
        warmer.seek(16);
        size_t warmed = 0;
        while (warmed < 32)
        {
            auto chain = warmer.readNextWindow();
            if (chain.empty())
                break;
            warmed += chain.range().size;
        }
        ASSERT_GE(warmed, 32u);
    }

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(makePageProvider(page_cache, "obj", block_size, file_size));
    caches.push_back(makeDiskProvider(fc));

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    ReaderExecutor::Options opts;
    opts.window_size = window;
    opts.min_bytes_for_seek = 8;   /// the 32-byte middle is NOT bridged: two Remote jobs
    opts.prefetch_pool = pool;
    opts.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
    ReaderExecutor executor(source, objects, caches, opts);

    String got;
    const auto read_window = [&]
    {
        auto chain = executor.readNextWindow();
        for (const auto & node : chain.getNodes())
            got.append(node.data(), node.size);
    };

    read_window();   /// [0,16): the pump fetches the head Remote; segment committed to 16
    EXPECT_TRUE(inspect(executor).hasInflightPrefetch())
        << "the tail Remote launches immediately - nothing orders it behind the page-held middle";
    read_window();   /// [16,32): page hit, served from the hit view (never written down)
    read_window();   /// [32,48): page hit
    read_window();   /// [48,64): the refused tail was banked at collect - served from the bank
    EXPECT_TRUE(executor.readNextWindow().empty());
    EXPECT_EQ(got, content);
}


/// A resumed partially-downloaded fs segment: its resident prefix is the segment's own
/// CONTENT (the tail write appends right after it), so read-ahead runs immediately and
/// the append continues the segment - a same-tier resident prefix never stalls the fill.
TEST_F(ReaderExecutorCacheChain, AheadRunsOverSameTierResidentPrefix)
{
    constexpr size_t segment_size = 64;
    constexpr size_t file_size = 64;
    constexpr size_t warm_prefix = 20;
    constexpr size_t window = 16;

    const String content = makePattern(file_size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto fc = makeFileCache("t3_resume", segment_size, /*max_size=*/1ull << 20);
    const auto & user = FileCache::getCommonOrigin();
    auto key = FileCacheKey::fromPath("obj");

    /// A live holder fills [0,20) and releases the downloader WITHOUT completing: the
    /// segment stays PARTIALLY_DOWNLOADED at its full [0,64) range for as long as the
    /// holder lives - the mid-life resume shape.
    auto partial_holder = fc->getOrSet(key, 0, segment_size, file_size, {}, 0, user);
    {
        auto & seg = partial_holder->front();
        ASSERT_EQ(seg.getOrSetDownloader(), FileSegment::getCallerId());
        std::string failure_reason;
        ASSERT_TRUE(seg.reserve(warm_prefix, 1000, failure_reason));
        seg.write(const_cast<char *>(content.data()), warm_prefix, 0);
        seg.completePartAndResetDownloader();
    }

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(makeDiskProvider(fc));

    auto pool = std::make_shared<PrefetchThreadPool>(2);
    ReaderExecutor::Options opts;
    opts.window_size = window;
    opts.min_bytes_for_seek = 8;
    opts.prefetch_pool = pool;
    opts.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
    ReaderExecutor executor(source, objects, caches, opts);

    String got;
    {
        auto chain = executor.readNextWindow();   /// [0,16): served from the resident prefix
        for (const auto & node : chain.getNodes())
            got.append(node.data(), node.size);
    }
    EXPECT_TRUE(inspect(executor).hasInflightPrefetch())
        << "the resident prefix is the segment's own content - the tail launch runs over it";

    while (true)
    {
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            got.append(node.data(), node.size);
    }
    EXPECT_EQ(got, content);
}


/// The other half of the pinning model the executor relies on: HIT views are PINNED FACTS.
/// the residency probe's (`lookAt`) readers hold the resident segments for the plan's life, so eviction
/// pressure between windows cannot take a classified hit out from under the plan - the hit
/// keeps serving from the cache with ZERO re-fetches. The control at the end proves the
/// pressure was real: once the plan dies, the same sweep removes the segment.
TEST_F(ReaderExecutorCacheChain, PlanHeldHitSurvivesEvictionPressure)
{
    constexpr size_t segment_size = 64;
    constexpr size_t file_size = 128;
    constexpr size_t window = 16;

    const String content = makePattern(file_size);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", content}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_size);

    auto fc = makeFileCache("t_view_pin", segment_size, /*max_size=*/1ull << 20);
    const auto & origin = FileCache::getCommonOrigin();

    /// Warm segment [0,64): after this executor dies its segment is DOWNLOADED and,
    /// with no holder, releasable.
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> warm_caches;
        warm_caches.push_back(makeDiskProvider(fc));
        ReaderExecutor::Options warm_opts;
        warm_opts.window_size = segment_size;
        warm_opts.min_bytes_for_seek = 0;
        ReaderExecutor warmer(source, objects, warm_caches, warm_opts);
        ASSERT_FALSE(warmer.readNextWindow().empty());
    }

    const size_t src_before = sourceRequestsSoFar();
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
        caches.push_back(makeDiskProvider(fc));
        ReaderExecutor::Options opts;
        opts.window_size = window;
        opts.min_bytes_for_seek = 0;
        opts.plan_look_ahead_max_window = file_size;   /// one plan: hit [0,64) + miss [64,128)
        opts.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        ReaderExecutor executor(source, objects, caches, opts);

        String got;
        {
            auto chain = executor.readNextWindow();   /// [0,16): the hit - the plan now holds the view
            for (const auto & node : chain.getNodes())
                got.append(node.data(), node.size);
        }
        ASSERT_EQ(got, content.substr(0, got.size()));

        /// Eviction pressure mid-plan: the sweep takes every releasable segment. The hit
        /// segment is HELD by the plan's view - it must survive.
        fc->removeAllReleasable(origin.user_id);

        while (true)
        {
            auto chain = executor.readNextWindow();
            if (chain.empty())
                break;
            for (const auto & node : chain.getNodes())
                got.append(node.data(), node.size);
        }
        EXPECT_EQ(got, content) << "the pinned hit must keep serving after the sweep";
    }
    /// The whole run re-fetched ONLY the cold tail [64,128): the swept-at hit range was
    /// never re-read from the source.
    EXPECT_EQ(sourceRequestsSoFar() - src_before, 1u)
        << "the plan-held hit must not be re-fetched after eviction pressure";

    /// Control - the pressure was real: with the plan gone the same sweep removes the
    /// segment, and a fresh probe sees it EMPTY.
    fc->removeAllReleasable(origin.user_id);
    auto probe = fc->getOrSet(FileCacheKey::fromPath("obj"), 0, segment_size, file_size, {}, 0, origin);
    EXPECT_EQ(probe->front().state(), FileSegment::State::EMPTY)
        << "without the plan's hold the sweep must take the segment";
}

#if USE_SSL

#include <IO/FileEncryptionCommon.h>
#include <IO/WriteBufferFromString.h>

namespace
{

String encryptedFileBytes(const String & key, FileEncryption::InitVector iv, const String & plaintext)
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
    FileEncryption::Encryptor enc(FileEncryption::Algorithm::AES_128_CTR, key, iv);
    enc.setOffset(0);
    String ciphertext(plaintext.size(), '\0');
    enc.decrypt(plaintext.data(), plaintext.size(), ciphertext.data());
    return file_bytes + ciphertext;
}

}

/// The encryption headers go through the cache chain like any other bytes: the cold
/// executor populates them (they are the FIRST bytes of the first cache cell, whose
/// append-only prefix would otherwise stay uncommitted forever - data writes start at
/// `data_start_offset` and cannot fill the hole below them), and a warm executor reads
/// them from the cache. So a fully-warm encrypted re-read adds ZERO source requests,
/// where it previously always paid at least the header fetch.
TEST_F(ReaderExecutorCacheChain, EncryptionHeaderGoesThroughTheCacheChain)
{
    constexpr size_t segment_size = 256;
    constexpr size_t block_size = 128;
    String key(16, 'k');
    FileEncryption::InitVector iv(UInt128{0xabcdef});
    const String plaintext = makePattern(1000);
    const String file_bytes = encryptedFileBytes(key, iv, plaintext);

    auto fc = makeFileCache("enc_header_fc", segment_size, /*max_size=*/1ull << 20);
    auto source = std::make_shared<MemorySourceReader>(
        std::unordered_map<String, String>{{"obj", file_bytes}});
    StoredObjects objects;
    objects.emplace_back("obj", "", file_bytes.size());

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(makeDiskProvider(fc));

    ReaderExecutor::Options opts;
    opts.window_size = block_size;
    opts.block_size = block_size;

    const auto key_finder = [&](UInt128, const String &) { return key; };

    const size_t src_before_cold = sourceRequestsSoFar();
    {
        ReaderExecutor cold(source, objects, caches, opts);
        cold.addDecryptionLayer("/t", key_finder);
        cold.initDecryption();
        EXPECT_EQ(drainAll(cold), plaintext) << "cold encrypted scan serves all plaintext";
    }
    EXPECT_GT(sourceRequestsSoFar() - src_before_cold, 0u) << "cold scan must hit the source";

    const size_t src_before_warm = sourceRequestsSoFar();
    {
        ReaderExecutor warm(source, objects, caches, opts);
        warm.addDecryptionLayer("/t", key_finder);
        warm.initDecryption();
        EXPECT_EQ(drainAll(warm), plaintext) << "warm encrypted scan serves all plaintext";
    }
    EXPECT_EQ(sourceRequestsSoFar() - src_before_warm, 0u)
        << "the warm re-read must serve the header AND the first cell from the cache";
}

#endif
