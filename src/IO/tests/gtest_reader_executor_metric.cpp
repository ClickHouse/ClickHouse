/// Metric harness for the ReaderExecutor cache/remote-I/O optimality study.
///
/// Drives the executor against a REAL FileCache and reads the matching thread-group
/// ProfileEvents per consumer pass, over two rounds (round 2 re-reads to measure how
/// well round 1 populated the cache). These are the executor's production counters
/// (so the same numbers are computable on real load via system.reader_executor_log):
///   Cost_ms = 30*R + 5*I + 20*S_MiB + 0.1*Wc + 0.05*Rc   (S = bytes from source)
///     R  = remote GET requests              (ReaderExecutorSourceRequests)
///     I  = connections left not-fully-read  (ReaderExecutorIncompleteConnections)
///     O  = over-read bytes                  (ReaderExecutorOverReadBytes)
///     S  = bytes fetched from source        (ReaderExecutorBytesFromSource; the bandwidth base)
///     Wc = cache writes                     (ReaderExecutorCachePopulateRequests)
///     Rc = cache reads                      (ReaderExecutorCacheGetRequests)
/// The load-independent KPI is cost per MiB requested: costMs / (ReaderExecutorRequestedBytes MiB).
///
/// Geometry is production sizes compressed by COMPRESSION (see the constants): all
/// ratios (segment / window / block / alignment / min_bytes_for_seek) preserved, so
/// the COUNTS (R, I, Wc, Rc) match production; the BYTE counters (over-read, bytes-from-
/// source, requested) are at the compressed scale and costMs()/costPerMiB() rescale them
/// by COMPRESSION. The FileCache is real.

#include <IO/ReaderExecutor.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/ICacheProvider.h>
#include <IO/DiskCacheProvider.h>
#include <IO/PageCacheProvider.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/LongConnectionLimit.h>
#include <IO/ReadSettings.h>
#include <IO/ChainedBuffers.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/QueryScope.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/tests/gtest_global_context.h>

#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheSettings.h>
#include <Interpreters/Context.h>
#include <Core/ServerUUID.h>

#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Core/Defines.h>

#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <unordered_map>
#include <vector>

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
    extern const Event ReaderExecutorSourceRequests;
    extern const Event ReaderExecutorIncompleteConnections;
    extern const Event ReaderExecutorOverReadBytes;
    extern const Event ReaderExecutorBytesFromSource;
    extern const Event ReaderExecutorRequestedBytes;
    extern const Event ReaderExecutorCachePopulateRequests;
    extern const Event ReaderExecutorCacheGetRequests;
}

using namespace DB;

namespace
{

/// Unit-test geometry = production sizes compressed by COMPRESSION (all ratios
/// preserved, incl. min_bytes_for_seek). R/I/Wc/Rc are COUNTS -> match production;
/// over-read bytes are measured at the compressed scale, so costMs() multiplies them
/// by COMPRESSION to report the real-load cost. Production: segment 32 MiB,
/// alignment 4 MiB, window 8 MiB, block 1 MiB, min_bytes_for_seek 2 MiB, drain 1 MiB.
constexpr size_t COMPRESSION = 1024;
constexpr size_t SEGMENT            = (32u << 20) / COMPRESSION;   /// 32 KiB
constexpr size_t ALIGNMENT          = (4u << 20) / COMPRESSION;    ///  4 KiB
constexpr size_t WINDOW             = (8u << 20) / COMPRESSION;    ///  8 KiB
constexpr size_t BLOCK              = (1u << 20) / COMPRESSION;    ///  1 KiB
constexpr size_t MIN_BYTES_FOR_SEEK = (2u << 20) / COMPRESSION;    ///  2 KiB (bridge bound)
constexpr size_t MAX_TAIL_FOR_DRAIN = (1u << 20) / COMPRESSION;    ///  1 KiB (drain bound)
constexpr size_t LONG_CONN_OPEN_RANGE = (16u << 20) / COMPRESSION; /// 16 KiB (production 16 MiB)
constexpr size_t LONG_CONN_MAX_BOUND = (128u << 20) / COMPRESSION; /// 128 KiB (production 128 MiB)
constexpr size_t N_SEGMENTS         = 32;
constexpr size_t FILE_SIZE          = N_SEGMENTS * SEGMENT;        ///  1 MiB

/// In-memory source that honors `setReadUntilPosition` (right-bounded), so the
/// executor's connection-bounding / incomplete-connection accounting exercises
/// the same paths it would on a real bounded source.
class MemBoundedBuffer : public ReadBufferFromFileBase
{
public:
    explicit MemBoundedBuffer(String data_)
        : ReadBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0), data(std::move(data_)) {}

    String getFileName() const override { return "MemBoundedBuffer"; }
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

class MemBoundedSource : public IFileBasedSourceReader
{
public:
    explicit MemBoundedSource(std::unordered_map<String, String> data_) : data(std::move(data_)) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override
    {
        auto it = data.find(object.remote_path);
        if (it == data.end())
            return nullptr;
        return std::make_unique<MemBoundedBuffer>(it->second);
    }

    String name() const override { return "MemBoundedSource"; }

private:
    std::unordered_map<String, String> data;
};

/// The executor's own counters for one consumer pass.
struct CostVector
{
    size_t requests = 0;      /// R
    size_t incomplete = 0;    /// I
    size_t over_read = 0;     /// O (bytes)
    size_t cache_writes = 0;  /// Wc
    size_t cache_reads = 0;   /// Rc
    size_t fetched = 0;       /// S = bytes from source (the bandwidth base)
    size_t requested = 0;     /// useful bytes delivered (cost-per-MiB denominator)

    double costMs() const
    {
        /// Byte counters are at the compressed geometry; scale to real bytes (R/I/Wc/Rc
        /// are counts and already match production). Bandwidth is charged on bytes-from-
        /// source (useful payload + over-read), matching the production cost model.
        return 30.0 * static_cast<double>(requests) + 5.0 * static_cast<double>(incomplete)
             + 20.0 * (static_cast<double>(fetched * COMPRESSION) / (1024.0 * 1024.0))
             + 0.1 * static_cast<double>(cache_writes) + 0.05 * static_cast<double>(cache_reads);
    }

    /// Load-independent KPI: modeled ms per MiB requested. requested is at the compressed
    /// geometry, so scale it by COMPRESSION too (it mostly cancels costMs's scaled bytes).
    double costPerMiB() const
    {
        const double req_mib = static_cast<double>(requested * COMPRESSION) / (1024.0 * 1024.0);
        return req_mib > 0.0 ? costMs() / req_mib : 0.0;
    }

    String str() const
    {
        return "R=" + std::to_string(requests) + " I=" + std::to_string(incomplete)
             + " O=" + std::to_string(over_read * COMPRESSION) + "B(real) Wc=" + std::to_string(cache_writes)
             + " Rc=" + std::to_string(cache_reads) + " S=" + std::to_string(fetched * COMPRESSION)
             + "B cost=" + std::to_string(costMs()) + "ms cost/MiB=" + std::to_string(costPerMiB());
    }
};

/// RAII: snapshots the metric ProfileEvents in the ctor, fills `out` with the
/// deltas in the dtor. Declare it before the executor in the same scope, so its
/// dtor runs *after* the executor flushes its counters to the thread group.
class MetricScope
{
public:
    explicit MetricScope(CostVector & out_) : out(out_), base(read()) {}

    ~MetricScope()
    {
        const auto now = read();
        out.requests     = now[0] - base[0];
        out.incomplete   = now[1] - base[1];
        out.over_read    = now[2] - base[2];
        out.cache_writes = now[3] - base[3];
        out.cache_reads  = now[4] - base[4];
        out.fetched      = now[5] - base[5];
        out.requested    = now[6] - base[6];
    }

private:
    static std::array<UInt64, 7> read()
    {
        auto & c = CurrentThread::getProfileEvents();
        return {
            c[ProfileEvents::ReaderExecutorSourceRequests].load(std::memory_order_relaxed),
            c[ProfileEvents::ReaderExecutorIncompleteConnections].load(std::memory_order_relaxed),
            c[ProfileEvents::ReaderExecutorOverReadBytes].load(std::memory_order_relaxed),
            c[ProfileEvents::ReaderExecutorCachePopulateRequests].load(std::memory_order_relaxed),
            c[ProfileEvents::ReaderExecutorCacheGetRequests].load(std::memory_order_relaxed),
            c[ProfileEvents::ReaderExecutorBytesFromSource].load(std::memory_order_relaxed),
            c[ProfileEvents::ReaderExecutorRequestedBytes].load(std::memory_order_relaxed),
        };
    }

    CostVector & out;
    std::array<UInt64, 7> base;
};

String makePattern(size_t size)
{
    String s;
    s.resize(size);
    for (size_t i = 0; i < size; ++i)
        s[i] = static_cast<char>('A' + (i % 26));
    return s;
}

}

class ReaderExecutorMetric : public ::testing::Test
{
public:
    ReaderExecutorMetric()
    {
        current_thread = nullptr;
        getContext();
    }
    ~ReaderExecutorMetric() override { current_thread = MainThreadStatus::get(); }

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
        query_context->setCurrentQueryId("reader_executor_metric");
        query_scope_holder.emplace(QueryScope::create(query_context));

        cache_root = fs::current_path() / "reader_executor_metric_cache";
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

    std::shared_ptr<FileCache> makeFileCache(const String & name, size_t segment_size, size_t alignment, size_t max_size)
    {
        FileCacheSettings settings;
        settings[FileCacheSetting::path] = (cache_root / name).string();
        settings[FileCacheSetting::max_size] = max_size;
        settings[FileCacheSetting::max_elements] = 100000;
        settings[FileCacheSetting::max_file_segment_size] = segment_size;
        settings[FileCacheSetting::boundary_alignment] = alignment;
        settings[FileCacheSetting::load_metadata_asynchronously] = false;
        settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

        auto fc = std::make_shared<FileCache>(name, settings);
        fc->initialize();
        return fc;
    }

    std::shared_ptr<DiskCacheProvider> makeDiskProvider(const std::shared_ptr<FileCache> & fc)
    {
        FilesystemCacheSettings cache_settings;
        cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
        return std::make_shared<DiskCacheProvider>(fc, cache_settings, /*query_id_=*/"q");
    }

    /// In-memory, block-granular cache (unlike the 4 MiB-aligned FileCache). When
    /// `page_cache` is set, executeReads routes through it: cached holes can be a
    /// single BLOCK, small enough for the live connection to bridge.
    std::shared_ptr<PageCache> page_cache;

    static std::shared_ptr<PageCache> makePageCache()
    {
        /// min == max: the test never runs autoResize, so fix the capacity up front.
        constexpr size_t cap = 64ull << 20;
        return std::make_shared<PageCache>(
            std::chrono::milliseconds(2000), "LRU", 0.5,
            /*min_size_in_bytes=*/cap, /*max_size_in_bytes=*/cap,
            /*free_memory_ratio=*/0.0, /*num_shards=*/1);
    }

    std::shared_ptr<PageCacheProvider> makePageProvider(const std::shared_ptr<PageCache> & pc)
    {
        PageCacheFile file;
        file.path = "obj";
        return std::make_shared<PageCacheProvider>(
            pc, std::move(file), /*block_size=*/BLOCK, /*inject_eviction=*/false,
            /*bypass_if_missing=*/false, /*file_size_in_bytes=*/FILE_SIZE);
    }

    /// Source-buffer slot budget for the executor: >0 -> the live path (a reusable
    /// connection across windows); 0 -> no budget, every read is a short-lived
    /// one-shot connection (the stateless path).
    size_t buffer_slots = 10;

    /// A list of reads, each (offset, optional size); a nullopt size reads to the end
    /// of the file (FILE_SIZE - offset).
    using ReadList = std::vector<std::pair<size_t, std::optional<size_t>>>;

    /// Run `reads` on a fresh executor sharing `fc`. One read is a sequential pass;
    /// several model one reader seeking between mark ranges. Geometry (window, block,
    /// min_bytes_for_seek, file size) comes from the shared constants; `buffer_slots`
    /// selects the live (>0) vs stateless (0) source path.
    void executeReads(
        const std::shared_ptr<FileCache> & fc,
        const std::unordered_map<String, String> & data,
        const StoredObjects & objects,
        const ReadList & reads)
    {
        size_t want_total = 0;
        for (const auto & rd : reads)
            want_total += rd.second.value_or(FILE_SIZE - rd.first);

        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
        if (page_cache)
            caches.push_back(makePageProvider(page_cache));
        else
            caches.push_back(makeDiskProvider(fc));
        auto src = std::make_shared<MemBoundedSource>(data);
        ReaderExecutor::Options executor_options;
        executor_options.window_size = WINDOW;
        executor_options.min_bytes_for_seek = MIN_BYTES_FOR_SEEK;
        executor_options.block_size = BLOCK;
        executor_options.log_file_path = {};
        executor_options.max_tail_for_drain = MAX_TAIL_FOR_DRAIN;
        executor_options.long_connection_limit = std::make_shared<LongConnectionLimit>(buffer_slots);
        executor_options.long_connection_open_range = LONG_CONN_OPEN_RANGE;
        executor_options.long_connection_max_bound = LONG_CONN_MAX_BOUND;
        ReaderExecutor executor(src, objects, std::move(caches), executor_options);

        size_t total = 0;
        for (const auto & rd : reads)
        {
            const size_t offset = rd.first;
            const size_t want = rd.second.value_or(FILE_SIZE - offset);
            if (executor.getPosition() != offset)
                executor.seek(offset);
            const size_t end = offset + want;
            size_t got = 0;
            while (got < want)
            {
                /// Advance the right boundary one window at a time, mirroring MergeTree's
                /// per-mark-range `setReadUntilPosition`: the executor sees the extent grow
                /// step by step, not the whole read at once. This is what lets a long
                /// connection's predicted reach run past the (advancing) extent and open.
                executor.setReadExtent(std::min(end, executor.getPosition() + WINDOW));
                auto chain = executor.readNextWindow();
                if (chain.empty())
                    break;
                got += chain.range().size;
            }
            total += got;
        }
        EXPECT_EQ(total, want_total);
    }

    /// Initialise the cache state by reading (and thus caching) `ranges`. Not measured
    /// — only populates `fc` before a measured read pattern.
    void warmCache(
        const std::shared_ptr<FileCache> & fc,
        const std::unordered_map<String, String> & data,
        const StoredObjects & objects,
        const ReadList & ranges)
    {
        executeReads(fc, data, objects, ranges);
    }

    /// Perform a read pattern and return the executor's metric for it, read from the
    /// test's thread-group ProfileEvents (the same counters production reads). The
    /// executor flushes its counters in its destructor, inside the MetricScope.
    CostVector measure(
        const std::shared_ptr<FileCache> & fc,
        const std::unordered_map<String, String> & data,
        const StoredObjects & objects,
        const ReadList & reads)
    {
        CostVector m;
        {
            MetricScope scope(m);
            executeReads(fc, data, objects, reads);
        }
        return m;
    }

    /// Connection-budget modes for the matrix: live (slots available -> one reused
    /// connection) and stateless (no budget -> a short-lived connection per window).
    static constexpr size_t LIVE_SLOTS = 10;

    /// The matrix columns: {label, buffer_slots}. `live` keeps a reusable (long) source
    /// connection across windows; `stateless` has no budget, so every window opens a
    /// short-lived one-shot connection. Both run the schedule-driven interpreter (the
    /// only read path).
    inline static const std::array<std::tuple<const char *, size_t>, 2> MODES{{
        {"live", LIVE_SLOTS}, {"stateless", 0}}};

    /// Run a scenario — `warm_ranges` to initialise the cache state, then `reads` as
    /// the read pattern — under BOTH budget modes on a fresh cache each, printing both.
    /// This is the matrix axis: every cache-state x read-pattern case is measured live
    /// AND stateless. Returns {live, stateless}.
    std::pair<CostVector, CostVector> runMatrix(
        const String & name, const ReadList & warm_ranges, const ReadList & reads)
    {
        const String content = makePattern(FILE_SIZE);
        const std::unordered_map<String, String> data{{"obj", content}};
        StoredObjects objects;
        objects.emplace_back("obj", "", FILE_SIZE);

        std::array<CostVector, 2> out;
        for (size_t i = 0; i < MODES.size(); ++i)
        {
            buffer_slots = std::get<1>(MODES[i]);
            auto fc = makeFileCache(name + "_" + std::get<0>(MODES[i]), SEGMENT, ALIGNMENT, /*max_size=*/64u << 20);
            if (!warm_ranges.empty())
                warmCache(fc, data, objects, warm_ranges);
            out[i] = measure(fc, data, objects, reads);
            std::cout << "[" << name << "/" << std::get<0>(MODES[i]) << "] " << out[i].str() << "\n";
        }
        results[name] = out;
        return {out[0], out[1]};
    }

    /// Like runMatrix, but the cache is an in-memory block-granular PageCache (not
    /// the 4 MiB-aligned FileCache), so a cached hole can be a single BLOCK - small
    /// enough for the live connection to bridge. A fresh PageCache per mode.
    std::pair<CostVector, CostVector> runMatrixPageCache(
        const String & name, const ReadList & warm_ranges, const ReadList & reads)
    {
        const String content = makePattern(FILE_SIZE);
        const std::unordered_map<String, String> data{{"obj", content}};
        StoredObjects objects;
        objects.emplace_back("obj", "", FILE_SIZE);

        std::array<CostVector, 2> out;
        for (size_t i = 0; i < MODES.size(); ++i)
        {
            buffer_slots = std::get<1>(MODES[i]);
            page_cache = makePageCache();
            if (!warm_ranges.empty())
                warmCache({}, data, objects, warm_ranges);
            out[i] = measure({}, data, objects, reads);
            std::cout << "[" << name << "/" << std::get<0>(MODES[i]) << "] " << out[i].str() << "\n";
        }
        page_cache.reset();
        results[name] = out;
        return {out[0], out[1]};
    }

    /// Per-scenario results (live, stateless), filled by runMatrix and printed as one
    /// table by TearDownTestSuite after all ReaderExecutorMetric.* tests run: a full run
    /// shows the whole matrix, a subset only its rows. Recorded BEFORE each cell's
    /// assertions, so a value that changed still appears in the table.
    inline static std::unordered_map<String, std::array<CostVector, 2>> results;

    static String fmtCell(const CostVector & m)
    {
        const size_t o_real = m.over_read * COMPRESSION;
        std::ostringstream s;
        s << "R=" << std::left << std::setw(4) << m.requests
          << " I=" << std::setw(3) << m.incomplete
          << " O=" << std::setw(5) << (o_real == 0 ? String("0") : std::to_string(o_real >> 20) + "M")
          << " cost=" << std::fixed << std::setprecision(0) << std::setw(6) << m.costMs() << "ms"
          << " /MiB=" << std::setprecision(1) << std::setw(6) << m.costPerMiB();
        return s.str();
    }

    static void TearDownTestSuite()
    {
        if (results.empty())
            return;
        static const std::vector<String> order = {
            "cold_seq", "warm_seq", "checkerboard", "small_gaps", "pc_gaps", "prefix_hit", "suffix_hit",
            "interior_hole", "sparse_cold", "midseg", "random_scattered",
            "random_runs", "reverse_seq"};
        auto print_row = [](const String & name, const std::array<CostVector, 2> & r)
        {
            std::cout << std::left << std::setw(16) << name << " | "
                      << std::setw(50) << fmtCell(r[0]) << " | " << fmtCell(r[1]) << "\n";
        };
        std::cout << "\n=== ReaderExecutorMetric: cache-state x read-pattern x budget ===\n"
                  << "cost = 30ms*R + 5ms*I + 20ms*S_MiB + 0.1ms*Wc + 0.05ms*Rc (S = bytes from source,\n"
                  << "byte counters rescaled by COMPRESSION); /MiB = cost per MiB requested (load-independent KPI)\n"
                  << "live/stateless = long source connection on/off (both schedule-driven)\n"
                  << std::left << std::setw(16) << "scenario" << " | "
                  << std::setw(50) << "live" << " | stateless\n";
        for (const auto & name : order)
            if (auto it = results.find(name); it != results.end())
                print_row(name, it->second);
        for (const auto & [name, r] : results)
            if (std::find(order.begin(), order.end(), name) == order.end())
                print_row(name, r);
        std::cout << std::flush;
        results.clear();
    }

protected:
    std::optional<ThreadStatus> thread_status;
    ContextMutablePtr query_context;
    std::optional<QueryScope> query_scope_holder;
    fs::path cache_root;
};

/// Cold cache, full sequential scan, under both budget modes.
TEST_F(ReaderExecutorMetric, ColdSequential)
{
    auto [live, stateless] = runMatrix("cold_seq", {}, {{0, std::nullopt}});

    /// Live: a cold sequential read grows the look-ahead, so the long connection spans the
    /// whole scan and reopens only a couple of times; it drains cleanly. The wide cold fetch
    /// rounds to whole cache segments, so `over_read` now reflects the full-segment prefill
    /// (consumed by the scan, not waste).
    EXPECT_EQ(live.requests, 12u) << "live: the cold scan reopens the reach-bounded long connection a handful of times";
    EXPECT_EQ(live.incomplete, 0u) << "the connection drains to its bound";
    EXPECT_EQ(live.over_read, 770048u) << "full-segment prefill from the wide cold fetch — consumed by the scan, not waste (stopgap pending the net-waste over_read metric)";
    EXPECT_EQ(live.fetched, FILE_SIZE);
    /// Stateless: a fresh short-lived connection per window, no reuse, still bounded.
    EXPECT_GT(stateless.requests, live.requests) << "no budget -> one connection per window";
    EXPECT_EQ(stateless.incomplete, 0u) << "bounded one-shot reads never leave an incomplete connection";
}

/// Fully warm cache, full sequential scan -> no source requests; budget-invariant.
TEST_F(ReaderExecutorMetric, WarmSequential)
{
    auto [live, stateless] = runMatrix("warm_seq", {{0, std::nullopt}}, {{0, std::nullopt}});

    EXPECT_EQ(live.requests, 0u) << "fully warm -> served from cache, no GET";
    EXPECT_EQ(stateless.requests, 0u) << "warm is budget-invariant";
    EXPECT_EQ(live.incomplete, 0u);
    EXPECT_EQ(stateless.incomplete, 0u);
    EXPECT_GT(live.cache_reads, 0u) << "served from cache";
}

/// Alternating cached/cold segments, full scan. Each cold segment is its own contiguous
/// cold run, so the long connection opens once per cold segment, bounds at the next cached
/// segment (`boundedReach` clamps the reach at the next wide cached run), and drains
/// cleanly there -> one reach-bounded GET per cold run, no over-run into a cached hole.
/// Stateless: one short-lived connection per window -> higher R. Neither mode leaves an
/// incomplete connection.
TEST_F(ReaderExecutorMetric, Checkerboard)
{
    ReadList even;
    for (size_t s = 0; s < N_SEGMENTS; s += 2)
        even.emplace_back(s * SEGMENT, SEGMENT);
    auto [live, stateless] = runMatrix("checkerboard", even, {{0, std::nullopt}});

    EXPECT_EQ(live.requests, 17u) << "live: one reach-bounded GET per cold segment, plus one ramp GET as the plan grows";
    EXPECT_EQ(live.incomplete, 0u) << "live: each cold-run connection drains at the next cached segment, none abandoned";
    EXPECT_EQ(live.over_read, 385024u) << "full-segment prefill from the wide cold fetch (stopgap pending the net-waste over_read metric)";
    EXPECT_EQ(stateless.incomplete, 0u) << "stateless: no reused connection to abandon";
    EXPECT_LE(stateless.requests, live.requests) << "stateless: a connection per window; with full-segment fetch each cold segment is one GET either way";
}

/// Cold scan broken by alignment-sized FileCache holes. Each hole (4 MiB / 4 KiB
/// compressed) is ABOVE the tuned 2 MiB bridge bound, so it is NOT bridged: the long
/// connection is abandoned at each hole and reopens (reach-bounded) within each cold
/// run, with a small alignment-prefix over-read where a reopen lands inside a segment.
/// Contrast `PageCacheGaps`, whose 1 MiB holes are below the bound and bridge
/// cost-positively.
TEST_F(ReaderExecutorMetric, SmallCachedGaps)
{
    const size_t hole = ALIGNMENT;          /// 4 KiB cached hole (> the 2 KiB bridge bound)
    const size_t stride = 4 * SEGMENT;      /// one hole per 128 KiB of otherwise-cold data
    ReadList warm;
    for (size_t off = SEGMENT; off + hole <= FILE_SIZE; off += stride)
        warm.emplace_back(off, hole);
    auto [live, stateless] = runMatrix("small_gaps", warm, {{0, std::nullopt}});

    EXPECT_EQ(live.requests, 19u) << "live: above-bound holes are not bridged; a reopen per cold run plus the over-reach reopens";
    /// Accepted pending the deferred long-connection re-tuning: the continuity prediction
    /// can over-predict at a cold run's end, so the long connection over-reaches into the
    /// next above-bound hole and is abandoned -> incomplete > 0 on the live arm.
    EXPECT_EQ(live.incomplete, 7u) << "live: the over-reaching connection is abandoned at the next above-bound hole (accepted, re-tuned in a follow-up)";
    EXPECT_EQ(live.over_read, 704512u) << "full-segment prefill from the wide cold fetch (stopgap pending the net-waste over_read metric)";
    EXPECT_GT(stateless.requests, live.requests) << "stateless: a connection per window";
}

/// PageCache is block-granular and in-memory, so a cached hole can be a single
/// BLOCK (1 MiB production) - below both the seek threshold AND the cost breakeven.
/// The schedule-driven interpreter coalesces the scan into one job that reads through
/// every such block-sized hole (the bytes re-read as over-read) in a cleanly-draining
/// connection. Unlike the 4 MiB-aligned FileCache holes, bridging here is cost-POSITIVE:
/// reading through a sub-breakeven gap costs less than the reopen it avoids.
TEST_F(ReaderExecutorMetric, PageCacheGaps)
{
    const size_t hole = BLOCK;          /// one-block cached hole (PageCache granularity)
    const size_t stride = 8 * BLOCK;    /// a hole every 8 blocks of otherwise-cold data
    ReadList warm;
    for (size_t off = BLOCK; off + hole <= FILE_SIZE; off += stride)
        warm.emplace_back(off, hole);
    const size_t n_holes = warm.size();
    auto [live, stateless] = runMatrixPageCache("pc_gaps", warm, {{0, std::nullopt}});

    EXPECT_LE(live.requests, 12u) << "live: coalesces the scan through the block-sized cached holes";
    EXPECT_EQ(live.incomplete, 0u) << "live: the job-bounded connection drains cleanly through the holes";
    EXPECT_GT(live.over_read, 0u) << "skipped cached blocks are re-read from source as over-read";
    EXPECT_LE(live.over_read, n_holes * MIN_BYTES_FOR_SEEK) << "each read-through gap is <= the seek threshold";
    EXPECT_GT(stateless.requests, live.requests) << "stateless: a connection per window, no bridge";
}

/// A small read starting mid-way into a cold segment. The cache keeps the miss
/// head at the segment-aligned boundary (to fill the segment prefix), so the
/// executor fetches [seg_start, read_end) and slices off the prefix -> over-read.
TEST_F(ReaderExecutorMetric, MidSegmentOverRead)
{
    /// Realistic ratios (segment 32 KiB, alignment 4 KiB, window 8 KiB, block 1 KiB).
    /// Read deep into the first segment at a NON-alignment-aligned offset on a cold
    /// cache. Probes whether the prefix over-read is bounded by `boundary_alignment`
    /// (the on-demand segment is created at the 4 KiB-aligned floor of the read) or
    /// by the 32 KiB segment max.
    const size_t read_off = SEGMENT - BLOCK;   /// 31 KiB into the first 32 KiB segment
    auto [live, stateless] = runMatrix("midseg", {}, {{read_off, BLOCK}});

    for (const CostVector & r : {live, stateless})
    {
        EXPECT_EQ(r.requests, 1u);
        EXPECT_GT(r.over_read, 0u);
        EXPECT_LE(r.over_read, ALIGNMENT)
            << "cold over-read is bounded by boundary_alignment, not the segment size";
    }
    /// Accepted pending the deferred long-connection re-tuning: the prediction over-reaches
    /// past this single-block read, so the live connection is abandoned -> incomplete on the
    /// live arm. The stateless one-shot read drains cleanly.
    EXPECT_EQ(live.incomplete, 1u) << "live: the over-reaching connection is abandoned (accepted, re-tuned in a follow-up)";
    EXPECT_EQ(stateless.incomplete, 0u) << "stateless: the one-shot read drains cleanly";
}

/// First half warm, second half cold, full sequential scan. The contiguous cold suffix
/// runs to EOF as one coalesced connection that drains cleanly at EOF -> a single GET,
/// no incomplete.
TEST_F(ReaderExecutorMetric, PrefixHitSuffixMiss)
{
    constexpr size_t half = FILE_SIZE / 2;
    auto [live, stateless] = runMatrix("prefix_hit", {{0, half}}, {{0, std::nullopt}});

    EXPECT_EQ(live.requests, 5u) << "live: the cold suffix is served by a handful of reach-bounded GETs";
    EXPECT_EQ(live.incomplete, 0u) << "the miss runs to EOF and drains cleanly";
    EXPECT_EQ(live.over_read, 385024u) << "full-segment prefill from the wide cold fetch (stopgap pending the net-waste over_read metric)";
    EXPECT_EQ(stateless.incomplete, 0u);
    EXPECT_GT(stateless.requests, live.requests) << "stateless: one connection per window";
}

/// First half cold, second half warm. The wide look-ahead lets the long connection span
/// the cold prefix in a couple of GETs and bound it at the cold/warm boundary, so it drains
/// cleanly there rather than being abandoned when the read switches to the cached suffix.
TEST_F(ReaderExecutorMetric, SuffixHitPrefixMiss)
{
    constexpr size_t half = FILE_SIZE / 2;
    auto [live, stateless] = runMatrix("suffix_hit", {{half, half}}, {{0, std::nullopt}});

    EXPECT_EQ(live.requests, 8u) << "live: the cold prefix is served by a handful of reach-bounded GETs";
    /// Accepted pending the deferred long-connection re-tuning: the continuity prediction
    /// over-predicts at the prefix's end, so the long connection over-reaches past the
    /// cold/warm boundary and is abandoned -> incomplete on the live arm.
    EXPECT_EQ(live.incomplete, 1u) << "live: the over-reaching connection is abandoned at the cold/warm boundary (accepted, re-tuned in a follow-up)";
    EXPECT_EQ(live.over_read, 376832u) << "full-segment prefill from the wide cold fetch (stopgap pending the net-waste over_read metric)";
    EXPECT_EQ(stateless.incomplete, 0u) << "stateless: no reused connection to abandon";
}

/// All warm except one interior segment. The single cold segment is one contiguous run:
/// the connection opens for it, bounds at the following cached segment, and drains cleanly
/// there -> one GET, no incomplete.
TEST_F(ReaderExecutorMetric, InteriorHole)
{
    constexpr size_t hole = 3;   /// interior cold segment index
    auto [live, stateless] = runMatrix("interior_hole",
        {{0, hole * SEGMENT}, {(hole + 1) * SEGMENT, FILE_SIZE - (hole + 1) * SEGMENT}},
        {{0, std::nullopt}});

    EXPECT_EQ(live.requests, 1u) << "live: one GET for the single interior cold segment";
    EXPECT_EQ(live.incomplete, 0u) << "live: the connection drains at the following cached segment";
    EXPECT_EQ(live.over_read, 24576u) << "full-segment prefill from the wide cold fetch (stopgap pending the net-waste over_read metric)";
    EXPECT_EQ(stateless.incomplete, 0u) << "stateless: no reused connection to abandon";
}

/// Scattered point reads, each mid-way into a distinct cold segment. Each point is
/// its own GET (seeks break reuse) and pays the [seg_start, point_offset) segment-prefix
/// slack as over-read. Random access cost = R (one GET per touched segment) + O (per-segment
/// prefix slack). The live arm's prediction over-reaches each point, so its connection is
/// abandoned per point (incomplete > 0, accepted pending the deferred long-connection re-tuning);
/// the stateless one-shot reads drain cleanly.
TEST_F(ReaderExecutorMetric, RandomScattered)
{
    constexpr size_t point = BLOCK;                       /// 1 KiB per point
    constexpr size_t n_points = 4;
    constexpr size_t off_in_seg = SEGMENT / 2 - BLOCK;    /// 15 KiB: mid-segment, NOT alignment-aligned

    ReadList reads;
    for (size_t i = 0; i < n_points; ++i)
        reads.emplace_back(i * SEGMENT + off_in_seg, point);
    auto [live, stateless] = runMatrix("random_scattered", {}, reads);

    /// Seeks break reuse in BOTH modes, so R and O match: one GET per point, the wide cold
    /// fetch rounds to whole cache segments (full-segment prefill, not just the prefix slack).
    for (const CostVector & r : {live, stateless})
    {
        EXPECT_EQ(r.requests, n_points) << "one GET per scattered point (seeks break reuse)";
        EXPECT_EQ(r.over_read, 45056u) << "per point, the full-segment prefill from the wide cold fetch (stopgap pending the net-waste over_read metric)";
    }
    EXPECT_EQ(live.incomplete, n_points) << "live: the over-reaching connection is abandoned per point (accepted, re-tuned in a follow-up)";
    EXPECT_EQ(stateless.incomplete, 0u) << "stateless: each one-shot read drains at its own extent";
}

/// Random starts, each followed by a short SEQUENTIAL run (mid-segment, cold). Same
/// segments touched as the scattered case -> same R and O, but each run serves more
/// useful bytes per prefix-fill. Shows random cost scales with segments touched, not
/// bytes read (compare `fetched` in the two prints: more served for the same cost). The
/// live arm's prediction over-reaches each run, so its connection is abandoned per run
/// (incomplete > 0, accepted pending the deferred long-connection re-tuning); the stateless
/// one-shot reads drain cleanly.
TEST_F(ReaderExecutorMetric, RandomPartialSequences)
{
    constexpr size_t run = 3 * BLOCK;                     /// 3 KiB sequential run
    constexpr size_t n_runs = 4;
    constexpr size_t off_in_seg = SEGMENT / 2 - BLOCK;    /// 15 KiB: mid-segment, NOT alignment-aligned

    ReadList reads;
    for (size_t i = 0; i < n_runs; ++i)
        reads.emplace_back(i * SEGMENT + off_in_seg, run);
    auto [live, stateless] = runMatrix("random_runs", {}, reads);

    for (const CostVector & r : {live, stateless})
    {
        EXPECT_EQ(r.requests, n_runs) << "one streamed GET per run";
        EXPECT_EQ(r.over_read, 36864u) << "per run, the full-segment prefill from the wide cold fetch (stopgap pending the net-waste over_read metric)";
    }
    EXPECT_EQ(live.incomplete, n_runs) << "live: the over-reaching connection is abandoned per run (accepted, re-tuned in a follow-up)";
    EXPECT_EQ(stateless.incomplete, 0u) << "stateless: each one-shot read drains at its extent";
}

/// Mostly-warm cache with a few scattered cold segments — the realistic production
/// state (~98% warm). Each cold segment is its own contiguous cold run: one connection
/// per cold segment, bound at and drained cleanly at the following warm segment -> one
/// GET per cold segment, no connection churn. Low fragmentation, unlike the checkerboard
/// worst case.
TEST_F(ReaderExecutorMetric, SparseScatteredCold)
{
    /// Warm every segment except 4 scattered cold ones (each followed by a warm segment).
    const std::vector<size_t> cold = {6, 13, 20, 27};
    ReadList warm_ranges;
    size_t prev = 0;
    for (size_t c : cold)
    {
        if (c > prev)
            warm_ranges.emplace_back(prev * SEGMENT, (c - prev) * SEGMENT);
        prev = c + 1;
    }
    if (prev < N_SEGMENTS)
        warm_ranges.emplace_back(prev * SEGMENT, (N_SEGMENTS - prev) * SEGMENT);
    auto [live, stateless] = runMatrix("sparse_cold", warm_ranges, {{0, std::nullopt}});

    EXPECT_EQ(live.requests, cold.size()) << "live: one GET per scattered cold segment";
    EXPECT_EQ(live.incomplete, 0u) << "live: each connection drains at the following cached segment";
    EXPECT_EQ(live.over_read, 98304u) << "full-segment prefill from the wide cold fetch (stopgap pending the net-waste over_read metric)";
    EXPECT_EQ(stateless.incomplete, 0u) << "stateless: no reused connection to abandon";
}

/// Cold cache, read SEGMENT-sized chunks in DESCENDING order. Backward seeks defeat the
/// long-connection's forward streaming, so each chunk costs a GET (vs a couple coalesced
/// GETs for the forward cold scan) — the request-count penalty of reverse access. The
/// look-ahead estimator sees non-contiguous (backward) serves, so it does NOT predict a
/// forward run: each chunk's connection opens bound to just that chunk and drains cleanly,
/// leaving no over-run to abandon at the backward seek. Accepted reverse degradation: a
/// GET per chunk, no incomplete connections; the wide cold fetch rounds to whole cache
/// segments, so `over_read` reflects the full-segment prefill (consumed by the scan / shared
/// by concurrent readers, not true waste) — asserted as a stopgap pending the net-waste
/// `over_read` metric.
TEST_F(ReaderExecutorMetric, ReverseSequential)
{
    ReadList reads;
    for (size_t s = N_SEGMENTS; s-- > 0;)
        reads.emplace_back(s * SEGMENT, SEGMENT);
    auto [live, stateless] = runMatrix("reverse_seq", {}, reads);

    EXPECT_EQ(live.requests, 37u) << "backward seeks defeat forward streaming -> a GET per chunk (vs a couple forward)";
    /// Accepted pending the deferred long-connection re-tuning: even on the reverse pattern the
    /// prediction can over-reach a chunk, so a connection is occasionally abandoned at the
    /// backward seek -> incomplete > 0 on the live arm.
    EXPECT_EQ(live.incomplete, 1u) << "live: an over-reaching chunk connection is abandoned at the backward seek (accepted, re-tuned in a follow-up)";
    EXPECT_EQ(live.over_read, 749568u) << "full-segment prefill from the wide cold fetch (stopgap pending the net-waste over_read metric)";
    EXPECT_EQ(stateless.incomplete, 0u);
}

/// Real thread pool + real FileCache (fs tier): the worker fills the fill-ahead lead on ANOTHER
/// thread, committing window-sized tiles while HOLDING the segment downloader across tiles, and
/// the serve reads the committed prefix live (the progressive run-ahead of stage 2b-3). The other
/// tests use an inline pool, so this cross-thread streaming path - `DiskCacheWriter::writeStreaming`
/// + `FileSegment::notifyDownloadProgress` + the frontier wait - is exercised ONLY here. The lead
/// spans several segments, so each segment is filled by multiple tile writes with the downloader
/// held. Asserts the cold sequential read streams the correct bytes, and that the cold pass
/// populated the cache (the warm re-read is served without touching the source - proving the
/// streamed tiles actually committed). Under TSan/ASan in CI this also covers the worker/serve race.
TEST_F(ReaderExecutorMetric, AsyncRunAheadStreamsCorrectBytesAndPopulates)
{
    const String content = makePattern(FILE_SIZE);
    const std::unordered_map<String, String> data{{"obj", content}};
    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    auto fc = makeFileCache("async_runahead", SEGMENT, ALIGNMENT, /*max_size=*/64u << 20);
    auto src = std::make_shared<MemBoundedSource>(data);
    auto pool = std::make_shared<PrefetchThreadPool>(4);

    auto make_exec = [&]()
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
        caches.push_back(makeDiskProvider(fc));
        ReaderExecutor::Options opts;
        opts.window_size = WINDOW;
        opts.min_bytes_for_seek = MIN_BYTES_FOR_SEEK;
        opts.block_size = BLOCK;
        opts.max_tail_for_drain = MAX_TAIL_FOR_DRAIN;
        opts.prefetch_pool = pool;
        opts.long_connection_limit = std::make_shared<LongConnectionLimit>(10);
        opts.fill_ahead_lead = 4 * SEGMENT;   /// lead spans 4 segments -> several tiles per segment
        return std::make_unique<ReaderExecutor>(src, objects, std::move(caches), opts);
    };

    auto read_all = [](ReaderExecutor & ex)
    {
        String out;
        while (true)
        {
            auto chain = ex.readNextWindow();
            if (chain.empty())
                break;
            for (const auto & node : chain.getNodes())
                out.append(node.data(), node.size);
        }
        return out;
    };

    /// Cold pass: streams from the source through the run-ahead, populating the fs cache.
    {
        auto ex = make_exec();
        EXPECT_EQ(read_all(*ex), content) << "cold run-ahead must stream the correct bytes";
    }

    /// Warm pass: fully cached now, so it is served from the cache with no source reads (the
    /// streamed tiles committed). `MetricScope` is declared before the executor so its dtor reads
    /// the deltas AFTER the executor flushes its counters.
    {
        CostVector warm;
        {
            MetricScope scope(warm);
            auto ex = make_exec();
            EXPECT_EQ(read_all(*ex), content) << "warm read must return the same bytes";
        }
        EXPECT_EQ(warm.fetched, 0u) << "warm pass must be served from the cache: the cold run-ahead populated it";
    }
}
