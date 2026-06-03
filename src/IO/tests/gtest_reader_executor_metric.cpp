/// Metric harness for the ReaderExecutor cache/remote-I/O optimality study.
///
/// Drives the executor against a REAL FileCache and reads the matching thread-group
/// ProfileEvents per consumer pass, over two rounds (round 2 re-reads to measure how
/// well round 1 populated the cache). These are the executor's production counters
/// (so the same numbers are computable on real load via system.reader_executor_log):
///   Cost_ms = 30*R + 5*I + 20*O_MiB + 0.1*Wc + 0.05*Rc
///     R  = remote GET requests              (ReaderExecutorSourceRequests)
///     I  = connections left not-fully-read  (ReaderExecutorIncompleteConnections)
///     O  = over-read bytes                  (ReaderExecutorOverReadBytes)
///     Wc = cache writes                     (ReaderExecutorCachePopulateRequests)
///     Rc = cache reads                      (ReaderExecutorCacheGetRequests)
///
/// Geometry is production sizes compressed by COMPRESSION (see the constants): all
/// ratios (segment / window / block / alignment / min_bytes_for_seek) preserved, so
/// the COUNTS (R, I, Wc, Rc) match production; over-read bytes are at the compressed
/// scale and costMs() rescales them by COMPRESSION. The FileCache is real.

#include <IO/ReaderExecutor.h>
#include <IO/ISourceReader.h>
#include <IO/ICacheProvider.h>
#include <IO/DiskCacheProvider.h>
#include <IO/SourceBufferLimit.h>
#include <IO/ReadSettings.h>
#include <IO/Rope.h>
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
#include <array>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
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
    extern const Event ReaderExecutorSourceRequests;
    extern const Event ReaderExecutorIncompleteConnections;
    extern const Event ReaderExecutorOverReadBytes;
    extern const Event ReaderExecutorBytesFromSource;
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
/// alignment 4 MiB, window 8 MiB, block 1 MiB, min_bytes_for_seek 8 MiB.
constexpr size_t COMPRESSION = 1024;
constexpr size_t SEGMENT            = (32u << 20) / COMPRESSION;   /// 32 KiB
constexpr size_t ALIGNMENT          = (4u << 20) / COMPRESSION;    ///  4 KiB
constexpr size_t WINDOW             = (8u << 20) / COMPRESSION;    ///  8 KiB
constexpr size_t BLOCK              = (1u << 20) / COMPRESSION;    ///  1 KiB
constexpr size_t MIN_BYTES_FOR_SEEK = (8u << 20) / COMPRESSION;    ///  8 KiB
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

class MemBoundedSource : public ISourceReader
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
    size_t fetched = 0;       /// bytes from source

    double costMs() const
    {
        /// over_read is measured at the compressed geometry; scale to real bytes
        /// (R/I/Wc/Rc are counts and already match production).
        return 30.0 * static_cast<double>(requests) + 5.0 * static_cast<double>(incomplete)
             + 20.0 * (static_cast<double>(over_read * COMPRESSION) / (1024.0 * 1024.0))
             + 0.1 * static_cast<double>(cache_writes) + 0.05 * static_cast<double>(cache_reads);
    }

    String str() const
    {
        return "R=" + std::to_string(requests) + " I=" + std::to_string(incomplete)
             + " O=" + std::to_string(over_read * COMPRESSION) + "B(real) Wc=" + std::to_string(cache_writes)
             + " Rc=" + std::to_string(cache_reads) + " fetched=" + std::to_string(fetched)
             + "B cost=" + std::to_string(costMs()) + "ms";
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
    }

private:
    static std::array<UInt64, 6> read()
    {
        auto & c = CurrentThread::getProfileEvents();
        return {
            c[ProfileEvents::ReaderExecutorSourceRequests].load(std::memory_order_relaxed),
            c[ProfileEvents::ReaderExecutorIncompleteConnections].load(std::memory_order_relaxed),
            c[ProfileEvents::ReaderExecutorOverReadBytes].load(std::memory_order_relaxed),
            c[ProfileEvents::ReaderExecutorCachePopulateRequests].load(std::memory_order_relaxed),
            c[ProfileEvents::ReaderExecutorCacheGetRequests].load(std::memory_order_relaxed),
            c[ProfileEvents::ReaderExecutorBytesFromSource].load(std::memory_order_relaxed),
        };
    }

    CostVector & out;
    std::array<UInt64, 6> base;
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

    /// One consumer pass over [0, file_size) on a fresh executor sharing `fc`.
    /// The metric is read entirely from the test's thread-group ProfileEvents (the
    /// fixture's QueryScope) — no inspection API on the executor; the same counters
    /// production reads. The executor flushes its counters to the thread group, so
    /// the deltas are taken across the executor's lifetime.
    CostVector runRound(
        const std::shared_ptr<FileCache> & fc,
        const std::unordered_map<String, String> & data,
        const StoredObjects & objects,
        size_t file_size, size_t window, size_t block,
        size_t read_offset = 0, std::optional<size_t> read_size = std::nullopt)
    {
        const size_t want = read_size.value_or(file_size - read_offset);
        CostVector m;
        size_t bytes_read = 0;
        {
            MetricScope scope(m);
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
            caches.push_back(makeDiskProvider(fc));

            auto src = std::make_shared<MemBoundedSource>(data);
            ReaderExecutor executor(src, objects, std::move(caches), window, /*min_bytes_for_seek=*/MIN_BYTES_FOR_SEEK, block);
            executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));
            if (read_offset > 0)
                executor.seek(read_offset);
            executor.setReadExtent(read_offset + want);

            while (true)
            {
                auto rope = executor.readNextWindow();
                if (rope.empty())
                    break;
                bytes_read += rope.range().size;
            }
        }
        EXPECT_EQ(bytes_read, want);
        return m;
    }

    /// Several seek+read operations on ONE executor (random access), accumulating
    /// the metric across all of them — models one reader seeking between mark ranges.
    /// `reads` is a list of (offset, size).
    CostVector runReads(
        const std::shared_ptr<FileCache> & fc,
        const std::unordered_map<String, String> & data,
        const StoredObjects & objects,
        size_t window, size_t block,
        const std::vector<std::pair<size_t, size_t>> & reads)
    {
        size_t total = 0;
        size_t want = 0;
        for (const auto & rd : reads)
            want += rd.second;

        CostVector m;
        {
            MetricScope scope(m);
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
            caches.push_back(makeDiskProvider(fc));

            auto src = std::make_shared<MemBoundedSource>(data);
            ReaderExecutor executor(src, objects, std::move(caches), window, /*min_bytes_for_seek=*/MIN_BYTES_FOR_SEEK, block);
            executor.setBufferLimit(std::make_shared<SourceBufferLimit>(10));

            for (const auto & rd : reads)
            {
                executor.seek(rd.first);
                executor.setReadExtent(rd.first + rd.second);
                size_t got = 0;
                while (got < rd.second)
                {
                    auto rope = executor.readNextWindow();
                    if (rope.empty())
                        break;
                    got += rope.range().size;
                }
                total += got;
            }
        }
        EXPECT_EQ(total, want);
        return m;
    }

protected:
    std::optional<ThreadStatus> thread_status;
    ContextMutablePtr query_context;
    std::optional<QueryScope> query_scope_holder;
    fs::path cache_root;
};

/// cold cache, full sequential scan. Round 1 fetches the whole file once and
/// populates; round 2 should be fully warm (no source).
TEST_F(ReaderExecutorMetric, ColdSequential)
{
    const String content = makePattern(FILE_SIZE);
    const std::unordered_map<String, String> data{{"obj", content}};
    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    auto fc = makeFileCache("cold_seq", SEGMENT, ALIGNMENT, /*max_size=*/64u << 20);

    const CostVector r1 = runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK);
    const CostVector r2 = runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK);

    std::cout << "[ColdSequential] round1: " << r1.str() << "\n"
              << "[ColdSequential] round2: " << r2.str() << "\n"
              << "[ColdSequential] total cost: " << (r1.costMs() + r2.costMs()) << "ms\n";

    /// Round 1: one streamed GET for the whole contiguous cold file, drained, no over-read.
    EXPECT_EQ(r1.requests, 1u) << "contiguous cold scan should be one streamed GET";
    EXPECT_EQ(r1.incomplete, 0u) << "the connection drains to its bound";
    EXPECT_EQ(r1.over_read, 0u) << "sequential cold scan over-reads nothing";
    EXPECT_EQ(r1.fetched, FILE_SIZE);
    EXPECT_GT(r1.cache_writes, 0u) << "round 1 populates the cache";
    EXPECT_EQ(r1.cache_reads, 0u) << "nothing cached yet";

    /// Round 2: fully warm.
    EXPECT_EQ(r2.requests, 0u) << "round 2 served entirely from cache";
    EXPECT_EQ(r2.fetched, 0u);
    EXPECT_EQ(r2.incomplete, 0u);
    EXPECT_EQ(r2.cache_writes, 0u) << "no misses to populate";
    EXPECT_GT(r2.cache_reads, 0u) << "round 2 reads from cache";
}

/// Alternating cached/cold SEGMENTS, sequential scan. The cached segment between
/// two cold ones breaks the live connection (it does not advance the connection's
/// position), so each cold run is its own GET and the broken connection is dropped
/// undrained -> incomplete. The warm-regression mechanism at production geometry.
TEST_F(ReaderExecutorMetric, CheckerboardLive)
{
    const String content = makePattern(FILE_SIZE);
    const std::unordered_map<String, String> data{{"obj", content}};
    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    auto fc = makeFileCache("checkerboard", SEGMENT, ALIGNMENT, /*max_size=*/64u << 20);

    /// Warm even segments; odd segments stay cold -> hit/miss/... on the scan.
    for (size_t s = 0; s < N_SEGMENTS; s += 2)
        runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK, /*read_offset=*/s * SEGMENT, /*read_size=*/SEGMENT);

    const CostVector r1 = runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK);   /// the checkerboard scan
    const CostVector r2 = runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK);   /// now fully warm

    std::cout << "[CheckerboardLive] round1: " << r1.str() << "\n"
              << "[CheckerboardLive] round2: " << r2.str() << "\n";

    EXPECT_EQ(r1.requests, N_SEGMENTS / 2) << "one streamed GET per cold (odd) segment - not coalesced across cached holes";
    EXPECT_EQ(r1.incomplete, N_SEGMENTS / 2 - 1) << "each cold segment's connection breaks at the next cached segment; the last drains at EOF";
    EXPECT_EQ(r1.over_read, 0u) << "segment-aligned cold runs, no prefix over-read";
    EXPECT_EQ(r2.requests, 0u) << "round 1 populated the odd segments";
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
    const String content = makePattern(FILE_SIZE);
    const std::unordered_map<String, String> data{{"obj", content}};
    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    auto fc = makeFileCache("midseg", SEGMENT, ALIGNMENT, /*max_size=*/16u << 20);

    const size_t read_off = SEGMENT - BLOCK;   /// 31 KiB into the first 32 KiB segment
    const CostVector r1 = runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK, read_off, BLOCK);

    std::cout << "[MidSegmentOverRead] " << r1.str()
              << "  (read_off=" << read_off << " ALIGNMENT=" << ALIGNMENT
              << " SEGMENT=" << SEGMENT << ")\n";

    EXPECT_EQ(r1.requests, 1u);
    EXPECT_GT(r1.over_read, 0u);
    EXPECT_LE(r1.over_read, ALIGNMENT)
        << "cold over-read should be bounded by boundary_alignment, not the segment size";
}

/// First half warm, second half cold, full sequential scan. The contiguous suffix
/// miss runs to EOF on one streamed connection that drains cleanly -> no incomplete.
TEST_F(ReaderExecutorMetric, PrefixHitSuffixMiss)
{
    constexpr size_t half = FILE_SIZE / 2;

    const String content = makePattern(FILE_SIZE);
    const std::unordered_map<String, String> data{{"obj", content}};
    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    auto fc = makeFileCache("prefix_hit", SEGMENT, ALIGNMENT, /*max_size=*/64u << 20);
    runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK, /*read_offset=*/0, /*read_size=*/half);  /// warm [0, half)

    const CostVector r1 = runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK);
    std::cout << "[PrefixHitSuffixMiss] " << r1.str() << "\n";

    EXPECT_EQ(r1.requests, 1u) << "the contiguous cold suffix is one streamed GET";
    EXPECT_EQ(r1.incomplete, 0u) << "the miss runs to EOF and drains cleanly";
    EXPECT_EQ(r1.over_read, 0u);
}

/// First half cold, second half warm. The miss run is followed by cached data, so
/// the connection (bounded to the full read extent) is dropped before its bound
/// when the read switches to cache -> one incomplete connection. Order-flip of the
/// case above: misses-then-hits costs an incomplete connection, hits-then-misses
/// does not.
TEST_F(ReaderExecutorMetric, SuffixHitPrefixMiss)
{
    constexpr size_t half = FILE_SIZE / 2;

    const String content = makePattern(FILE_SIZE);
    const std::unordered_map<String, String> data{{"obj", content}};
    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    auto fc = makeFileCache("suffix_hit", SEGMENT, ALIGNMENT, /*max_size=*/64u << 20);
    runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK, /*read_offset=*/half, /*read_size=*/half);  /// warm [half, end)

    const CostVector r1 = runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK);
    std::cout << "[SuffixHitPrefixMiss] " << r1.str() << "\n";

    EXPECT_EQ(r1.requests, 1u) << "the contiguous cold prefix is one streamed GET";
    EXPECT_EQ(r1.incomplete, 1u) << "the connection is abandoned when the read switches to the cached suffix";
    EXPECT_EQ(r1.over_read, 0u);
}

/// All warm except one interior block. The single miss's connection is broken by
/// the following hit -> one GET, one incomplete connection.
TEST_F(ReaderExecutorMetric, InteriorHole)
{
    constexpr size_t hole = 3;   /// interior cold segment index

    const String content = makePattern(FILE_SIZE);
    const std::unordered_map<String, String> data{{"obj", content}};
    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    auto fc = makeFileCache("interior_hole", SEGMENT, ALIGNMENT, /*max_size=*/64u << 20);
    runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK, /*read_offset=*/0, /*read_size=*/hole * SEGMENT);
    runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK,
             /*read_offset=*/(hole + 1) * SEGMENT, /*read_size=*/FILE_SIZE - (hole + 1) * SEGMENT);

    const CostVector r1 = runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK);
    std::cout << "[InteriorHole] " << r1.str() << "\n";

    EXPECT_EQ(r1.requests, 1u);
    EXPECT_EQ(r1.incomplete, 1u) << "the single cold segment's connection is broken by the following hit";
    EXPECT_EQ(r1.over_read, 0u);
}

/// Scattered point reads, each mid-way into a distinct cold segment. Each point is
/// its own GET (seeks break reuse), drains at the point's extent (no incomplete),
/// and pays the [seg_start, point_offset) segment-prefix slack as over-read. Random
/// access cost = R (one GET per touched segment) + O (per-segment prefix slack).
TEST_F(ReaderExecutorMetric, RandomScattered)
{
    constexpr size_t point = BLOCK;                       /// 1 KiB per point
    constexpr size_t n_points = 4;
    constexpr size_t off_in_seg = SEGMENT / 2 - BLOCK;    /// 15 KiB: mid-segment, NOT alignment-aligned
    constexpr size_t over_per = off_in_seg % ALIGNMENT;   /// 3 KiB prefix slack per point

    const String content = makePattern(FILE_SIZE);
    const std::unordered_map<String, String> data{{"obj", content}};
    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    auto fc = makeFileCache("random_scattered", SEGMENT, ALIGNMENT, /*max_size=*/64u << 20);

    std::vector<std::pair<size_t, size_t>> reads;
    for (size_t i = 0; i < n_points; ++i)
        reads.emplace_back(i * SEGMENT + off_in_seg, point);

    const CostVector r1 = runReads(fc, data, objects, WINDOW, BLOCK, reads);
    std::cout << "[RandomScattered] " << r1.str() << "\n";

    EXPECT_EQ(r1.requests, n_points) << "one GET per scattered point (seeks break reuse)";
    EXPECT_EQ(r1.incomplete, 0u) << "each point drains at its own extent";
    EXPECT_EQ(r1.over_read, n_points * over_per) << "per point, the alignment-bounded prefix slack";
}

/// Random starts, each followed by a short SEQUENTIAL run (mid-segment, cold). Same
/// segments touched as the scattered case -> same R, I, O, but each run serves more
/// useful bytes per prefix-fill. Shows random cost scales with segments touched, not
/// bytes read (compare `fetched` in the two prints: more served for the same cost).
TEST_F(ReaderExecutorMetric, RandomPartialSequences)
{
    constexpr size_t run = 3 * BLOCK;                     /// 3 KiB sequential run
    constexpr size_t n_runs = 4;
    constexpr size_t off_in_seg = SEGMENT / 2 - BLOCK;    /// 15 KiB: mid-segment, NOT alignment-aligned
    constexpr size_t over_per = off_in_seg % ALIGNMENT;   /// 3 KiB prefix slack per run

    const String content = makePattern(FILE_SIZE);
    const std::unordered_map<String, String> data{{"obj", content}};
    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    auto fc = makeFileCache("random_runs", SEGMENT, ALIGNMENT, /*max_size=*/64u << 20);

    std::vector<std::pair<size_t, size_t>> reads;
    for (size_t i = 0; i < n_runs; ++i)
        reads.emplace_back(i * SEGMENT + off_in_seg, run);

    const CostVector r1 = runReads(fc, data, objects, WINDOW, BLOCK, reads);
    std::cout << "[RandomPartialSequences] " << r1.str() << "\n";

    EXPECT_EQ(r1.requests, n_runs) << "one streamed GET per run";
    EXPECT_EQ(r1.incomplete, 0u) << "each run drains at its extent";
    EXPECT_EQ(r1.over_read, n_runs * over_per) << "per run, the alignment-bounded prefix slack (same as scattered)";
}

/// Mostly-warm cache with a few scattered cold segments — the realistic production
/// state (~98% warm). Each cold segment is a one-GET miss-run broken by the next
/// (warm) segment -> R = #cold, I = #cold. Low fragmentation, unlike the checkerboard
/// worst case.
TEST_F(ReaderExecutorMetric, SparseScatteredCold)
{
    const String content = makePattern(FILE_SIZE);
    const std::unordered_map<String, String> data{{"obj", content}};
    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    auto fc = makeFileCache("sparse_cold", SEGMENT, ALIGNMENT, /*max_size=*/64u << 20);

    /// Warm every segment except 4 scattered cold ones (each followed by a warm segment).
    const std::vector<size_t> cold = {6, 13, 20, 27};
    size_t prev = 0;
    for (size_t c : cold)
    {
        if (c > prev)
            runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK,
                     /*read_offset=*/prev * SEGMENT, /*read_size=*/(c - prev) * SEGMENT);
        prev = c + 1;
    }
    if (prev < N_SEGMENTS)
        runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK,
                 /*read_offset=*/prev * SEGMENT, /*read_size=*/(N_SEGMENTS - prev) * SEGMENT);

    const CostVector r1 = runRound(fc, data, objects, FILE_SIZE, WINDOW, BLOCK);
    std::cout << "[SparseScatteredCold] " << r1.str() << "\n";

    EXPECT_EQ(r1.requests, cold.size()) << "one GET per cold segment";
    EXPECT_EQ(r1.incomplete, cold.size()) << "each cold segment's connection is broken by the next warm segment";
    EXPECT_EQ(r1.over_read, 0u) << "segment-aligned cold runs, no prefix over-read";
}

/// Cold cache, read SEGMENT-sized chunks in DESCENDING order. Backward seeks defeat
/// the live-buffer's forward streaming, so each chunk is its own GET (vs ONE GET for
/// the forward cold scan) — the request-count penalty of reverse access. Each chunk
/// is extent-bounded, so it drains cleanly (I=0); the cost is pure R.
TEST_F(ReaderExecutorMetric, ReverseSequential)
{
    const String content = makePattern(FILE_SIZE);
    const std::unordered_map<String, String> data{{"obj", content}};
    StoredObjects objects;
    objects.emplace_back("obj", "", FILE_SIZE);

    auto fc = makeFileCache("reverse_seq", SEGMENT, ALIGNMENT, /*max_size=*/64u << 20);

    std::vector<std::pair<size_t, size_t>> reads;
    for (size_t s = N_SEGMENTS; s-- > 0;)
        reads.emplace_back(s * SEGMENT, SEGMENT);

    const CostVector r1 = runReads(fc, data, objects, WINDOW, BLOCK, reads);
    std::cout << "[ReverseSequential] " << r1.str() << "\n";

    EXPECT_EQ(r1.requests, N_SEGMENTS) << "one GET per chunk - backward seeks defeat forward streaming (vs 1 for forward cold)";
    EXPECT_EQ(r1.incomplete, 0u) << "each extent-bounded chunk drains cleanly";
    EXPECT_EQ(r1.over_read, 0u) << "segment-aligned chunks, no prefix over-read";
}
