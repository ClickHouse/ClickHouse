#include <IO/ReaderExecutor.h>
#include <IO/LocalSourceReader.h>
#include <IO/LongConnectionLimit.h>
#include <IO/PipelineReadBuffer.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

/// KPI use-case bench for the experimental `ReaderExecutor`: a handful of representative read
/// patterns measured under "live" (a held long connection is allowed) vs "stateless" (one-shot per
/// window), reading the executor's modeled-cost ProfileEvents back from an isolated ThreadGroup.
/// It asserts the directional invariants (reuse cuts requests, bridging trades a request for
/// over-read) and prints the per-pattern matrix (requests / over-read / cost-per-MiB, live vs
/// stateless) so a change's KPI impact is visible in the test output.
///
/// Only cache-free patterns live here. The cache-state cases (warm, checkerboard, cached gaps) come
/// with the ReaderExecutor cache-tier PRs, which extend this file.

namespace ProfileEvents
{
    extern const Event ReaderExecutorSourceRequests;
    extern const Event ReaderExecutorBytesFromSource;
    extern const Event ReaderExecutorRequestedBytes;
    extern const Event ReaderExecutorModeledCostMicroseconds;
    extern const Event ReaderExecutorIncompleteConnections;
    extern const Event ReaderExecutorLongConnectionHits;
}

using namespace DB;

namespace
{

unsigned char patternByte(size_t i)
{
    return static_cast<unsigned char>(i % 256);
}

/// RAII ThreadGroup with its own ProfileEvents counters, so each measured pattern is read in
/// isolation (mirrors the helper in gtest_reader_executor.cpp).
struct TestThreadGroup
{
    std::optional<DB::ThreadStatus> thread_status_holder{
        current_thread ? std::nullopt : std::optional<DB::ThreadStatus>(std::in_place)};
    DB::ThreadGroupPtr thread_group = DB::ThreadGroup::createForQuery(getContext().context);
    DB::ThreadGroupSwitcher switcher{thread_group, ThreadName::UNKNOWN};

    ProfileEvents::Count get(ProfileEvents::Event event) const
    {
        return thread_group->performance_counters[event];
    }
};

/// The KPI of one measured read pattern.
struct Metric
{
    ProfileEvents::Count requests = 0;    /// source GETs opened (the dominant cost term)
    ProfileEvents::Count incomplete = 0;  /// connections abandoned before their bound
    ProfileEvents::Count hits = 0;        /// windows served by reusing a held connection
    size_t requested = 0;                 /// useful bytes delivered
    size_t from_source = 0;               /// bytes read from source (>= requested when bridging)

    double cost_ms = 0.0;                 /// modeled I/O cost

    size_t overRead() const { return from_source >= requested ? from_source - requested : 0; }
    double costPerMiB() const
    {
        return requested ? cost_ms / (static_cast<double>(requested) / (1024 * 1024)) : 0.0;
    }
};

}

class ReaderExecutorMetric : public ::testing::Test
{
protected:
    static constexpr size_t FILE_SIZE = 4 * 1024 * 1024;
    static constexpr size_t BLOCK = 64 * 1024;
    static constexpr size_t MIN_BYTES_FOR_SEEK = 256 * 1024;
    static constexpr size_t MAX_TAIL_FOR_DRAIN = 256 * 1024;

    std::filesystem::path tmp_dir;

    void SetUp() override
    {
        tmp_dir = std::filesystem::temp_directory_path() / "test_reader_executor_metric";
        std::filesystem::create_directories(tmp_dir);
    }
    void TearDown() override { std::filesystem::remove_all(tmp_dir); }

    StoredObject makeFile(size_t size)
    {
        auto path = tmp_dir / "a.bin";
        std::ofstream f(path, std::ios::binary);
        for (size_t i = 0; i < size; ++i)
            f.put(static_cast<char>(patternByte(i)));
        f.close();
        StoredObject obj;
        obj.remote_path = path.string();
        obj.bytes_size = size;
        return obj;
    }

    std::unique_ptr<PipelineReadBuffer> makeBuffer(const StoredObject & obj, std::shared_ptr<LongConnectionLimit> limit)
    {
        auto ex = std::make_unique<ReaderExecutor>(
            std::make_shared<LocalSourceReader>(), StoredObjects{obj}, ReaderExecutor::Options{
                .min_bytes_for_seek = MIN_BYTES_FOR_SEEK, .block_size = BLOCK,
                .max_tail_for_drain = MAX_TAIL_FOR_DRAIN, .long_connection_limit = std::move(limit)});
        return std::make_unique<PipelineReadBuffer>(std::move(ex));
    }

    static Metric readMetric(const TestThreadGroup & tg)
    {
        Metric m;
        m.requests = tg.get(ProfileEvents::ReaderExecutorSourceRequests);
        m.incomplete = tg.get(ProfileEvents::ReaderExecutorIncompleteConnections);
        m.hits = tg.get(ProfileEvents::ReaderExecutorLongConnectionHits);
        m.requested = tg.get(ProfileEvents::ReaderExecutorRequestedBytes);
        m.from_source = tg.get(ProfileEvents::ReaderExecutorBytesFromSource);
        m.cost_ms = static_cast<double>(tg.get(ProfileEvents::ReaderExecutorModeledCostMicroseconds)) / 1000.0;
        return m;
    }

    /// Run `pattern` over a fresh buffer (live = with a slot, stateless = without) and return its KPI.
    template <typename Pattern>
    Metric measure(const StoredObject & obj, bool live, Pattern && pattern)
    {
        TestThreadGroup tg;
        auto limit = live ? std::make_shared<LongConnectionLimit>(16) : nullptr;
        auto buf = makeBuffer(obj, std::move(limit));
        pattern(*buf);
        return readMetric(tg);
    }

    /// --- the read patterns ---

    static void sequentialScan(PipelineReadBuffer & buf)
    {
        std::vector<char> tmp(BLOCK);
        while (size_t n = buf.read(tmp.data(), tmp.size()))
            (void)n;
    }

    /// The compressed reader's signature: read a full block from each mark, marks advancing by half
    /// a block, so each read seeks back into the previous (over-read) window.
    static void fragmentedReadback(PipelineReadBuffer & buf)
    {
        std::vector<char> win(BLOCK);
        for (size_t mark = 0; mark + BLOCK <= FILE_SIZE; mark += BLOCK / 2)
        {
            buf.seek(static_cast<off_t>(mark), SEEK_SET);
            buf.readStrict(win.data(), BLOCK);
        }
    }

    /// Read one block, skip a forward `gap`, repeat. A small gap is bridged on the held connection;
    /// a large gap (> MIN_BYTES_FOR_SEEK) breaks it.
    static void sparseRead(PipelineReadBuffer & buf, size_t gap)
    {
        std::vector<char> win(BLOCK);
        for (size_t pos = 0; pos + BLOCK <= FILE_SIZE; pos += BLOCK + gap)
        {
            buf.seek(static_cast<off_t>(pos), SEEK_SET);
            buf.readStrict(win.data(), BLOCK);
        }
    }

    struct Row { std::string name; Metric live; Metric stateless; };
    static std::vector<Row> results;

    void record(const std::string & name, const Metric & live, const Metric & stateless)
    {
        results.push_back({name, live, stateless});
    }

    static void TearDownTestSuite()
    {
        std::cout << "\n=== ReaderExecutor KPI (live = long connection, stateless = one-shot/window) ===\n";
        std::cout << std::left << std::setw(18) << "case" << "  " << std::setw(9) << "mode"
                  << std::right << std::setw(6) << "reqs" << std::setw(8) << "incompl"
                  << std::setw(6) << "hits" << std::setw(12) << "over_read" << std::setw(13) << "cost/MiB(ms)" << "\n";
        auto line = [](const std::string & name, const char * mode, const Metric & m)
        {
            std::cout << std::left << std::setw(18) << name << "  " << std::setw(9) << mode
                      << std::right << std::setw(6) << m.requests << std::setw(8) << m.incomplete
                      << std::setw(6) << m.hits << std::setw(12) << m.overRead()
                      << std::setw(13) << std::fixed << std::setprecision(1) << m.costPerMiB() << "\n";
        };
        for (const auto & r : results)
        {
            line(r.name, "live", r.live);
            line(r.name, "stateless", r.stateless);
        }
        results.clear();
    }
};

std::vector<ReaderExecutorMetric::Row> ReaderExecutorMetric::results;

TEST_F(ReaderExecutorMetric, ColdSequential)
{
    auto obj = makeFile(FILE_SIZE);
    auto live = measure(obj, /*live=*/true, sequentialScan);
    auto stateless = measure(obj, /*live=*/false, sequentialScan);
    record("ColdSequential", live, stateless);

    /// Reuse collapses the per-window GETs; a clean forward scan never over-reads or abandons.
    EXPECT_LT(live.requests, stateless.requests);
    EXPECT_GT(live.hits, 0u);
    EXPECT_EQ(live.overRead(), 0u);
    EXPECT_EQ(live.incomplete, 0u);
    EXPECT_LT(live.costPerMiB(), stateless.costPerMiB());
}

TEST_F(ReaderExecutorMetric, FragmentedReadback)
{
    auto obj = makeFile(FILE_SIZE);
    auto live = measure(obj, /*live=*/true, fragmentedReadback);
    auto stateless = measure(obj, /*live=*/false, fragmentedReadback);
    record("FragmentedReadback", live, stateless);

    /// The held connection survives the in-buffer seek-back and is reused across the fragmentation.
    EXPECT_LT(live.requests, stateless.requests);
    EXPECT_GT(live.hits, 0u);
    EXPECT_LT(live.costPerMiB(), stateless.costPerMiB());
}

TEST_F(ReaderExecutorMetric, SparseSmallGaps)
{
    auto obj = makeFile(FILE_SIZE);
    auto pattern = [](PipelineReadBuffer & buf) { sparseRead(buf, BLOCK); };  /// gap = 64 KiB < MIN_BYTES_FOR_SEEK
    auto live = measure(obj, /*live=*/true, pattern);
    auto stateless = measure(obj, /*live=*/false, pattern);
    record("SparseSmallGaps", live, stateless);

    /// Small forward gaps are bridged on the held connection: fewer GETs, paid for with over-read.
    EXPECT_LT(live.requests, stateless.requests);
    EXPECT_GT(live.overRead(), 0u);
}

TEST_F(ReaderExecutorMetric, SparseLargeGaps)
{
    auto obj = makeFile(FILE_SIZE);
    auto pattern = [](PipelineReadBuffer & buf) { sparseRead(buf, 512 * 1024); };  /// gap > MIN_BYTES_FOR_SEEK
    auto live = measure(obj, /*live=*/true, pattern);
    auto stateless = measure(obj, /*live=*/false, pattern);
    record("SparseLargeGaps", live, stateless);

    /// A gap past the seek threshold is NOT bridged: the connection breaks, so no over-read and no
    /// request saving over the stateless path.
    EXPECT_EQ(live.overRead(), 0u);
    EXPECT_EQ(live.requests, stateless.requests);
}
