#include <IO/ReaderExecutor.h>
#include <IO/LocalSourceReader.h>
#include <Interpreters/Cache/EncryptionHeaderCache.h>
#include <IO/LongConnectionLimit.h>
#include <IO/PipelineReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <cstring>

#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>

#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "config.h"
#if USE_SSL
#include <IO/ReaderExecutorDecryptor.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/WriteBufferFromString.h>
#include <latch>
#include <thread>
#endif

namespace ProfileEvents
{
    extern const Event ReaderExecutorSourceRequests;
    extern const Event ReaderExecutorBytesFromSource;
    extern const Event ReaderExecutorRequestedBytes;
    extern const Event ReaderExecutorModeledCostMicroseconds;
    extern const Event ReaderExecutorCacheGetRequests;
    extern const Event ReaderExecutorCachePopulateRequests;
    extern const Event ReaderExecutorIncompleteConnections;
    extern const Event ReaderExecutorLongConnectionOpened;
    extern const Event ReaderExecutorLongConnectionHits;
    extern const Event ReaderExecutorLongConnectionFallbacks;
    extern const Event ReaderExecutorLongConnectionBytes;
}

namespace DB::ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

using namespace DB;

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

/// Byte value at logical offset `i` within a file: deterministic pattern.
unsigned char patternByte(size_t i)
{
    return static_cast<unsigned char>(i % 256);
}

/// A source buffer that mimics object storage opened with `use_external_buffer=true`: it owns no
/// read memory, and `nextImpl` fills the caller's externally `set()` buffer (`internal_buffer`).
/// This is the path where a raw `read` would refill a stale external pointer; a local
/// `ReadBufferFromFileDescriptor` cannot reproduce it because it falls back to its own memory.
class ExternalBufferReader : public ReadBufferFromFileBase
{
public:
    explicit ExternalBufferReader(std::shared_ptr<const std::string> data_)
        : ReadBufferFromFileBase(/*buf_size=*/0, /*existing_memory=*/nullptr, /*alignment=*/0, data_->size())
        , data(std::move(data_))
    {
    }

    bool nextImpl() override
    {
        const size_t cap = read_until ? std::min(*read_until, data->size()) : data->size();
        if (file_pos >= cap || internal_buffer.empty())
            return false;
        const size_t n = std::min(internal_buffer.size(), cap - file_pos);
        memcpy(internal_buffer.begin(), data->data() + file_pos, n);   /// into the external set() buffer
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + n);
        pos = working_buffer.begin();
        file_pos += n;
        return n != 0;
    }

    off_t seek(off_t off, int) override { file_pos = static_cast<size_t>(off); resetWorkingBuffer(); return off; }
    off_t getPosition() override { return static_cast<off_t>(file_pos) - static_cast<off_t>(available()); }
    String getFileName() const override { return "external_mock"; }
    void setReadUntilPosition(size_t position) override { read_until = position; }
    void setReadUntilEnd() override { read_until.reset(); }
    bool supportsRightBoundedReads() const override { return true; }
    bool supportsExternalBufferMode() const override { return true; }

private:
    std::shared_ptr<const std::string> data;
    size_t file_pos = 0;
    std::optional<size_t> read_until;
};

class ExternalBufferSourceReader : public IFileBasedSourceReader
{
public:
    explicit ExternalBufferSourceReader(std::shared_ptr<const std::string> data_) : data(std::move(data_)) {}
    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject &) override
    {
        return std::make_unique<ExternalBufferReader>(data);
    }
    String name() const override { return "ExternalBufferSourceReader"; }

private:
    std::shared_ptr<const std::string> data;
};

/// An external-buffer source (like `ExternalBufferReader`) that throws once it has delivered more
/// than `budget` bytes, simulating a transient failure / closed stream mid-response. The budget is
/// per buffer instance, so a freshly opened buffer starts over -- letting a test fail the drain on a
/// held connection while a subsequently opened connection reads cleanly.
class FaultBudgetReader : public ReadBufferFromFileBase
{
public:
    FaultBudgetReader(std::shared_ptr<const std::string> data_, size_t budget_)
        : ReadBufferFromFileBase(/*buf_size=*/0, /*existing_memory=*/nullptr, /*alignment=*/0, data_->size())
        , data(std::move(data_)), budget(budget_)
    {
    }

    bool nextImpl() override
    {
        const size_t cap = read_until ? std::min(*read_until, data->size()) : data->size();
        if (file_pos >= cap || internal_buffer.empty())
            return false;
        const size_t n = std::min(internal_buffer.size(), cap - file_pos);
        if (delivered + n > budget)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "FaultBudgetReader: injected read failure past budget");
        memcpy(internal_buffer.begin(), data->data() + file_pos, n);
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + n);
        pos = working_buffer.begin();
        file_pos += n;
        delivered += n;
        return n != 0;
    }

    off_t seek(off_t off, int) override { file_pos = static_cast<size_t>(off); resetWorkingBuffer(); return off; }
    off_t getPosition() override { return static_cast<off_t>(file_pos) - static_cast<off_t>(available()); }
    String getFileName() const override { return "fault_mock"; }
    void setReadUntilPosition(size_t position) override { read_until = position; }
    void setReadUntilEnd() override { read_until.reset(); }
    bool supportsRightBoundedReads() const override { return true; }
    bool supportsExternalBufferMode() const override { return true; }

private:
    std::shared_ptr<const std::string> data;
    size_t budget;
    size_t file_pos = 0;
    size_t delivered = 0;
    std::optional<size_t> read_until;
};

class FaultBudgetSourceReader : public IFileBasedSourceReader
{
public:
    FaultBudgetSourceReader(std::shared_ptr<const std::string> data_, size_t budget_)
        : data(std::move(data_)), budget(budget_) {}
    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject &) override
    {
        return std::make_unique<FaultBudgetReader>(data, budget);
    }
    String name() const override { return "FaultBudgetSourceReader"; }

private:
    std::shared_ptr<const std::string> data;
    size_t budget;
};

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
            std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{
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

TEST_F(ReaderExecutorTest, SequentialReadSingleObject)
{
    StoredObjects objects{makeFile("a.bin", 1024)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{.block_size = 256});

    EXPECT_EQ(ex.totalSize(), 1024u);
    EXPECT_FALSE(ex.hasUnknownSize());

    auto data = drain(ex);
    ASSERT_EQ(data.size(), 1024u);
    for (size_t i = 0; i < data.size(); ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[i]), patternByte(i)) << "at offset " << i;
    EXPECT_EQ(ex.getPosition(), 1024u);
}

TEST_F(ReaderExecutorTest, WindowNeverExceedsBlockSize)
{
    StoredObjects objects{makeFile("a.bin", 1000)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{.block_size = 100});

    size_t total = 0;
    size_t windows = 0;
    while (true)
    {
        ChainedBuffers w = ex.readNextWindow();
        if (w.atEnd())
            break;
        EXPECT_LE(w.totalBytes(), 100u);
        EXPECT_EQ(w.peek().logical_offset, total);
        total += w.totalBytes();
        ++windows;
    }
    EXPECT_EQ(total, 1000u);
    EXPECT_EQ(windows, 10u);
}

TEST_F(ReaderExecutorTest, SeekThenRead)
{
    StoredObjects objects{makeFile("a.bin", 1024)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{.block_size = 256});

    ex.seek(500);
    EXPECT_EQ(ex.getPosition(), 500u);

    ChainedBuffers w = ex.readNextWindow();
    ASSERT_FALSE(w.atEnd());
    auto span = w.peek();
    EXPECT_EQ(span.logical_offset, 500u);
    EXPECT_EQ(static_cast<unsigned char>(span.data[0]), patternByte(500));

    /// Seek backward and re-read.
    ex.seek(10);
    ChainedBuffers w2 = ex.readNextWindow();
    ASSERT_FALSE(w2.atEnd());
    auto span2 = w2.peek();
    EXPECT_EQ(span2.logical_offset, 10u);
    EXPECT_EQ(static_cast<unsigned char>(span2.data[0]), patternByte(10));
}

TEST_F(ReaderExecutorTest, MultiObjectConcatenationNeverCrossesBoundary)
{
    StoredObjects objects{makeFile("a.bin", 300), makeFile("b.bin", 200)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{.block_size = 256});

    EXPECT_EQ(ex.totalSize(), 500u);

    /// A window must never straddle the object boundary at 300.
    while (true)
    {
        size_t pos = ex.getPosition();
        ChainedBuffers w = ex.readNextWindow();
        if (w.atEnd())
            break;
        if (pos < 300)
            EXPECT_LE(w.peek().logical_offset + w.totalBytes(), 300u) << "window from " << pos << " crossed boundary";
    }
    EXPECT_EQ(ex.getPosition(), 500u);
}

TEST_F(ReaderExecutorTest, MultiObjectDataIsCorrect)
{
    StoredObjects objects{makeFile("a.bin", 300), makeFile("b.bin", 200)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{.block_size = 64});

    auto data = drain(ex);
    ASSERT_EQ(data.size(), 500u);
    /// Object A holds pattern[0..299], object B holds pattern[0..199].
    for (size_t i = 0; i < 300; ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[i]), patternByte(i)) << "object A at " << i;
    for (size_t i = 0; i < 200; ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[300 + i]), patternByte(i)) << "object B at " << i;
}

TEST_F(ReaderExecutorTest, EmptyFileIsImmediateEOF)
{
    StoredObjects objects{makeFile("empty.bin", 0)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{.block_size = 256});

    EXPECT_EQ(ex.totalSize(), 0u);
    EXPECT_TRUE(ex.readNextWindow().atEnd());
}

TEST_F(ReaderExecutorTest, MissingFileWithUnknownSizeThrows)
{
    /// `DiskLocal::prepareRead` marks an unstatable file `UnknownSize`; the
    /// executor must then open it and surface the real error (e.g. file does not
    /// exist) instead of treating it as an empty read.
    StoredObject missing;
    missing.remote_path = (tmp_dir / "does_not_exist.bin").string();
    missing.bytes_size = StoredObject::UnknownSize;
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), {missing}, ReaderExecutor::Options{.block_size = 256});

    EXPECT_ANY_THROW(ex.readNextWindow());
}

TEST_F(ReaderExecutorTest, TruncatedKnownSizeFileThrows)
{
    /// A known-size object whose file is shorter than its declared size is
    /// truncated/corrupt; the executor must throw rather than return a short read.
    StoredObject obj = makeFile("short.bin", 100);
    obj.bytes_size = 1000;  // pretend the object is larger than the file on disk
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), {obj}, ReaderExecutor::Options{.block_size = 256});

    EXPECT_ANY_THROW(ex.readNextWindow());
}

/// The metrics tests read the executor's ProfileEvents from a fresh per-test ThreadGroup
/// (starts at zero) -- the same path that feeds `system.events`.
TEST_F(ReaderExecutorTest, ProfileEventsCountSourceReadsAndBytes)
{
    TestThreadGroup tg;

    /// 1 MiB file read in 256 KiB blocks -> 4 source reads, all bytes served.
    constexpr size_t size = 1024 * 1024;
    StoredObjects objects{makeFile("a.bin", size)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{.block_size = 256 * 1024});
    drain(ex);

    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorSourceRequests), 4u);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorBytesFromSource), size);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorRequestedBytes), size);
    /// The cache / connection KPI inputs are not implemented in this slice.
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorCacheGetRequests), 0u);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorCachePopulateRequests), 0u);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorIncompleteConnections), 0u);
}

TEST_F(ReaderExecutorTest, ModeledCostMatchesFormula)
{
    TestThreadGroup tg;

    /// Modeled cost = 30ms/source request + 20ms/MiB from source (cache/conn terms 0).
    constexpr size_t size = 1024 * 1024;
    StoredObjects objects{makeFile("a.bin", size)};
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{.block_size = 256 * 1024});
    drain(ex);

    const auto cost = tg.get(ProfileEvents::ReaderExecutorModeledCostMicroseconds);
    const auto requested = tg.get(ProfileEvents::ReaderExecutorRequestedBytes);
    EXPECT_EQ(cost, 30000u * 4 + 20000u);  // 4 reads + 1 MiB
    EXPECT_EQ(requested, size);

    /// The KPI: modeled ms per requested MiB.
    const double ms_per_mib = (static_cast<double>(cost) / 1000.0)
        / (static_cast<double>(requested) / (1024.0 * 1024.0));
    EXPECT_DOUBLE_EQ(ms_per_mib, 140.0);
}

TEST_F(ReaderExecutorTest, ModeledCostScalesWithSourceRequests)
{
    TestThreadGroup tg;

    /// Smaller blocks over the same data -> more source requests -> higher modeled cost,
    /// so the KPI (cost per requested MiB) rises even though the bytes are unchanged.
    constexpr size_t size = 1024 * 1024;
    {
        StoredObjects big_block{makeFile("a.bin", size)};
        ReaderExecutor coarse(std::make_shared<LocalSourceReader>(), big_block, ReaderExecutor::Options{.block_size = 1024 * 1024});
        drain(coarse);
    }
    const auto cost_after_coarse = tg.get(ProfileEvents::ReaderExecutorModeledCostMicroseconds);
    const auto requests_after_coarse = tg.get(ProfileEvents::ReaderExecutorSourceRequests);
    {
        StoredObjects small_block{makeFile("b.bin", size)};
        ReaderExecutor fine(std::make_shared<LocalSourceReader>(), small_block, ReaderExecutor::Options{.block_size = 64 * 1024});
        drain(fine);
    }
    const auto cost_after_fine = tg.get(ProfileEvents::ReaderExecutorModeledCostMicroseconds);
    const auto requests_after_fine = tg.get(ProfileEvents::ReaderExecutorSourceRequests);

    EXPECT_EQ(requests_after_coarse, 1u);
    EXPECT_EQ(requests_after_fine - requests_after_coarse, 16u);
    EXPECT_GT(cost_after_fine - cost_after_coarse, cost_after_coarse);
}

TEST_F(ReaderExecutorTest, LongConnectionsOffByDefault)
{
    TestThreadGroup tg;
    constexpr size_t size = 1024 * 1024;
    StoredObjects objects{makeFile("a.bin", size)};
    /// No LongConnectionLimit -> the stateless path; behavior must be unchanged.
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{.block_size = 128 * 1024});
    auto data = drain(ex);

    ASSERT_EQ(data.size(), size);
    for (size_t i = 0; i < size; ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[i]), patternByte(i)) << "at " << i;
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorLongConnectionOpened), 0u);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorLongConnectionHits), 0u);
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorLongConnectionFallbacks), 0u);
}

TEST_F(ReaderExecutorTest, SequentialScanOpensAndReusesConnection)
{
    TestThreadGroup tg;
    constexpr size_t size = 1024 * 1024;
    StoredObjects objects{makeFile("a.bin", size)};
    auto limit = std::make_shared<LongConnectionLimit>(4);
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{
        .min_bytes_for_seek = 2 * 1024 * 1024, .block_size = 128 * 1024,
        .max_tail_for_drain = 1024 * 1024, .long_connection_limit = limit});
    auto data = drain(ex);

    ASSERT_EQ(data.size(), size);
    for (size_t i = 0; i < size; ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[i]), patternByte(i)) << "at " << i;
    /// A purely sequential scan opens at least one long connection and reuses it.
    EXPECT_GE(tg.get(ProfileEvents::ReaderExecutorLongConnectionOpened), 1u);
    EXPECT_GE(tg.get(ProfileEvents::ReaderExecutorLongConnectionHits), 1u);
    /// Forward-scan connections are read to their bound, so none are abandoned.
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorIncompleteConnections), 0u);
}

TEST_F(ReaderExecutorTest, InBufferSeekIsServedWithoutRefetch)
{
    /// Regression for PipelineReadBuffer::seek absorbing in-buffer seeks. A seek whose target is
    /// already inside the working buffer must be served by repositioning, not by re-seeking the
    /// executor (which refetches the block). The compressed reader does exactly this -- over-read a
    /// block, then seek back to a mark inside it -- and a refetch there both wastes a request and,
    /// because a held source connection is forward-only, breaks long-connection reuse. Without the
    /// absorption this issues 2 source requests instead of 1.
    TestThreadGroup tg;
    constexpr size_t size = 64 * 1024;
    StoredObjects objects{makeFile("a.bin", size)};
    auto ex = std::make_unique<ReaderExecutor>(
        std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{.block_size = 16 * 1024});
    PipelineReadBuffer buf(std::move(ex));

    /// Fetch one window [0, 16K) and partly consume it (one source request).
    std::vector<char> head(8 * 1024);
    buf.readStrict(head.data(), head.size());
    ASSERT_EQ(tg.get(ProfileEvents::ReaderExecutorSourceRequests), 1u);

    /// Seek back to an offset still inside [0, 16K) and read: served from the buffer, no refetch.
    buf.seek(2048, SEEK_SET);
    char c = 0;
    buf.readStrict(&c, 1);
    EXPECT_EQ(static_cast<unsigned char>(c), patternByte(2048));
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorSourceRequests), 1u);
}

TEST_F(ReaderExecutorTest, LongConnectionsReduceSourceRequestsOnScan)
{
    /// Long connections are active and reused on a scan (opened + Hits > 0), and the KPI: a held
    /// connection issues far fewer source requests than a fresh one-shot per window. Driven through
    /// PipelineReadBuffer in the compressed reader's over-read / seek-back pattern.
    constexpr size_t size = 64 * 1024;
    StoredObjects objects{makeFile("a.bin", size)};
    const auto with_long = overReadScan(objects, size, /*block=*/4096, /*mark_step=*/2048,
        std::make_shared<LongConnectionLimit>(4));
    const auto stateless = overReadScan(objects, size, 4096, 2048, /*limit=*/nullptr);

    EXPECT_GE(with_long.opened, 1u);
    EXPECT_GE(with_long.hits, 1u);
    EXPECT_EQ(stateless.opened, 0u);
    EXPECT_GT(stateless.source_requests, 0u);
    EXPECT_LT(with_long.source_requests, stateless.source_requests);
}

TEST_F(ReaderExecutorTest, CapacityZeroAlwaysFallsBack)
{
    TestThreadGroup tg;
    constexpr size_t size = 1024 * 1024;
    StoredObjects objects{makeFile("a.bin", size)};
    auto limit = std::make_shared<LongConnectionLimit>(0);   /// no slots available
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{
        .min_bytes_for_seek = 2 * 1024 * 1024, .block_size = 128 * 1024,
        .max_tail_for_drain = 1024 * 1024, .long_connection_limit = limit});
    auto data = drain(ex);

    ASSERT_EQ(data.size(), size);
    for (size_t i = 0; i < size; ++i)
        ASSERT_EQ(static_cast<unsigned char>(data[i]), patternByte(i)) << "at " << i;
    EXPECT_EQ(tg.get(ProfileEvents::ReaderExecutorLongConnectionOpened), 0u);
    EXPECT_GE(tg.get(ProfileEvents::ReaderExecutorLongConnectionFallbacks), 1u);   /// wanted long, no slot
}

TEST_F(ReaderExecutorTest, DataCorrectAcrossSeeksWithLongConnections)
{
    /// Exercises the bridge (small forward seek) and drop (backward seek) paths for
    /// correctness: every served byte must match the pattern regardless of reuse.
    constexpr size_t size = 1024 * 1024;
    StoredObjects objects{makeFile("a.bin", size)};
    auto limit = std::make_shared<LongConnectionLimit>(4);
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{
        .min_bytes_for_seek = 2 * 1024 * 1024, .block_size = 128 * 1024,
        .max_tail_for_drain = 1024 * 1024, .long_connection_limit = limit});

    auto read_at = [&](size_t pos, size_t len)
    {
        ex.seek(pos);
        ChainedBuffers w = ex.readNextWindow();
        EXPECT_FALSE(w.atEnd());
        if (w.atEnd())
            return;
        auto span = w.peek();
        EXPECT_GE(span.size, len);
        for (size_t i = 0; i < len && i < span.size; ++i)
            EXPECT_EQ(static_cast<unsigned char>(span.data[i]), patternByte(pos + i)) << "at " << (pos + i);
    };

    /// Warm up a long connection with a few sequential windows.
    for (int i = 0; i < 4; ++i)
        ex.readNextWindow();
    read_at(700 * 1024, 1024);   /// small forward gap -> bridge (or reopen); data must match
    read_at(10 * 1024, 1024);    /// backward -> drop + reread
    read_at(900 * 1024, 1024);   /// forward again
}

TEST_F(ReaderExecutorTest, IncompleteConnectionOnAbandonedDrop)
{
    TestThreadGroup tg;
    constexpr size_t size = 2 * 1024 * 1024;
    StoredObjects objects{makeFile("a.bin", size)};
    auto limit = std::make_shared<LongConnectionLimit>(4);
    /// max_tail_for_drain = 0: a connection dropped before its bound is never drained, so it
    /// is abandoned mid-response and must count as incomplete.
    ReaderExecutor ex(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{
        .min_bytes_for_seek = 2 * 1024 * 1024, .block_size = 128 * 1024,
        .max_tail_for_drain = 0, .long_connection_limit = limit});

    /// Read until a long connection is open (it has a large bound), then seek backward to
    /// abandon it mid-response.
    for (int i = 0; i < 8 && tg.get(ProfileEvents::ReaderExecutorLongConnectionOpened) == 0; ++i)
        ex.readNextWindow();
    ASSERT_GE(tg.get(ProfileEvents::ReaderExecutorLongConnectionOpened), 1u);
    ex.seek(0);
    ex.readNextWindow();   /// drops the held connection (backward, undrained tail)
    EXPECT_GE(tg.get(ProfileEvents::ReaderExecutorIncompleteConnections), 1u);
}

TEST_F(ReaderExecutorTest, DrainFailureDoesNotAbortQuery)
{
    /// The drain in `dropLongConnection` is best-effort: it completes the held GET on discarded tail bytes so
    /// the connection returns to the keep-alive pool. If the held response throws while draining,
    /// the query must NOT fail -- the connection is released as incomplete and the required
    /// (backward) read still succeeds on a fresh connection. Fails before the fix, when the drain
    /// exception escaped `dropLongConnection` on the foreground path.
    TestThreadGroup tg;
    constexpr size_t size = 2 * 1024 * 1024;
    constexpr size_t block = 128 * 1024;
    auto data = std::make_shared<std::string>(size, '\0');
    for (size_t i = 0; i < size; ++i)
        (*data)[i] = static_cast<char>(patternByte(i));
    StoredObject obj;
    obj.remote_path = "fault_mock";
    obj.bytes_size = size;

    auto limit = std::make_shared<LongConnectionLimit>(4);
    /// budget covers the one window served through the long connection (`block`), but the drain --
    /// which reads the remaining tail up to the bound -- crosses it and throws. A fresh connection
    /// (opened for the backward read) starts with a full budget again.
    ReaderExecutor ex(std::make_shared<FaultBudgetSourceReader>(data, /*budget=*/block + block / 2),
        StoredObjects{obj}, ReaderExecutor::Options{
            .min_bytes_for_seek = 2 * 1024 * 1024, .block_size = block,
            .max_tail_for_drain = size, .long_connection_limit = limit});

    /// Open a long connection with a large bound (an undrained tail remains), serving one window.
    for (int i = 0; i < 8 && tg.get(ProfileEvents::ReaderExecutorLongConnectionOpened) == 0; ++i)
        ex.readNextWindow();
    ASSERT_GE(tg.get(ProfileEvents::ReaderExecutorLongConnectionOpened), 1u);

    /// Backward seek: `dropLongConnection` drains the tail and the held buffer throws past its budget. The
    /// drain is swallowed; the required read must still return correct data without throwing.
    ex.seek(0);
    ChainedBuffers w;
    ASSERT_NO_THROW(w = ex.readNextWindow());
    ASSERT_FALSE(w.atEnd());
    const auto span = w.peek();
    ASSERT_EQ(span.logical_offset, 0u);
    ASSERT_GE(span.size, block);
    for (size_t i = 0; i < block; ++i)
        ASSERT_EQ(static_cast<unsigned char>(span.data[i]), patternByte(i)) << "at " << i;

    /// The failed drain leaves the connection incomplete.
    EXPECT_GE(tg.get(ProfileEvents::ReaderExecutorIncompleteConnections), 1u);
}

TEST_F(ReaderExecutorTest, BridgeDoesNotClobberServedWindow)
{
    /// Regression for the external-buffer discard path. With a source opened in external-buffer mode
    /// (object storage), skipForward must read the bridged gap into its own scratch, not the source
    /// buffer's stale external pointer -- the last served window's block.
    ///
    /// A small file (4 blocks) makes the first connection's bound reach EOF, so a window it serves
    /// and the bridge below stay on the SAME connection (no rotation): hold that window, bridge a
    /// small forward gap, and assert the held window's bytes are not overwritten.
    constexpr size_t size = 512 * 1024;
    auto data = std::make_shared<std::string>(size, '\0');
    for (size_t i = 0; i < size; ++i)
        (*data)[i] = static_cast<char>(patternByte(i));
    StoredObject obj;
    obj.remote_path = "external_mock";
    obj.bytes_size = size;
    TestThreadGroup tg;
    auto limit = std::make_shared<LongConnectionLimit>(4);
    ReaderExecutor ex(std::make_shared<ExternalBufferSourceReader>(data), StoredObjects{obj},
        ReaderExecutor::Options{.min_bytes_for_seek = 2 * 1024 * 1024, .block_size = 128 * 1024, .long_connection_limit = limit});

    /// Read until a long connection opens, and hold that connection's first served window.
    ChainedBuffers held;
    for (int i = 0; i < 4 && held.atEnd(); ++i)
    {
        const auto opened_before = tg.get(ProfileEvents::ReaderExecutorLongConnectionOpened);
        auto w = ex.readNextWindow();
        if (!w.atEnd() && tg.get(ProfileEvents::ReaderExecutorLongConnectionOpened) > opened_before)
            held = std::move(w);
    }
    ASSERT_FALSE(held.atEnd()) << "expected a long connection to open";
    const auto span = held.peek();
    const size_t off = span.logical_offset;

    /// A small forward gap on the open connection -> serveFromLongConnection bridges via skipForward.
    ex.seek(off + span.size + 4096);
    ex.readNextWindow();

    /// The held window must be intact -- skipForward must not write into its block.
    const auto check = held.peek();
    ASSERT_EQ(check.size, span.size);
    for (size_t i = 0; i < check.size; ++i)
        ASSERT_EQ(static_cast<unsigned char>(check.data[i]), patternByte(off + i))
            << "served window clobbered by bridge at " << (off + i);
}

#if USE_SSL

/// Encrypt `plaintext` with the given key/iv at stream offset 0 using AES_128_CTR. CTR is
/// symmetric -- encryption and decryption are the same operation.
String aesCtrEncrypt(const String & key, FileEncryption::InitVector iv, const String & plaintext)
{
    FileEncryption::Encryptor enc(FileEncryption::Algorithm::AES_128_CTR, key, iv);
    enc.setOffset(0);
    String out(plaintext.size(), '\0');
    enc.decrypt(plaintext.data(), plaintext.size(), out.data());
    return out;
}

/// Same as `aesCtrEncrypt` but keyed at an arbitrary stream offset -- needed when reproducing a
/// legacy stacked-encryption write where an outer layer's keystream covers `[inner_header (64),
/// inner_ciphertext]` at offsets `[0, inner_ciphertext_size + 64)`. CTR is position-addressable, so
/// encrypting two contiguous chunks at adjacent offsets equals encrypting the concatenation.
String aesCtrEncryptAt(const String & key, FileEncryption::InitVector iv,
    size_t stream_offset, const char * data, size_t size)
{
    FileEncryption::Encryptor enc(FileEncryption::Algorithm::AES_128_CTR, key, iv);
    enc.setOffset(stream_offset);
    String out(size, '\0');
    enc.decrypt(data, size, out.data());
    return out;
}

/// Build the on-disk encrypted byte stream: Header(64 bytes) + ciphertext.
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

/// Write raw `bytes` to `dir/name` and return the matching StoredObject (physical size).
StoredObject writeBytesObject(const std::filesystem::path & dir, const std::string & name, const String & bytes)
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

    ReaderExecutor executor(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{});
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
        ReaderExecutor::Options{.block_size = 4096});
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
        ReaderExecutor::Options{.block_size = 1024});
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
        ReaderExecutor::Options{.block_size = 4096});
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

    ReaderExecutor executor(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{});
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

    ReaderExecutor executor(std::make_shared<LocalSourceReader>(), objects, ReaderExecutor::Options{});
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
        ReaderExecutor::Options{.min_bytes_for_seek = 64, .block_size = 512, .long_connection_limit = limit});
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
            ReaderExecutor::Options{.block_size = 4096, .encryption_header_cache = cache});
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

TEST(ReaderExecutorDecryptor, ConcurrentDecryptIsReentrant)
{
    /// `ReaderExecutorDecryptor::decrypt` must be reentrant: it builds fresh per-call encryptors, so
    /// several threads decrypting DISTINCT logical offsets concurrently must not cross-talk through a
    /// shared keystream offset. Parse a known single-layer header, then have N threads each decrypt a
    /// distinct chunk; every chunk must match a single-threaded reference decrypt of the same chunk.
    String key(16, 'r');
    const FileEncryption::InitVector iv(UInt128{0xabcdef0123456789ULL});

    const size_t chunk_size = 4096;
    const size_t num_threads = 8;
    const size_t plaintext_size = chunk_size * num_threads;
    String plaintext(plaintext_size, '\0');
    for (size_t i = 0; i < plaintext_size; ++i)
        plaintext[i] = static_cast<char>((i * 37 + 11) & 0xFF);

    const String ciphertext = aesCtrEncrypt(key, iv, plaintext);

    String header_str;
    {
        WriteBufferFromString wb(header_str);
        FileEncryption::Header header;
        header.algorithm = FileEncryption::Algorithm::AES_128_CTR;
        header.key_fingerprint = FileEncryption::calculateKeyFingerprint(key);
        header.init_vector = iv;
        header.write(wb);
        wb.finalize();
    }
    ASSERT_EQ(header_str.size(), FileEncryption::Header::kSize);

    ChainedBuffers header_chain;
    {
        auto buf = std::make_shared<OwnedChainedBuffer>(header_str.size());
        memcpy(buf->data(), header_str.data(), header_str.size());
        header_chain.append(ChainedBufferNode{buf, 0, header_str.size(), 0});
    }

    ReaderExecutorDecryptor decryptor;
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

    /// Concurrent: N threads decrypt distinct offsets at once through the same const decryptor.
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

}
