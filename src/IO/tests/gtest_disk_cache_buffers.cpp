/// Unit grid for the NEW per-range cache-buffer API on `DiskCacheProvider`
/// (`planResidencyView` / `openWriteBuffers` + `DiskCacheReadBuffer` /
/// `DiskCacheWriteBuffer` / `DiskCacheView`). Backed by a REAL `FileCache` over a
/// temp dir, mirroring `RealDiskCacheSequentialEvictionKeepsConnection` in
/// `gtest_reader_executor.cpp` (same `ThreadStatus` + `QueryScope` machinery so
/// `FileSegment::reserve` finds a query budget).

#include <IO/DiskCacheProvider.h>
#include <IO/Rope.h>
#include <IO/IntervalSet.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheSettings.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Interpreters/Context.h>
#include <Core/ServerUUID.h>
#include <Common/CurrentThread.h>
#include <Common/QueryScope.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/VectorWithMemoryTracking.h>

#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <cstring>

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

namespace
{

constexpr size_t kSegmentSize = 4 * 1024;

/// Make a contiguous file-level rope of `byte` over `[offset, offset + size)`.
Rope makeRope(size_t offset, size_t size, char byte)
{
    Rope r;
    auto buf = std::make_shared<OwnedRopeBuffer>(size);
    std::memset(buf->data(), byte, size);
    r.append(RopeNode{std::move(buf), 0, size, offset});
    return r;
}

/// Flatten a rope's bytes (in logical order) into a string for comparison.
std::string flatten(const Rope & r)
{
    std::string out;
    for (const auto & node : r.getNodes())
        out.append(node.data(), node.size);
    return out;
}

/// A fully self-contained real-`FileCache` fixture per test case.
struct DiskCacheBuffers : public ::testing::Test
{
    std::filesystem::path cache_path;
    std::shared_ptr<FileCache> cache;
    FileCacheOriginInfo origin;

    ThreadStatus * saved_thread = nullptr;
    std::unique_ptr<ThreadStatus> thread_status;
    DB::ContextMutablePtr query_context;
    std::optional<DB::QueryScope> query_scope;

    void SetUp() override
    {
        DB::ServerUUID::setRandomForUnitTests();

        /// `FileSegment::reserve` charges the per-query budget via
        /// `CurrentThread::getQueryId()`, so a real `ThreadStatus` + `QueryScope`
        /// must be in scope. Clear/restore `current_thread` like the existing
        /// `RealDiskCache*` test (the singleton's dtor asserts it owns it).
        saved_thread = DB::current_thread;
        DB::current_thread = nullptr;
        thread_status = std::make_unique<ThreadStatus>();

        Poco::XML::DOMParser dom_parser;
        std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
        Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
        Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
        getMutableContext().context->setConfig(config);

        query_context = DB::Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId("disk_cache_buffers_test");
        query_scope.emplace(DB::QueryScope::create(query_context));

        namespace fs = std::filesystem;
        cache_path = fs::temp_directory_path() / "disk_cache_buffers_cache";
        fs::remove_all(cache_path);
        fs::create_directories(cache_path);

        FileCacheSettings settings;
        settings[FileCacheSetting::path] = cache_path.string();
        settings[FileCacheSetting::max_size] = 1024 * 1024;
        settings[FileCacheSetting::max_elements] = 64;
        settings[FileCacheSetting::max_file_segment_size] = kSegmentSize;
        settings[FileCacheSetting::boundary_alignment] = kSegmentSize;
        settings[FileCacheSetting::load_metadata_asynchronously] = false;
        settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

        cache = std::make_shared<FileCache>("disk_cache_buffers", settings);
        cache->initialize();
        origin = FileCache::getCommonOrigin();
    }

    void TearDown() override
    {
        query_scope.reset();
        query_context.reset();
        cache.reset();
        thread_status.reset();
        DB::current_thread = saved_thread;
        std::filesystem::remove_all(cache_path);
    }

    std::shared_ptr<DiskCacheProvider> makeProvider(bool bypass = false)
    {
        FilesystemCacheSettings cache_settings;
        cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
        cache_settings.boundary_alignment = kSegmentSize;
        cache_settings.read_if_exists_otherwise_bypass = bypass;
        return std::make_shared<DiskCacheProvider>(cache, cache_settings, /*query_id_=*/String{});
    }
};

StoredObject makeObject(const String & name, size_t size)
{
    return StoredObject{name, name, size};
}

}


/// (a) openWriteBuffers over an empty miss range → fill across TWO window-by-window
/// write() calls into ONE held buffer; the segment stays PARTIALLY_DOWNLOADED
/// between calls and the second write appends from the grown cwo (no re-getOrSet);
/// committed() grows; complete() true at the end; a later planResidencyView reports
/// it as a hit and read() returns the written bytes.
TEST_F(DiskCacheBuffers, WriteAcrossWindowsThenHit)
{
    auto provider = makeProvider();
    const size_t object_size = kSegmentSize;        // single segment
    auto object = makeObject("obj_a", object_size);

    auto misses = provider->openWriteBuffers(object, /*object_file_offset=*/0, {ByteRange{0, kSegmentSize}});
    ASSERT_EQ(misses.size(), 1u);
    ASSERT_NE(misses[0].writer, nullptr);
    auto & writer = *misses[0].writer;
    EXPECT_EQ(writer.range().offset, 0u);
    EXPECT_EQ(writer.range().size, kSegmentSize);
    EXPECT_FALSE(writer.complete());

    const size_t half = kSegmentSize / 2;

    // First window: [0, half) of 'A'.
    size_t n1 = writer.write(makeRope(0, half, 'A'));
    EXPECT_EQ(n1, half);
    EXPECT_FALSE(writer.complete());
    EXPECT_TRUE(writer.committed().subtract(ByteRange{0, half}).empty());
    EXPECT_FALSE(writer.committed().subtract(ByteRange{0, kSegmentSize}).empty());

    // Second window: [half, segment) of 'B' — appends from the grown cwo.
    size_t n2 = writer.write(makeRope(half, kSegmentSize - half, 'B'));
    EXPECT_EQ(n2, kSegmentSize - half);
    EXPECT_TRUE(writer.complete());
    EXPECT_TRUE(writer.committed().subtract(ByteRange{0, kSegmentSize}).empty());

    // read() from the write buffer returns what was written.
    {
        Rope got = writer.read(ByteRange{0, kSegmentSize});
        ASSERT_TRUE(got.covers(ByteRange{0, kSegmentSize}));
        std::string s = flatten(got);
        ASSERT_EQ(s.size(), kSegmentSize);
        EXPECT_EQ(s.substr(0, half), std::string(half, 'A'));
        EXPECT_EQ(s.substr(half), std::string(kSegmentSize - half, 'B'));
    }

    // Drop the writer so the segment finalizes (holder reset → DOWNLOADED on full fill).
    misses.clear();

    // planResidencyView now reports the whole range as a hit.
    auto view = provider->planResidencyView(object, /*object_file_offset=*/0, ByteRange{0, kSegmentSize});
    ASSERT_TRUE(view->allHit());
    ASSERT_EQ(view->hits().size(), 1u);
    const auto & hit = view->hits()[0];
    EXPECT_EQ(hit.range.offset, 0u);
    EXPECT_EQ(hit.range.size, kSegmentSize);
    ASSERT_NE(hit.reader, nullptr);
    EXPECT_EQ(hit.reader->readable(), kSegmentSize);

    Rope got = hit.reader->read(ByteRange{0, kSegmentSize});
    ASSERT_TRUE(got.covers(ByteRange{0, kSegmentSize}));
    std::string s = flatten(got);
    EXPECT_EQ(s.substr(0, half), std::string(half, 'A'));
    EXPECT_EQ(s.substr(half), std::string(kSegmentSize - half, 'B'));
}


/// (b) committed-set idempotency: writing the same / overlapping range twice →
/// the second write returns 0 and committed() is unchanged.
TEST_F(DiskCacheBuffers, IdempotentReWriteReturnsZero)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_b", kSegmentSize);

    auto misses = provider->openWriteBuffers(object, 0, {ByteRange{0, kSegmentSize}});
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    size_t n1 = writer.write(makeRope(0, kSegmentSize, 'X'));
    EXPECT_EQ(n1, kSegmentSize);
    EXPECT_TRUE(writer.complete());

    // Re-write the same range: append-only at a now-exhausted cwo → 0 bytes.
    size_t n2 = writer.write(makeRope(0, kSegmentSize, 'Y'));
    EXPECT_EQ(n2, 0u);
    EXPECT_TRUE(writer.committed().subtract(ByteRange{0, kSegmentSize}).empty());

    // Overlapping re-write also lands nothing new.
    size_t n3 = writer.write(makeRope(0, kSegmentSize / 2, 'Z'));
    EXPECT_EQ(n3, 0u);
}


/// (c) write a Rope starting past the segment cwo (a gap at the front) → only the
/// contiguous prefix lands; the rest is left for a later write (no throw).
TEST_F(DiskCacheBuffers, GapAtFrontWritesOnlyContiguousPrefix)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_c", kSegmentSize);

    auto misses = provider->openWriteBuffers(object, 0, {ByteRange{0, kSegmentSize}});
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    const size_t quarter = kSegmentSize / 4;

    // Data starts at `quarter` while cwo is still 0 → nothing contiguous lands.
    size_t n0 = writer.write(makeRope(quarter, quarter, 'G'));
    EXPECT_EQ(n0, 0u);
    EXPECT_FALSE(writer.complete());
    // The WHOLE segment is still uncommitted: the single uncovered sub-range
    // spans the full range.
    ASSERT_EQ(writer.committed().subtract(ByteRange{0, kSegmentSize}).size(), 1u);
    EXPECT_EQ(writer.committed().subtract(ByteRange{0, kSegmentSize})[0].size, kSegmentSize);

    // Now write the front prefix; it lands and advances cwo.
    size_t n1 = writer.write(makeRope(0, quarter, 'H'));
    EXPECT_EQ(n1, quarter);
    EXPECT_TRUE(writer.committed().subtract(ByteRange{0, quarter}).empty());

    // The earlier gap can now be filled contiguously.
    size_t n2 = writer.write(makeRope(quarter, kSegmentSize - quarter, 'I'));
    EXPECT_EQ(n2, kSegmentSize - quarter);
    EXPECT_TRUE(writer.complete());
}


/// (d) planResidencyView is READ-ONLY: over an uncached range it creates NO
/// segments (a later getOrSet/openWriteBuffers sees them still empty); misses
/// carry writer==nullptr and cache-aligned ranges.
TEST_F(DiskCacheBuffers, PlanResidencyViewIsReadOnly)
{
    auto provider = makeProvider();
    const size_t object_size = 3 * kSegmentSize;
    auto object = makeObject("obj_d", object_size);

    // Probe a sub-range that is unaligned on both ends within the object.
    auto view = provider->planResidencyView(object, 0, ByteRange{kSegmentSize / 2, kSegmentSize});
    EXPECT_TRUE(view->allMiss());
    ASSERT_FALSE(view->misses().empty());
    for (const auto & m : view->misses())
    {
        EXPECT_EQ(m.writer, nullptr);
        // Cache-aligned to the boundary.
        EXPECT_EQ(m.range.offset % kSegmentSize, 0u);
        EXPECT_EQ(m.range.size % kSegmentSize, 0u);
    }

    // Read-only: a subsequent openWriteBuffers over the same aligned range sees a
    // fresh EMPTY segment (the probe did not create or fill anything).
    auto misses = provider->openWriteBuffers(object, 0, {ByteRange{0, kSegmentSize}});
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;
    EXPECT_FALSE(writer.complete());
    // The WHOLE segment is still uncommitted: the single uncovered sub-range
    // spans the full range.
    ASSERT_EQ(writer.committed().subtract(ByteRange{0, kSegmentSize}).size(), 1u);
    EXPECT_EQ(writer.committed().subtract(ByteRange{0, kSegmentSize})[0].size, kSegmentSize);
    // It is genuinely empty: a full write succeeds entirely.
    EXPECT_EQ(writer.write(makeRope(0, kSegmentSize, 'D')), kSegmentSize);
}


/// (e) bypass: a DiskCacheProvider with read_if_exists_otherwise_bypass=true →
/// openWriteBuffers returns empty; planResidencyView misses carry writer==nullptr.
TEST_F(DiskCacheBuffers, BypassNoWriters)
{
    auto provider = makeProvider(/*bypass=*/true);
    auto object = makeObject("obj_e", kSegmentSize);

    auto misses = provider->openWriteBuffers(object, 0, {ByteRange{0, kSegmentSize}});
    EXPECT_TRUE(misses.empty());

    auto view = provider->planResidencyView(object, 0, ByteRange{0, kSegmentSize});
    EXPECT_TRUE(view->allMiss());
    for (const auto & m : view->misses())
        EXPECT_EQ(m.writer, nullptr);
}


/// (f) pin(frontier): nullptr for empty / at-boundary / fully-downloaded segments,
/// non-null (a FileSegmentPtr into the held holder) for a partially-downloaded
/// segment with a committed prefix; the pin keeps it non-evictable (use_count
/// reflects the extra owner).
TEST_F(DiskCacheBuffers, PinFrontier)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_f", kSegmentSize);

    auto misses = provider->openWriteBuffers(object, 0, {ByteRange{0, kSegmentSize}});
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    // EMPTY segment: nothing to pin.
    EXPECT_EQ(writer.pin(0), nullptr);

    // Partially fill so a committed prefix exists and the segment stays PARTIAL.
    const size_t half = kSegmentSize / 2;
    ASSERT_EQ(writer.write(makeRope(0, half, 'P')), half);

    // At-boundary frontier (== range().left) → cwo > left holds, so a pin at the
    // segment start is valid; a pin at the committed frontier is also valid since
    // it is still inside the segment.
    auto pin_start = writer.pin(0);
    EXPECT_NE(pin_start, nullptr);
    auto pin_mid = writer.pin(half);
    EXPECT_NE(pin_mid, nullptr);

    // Both frontiers fall in the SAME single segment, so the two pins alias the
    // very same `FileSegment`, and each pin is a real extra owner (holder + pin).
    EXPECT_EQ(pin_start.get(), pin_mid.get());
    EXPECT_GE(std::static_pointer_cast<FileSegment>(pin_start).use_count(), 2L);

    // The pin is a FileSegmentPtr aliased as void; releasing it must not break the
    // buffer. Keep it while completing the fill.
    ASSERT_EQ(writer.write(makeRope(half, kSegmentSize - half, 'Q')), kSegmentSize - half);
    EXPECT_TRUE(writer.complete());
    pin_start.reset();
    pin_mid.reset();

    // Finalize the buffer → segment becomes DOWNLOADED; pin then returns nullptr.
    misses.clear();
    auto view = provider->planResidencyView(object, 0, ByteRange{0, kSegmentSize});
    ASSERT_TRUE(view->allHit());

    // Re-open a writer over the now-resident range: there is nothing to download,
    // so a pin returns nullptr (segment fully downloaded, not PARTIAL).
    auto misses2 = provider->openWriteBuffers(object, 0, {ByteRange{0, kSegmentSize}});
    ASSERT_EQ(misses2.size(), 1u);
    EXPECT_EQ(misses2[0].writer->pin(0), nullptr);
}


/// (g) readable() grows: after a partial write, a read buffer obtained via
/// planResidencyView reflects the new cwo and reads up to it.
TEST_F(DiskCacheBuffers, ReadableGrowsWithPartialWrite)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_g", kSegmentSize);

    auto misses = provider->openWriteBuffers(object, 0, {ByteRange{0, kSegmentSize}});
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    const size_t quarter = kSegmentSize / 4;     // sub-segment partial fill
    ASSERT_EQ(writer.write(makeRope(0, quarter, 'R')), quarter);

    // A view over the same range reports the committed prefix as a hit; its read
    // buffer's readable() reflects the partial cwo.
    auto view = provider->planResidencyView(object, 0, ByteRange{0, kSegmentSize});
    ASSERT_FALSE(view->hits().empty());
    const auto & hit = view->hits()[0];
    EXPECT_EQ(hit.range.offset, 0u);
    EXPECT_EQ(hit.reader->readable(), quarter);

    Rope got = hit.reader->read(ByteRange{0, quarter});
    ASSERT_TRUE(got.covers(ByteRange{0, quarter}));
    EXPECT_EQ(flatten(got), std::string(quarter, 'R'));

    // The rest of the segment is still a miss: writer==nullptr (read-only view),
    // cache-ALIGNED to the segment boundary, and covering the uncommitted tail.
    ASSERT_FALSE(view->misses().empty());
    EXPECT_EQ(view->misses()[0].writer, nullptr);
    EXPECT_EQ(view->misses()[0].range.offset % kSegmentSize, 0u);
    EXPECT_GE(view->misses()[0].range.end(), kSegmentSize);
}


/// (h) a single aligned miss range spanning TWO segments → ONE write buffer fills
/// both, advancing across the segment boundary; complete() flips only once both
/// segments are committed; committed() accumulates over the two intervals; a later
/// planResidencyView reports both as hits and each segment's bytes round-trip.
TEST_F(DiskCacheBuffers, WriteAcrossTwoSegments)
{
    auto provider = makeProvider();
    const size_t object_size = 2 * kSegmentSize;     // two segments
    auto object = makeObject("obj_h", object_size);

    // One aligned miss range covering both segments → one held holder, one writer.
    auto misses = provider->openWriteBuffers(object, 0, {ByteRange{0, 2 * kSegmentSize}});
    ASSERT_EQ(misses.size(), 1u);
    ASSERT_NE(misses[0].writer, nullptr);
    auto & writer = *misses[0].writer;
    EXPECT_EQ(writer.range().offset, 0u);
    EXPECT_EQ(writer.range().size, 2 * kSegmentSize);
    EXPECT_FALSE(writer.complete());

    // First segment only: complete() stays false; committed() spans just segment 0.
    size_t n1 = writer.write(makeRope(0, kSegmentSize, 'A'));
    EXPECT_EQ(n1, kSegmentSize);
    EXPECT_FALSE(writer.complete());
    EXPECT_TRUE(writer.committed().subtract(ByteRange{0, kSegmentSize}).empty());
    EXPECT_FALSE(writer.committed().subtract(ByteRange{0, 2 * kSegmentSize}).empty());

    // Second segment: crosses the boundary, completes the buffer.
    size_t n2 = writer.write(makeRope(kSegmentSize, kSegmentSize, 'B'));
    EXPECT_EQ(n2, kSegmentSize);
    EXPECT_TRUE(writer.complete());
    EXPECT_TRUE(writer.committed().subtract(ByteRange{0, 2 * kSegmentSize}).empty());

    // Finalize → both segments DOWNLOADED.
    misses.clear();

    // Both segments are hits and the bytes round-trip per segment.
    auto view = provider->planResidencyView(object, 0, ByteRange{0, 2 * kSegmentSize});
    ASSERT_TRUE(view->allHit());
    ASSERT_FALSE(view->hits().empty());

    auto read_segment = [&](size_t off, char byte)
    {
        Rope acc;
        for (const auto & hit : view->hits())
            acc.append(hit.reader->read(ByteRange{off, kSegmentSize}));
        ASSERT_TRUE(acc.covers(ByteRange{off, kSegmentSize}));
        std::string s = flatten(acc);
        ASSERT_EQ(s.size(), kSegmentSize);
        EXPECT_EQ(std::memcmp(s.data(), std::string(kSegmentSize, byte).data(), kSegmentSize), 0);
    };
    read_segment(0, 'A');
    read_segment(kSegmentSize, 'B');
}


/// (i) object at a non-zero file offset: all public ByteRanges are FILE-LEVEL while
/// the cache keys object-local. Writing the object's only segment at its file-level
/// offset commits the file-level range; a planResidencyView returns a hit whose
/// range is file-level. A second, partially-uncached object-with-offset yields a
/// MISS that is file-level and cache-aligned in FILE space relative to the offset.
TEST_F(DiskCacheBuffers, WriteWithObjectFileOffset)
{
    auto provider = makeProvider();

    // Object occupies file range [kSegmentSize, 2*kSegmentSize): one segment.
    const size_t object_file_offset = kSegmentSize;
    auto object = makeObject("obj_i", kSegmentSize);

    auto misses = provider->openWriteBuffers(
        object, object_file_offset, {ByteRange{kSegmentSize, kSegmentSize}});
    ASSERT_EQ(misses.size(), 1u);
    ASSERT_NE(misses[0].writer, nullptr);
    auto & writer = *misses[0].writer;
    EXPECT_EQ(writer.range().offset, kSegmentSize);
    EXPECT_EQ(writer.range().size, kSegmentSize);

    // Write at the FILE-LEVEL offset; the file-level committed interval lands.
    size_t n = writer.write(makeRope(kSegmentSize, kSegmentSize, 'F'));
    EXPECT_EQ(n, kSegmentSize);
    EXPECT_TRUE(writer.complete());
    EXPECT_TRUE(writer.committed().subtract(ByteRange{kSegmentSize, kSegmentSize}).empty());

    misses.clear();

    // planResidencyView returns a hit whose range is the file-level segment.
    auto view = provider->planResidencyView(
        object, object_file_offset, ByteRange{kSegmentSize, kSegmentSize});
    ASSERT_TRUE(view->allHit());
    ASSERT_EQ(view->hits().size(), 1u);
    const auto & hit = view->hits()[0];
    EXPECT_EQ(hit.range.offset, kSegmentSize);
    EXPECT_EQ(hit.range.size, kSegmentSize);
    ASSERT_NE(hit.reader, nullptr);

    Rope got = hit.reader->read(ByteRange{kSegmentSize, kSegmentSize});
    ASSERT_TRUE(got.covers(ByteRange{kSegmentSize, kSegmentSize}));
    EXPECT_EQ(flatten(got), std::string(kSegmentSize, 'F'));

    // Sub-check: a PARTIALLY-uncached object-with-offset. The object occupies file
    // range [kSegmentSize, 3*kSegmentSize): two segments. Fill only the first; the
    // second is a miss whose range is file-level and cache-aligned in FILE space
    // relative to the offset.
    auto object2 = makeObject("obj_i2", 2 * kSegmentSize);
    auto misses2 = provider->openWriteBuffers(
        object2, object_file_offset, {ByteRange{kSegmentSize, kSegmentSize}});
    ASSERT_EQ(misses2.size(), 1u);
    ASSERT_NE(misses2[0].writer, nullptr);
    ASSERT_EQ(misses2[0].writer->write(makeRope(kSegmentSize, kSegmentSize, 'G')), kSegmentSize);
    misses2.clear();

    auto view2 = provider->planResidencyView(
        object2, object_file_offset, ByteRange{kSegmentSize, 2 * kSegmentSize});
    ASSERT_FALSE(view2->allHit());
    ASSERT_FALSE(view2->misses().empty());
    const auto & miss = view2->misses()[0];
    EXPECT_EQ(miss.writer, nullptr);
    // File-level range: the uncached tail is the SECOND file segment.
    EXPECT_EQ(miss.range.offset, 2 * kSegmentSize);
    // Cache-aligned in FILE space relative to the object's file offset.
    EXPECT_EQ((miss.range.offset - object_file_offset) % kSegmentSize, 0u);
    EXPECT_EQ(miss.range.size % kSegmentSize, 0u);
}


/// (j) partial fill then finalize: write only the first half of a segment, then drop
/// the writer so the held holder finalizes (last owner). The finalized segment
/// reflects only the downloaded prefix (cwo = half), so a later planResidencyView
/// reports ONLY the written half as a hit and the uncommitted remainder as a miss.
TEST_F(DiskCacheBuffers, PartialFillFinalizationShrinks)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_j", kSegmentSize);

    auto misses = provider->openWriteBuffers(object, 0, {ByteRange{0, kSegmentSize}});
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    const size_t half = kSegmentSize / 2;
    ASSERT_EQ(writer.write(makeRope(0, half, 'S')), half);
    EXPECT_FALSE(writer.complete());

    // Drop the writer → the held holder finalizes the partial segment as the last
    // owner; only the downloaded prefix (half) is committed/resident.
    misses.clear();

    auto view = provider->planResidencyView(object, 0, ByteRange{0, kSegmentSize});

    // The hit reflects only the downloaded half, NOT the whole segment.
    ASSERT_FALSE(view->hits().empty());
    const auto & hit = view->hits()[0];
    EXPECT_EQ(hit.range.offset, 0u);
    EXPECT_EQ(hit.range.size, half);
    ASSERT_NE(hit.reader, nullptr);
    EXPECT_EQ(hit.reader->readable(), half);
    Rope got = hit.reader->read(ByteRange{0, half});
    ASSERT_TRUE(got.covers(ByteRange{0, half}));
    EXPECT_EQ(flatten(got), std::string(half, 'S'));

    // The uncommitted remainder is a miss (cache-aligned, writer-null read-only view).
    ASSERT_FALSE(view->misses().empty());
    const auto & miss = view->misses()[0];
    EXPECT_EQ(miss.writer, nullptr);
    EXPECT_EQ(miss.range.offset % kSegmentSize, 0u);
    EXPECT_GE(miss.range.end(), kSegmentSize);
}


/// (k) the deferred LRU bump runs in the CacheView destructor. After a read records
/// ranges on the view, explicitly destroying the view re-fetches and bumps those
/// segments — it must not throw (e.g. on a partially-downloaded / gone segment).
TEST_F(DiskCacheBuffers, DeferredBumpOnViewDestroyDoesNotThrow)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_k", kSegmentSize);

    // Fully fill the segment so a later view sees a hit to read + bump.
    {
        auto misses = provider->openWriteBuffers(object, 0, {ByteRange{0, kSegmentSize}});
        ASSERT_EQ(misses.size(), 1u);
        ASSERT_EQ(misses[0].writer->write(makeRope(0, kSegmentSize, 'K')), kSegmentSize);
    }

    auto view = provider->planResidencyView(object, 0, ByteRange{0, kSegmentSize});
    ASSERT_TRUE(view->allHit());
    ASSERT_FALSE(view->hits().empty());

    // Record ranges for the deferred bump.
    Rope got = view->hits()[0].reader->read(ByteRange{0, kSegmentSize});
    ASSERT_TRUE(got.covers(ByteRange{0, kSegmentSize}));

    // Destroying the view runs the deferred bump over the recorded ranges; must not throw.
    EXPECT_NO_THROW(view.reset());
}
