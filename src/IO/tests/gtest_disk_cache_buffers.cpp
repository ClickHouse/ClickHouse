/// Unit grid for the per-range cache-buffer API on `DiskCacheProvider`
/// (`resolve` + `DiskCacheReader` / `DiskCacheWriter` / `CacheView`). Backed by a REAL `FileCache` over a
/// temp dir, mirroring `RealDiskCacheSequentialEvictionKeepsConnection` in
/// `gtest_reader_executor.cpp` (same `ThreadStatus` + `QueryScope` machinery so
/// `FileSegment::reserve` finds a query budget).

#include <IO/DiskCacheProvider.h>
#include <IO/ResidencyIterator.h>
#include <IO/ChainedBuffers.h>
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
#include <latch>
#include <thread>

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

/// Make a contiguous file-level chain of `byte` over `[offset, offset + size)`.
ChainedBuffers makeChain(size_t offset, size_t size, char byte)
{
    ChainedBuffers r;
    auto buf = std::make_shared<OwnedChainedBuffer>(size);
    std::memset(buf->data(), byte, size);
    r.append(ChainedBufferNode{std::move(buf), 0, size, offset});
    return r;
}

/// Write under the production contract: a `claim` over the target must be open -
/// `claim` is the sole role-acquisition site, `write` never adopts a role. Claim the
/// writer's whole range for the duration of one write, as the executor's sync paths do.
size_t claimedWrite(CacheWriter & writer, ChainedBuffers chain)
{
    auto fill_claim = writer.claim(writer.range());
    return writer.write(std::move(chain));
}

/// Flatten a chain's bytes (in logical order) into a string for comparison.
std::string flatten(const ChainedBuffers & r)
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

    /// A read-only (bypass) provider over `fc` for INSPECTION: its `resolve` runs
    /// `cache->get` only - writer-less misses, no segment creation - so a probe
    /// never mutates the cache (a populating provider's `resolve` allocates).
    std::shared_ptr<DiskCacheProvider> makeReadOnlyProvider(const std::shared_ptr<FileCache> & fc)
    {
        FilesystemCacheSettings cache_settings;
        cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
        cache_settings.boundary_alignment = kSegmentSize;
        cache_settings.read_if_exists_otherwise_bypass = true;
        return std::make_shared<DiskCacheProvider>(fc, cache_settings, /*query_id_=*/String{});
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


/// Test twin of the plan's open step: resolve each cell and collect its miss
/// resolutions - the provider attaches a writer to each (one per cache segment
/// the cell spans). Any resident prefix comes back as a hit, not a miss.
CacheViewPtr openWriters(ICacheProvider & provider, const StoredObject & object,
                         size_t object_file_offset, std::vector<ByteRange> cells)
{
    auto view = std::make_unique<CacheView>();
    for (auto c : cells)
        for (auto & r : provider.resolve(object, object_file_offset, c))
            if (r.kind == ICacheProvider::Resolution::Kind::Miss)
                view->miss_entries.push_back(MissEntry{r.range, std::move(r.writer)});
    return view;
}

StoredObject makeObject(const String & name, size_t size)
{
    return StoredObject{name, name, size};
}

}


/// (a) openWriter over an empty miss range → fill across TWO window-by-window
/// write() calls into ONE held buffer; the segment stays PARTIALLY_DOWNLOADED
/// between calls and the second write appends from the grown cwo (no re-getOrSet);
/// committed() grows; complete() true at the end; a later probeView reports
/// it as a hit and read() returns the written bytes.
TEST_F(DiskCacheBuffers, WriteAcrossWindowsThenHit)
{
    auto provider = makeProvider();
    const size_t object_size = kSegmentSize;        // single segment
    auto object = makeObject("obj_a", object_size);

    auto view_misses = openWriters(*provider, object, /*object_file_offset=*/0, {ByteRange{0, kSegmentSize}}); const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    ASSERT_NE(misses[0].writer, nullptr);
    auto & writer = *misses[0].writer;
    EXPECT_EQ(writer.range().offset, 0u);
    EXPECT_EQ(writer.range().size, kSegmentSize);
    EXPECT_FALSE(writer.complete());

    const size_t half = kSegmentSize / 2;

    // First window: [0, half) of 'A'.
    size_t n1 = claimedWrite(writer, makeChain(0, half, 'A'));
    EXPECT_EQ(n1, half);
    EXPECT_FALSE(writer.complete());
    EXPECT_TRUE(writer.committed().subtract(ByteRange{0, half}).empty());
    EXPECT_FALSE(writer.committed().subtract(ByteRange{0, kSegmentSize}).empty());

    // Second window: [half, segment) of 'B' — appends from the grown cwo.
    size_t n2 = claimedWrite(writer, makeChain(half, kSegmentSize - half, 'B'));
    EXPECT_EQ(n2, kSegmentSize - half);
    EXPECT_TRUE(writer.complete());
    EXPECT_TRUE(writer.committed().subtract(ByteRange{0, kSegmentSize}).empty());

    // read() from the write buffer returns what was written.
    {
        ChainedBuffers got = writer.read(ByteRange{0, kSegmentSize});
        ASSERT_TRUE(got.covers(ByteRange{0, kSegmentSize}));
        std::string s = flatten(got);
        ASSERT_EQ(s.size(), kSegmentSize);
        EXPECT_EQ(s.substr(0, half), std::string(half, 'A'));
        EXPECT_EQ(s.substr(half), std::string(kSegmentSize - half, 'B'));
    }

    // Drop the writer so the segment finalizes (holder reset → DOWNLOADED on full fill).
    view_misses->miss_entries.clear();

    // probeView now reports the whole range as a hit.
    auto view = probeView(*provider, object, /*object_file_offset=*/0, ByteRange{0, kSegmentSize});
    ASSERT_TRUE(view->allHit());
    ASSERT_EQ(view->hits().size(), 1u);
    const auto & hit = view->hits()[0];
    EXPECT_EQ(hit.range.offset, 0u);
    EXPECT_EQ(hit.range.size, kSegmentSize);
    ASSERT_NE(hit.reader, nullptr);

    ChainedBuffers got = hit.reader->read(ByteRange{0, kSegmentSize});
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

    auto view_misses = openWriters(*provider, object, 0, {ByteRange{0, kSegmentSize}}); const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    size_t n1 = claimedWrite(writer, makeChain(0, kSegmentSize, 'X'));
    EXPECT_EQ(n1, kSegmentSize);
    EXPECT_TRUE(writer.complete());

    // Re-write the same range: append-only at a now-exhausted cwo → 0 bytes.
    size_t n2 = claimedWrite(writer, makeChain(0, kSegmentSize, 'Y'));
    EXPECT_EQ(n2, 0u);
    EXPECT_TRUE(writer.committed().subtract(ByteRange{0, kSegmentSize}).empty());

    // Overlapping re-write also lands nothing new.
    size_t n3 = claimedWrite(writer, makeChain(0, kSegmentSize / 2, 'Z'));
    EXPECT_EQ(n3, 0u);
}


/// (c) write a ChainedBuffers starting past the segment cwo (a gap at the front) → only the
/// contiguous prefix lands; the rest is left for a later write (no throw).
TEST_F(DiskCacheBuffers, GapAtFrontWritesOnlyContiguousPrefix)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_c", kSegmentSize);

    auto view_misses = openWriters(*provider, object, 0, {ByteRange{0, kSegmentSize}}); const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    const size_t quarter = kSegmentSize / 4;

    // Data starts at `quarter` while cwo is still 0 → nothing contiguous lands.
    size_t n0 = claimedWrite(writer, makeChain(quarter, quarter, 'G'));
    EXPECT_EQ(n0, 0u);
    EXPECT_FALSE(writer.complete());
    // The WHOLE segment is still uncommitted: the single uncovered sub-range
    // spans the full range.
    ASSERT_EQ(writer.committed().subtract(ByteRange{0, kSegmentSize}).size(), 1u);
    EXPECT_EQ(writer.committed().subtract(ByteRange{0, kSegmentSize})[0].size, kSegmentSize);

    // Now write the front prefix; it lands and advances cwo.
    size_t n1 = claimedWrite(writer, makeChain(0, quarter, 'H'));
    EXPECT_EQ(n1, quarter);
    EXPECT_TRUE(writer.committed().subtract(ByteRange{0, quarter}).empty());

    // The earlier gap can now be filled contiguously.
    size_t n2 = claimedWrite(writer, makeChain(quarter, kSegmentSize - quarter, 'I'));
    EXPECT_EQ(n2, kSegmentSize - quarter);
    EXPECT_TRUE(writer.complete());
}


/// (d) the residency probe (`lookAt`) is READ-ONLY: over an uncached range it creates NO
/// segments (a later getOrSet/openWriter sees them still empty); misses
/// carry writer==nullptr and cache-aligned ranges.
TEST_F(DiskCacheBuffers, ProbeIsReadOnly)
{
    auto provider = makeProvider();
    auto ro = makeReadOnlyProvider(cache);
    const size_t object_size = 3 * kSegmentSize;
    auto object = makeObject("obj_d", object_size);

    // A read-only provider observes without allocating (a populating provider's
    // resolve would open writers + create segments here). Probe a sub-range
    // unaligned on both ends within the object.
    auto view = probeView(*ro, object, 0, ByteRange{kSegmentSize / 2, kSegmentSize});
    EXPECT_TRUE(view->allMiss());
    ASSERT_FALSE(view->misses().empty());
    for (const auto & m : view->misses())
    {
        EXPECT_EQ(m.writer, nullptr);
        // Cache-aligned to the boundary.
        EXPECT_EQ(m.range.offset % kSegmentSize, 0u);
        EXPECT_EQ(m.range.size % kSegmentSize, 0u);
    }

    // Read-only: a subsequent openWriter over the same aligned range sees a
    // fresh EMPTY segment (the probe did not create or fill anything).
    auto view_misses = openWriters(*provider, object, 0, {ByteRange{0, kSegmentSize}}); const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;
    EXPECT_FALSE(writer.complete());
    // The WHOLE segment is still uncommitted: the single uncovered sub-range
    // spans the full range.
    ASSERT_EQ(writer.committed().subtract(ByteRange{0, kSegmentSize}).size(), 1u);
    EXPECT_EQ(writer.committed().subtract(ByteRange{0, kSegmentSize})[0].size, kSegmentSize);
    // It is genuinely empty: a full write succeeds entirely.
    EXPECT_EQ(claimedWrite(writer, makeChain(0, kSegmentSize, 'D')), kSegmentSize);
}


/// (e) bypass: a DiskCacheProvider with read_if_exists_otherwise_bypass=true →
/// openWriter returns nullptr; `lookAt` misses carry writer==nullptr.
TEST_F(DiskCacheBuffers, BypassNoWriters)
{
    auto provider = makeProvider(/*bypass=*/true);
    auto object = makeObject("obj_e", kSegmentSize);

    /// The bypass upgrade is a no-op: the miss cells remain, the writers stay null.
    auto view_misses = openWriters(*provider, object, 0, {ByteRange{0, kSegmentSize}}); const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    EXPECT_EQ(misses[0].writer, nullptr);

    auto view = probeView(*provider, object, 0, ByteRange{0, kSegmentSize});
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

    auto view_misses = openWriters(*provider, object, 0, {ByteRange{0, kSegmentSize}}); const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    // EMPTY segment: nothing to pin.
    EXPECT_EQ(writer.pin(0), nullptr);

    // Partially fill so a committed prefix exists and the segment stays PARTIAL.
    const size_t half = kSegmentSize / 2;
    ASSERT_EQ(claimedWrite(writer, makeChain(0, half, 'P')), half);

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
    ASSERT_EQ(claimedWrite(writer, makeChain(half, kSegmentSize - half, 'Q')), kSegmentSize - half);
    EXPECT_TRUE(writer.complete());
    pin_start.reset();
    pin_mid.reset();

    // Finalize the buffer → the segment becomes DOWNLOADED and now probes as a
    // hit (no miss, no writer - a resident range is served by a reader).
    view_misses->miss_entries.clear();
    auto view = probeView(*provider, object, 0, ByteRange{0, kSegmentSize});
    EXPECT_TRUE(view->allHit());
}


/// (g) the hit tracks the cwo: after a partial write, a fresh probe reports the
/// committed prefix as the hit and its reader serves it in full.
TEST_F(DiskCacheBuffers, HitTracksPartialWrite)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_g", kSegmentSize);

    auto view_misses = openWriters(*provider, object, 0, {ByteRange{0, kSegmentSize}}); const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    const size_t quarter = kSegmentSize / 4;     // sub-segment partial fill
    ASSERT_EQ(claimedWrite(writer, makeChain(0, quarter, 'R')), quarter);

    // A read-only view over the same range reports the committed prefix as a hit.
    // The provider must outlive the view: its readers hold raw pointers into the
    // provider's `reader_anchors` / `streaming_slot` (as in production, where the
    // pipeline owns the provider across every read window).
    auto ro = makeReadOnlyProvider(cache);
    auto view = probeView(*ro, object, 0, ByteRange{0, kSegmentSize});
    ASSERT_FALSE(view->hits().empty());
    const auto & hit = view->hits()[0];
    EXPECT_EQ(hit.range.offset, 0u);
    EXPECT_EQ(hit.range.size, quarter);

    ChainedBuffers got = hit.reader->read(ByteRange{0, quarter});
    ASSERT_TRUE(got.covers(ByteRange{0, quarter}));
    EXPECT_EQ(flatten(got), std::string(quarter, 'R'));

    // The rest of the segment is still a miss: writer==nullptr (read-only view),
    // cache-ALIGNED to the segment boundary, and covering the uncommitted tail.
    ASSERT_FALSE(view->misses().empty());
    EXPECT_EQ(view->misses()[0].writer, nullptr);
    EXPECT_EQ(view->misses()[0].range.offset % kSegmentSize, 0u);
    EXPECT_GE(view->misses()[0].range.end(), kSegmentSize);
}


/// RM1 pin: a SECOND writer opened for the same cell while the first is gone
/// mid-fill (plan restart) continues at the live committed frontier - `getOrSet`
/// returns the existing PARTIALLY-filled segment, `write` appends at `cwo`, and
/// the segment completes across the two writer lifetimes. This is the coupling
/// the request map's demand-shaped tiles rely on: attaching to a partial
/// segment needs no cell<->extent exact match, only the claim and the frontier.
TEST_F(DiskCacheBuffers, SecondWriterContinuesFromCommittedFrontier)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_rm1a", kSegmentSize);

    const size_t half = kSegmentSize / 2;
    auto first = openWriters(*provider, object, 0, {ByteRange{0, kSegmentSize}});
    ASSERT_EQ(claimedWrite(*first->misses()[0].writer, makeChain(0, half, 'A')), half);

    /// Second writer on the same cell while the first is still HELD: the
    /// provider hands back the same segment; writes serialize via claims.
    auto second = openWriters(*provider, object, 0, {ByteRange{0, kSegmentSize}});
    auto & writer2 = *second->misses()[0].writer;
    ASSERT_EQ(claimedWrite(writer2, makeChain(half, kSegmentSize - half, 'B')), kSegmentSize - half);
    /// `complete()` is WRITER-LOCAL by design (its own committed ledger vs its
    /// cell), so a continuation writer never reports the sibling's prefix; the
    /// segment truth is the probe's (`allHit` below). Nothing in the engine
    /// gates on `complete()`.
    EXPECT_FALSE(writer2.complete());

    first->miss_entries.clear();
    second->miss_entries.clear();

    auto view = probeView(*provider, object, 0, ByteRange{0, kSegmentSize});
    ASSERT_TRUE(view->allHit());
    ChainedBuffers got = view->hits()[0].reader->read(ByteRange{0, kSegmentSize});
    ASSERT_TRUE(got.covers(ByteRange{0, kSegmentSize}));
    std::string bytes = flatten(got);
    EXPECT_EQ(bytes.substr(0, half), std::string(half, 'A'));
    EXPECT_EQ(bytes.substr(half), std::string(kSegmentSize - half, 'B'));
}

/// A FRESH claim whose range covers an already-committed prefix of a
/// PARTIALLY_DOWNLOADED segment must return that prefix as `available` (read from
/// cache) and put ONLY the missing tail in `to_fetch` - never refetch the cached
/// prefix from the source. Crucially `available` is NOT `sibling_led`, so it carries
/// no contention meaning. This is the concurrent-populate race: one reader commits a
/// prefix and releases, then a reader whose plan predates the commit claims the range.
TEST_F(DiskCacheBuffers, FreshClaimServesCommittedPrefixInsteadOfRefetching)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_partial_claim", kSegmentSize);

    const size_t half = kSegmentSize / 2;
    auto view = openWriters(*provider, object, 0, {ByteRange{0, kSegmentSize}});
    ASSERT_EQ(view->misses().size(), 1u);
    auto & writer = *view->misses()[0].writer;
    ASSERT_EQ(writer.range().size, kSegmentSize);

    /// Commit the prefix and release the claim -> PARTIALLY_DOWNLOADED at cwo = half.
    ASSERT_EQ(claimedWrite(writer, makeChain(0, half, 'A')), half);

    /// Fresh claim over the WHOLE range on the now-partial segment.
    auto claim = writer.claim(ByteRange{0, kSegmentSize});

    ASSERT_EQ(claim.available.size(), 1u) << "the committed prefix must be available (cache read)";
    EXPECT_EQ(claim.available[0].offset, 0u);
    EXPECT_EQ(claim.available[0].size, half);

    ASSERT_EQ(claim.to_fetch.size(), 1u) << "only the missing tail may be fetched";
    EXPECT_EQ(claim.to_fetch[0].offset, half);
    EXPECT_EQ(claim.to_fetch[0].size, kSegmentSize - half);

    EXPECT_TRUE(claim.sibling_led.empty()) << "our own committed prefix is not contention";

    /// The won role still fills the tail; the segment completes across the two claims.
    ASSERT_EQ(writer.write(makeChain(half, kSegmentSize - half, 'B')), kSegmentSize - half);
}

/// RM1 pin: while a sibling HOLDS the claim, a second writer's write is a
/// graceful no-op (role not adopted, nothing thrown, 0 bytes); once the claim
/// releases, the same writer object continues from the frontier.
TEST_F(DiskCacheBuffers, SecondWriterYieldsToHeldClaimThenContinues)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_rm1b", kSegmentSize);

    const size_t half = kSegmentSize / 2;
    auto first = openWriters(*provider, object, 0, {ByteRange{0, kSegmentSize}});
    auto & writer1 = *first->misses()[0].writer;

    auto second = openWriters(*provider, object, 0, {ByteRange{0, kSegmentSize}});
    auto & writer2 = *second->misses()[0].writer;

    /// Claims are identity-keyed per THREAD, so the sibling must be one: it
    /// claims, commits the prefix, HOLDS the claim until released, then exits.
    std::latch prefix_committed(1);
    std::latch release_sibling(1);
    std::thread sibling([&]
    {
        auto sibling_claim = writer1.claim(writer1.range());
        ASSERT_EQ(writer1.write(makeChain(0, half, 'A')), half);
        prefix_committed.count_down();
        release_sibling.wait();
    });

    prefix_committed.wait();
    EXPECT_EQ(claimedWrite(writer2, makeChain(half, kSegmentSize - half, 'B')), 0u)
        << "the role is held by the sibling's open claim - no adoption, no exception";

    release_sibling.count_down();
    sibling.join();

    /// Claim released: the second writer wins the role and completes the segment.
    ASSERT_EQ(claimedWrite(writer2, makeChain(half, kSegmentSize - half, 'B')), kSegmentSize - half);
}

/// (h) a single aligned miss range spanning TWO segments → ONE write buffer fills
/// both, advancing across the segment boundary; complete() flips only once both
/// segments are committed; committed() accumulates over the two intervals; a later
/// probeView reports both as hits and each segment's bytes round-trip.
TEST_F(DiskCacheBuffers, WriteAcrossTwoSegments)
{
    auto provider = makeProvider();
    const size_t object_size = 2 * kSegmentSize;     // two segments
    auto object = makeObject("obj_h", object_size);

    // The cache grid is one segment per cell, so resolving the two-segment range
    // yields one writer per segment (the production shape - `resolve` decomposes
    // a miss onto the cache's own segments).
    auto view_misses = openWriters(*provider, object, 0, {ByteRange{0, 2 * kSegmentSize}}); const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 2u);
    ASSERT_NE(misses[0].writer, nullptr);
    ASSERT_NE(misses[1].writer, nullptr);
    auto & w0 = *misses[0].writer;
    auto & w1 = *misses[1].writer;
    EXPECT_EQ(w0.range().offset, 0u);
    EXPECT_EQ(w0.range().size, kSegmentSize);
    EXPECT_EQ(w1.range().offset, kSegmentSize);
    EXPECT_EQ(w1.range().size, kSegmentSize);

    // Fill each segment's writer; each completes independently at its boundary.
    EXPECT_EQ(claimedWrite(w0, makeChain(0, kSegmentSize, 'A')), kSegmentSize);
    EXPECT_TRUE(w0.complete());
    EXPECT_FALSE(w1.complete());
    EXPECT_EQ(claimedWrite(w1, makeChain(kSegmentSize, kSegmentSize, 'B')), kSegmentSize);
    EXPECT_TRUE(w1.complete());
    EXPECT_TRUE(w0.committed().subtract(ByteRange{0, kSegmentSize}).empty());
    EXPECT_TRUE(w1.committed().subtract(ByteRange{kSegmentSize, kSegmentSize}).empty());

    // Finalize → both segments DOWNLOADED.
    view_misses->miss_entries.clear();

    // Both segments are hits and the bytes round-trip per segment.
    auto view = probeView(*provider, object, 0, ByteRange{0, 2 * kSegmentSize});
    ASSERT_TRUE(view->allHit());
    ASSERT_FALSE(view->hits().empty());

    auto read_segment = [&](size_t off, char byte)
    {
        ChainedBuffers acc;
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
/// offset commits the file-level range; a probeView returns a hit whose
/// range is file-level. A second, partially-uncached object-with-offset yields a
/// MISS that is file-level and cache-aligned in FILE space relative to the offset.
TEST_F(DiskCacheBuffers, WriteWithObjectFileOffset)
{
    auto provider = makeProvider();

    // Object occupies file range [kSegmentSize, 2*kSegmentSize): one segment.
    const size_t object_file_offset = kSegmentSize;
    auto object = makeObject("obj_i", kSegmentSize);

    auto view_misses = openWriters(*provider, 
        object, object_file_offset, {ByteRange{kSegmentSize, kSegmentSize}}); const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    ASSERT_NE(misses[0].writer, nullptr);
    auto & writer = *misses[0].writer;
    EXPECT_EQ(writer.range().offset, kSegmentSize);
    EXPECT_EQ(writer.range().size, kSegmentSize);

    // Write at the FILE-LEVEL offset; the file-level committed interval lands.
    size_t n = claimedWrite(writer, makeChain(kSegmentSize, kSegmentSize, 'F'));
    EXPECT_EQ(n, kSegmentSize);
    EXPECT_TRUE(writer.complete());
    EXPECT_TRUE(writer.committed().subtract(ByteRange{kSegmentSize, kSegmentSize}).empty());

    view_misses->miss_entries.clear();

    // probeView returns a hit whose range is the file-level segment. One read-only
    // provider, held for the whole test so its reader-anchored views stay valid.
    auto ro = makeReadOnlyProvider(cache);
    auto view = probeView(
        *ro, object, object_file_offset, ByteRange{kSegmentSize, kSegmentSize});
    ASSERT_TRUE(view->allHit());
    ASSERT_EQ(view->hits().size(), 1u);
    const auto & hit = view->hits()[0];
    EXPECT_EQ(hit.range.offset, kSegmentSize);
    EXPECT_EQ(hit.range.size, kSegmentSize);
    ASSERT_NE(hit.reader, nullptr);

    ChainedBuffers got = hit.reader->read(ByteRange{kSegmentSize, kSegmentSize});
    ASSERT_TRUE(got.covers(ByteRange{kSegmentSize, kSegmentSize}));
    EXPECT_EQ(flatten(got), std::string(kSegmentSize, 'F'));

    // Sub-check: a PARTIALLY-uncached object-with-offset. The object occupies file
    // range [kSegmentSize, 3*kSegmentSize): two segments. Fill only the first; the
    // second is a miss whose range is file-level and cache-aligned in FILE space
    // relative to the offset.
    auto object2 = makeObject("obj_i2", 2 * kSegmentSize);
    auto view_misses2 = openWriters(*provider, 
        object2, object_file_offset, {ByteRange{kSegmentSize, kSegmentSize}}); const auto & misses2 = view_misses2->misses();
    ASSERT_EQ(misses2.size(), 1u);
    ASSERT_NE(misses2[0].writer, nullptr);
    ASSERT_EQ(claimedWrite(*misses2[0].writer, makeChain(kSegmentSize, kSegmentSize, 'G')), kSegmentSize);
    view_misses2->miss_entries.clear();

    auto view2 = probeView(
        *ro, object2, object_file_offset, ByteRange{kSegmentSize, 2 * kSegmentSize});
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
/// reflects only the downloaded prefix (cwo = half), so a later probeView
/// reports ONLY the written half as a hit and the uncommitted remainder as a miss.
TEST_F(DiskCacheBuffers, PartialFillFinalizationShrinks)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_j", kSegmentSize);

    auto view_misses = openWriters(*provider, object, 0, {ByteRange{0, kSegmentSize}}); const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    const size_t half = kSegmentSize / 2;
    ASSERT_EQ(claimedWrite(writer, makeChain(0, half, 'S')), half);
    EXPECT_FALSE(writer.complete());

    // Drop the writer → the held holder finalizes the partial segment as the last
    // owner; only the downloaded prefix (half) is committed/resident.
    view_misses->miss_entries.clear();

    auto ro = makeReadOnlyProvider(cache);
    auto view = probeView(*ro, object, 0, ByteRange{0, kSegmentSize});

    // The hit reflects only the downloaded half, NOT the whole segment.
    ASSERT_FALSE(view->hits().empty());
    const auto & hit = view->hits()[0];
    EXPECT_EQ(hit.range.offset, 0u);
    EXPECT_EQ(hit.range.size, half);
    ASSERT_NE(hit.reader, nullptr);
    ChainedBuffers got = hit.reader->read(ByteRange{0, half});
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
        auto view_misses = openWriters(*provider, object, 0, {ByteRange{0, kSegmentSize}}); const auto & misses = view_misses->misses();
        ASSERT_EQ(misses.size(), 1u);
        ASSERT_EQ(claimedWrite(*misses[0].writer, makeChain(0, kSegmentSize, 'K')), kSegmentSize);
    }

    auto view = probeView(*provider, object, 0, ByteRange{0, kSegmentSize});
    ASSERT_TRUE(view->allHit());
    ASSERT_FALSE(view->hits().empty());

    // Record ranges for the deferred bump.
    ChainedBuffers got = view->hits()[0].reader->read(ByteRange{0, kSegmentSize});
    ASSERT_TRUE(got.covers(ByteRange{0, kSegmentSize}));

    // Destroying the view runs the deferred bump over the recorded ranges; must not throw.
    EXPECT_NO_THROW(view.reset());
}

/// Regression for the `chassert(!is_last_holder)` abort (seen in
/// test_reader_executor_metric, reached via `ReaderExecutor::seek` tearing down the read
/// plan on a thread other than the one that claimed). A prefetch worker claims a cold
/// segment - `claim` moves it to DOWNLOADING, downloader = the worker thread - but the
/// fetch that would fill it via `write()` is interrupted before reaching this writer, so
/// no `write()` runs for it. The claim's destructor (on the worker = downloader thread)
/// must reset the segment; otherwise it stays DOWNLOADING and, when the foreground tears
/// the plan down on another thread, `~FileSegmentsHolder` -> `FileSegment::complete`
/// cannot reset the foreign downloader and aborts on `chassert(!is_last_holder)` as the
/// sole remaining holder.
///
/// This models that path: claim-and-drop on a worker thread (no `write()`), then destroy
/// the writer on a different (foreground) thread. Without the claim's release resetting
/// the segment, `misses.clear()` aborts.
TEST_F(DiskCacheBuffers, ClaimReleaseMakesForeignThreadTeardownSafe)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_elect", kSegmentSize);

    auto view_misses = openWriters(*provider, object, /*object_file_offset=*/0, {ByteRange{0, kSegmentSize}}); const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    ASSERT_NE(misses[0].writer, nullptr);

    std::thread worker([&]
    {
        auto fill_claim = misses[0].writer->claim(ByteRange{0, kSegmentSize});
        ASSERT_FALSE(fill_claim.to_fetch.empty()) << "the cold segment is led (downloader role won)";
        /// The fetch never reached this writer (interrupted); the claim going out of
        /// scope resets the segment - what the claims' lifetime does in production.
    });
    worker.join();

    /// Tear down on THIS (foreign) thread. The segment was reset by the worker's claim,
    /// so the holder dtor finalizes it cleanly instead of aborting on `chassert(!is_last_holder)`.
    view_misses->miss_entries.clear();
    SUCCEED();
}

/// The claim-scoped release: a nested claim over segments this thread ALREADY leads (a
/// tile write inside a window-long claim) must release NOTHING of the outer claim's - its
/// destructor completes only roles it newly won. The outer claim must survive the nested
/// one and still own the role (`write` under it keeps landing bytes); only the outer
/// claim's drop releases the segment.
TEST_F(DiskCacheBuffers, NestedClaimDoesNotReleaseOuterRoles)
{
    auto provider = makeProvider();
    auto object = makeObject("obj_nested", kSegmentSize);

    auto view_misses = openWriters(*provider, object, /*object_file_offset=*/0, {ByteRange{0, kSegmentSize}}); const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    ASSERT_NE(misses[0].writer, nullptr);
    auto & writer = *misses[0].writer;

    auto outer = writer.claim(ByteRange{0, kSegmentSize});
    ASSERT_FALSE(outer.to_fetch.empty());

    {
        auto nested = writer.claim(ByteRange{0, kSegmentSize});
        /// Already ours: the nested claim still reports the run to-fetch (the caller may
        /// write through it), but wins no new role.
        ASSERT_FALSE(nested.to_fetch.empty());
    }   /// nested drop must NOT release the outer role

    /// The role survived the nested drop: a write under the outer claim still lands.
    String payload(kSegmentSize / 2, 'x');
    ChainedBuffers chain;
    auto block = std::make_shared<OwnedChainedBuffer>(payload.size());
    memcpy(block->data(), payload.data(), payload.size());
    chain.append(ChainedBufferNode{block, 0, payload.size(), 0});
    EXPECT_EQ(writer.write(std::move(chain)), payload.size())
        << "the outer claim's role must survive the nested claim's drop";
}

/// Virgin miss runs are TILED into max-fill-cell tiles, one MissEntry per cell,
/// so the cells `openWriter` opens coincide with the fetch tail grid.
/// In this fixture boundary == max segment == 4 KiB, so the tile is one
/// segment and a 3-segment virgin probe yields exactly 3 single-segment tiles.
TEST_F(DiskCacheBuffers, VirginMissRunsTileIntoOptimalCells)
{
    auto provider = makeProvider();

    const size_t object_size = 3 * kSegmentSize;
    auto object = makeObject("obj_tile", object_size);

    auto view = probeView(*provider, object, /*object_file_offset=*/0, ByteRange{0, object_size});
    EXPECT_TRUE(view->allMiss());
    ASSERT_EQ(view->misses().size(), 3u);
    for (size_t i = 0; i < 3; ++i)
    {
        EXPECT_EQ(view->misses()[i].range.offset, i * kSegmentSize);
        EXPECT_EQ(view->misses()[i].range.size, kSegmentSize);
    }

    /// resolve attaches one writer per tile inline; each writer's range is its cell.
    for (size_t i = 0; i < 3; ++i)
    {
        ASSERT_NE(view->misses()[i].writer, nullptr);
        EXPECT_EQ(view->misses()[i].writer->range().offset, i * kSegmentSize);
    }
}
