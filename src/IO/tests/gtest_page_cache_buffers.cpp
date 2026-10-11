#include <IO/PageCacheProvider.h>
#include <IO/ChainedBuffers.h>
#include <Common/PageCache.h>

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

using namespace DB;

namespace
{

/// Test-local twins of the executor's residency-walk collectors. The real
/// `ResidencyIterator` / `probeView` land with the executor cache-chain in a
/// later PR; here `probeView` just buckets a provider's `resolve` output into
/// hits and misses (the provider itself coalesces contiguous cached blocks).
struct HitEntry { ByteRange range; CacheReaderPtr reader; };
struct MissEntry { ByteRange range; CacheWriterPtr writer; };

class CacheView
{
public:
    const std::vector<HitEntry> & hits() const { return hit_entries; }
    const std::vector<MissEntry> & misses() const { return miss_entries; }
    std::vector<HitEntry> hit_entries;
    std::vector<MissEntry> miss_entries;
};
using CacheViewPtr = std::unique_ptr<CacheView>;

/// Read-only one-shot: bucket the provider's ranged `resolve` output (hits with
/// readers, misses with writers when it populates) into a view for assertions.
CacheViewPtr probeView(
    ICacheProvider & provider, const StoredObject & object, size_t object_file_offset, ByteRange span)
{
    auto view = std::make_unique<CacheView>();
    for (auto & r : provider.resolve(object, object_file_offset, span))
    {
        if (r.kind == ICacheProvider::CacheResolution::Kind::Hit)
            view->hit_entries.push_back(HitEntry{r.range, std::move(r.reader)});
        else if (r.kind == ICacheProvider::CacheResolution::Kind::Miss)
            view->miss_entries.push_back(MissEntry{r.range, std::move(r.writer)});
    }
    return view;
}

/// Test twin of the plan's open step: resolve each cell and collect its miss
/// resolutions - the provider attaches a writer to each (one per cache block
/// the cell spans). Any resident prefix comes back as a hit, not a miss.
CacheViewPtr openWriters(ICacheProvider & provider, const StoredObject & object,
                         size_t object_file_offset, std::vector<ByteRange> cells)
{
    auto view = std::make_unique<CacheView>();
    for (auto c : cells)
        for (auto & r : provider.resolve(object, object_file_offset, c))
            if (r.kind == ICacheProvider::CacheResolution::Kind::Miss)
                view->miss_entries.push_back(MissEntry{r.range, std::move(r.writer)});
    return view;
}

/// `min_size_in_bytes` is the initial per-shard capacity. Tests don't call
/// `autoResize`, so set it equal to `max_size_in_bytes` so the shard can store
/// entries from the get-go.
PageCachePtr makeCache(size_t capacity = (1ull << 20))
{
    return std::make_shared<PageCache>(
        std::chrono::milliseconds(2000), "LRU", 0.5,
        /*min_size_in_bytes=*/capacity,
        /*max_size_in_bytes=*/capacity,
        /*free_memory_ratio=*/0.0,
        /*num_shards=*/1);
}

PageCacheFile makeFile(const std::string & path)
{
    PageCacheFile file;
    file.path = path;
    file.file_version = "v1";
    return file;
}

/// Build a single-node ChainedBuffers of `size` bytes filled with `fill`, logically at
/// `[offset, offset + size)`.
ChainedBuffers makeChain(size_t offset, size_t size, char fill)
{
    ChainedBuffers chain;
    auto buf = std::make_shared<OwnedChainedBuffer>(size);
    std::memset(buf->data(), fill, size);
    chain.append(ChainedBufferNode{buf, 0, size, offset});
    return chain;
}

/// FillRole the writer's range then write - the page cache has no downloader role, so its default
/// role always authorizes; mirrors how the executor drives a write under a held role.
size_t claimedWrite(CacheWriter & writer, ChainedBuffers chain)
{
    auto role = writer.takeFillRole();
    if (!role)
        return 0;
    return writer.write(std::move(chain), role);
}

/// Flatten `chain`'s coverage of `[offset, offset + size)` into a std::string,
/// asserting full coverage first.
std::string flatten(const ChainedBuffers & chain, size_t offset, size_t size)
{
    EXPECT_TRUE(chain.covers(ByteRange{offset, size}));
    std::string out(size, '\0');
    chain.copyTo(out.data(), ByteRange{offset, size});
    return out;
}

}

/// (a) openWriter over a miss range, write a whole block => complete() true,
/// committed() spans the range, probeView afterward reports a hit, and
/// read() round-trips the bytes.
TEST(PageCacheBuffers, WriteWholeBlockThenHit)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-write-whole-block");
    constexpr size_t block_size = 4096;
    PageCacheProvider provider(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);

    /// One openWriter over the aligned miss block.
    auto view_misses = openWriters(provider, StoredObject{}, 0, {ByteRange{0, block_size}});
    const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    ASSERT_NE(misses[0].writer, nullptr);
    auto & writer = *misses[0].writer;

    EXPECT_FALSE(writer.complete());

    size_t wrote = claimedWrite(writer, makeChain(0, block_size, 'A'));
    EXPECT_EQ(wrote, block_size) << "the whole block newly landed";
    EXPECT_TRUE(writer.complete());

    /// committed() spans the whole range.
    EXPECT_EQ(writer.committed(), block_size);

    /// probeView now reports the block as a hit (cell registered).
    auto view = probeView(provider, StoredObject{}, 0, ByteRange{0, block_size});
    ASSERT_EQ(view->hits().size(), 1u);
    ASSERT_EQ(view->misses().size(), 0u);
    EXPECT_EQ(view->hits()[0].range.offset, 0u);
    EXPECT_EQ(view->hits()[0].range.size, block_size);

    /// The hit read buffer round-trips the written bytes.
    auto chain = view->hits()[0].reader->read(ByteRange{0, block_size});
    EXPECT_EQ(flatten(chain, 0, block_size), std::string(block_size, 'A'));
}

/// (b) The write buffer doubles as a read buffer: after write(), writer.read(sub)
/// returns the written bytes.
TEST(PageCacheBuffers, WriteBufferDoublesAsReadBuffer)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-write-doubles-read");
    constexpr size_t block_size = 4096;
    PageCacheProvider provider(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);

    auto view_misses = openWriters(provider, StoredObject{}, 0, {ByteRange{0, block_size}});
    const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    claimedWrite(writer, makeChain(0, block_size, 'Z'));

    /// Whole-block read back.
    auto whole = writer.read(ByteRange{0, block_size});
    EXPECT_EQ(flatten(whole, 0, block_size), std::string(block_size, 'Z'));

    /// Sub-range read back (zero-copy slice of the same cell).
    auto sub = writer.read(ByteRange{1000, 100});
    EXPECT_EQ(flatten(sub, 1000, 100), std::string(100, 'Z'));
}

/// (c) EOF tail block: a file whose size is not a block multiple => the tail block
/// is short; write the tail, probeView reports it as a hit of the short
/// size, read returns exactly the valid bytes (no past-EOF bytes).
TEST(PageCacheBuffers, EofTailBlockShort)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-eof-tail");
    constexpr size_t block_size = 1024;
    constexpr size_t file_size = 1500;  /// tail block = 476 bytes
    constexpr size_t tail_off = 1024;
    constexpr size_t tail_size = file_size - tail_off;  /// 476
    PageCacheProvider provider(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, file_size);

    /// The aligned miss range for the tail is clamped to the file's real length.
    auto view_misses = openWriters(provider, StoredObject{}, 0, {ByteRange{tail_off, tail_size}});
    const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;
    EXPECT_EQ(writer.range().size, tail_size);

    size_t wrote = claimedWrite(writer, makeChain(tail_off, tail_size, 'T'));
    EXPECT_EQ(wrote, tail_size);
    EXPECT_TRUE(writer.complete());

    /// probeView reports the tail as a hit of the SHORT size.
    auto view = probeView(provider, StoredObject{}, 0, ByteRange{tail_off, block_size});
    ASSERT_EQ(view->hits().size(), 1u);
    EXPECT_EQ(view->hits()[0].range.offset, tail_off);
    EXPECT_EQ(view->hits()[0].range.size, tail_size) << "tail hit sized to valid bytes, not full block";

    /// A read asking for a full block returns only the 476 valid bytes — the cell
    /// physically has no more (no past-EOF region).
    auto chain = view->hits()[0].reader->read(ByteRange{tail_off, block_size});
    EXPECT_EQ(chain.totalBytes(), tail_size);
    EXPECT_EQ(flatten(chain, tail_off, tail_size), std::string(tail_size, 'T'));
}

/// (d) bypass: a provider with bypass_if_missing=true => openWriter returns
/// nullptr; `lookAt` misses carry writer == nullptr; a direct write on a
/// bypass write buffer returns 0 and creates no registered cell.
TEST(PageCacheBuffers, BypassOpensNoWritersAndPopulatesNothing)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-bypass");
    constexpr size_t block_size = 4096;
    PageCacheProvider bypass_provider(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/true, /*file_size_in_bytes=*/block_size);

    /// The bypass upgrade is a no-op: the miss cell remains, the writer stays null.
    auto view_misses = openWriters(bypass_provider, StoredObject{}, 0, {ByteRange{0, block_size}});
    const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    EXPECT_EQ(misses[0].writer, nullptr);

    /// `lookAt` misses carry writer == nullptr (it never opens writers).
    auto view = probeView(bypass_provider, StoredObject{}, 0, ByteRange{0, block_size});
    ASSERT_EQ(view->misses().size(), 1u);
    EXPECT_EQ(view->misses()[0].writer, nullptr);

    /// A direct write on a bypass write buffer returns 0 and registers no cell.
    {
        PageCacheWriter writer(
            cache, file, /*inject_eviction=*/false, /*bypass_if_missing=*/true, ByteRange{0, block_size});
        size_t wrote = claimedWrite(writer, makeChain(0, block_size, 'X'));
        EXPECT_EQ(wrote, 0u);
        EXPECT_FALSE(writer.complete()) << "bypass write commits nothing";
    }

    /// A non-bypass provider on the same file still misses — nothing was registered.
    PageCacheProvider observer(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);
    auto observed = probeView(observer, StoredObject{}, 0, ByteRange{0, block_size});
    ASSERT_EQ(observed->hits().size(), 0u);
    ASSERT_EQ(observed->misses().size(), 1u);
}

/// (e) the residency probe (`lookAt`) is READ-ONLY: over an uncached range it creates no cells
/// (a subsequent probe still reports misses).
TEST(PageCacheBuffers, ProbeIsReadOnly)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-plan-readonly");
    constexpr size_t block_size = 4096;
    PageCacheProvider provider(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);

    auto view1 = probeView(provider, StoredObject{}, 0, ByteRange{0, block_size});
    ASSERT_EQ(view1->hits().size(), 0u);
    ASSERT_EQ(view1->misses().size(), 1u);
    view1.reset();

    /// A second probe still misses — the first probe created no cell.
    auto view2 = probeView(provider, StoredObject{}, 0, ByteRange{0, block_size});
    EXPECT_EQ(view2->hits().size(), 0u);
    ASSERT_EQ(view2->misses().size(), 1u);
}

/// (f) first-writer-wins: pre-populate a block via one write buffer, then a second
/// write buffer write() over the same block returns 0 newly-loaded but marks
/// committed() (and read returns the first writer's bytes).
TEST(PageCacheBuffers, FirstWriterWins)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-first-writer-wins");
    constexpr size_t block_size = 4096;
    PageCacheProvider provider(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);

    /// Open TWO writers over the still-uncached block: page `resolve` uses
    /// `cache->get`, so it creates no cell - both see a miss until a write lands.
    auto view_first = openWriters(provider, StoredObject{}, 0, {ByteRange{0, block_size}});
    const auto & first = view_first->misses();
    ASSERT_EQ(first.size(), 1u);
    auto view_second = openWriters(provider, StoredObject{}, 0, {ByteRange{0, block_size}});
    const auto & second = view_second->misses();
    ASSERT_EQ(second.size(), 1u);

    /// First writer populates the block with 'F'.
    EXPECT_EQ(claimedWrite(*first[0].writer, makeChain(0, block_size, 'F')), block_size);

    /// Second writer tries 'S' over the same block: it loses the race.
    auto & writer = *second[0].writer;
    size_t wrote = claimedWrite(writer, makeChain(0, block_size, 'S'));
    EXPECT_EQ(wrote, 0u) << "lost the first-writer-wins race: nothing newly landed";
    EXPECT_TRUE(writer.complete()) << "the byte IS cached (by the first writer), so committed must advance";
    EXPECT_EQ(writer.committed(), block_size);

    /// read returns the FIRST writer's bytes (the adopted existing cell).
    auto chain = writer.read(ByteRange{0, block_size});
    EXPECT_EQ(flatten(chain, 0, block_size), std::string(block_size, 'F'));
}

/// (g) multi-block write + read-back per block: a range spanning >= 2 blocks resolves to one miss per
/// block, write each, complete() true, each block round-trips (mirrors DiskCache's WriteAcrossTwoSegments).
TEST(PageCacheBuffers, WriteAcrossTwoBlocks)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-two-blocks");
    constexpr size_t block_size = 4096;
    constexpr size_t file_size = 3 * block_size;
    /// Span the first two blocks: [0, 8192).
    constexpr size_t span = 2 * block_size;
    PageCacheProvider provider(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, file_size);

    auto view_misses = openWriters(provider, StoredObject{}, 0, {ByteRange{0, span}});
    const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 2u);
    auto & w0 = *misses[0].writer;
    auto & w1 = *misses[1].writer;
    /// One writer per block (the page grid is one block per cell).
    EXPECT_EQ(w0.range().offset, 0u);
    EXPECT_EQ(w0.range().size, block_size);
    EXPECT_EQ(w1.range().offset, block_size);
    EXPECT_EQ(w1.range().size, block_size);

    /// Fill block 0 with '0', block 1 with '1'; each whole block lands.
    EXPECT_EQ(claimedWrite(w0, makeChain(0, block_size, '0')), block_size);
    EXPECT_TRUE(w0.complete());
    EXPECT_EQ(claimedWrite(w1, makeChain(block_size, block_size, '1')), block_size);
    EXPECT_TRUE(w1.complete());

    /// One resolution per block: each cached block is its own hit, read from its own reader.
    auto view = probeView(provider, StoredObject{}, 0, ByteRange{0, span});
    ASSERT_EQ(view->hits().size(), 2u) << "one hit per cached block";
    ASSERT_EQ(view->misses().size(), 0u);
    EXPECT_EQ(view->hits()[0].range.offset, 0u);
    EXPECT_EQ(view->hits()[0].range.size, block_size);
    EXPECT_EQ(view->hits()[1].range.offset, block_size);
    EXPECT_EQ(view->hits()[1].range.size, block_size);

    auto rope0 = view->hits()[0].reader->read(ByteRange{0, block_size});
    EXPECT_EQ(flatten(rope0, 0, block_size), std::string(block_size, '0'));
    auto rope1 = view->hits()[1].reader->read(ByteRange{block_size, block_size});
    EXPECT_EQ(flatten(rope1, block_size, block_size), std::string(block_size, '1'));
}

/// (h) partial-block write is skipped: write() over a block the data does not fully
/// cover takes the `if (!data.covers(block_range)) continue;` skip — nothing newly
/// lands, complete() stays false, committed() is empty, and no cell is registered.
TEST(PageCacheBuffers, PartialBlockWriteIsSkipped)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-partial-block-skipped");
    constexpr size_t block_size = 4096;
    PageCacheProvider provider(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);

    auto view_misses = openWriters(provider, StoredObject{}, 0, {ByteRange{0, block_size}});
    const auto & misses = view_misses->misses();
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    /// Data covers only the first half of the block — the whole block is not covered.
    size_t wrote = claimedWrite(writer, makeChain(0, block_size / 2, 'P'));
    EXPECT_EQ(wrote, 0u) << "a partially-covered block is left for a later write";
    EXPECT_FALSE(writer.complete());
    EXPECT_EQ(writer.committed(), 0u) << "the whole range is still uncommitted";

    /// No partial cell was registered: a subsequent probe still misses.
    auto view = probeView(provider, StoredObject{}, 0, ByteRange{0, block_size});
    EXPECT_TRUE(view->hits().empty());
    ASSERT_EQ(view->misses().size(), 1u);
}

/// (i) hit/miss interleaving: a 3-block file with ONLY the middle block resident
/// drives the probe's kind-flip coalescing (miss → hit → miss), which the all-hit
/// and all-miss tests never exercise.
TEST(PageCacheBuffers, HitMissInterleavedTiling)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-hit-miss-interleaved");
    constexpr size_t block_size = 4096;
    constexpr size_t file_size = 3 * block_size;
    PageCacheProvider provider(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, file_size);

    /// Populate ONLY the middle block (block 1), leaving blocks 0 and 2 cold.
    {
        auto view_middle = openWriters(provider, StoredObject{}, 0, {ByteRange{block_size, block_size}});
        const auto & middle = view_middle->misses();
        ASSERT_EQ(middle.size(), 1u);
        size_t wrote = claimedWrite(*middle[0].writer, makeChain(block_size, block_size, 'M'));
        EXPECT_EQ(wrote, block_size);
    }

    /// Probe the whole file: miss[0] → hit[1] → miss[2].
    auto view = probeView(provider, StoredObject{}, 0, ByteRange{0, file_size});
    ASSERT_EQ(view->hits().size(), 1u);
    ASSERT_EQ(view->misses().size(), 2u);

    EXPECT_EQ(view->hits()[0].range.offset, block_size);
    EXPECT_EQ(view->hits()[0].range.size, block_size);

    EXPECT_EQ(view->misses()[0].range.offset, 0u);
    EXPECT_EQ(view->misses()[0].range.size, block_size);
    EXPECT_EQ(view->misses()[1].range.offset, 2 * block_size);
    EXPECT_EQ(view->misses()[1].range.size, block_size);

    /// The middle hit reader round-trips the written bytes.
    auto chain = view->hits()[0].reader->read(ByteRange{block_size, block_size});
    EXPECT_EQ(flatten(chain, block_size, block_size), std::string(block_size, 'M'));
}

/// (j) first-writer-wins is keyed by the cache + file identity, not the provider
/// instance: a SECOND provider over the SAME cache and file loses the race against
/// an already-resident block and adopts the first writer's bytes.
TEST(PageCacheBuffers, FirstWriterWinsAcrossProviders)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-first-writer-wins-cross-provider");
    constexpr size_t block_size = 4096;
    PageCacheProvider provider1(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);

    /// A SECOND provider over the SAME cache and file produces the SAME cache key.
    PageCacheProvider provider2(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);

    /// Open a writer through EACH provider over the still-uncached block (page
    /// `resolve` creates no cell, so both see a miss until a write lands).
    auto view_first = openWriters(provider1, StoredObject{}, 0, {ByteRange{0, block_size}});
    const auto & first = view_first->misses();
    ASSERT_EQ(first.size(), 1u);
    auto view_second = openWriters(provider2, StoredObject{}, 0, {ByteRange{0, block_size}});
    const auto & second = view_second->misses();
    ASSERT_EQ(second.size(), 1u);

    /// provider1 populates the block with 'F'.
    EXPECT_EQ(claimedWrite(*first[0].writer, makeChain(0, block_size, 'F')), block_size);

    /// provider2's writer tries 'S' over the same cache key: it loses the race.
    auto & writer = *second[0].writer;
    size_t wrote = claimedWrite(writer, makeChain(0, block_size, 'S'));
    EXPECT_EQ(wrote, 0u) << "lost the cross-provider first-writer-wins race: nothing newly landed";
    EXPECT_TRUE(writer.complete()) << "the byte IS cached (by provider1), so committed must advance";
    EXPECT_EQ(writer.committed(), block_size);

    /// read returns the FIRST provider's bytes (the adopted existing cell).
    auto chain = writer.read(ByteRange{0, block_size});
    EXPECT_EQ(flatten(chain, 0, block_size), std::string(block_size, 'F'));
}

/// (k) takeFillRole re-probes the cache: a block populated by another writer between the openWriter
/// (a read-only `resolve`) and the role is adopted and reported as `available` with NO role, so the
/// caller serves it from cache instead of re-reading it from the source.
TEST(PageCacheBuffers, ClaimLeadRoleAdoptsBlockCachedSinceResolve)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-role-recheck");
    constexpr size_t block_size = 4096;
    PageCacheProvider provider(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);

    /// Two writers over the still-uncached block (both saw a miss at resolve).
    auto view_late = openWriters(provider, StoredObject{}, 0, {ByteRange{0, block_size}});
    const auto & late = view_late->misses();
    ASSERT_EQ(late.size(), 1u);
    auto view_early = openWriters(provider, StoredObject{}, 0, {ByteRange{0, block_size}});
    const auto & early = view_early->misses();
    ASSERT_EQ(early.size(), 1u);

    /// A concurrent writer populates the block with 'C'.
    EXPECT_EQ(claimedWrite(*early[0].writer, makeChain(0, block_size, 'C')), block_size);

    /// The late writer's takeFillRole re-probes: the block is now resident, so it is reported as
    /// available (the whole block) with no role to fill.
    auto & late_writer = *late[0].writer;
    auto role = late_writer.takeFillRole();
    EXPECT_FALSE(static_cast<bool>(role)) << "nothing left to fill: the block is already committed";
    EXPECT_EQ(late_writer.committed(), block_size);

    /// The late writer serves the concurrently-written bytes from cache (adopted its cell).
    auto chain = late_writer.read(ByteRange{0, block_size});
    EXPECT_EQ(flatten(chain, 0, block_size), std::string(block_size, 'C'));
}
