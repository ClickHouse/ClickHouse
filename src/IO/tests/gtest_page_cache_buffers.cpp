#include <IO/PageCacheProvider.h>
#include <IO/Rope.h>
#include <Common/PageCache.h>

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

using namespace DB;

namespace
{

/// `min_size_in_bytes` is the initial per-shard capacity. Tests don't call
/// `autoResize`, so set it equal to `max_size_in_bytes` so the shard can store
/// entries from the get-go (same setup as the existing `PageCacheProvider` tests
/// in `gtest_reader_executor.cpp`).
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

/// Build a single-node Rope of `size` bytes filled with `fill`, logically at
/// `[offset, offset + size)`.
Rope makeRope(size_t offset, size_t size, char fill)
{
    Rope rope;
    auto buf = std::make_shared<OwnedRopeBuffer>(size);
    std::memset(buf->data(), fill, size);
    rope.append(RopeNode{buf, 0, size, offset});
    return rope;
}

/// Flatten `rope`'s coverage of `[offset, offset + size)` into a std::string,
/// asserting full coverage first.
std::string flatten(const Rope & rope, size_t offset, size_t size)
{
    EXPECT_TRUE(rope.covers(ByteRange{offset, size}));
    std::string out(size, '\0');
    rope.copyTo(out.data(), ByteRange{offset, size});
    return out;
}

}

/// (a) openWriteBuffers over a miss range, write a whole block => complete() true,
/// committed() spans the range, planResidencyView afterward reports a hit, and
/// read() round-trips the bytes.
TEST(PageCacheBuffers, WriteWholeBlockThenHit)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-write-whole-block");
    constexpr size_t block_size = 4096;
    PageCacheProvider provider(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);

    /// One openWriteBuffers over the aligned miss block.
    auto misses = provider.openWriteBuffers(StoredObject{}, 0, {ByteRange{0, block_size}});
    ASSERT_EQ(misses.size(), 1u);
    ASSERT_NE(misses[0].writer, nullptr);
    auto & writer = *misses[0].writer;

    EXPECT_FALSE(writer.complete());

    size_t wrote = writer.write(makeRope(0, block_size, 'A'));
    EXPECT_EQ(wrote, block_size) << "the whole block newly landed";
    EXPECT_TRUE(writer.complete());

    /// committed() spans the whole range.
    EXPECT_TRUE(writer.committed().subtract(ByteRange{0, block_size}).empty());

    /// planResidencyView now reports the block as a hit (cell registered).
    auto view = provider.planResidencyView(StoredObject{}, 0, ByteRange{0, block_size});
    ASSERT_EQ(view->hits().size(), 1u);
    ASSERT_EQ(view->misses().size(), 0u);
    EXPECT_EQ(view->hits()[0].range.offset, 0u);
    EXPECT_EQ(view->hits()[0].range.size, block_size);

    /// The hit read buffer round-trips the written bytes.
    auto rope = view->hits()[0].reader->read(ByteRange{0, block_size});
    EXPECT_EQ(flatten(rope, 0, block_size), std::string(block_size, 'A'));
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

    auto misses = provider.openWriteBuffers(StoredObject{}, 0, {ByteRange{0, block_size}});
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    writer.write(makeRope(0, block_size, 'Z'));

    /// Whole-block read back.
    auto whole = writer.read(ByteRange{0, block_size});
    EXPECT_EQ(flatten(whole, 0, block_size), std::string(block_size, 'Z'));

    /// Sub-range read back (zero-copy slice of the same cell).
    auto sub = writer.read(ByteRange{1000, 100});
    EXPECT_EQ(flatten(sub, 1000, 100), std::string(100, 'Z'));
}

/// (c) EOF tail block: a file whose size is not a block multiple => the tail block
/// is short; write the tail, planResidencyView reports it as a hit of the short
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
    auto misses = provider.openWriteBuffers(StoredObject{}, 0, {ByteRange{tail_off, tail_size}});
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;
    EXPECT_EQ(writer.range().size, tail_size);

    size_t wrote = writer.write(makeRope(tail_off, tail_size, 'T'));
    EXPECT_EQ(wrote, tail_size);
    EXPECT_TRUE(writer.complete());

    /// planResidencyView reports the tail as a hit of the SHORT size.
    auto view = provider.planResidencyView(StoredObject{}, 0, ByteRange{tail_off, block_size});
    ASSERT_EQ(view->hits().size(), 1u);
    EXPECT_EQ(view->hits()[0].range.offset, tail_off);
    EXPECT_EQ(view->hits()[0].range.size, tail_size) << "tail hit sized to valid bytes, not full block";

    /// A read asking for a full block returns only the 476 valid bytes — the cell
    /// physically has no more (no past-EOF region).
    auto rope = view->hits()[0].reader->read(ByteRange{tail_off, block_size});
    EXPECT_EQ(rope.totalBytes(), tail_size);
    EXPECT_EQ(flatten(rope, tail_off, tail_size), std::string(tail_size, 'T'));
}

/// (d) bypass: a provider with bypass_if_missing=true => openWriteBuffers returns
/// empty; planResidencyView misses carry writer == nullptr; a direct write on a
/// bypass write buffer returns 0 and creates no registered cell.
TEST(PageCacheBuffers, BypassOpensNoWritersAndPopulatesNothing)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-bypass");
    constexpr size_t block_size = 4096;
    PageCacheProvider bypass_provider(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/true, /*file_size_in_bytes=*/block_size);

    /// openWriteBuffers returns empty.
    auto misses = bypass_provider.openWriteBuffers(StoredObject{}, 0, {ByteRange{0, block_size}});
    EXPECT_TRUE(misses.empty());

    /// planResidencyView misses carry writer == nullptr (it never opens writers).
    auto view = bypass_provider.planResidencyView(StoredObject{}, 0, ByteRange{0, block_size});
    ASSERT_EQ(view->misses().size(), 1u);
    EXPECT_EQ(view->misses()[0].writer, nullptr);

    /// A direct write on a bypass write buffer returns 0 and registers no cell.
    {
        PageCacheWriter writer(
            cache, file, block_size, /*file_size_in_bytes=*/block_size,
            /*inject_eviction=*/false, /*bypass_if_missing=*/true, ByteRange{0, block_size});
        size_t wrote = writer.write(makeRope(0, block_size, 'X'));
        EXPECT_EQ(wrote, 0u);
        EXPECT_FALSE(writer.complete()) << "bypass write commits nothing";
    }

    /// A non-bypass provider on the same file still misses — nothing was registered.
    PageCacheProvider observer(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);
    auto observed = observer.planResidencyView(StoredObject{}, 0, ByteRange{0, block_size});
    ASSERT_EQ(observed->hits().size(), 0u);
    ASSERT_EQ(observed->misses().size(), 1u);
}

/// (e) planResidencyView is READ-ONLY: over an uncached range it creates no cells
/// (a subsequent planResidencyView still reports misses).
TEST(PageCacheBuffers, PlanResidencyViewIsReadOnly)
{
    auto cache = makeCache();
    auto file = makeFile("buffers-plan-readonly");
    constexpr size_t block_size = 4096;
    PageCacheProvider provider(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);

    auto view1 = provider.planResidencyView(StoredObject{}, 0, ByteRange{0, block_size});
    ASSERT_EQ(view1->hits().size(), 0u);
    ASSERT_EQ(view1->misses().size(), 1u);
    view1.reset();

    /// A second probe still misses — the first probe created no cell.
    auto view2 = provider.planResidencyView(StoredObject{}, 0, ByteRange{0, block_size});
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

    /// First writer populates the block with 'F'.
    {
        auto first = provider.openWriteBuffers(StoredObject{}, 0, {ByteRange{0, block_size}});
        ASSERT_EQ(first.size(), 1u);
        size_t wrote = first[0].writer->write(makeRope(0, block_size, 'F'));
        EXPECT_EQ(wrote, block_size);
    }

    /// Second writer tries to write 'S' over the same block.
    auto second = provider.openWriteBuffers(StoredObject{}, 0, {ByteRange{0, block_size}});
    ASSERT_EQ(second.size(), 1u);
    auto & writer = *second[0].writer;

    size_t wrote = writer.write(makeRope(0, block_size, 'S'));
    EXPECT_EQ(wrote, 0u) << "lost the first-writer-wins race: nothing newly landed";
    EXPECT_TRUE(writer.complete()) << "the byte IS cached (by the first writer), so committed must advance";
    EXPECT_TRUE(writer.committed().subtract(ByteRange{0, block_size}).empty());

    /// read returns the FIRST writer's bytes (the adopted existing cell).
    auto rope = writer.read(ByteRange{0, block_size});
    EXPECT_EQ(flatten(rope, 0, block_size), std::string(block_size, 'F'));
}

/// (g) multi-block write + read-back per block: a range spanning >= 2 blocks, one
/// openWriteBuffers, write all, complete() true, each block round-trips (mirrors
/// DiskCache's WriteAcrossTwoSegments).
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

    auto misses = provider.openWriteBuffers(StoredObject{}, 0, {ByteRange{0, span}});
    ASSERT_FALSE(misses.empty());
    auto & writer = *misses[0].writer;
    /// The single writer carries the correct multi-block aligned range.
    EXPECT_EQ(writer.range().offset, 0u);
    EXPECT_EQ(writer.range().size, span);

    /// Two-node rope: block 0 filled '0', block 1 filled '1'.
    Rope data;
    {
        auto b0 = std::make_shared<OwnedRopeBuffer>(block_size);
        std::memset(b0->data(), '0', block_size);
        data.append(RopeNode{b0, 0, block_size, 0});
        auto b1 = std::make_shared<OwnedRopeBuffer>(block_size);
        std::memset(b1->data(), '1', block_size);
        data.append(RopeNode{b1, 0, block_size, block_size});
    }

    size_t wrote = writer.write(std::move(data));
    EXPECT_EQ(wrote, span) << "both whole blocks newly landed";
    EXPECT_TRUE(writer.complete());

    /// planResidencyView coalesces the two adjacent hit blocks into one HitEntry.
    auto view = provider.planResidencyView(StoredObject{}, 0, ByteRange{0, span});
    ASSERT_EQ(view->hits().size(), 1u) << "adjacent hit blocks coalesce into one HitEntry";
    EXPECT_EQ(view->hits()[0].range.offset, 0u);
    EXPECT_EQ(view->hits()[0].range.size, span);
    ASSERT_EQ(view->misses().size(), 0u);

    /// Each block round-trips through the coalesced hit reader.
    auto & reader = *view->hits()[0].reader;
    auto rope0 = reader.read(ByteRange{0, block_size});
    EXPECT_EQ(flatten(rope0, 0, block_size), std::string(block_size, '0'));
    auto rope1 = reader.read(ByteRange{block_size, block_size});
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

    auto misses = provider.openWriteBuffers(StoredObject{}, 0, {ByteRange{0, block_size}});
    ASSERT_EQ(misses.size(), 1u);
    auto & writer = *misses[0].writer;

    /// Data covers only the first half of the block — the whole block is not covered.
    size_t wrote = writer.write(makeRope(0, block_size / 2, 'P'));
    EXPECT_EQ(wrote, 0u) << "a partially-covered block is left for a later write";
    EXPECT_FALSE(writer.complete());
    auto uncommitted = writer.committed().subtract(ByteRange{0, block_size});
    ASSERT_EQ(uncommitted.size(), 1u);
    EXPECT_EQ(uncommitted[0].size, block_size) << "the whole range is still uncommitted";

    /// No partial cell was registered: a subsequent probe still misses.
    auto view = provider.planResidencyView(StoredObject{}, 0, ByteRange{0, block_size});
    EXPECT_TRUE(view->hits().empty());
    ASSERT_EQ(view->misses().size(), 1u);
}

/// (i) hit/miss interleaving: a 3-block file with ONLY the middle block resident
/// drives `buildView`'s kind-flip coalescing (miss → hit → miss), which the all-hit
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
        auto middle = provider.openWriteBuffers(StoredObject{}, 0, {ByteRange{block_size, block_size}});
        ASSERT_EQ(middle.size(), 1u);
        size_t wrote = middle[0].writer->write(makeRope(block_size, block_size, 'M'));
        EXPECT_EQ(wrote, block_size);
    }

    /// Probe the whole file: miss[0] → hit[1] → miss[2].
    auto view = provider.planResidencyView(StoredObject{}, 0, ByteRange{0, file_size});
    ASSERT_EQ(view->hits().size(), 1u);
    ASSERT_EQ(view->misses().size(), 2u);

    EXPECT_EQ(view->hits()[0].range.offset, block_size);
    EXPECT_EQ(view->hits()[0].range.size, block_size);

    EXPECT_EQ(view->misses()[0].range.offset, 0u);
    EXPECT_EQ(view->misses()[0].range.size, block_size);
    EXPECT_EQ(view->misses()[1].range.offset, 2 * block_size);
    EXPECT_EQ(view->misses()[1].range.size, block_size);

    /// The middle hit reader round-trips the written bytes.
    auto rope = view->hits()[0].reader->read(ByteRange{block_size, block_size});
    EXPECT_EQ(flatten(rope, block_size, block_size), std::string(block_size, 'M'));
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

    /// First provider populates the block with 'F'.
    {
        auto first = provider1.openWriteBuffers(StoredObject{}, 0, {ByteRange{0, block_size}});
        ASSERT_EQ(first.size(), 1u);
        size_t wrote = first[0].writer->write(makeRope(0, block_size, 'F'));
        EXPECT_EQ(wrote, block_size);
    }

    /// A SECOND provider over the SAME cache and file produces the SAME cache key.
    PageCacheProvider provider2(
        cache, file, block_size, /*inject_eviction=*/false,
        /*bypass_if_missing=*/false, /*file_size_in_bytes=*/block_size);

    auto second = provider2.openWriteBuffers(StoredObject{}, 0, {ByteRange{0, block_size}});
    ASSERT_EQ(second.size(), 1u);
    auto & writer = *second[0].writer;

    size_t wrote = writer.write(makeRope(0, block_size, 'S'));
    EXPECT_EQ(wrote, 0u) << "lost the cross-provider first-writer-wins race: nothing newly landed";
    EXPECT_TRUE(writer.complete()) << "the byte IS cached (by provider1), so committed must advance";
    EXPECT_TRUE(writer.committed().subtract(ByteRange{0, block_size}).empty());

    /// read returns the FIRST provider's bytes (the adopted existing cell).
    auto rope = writer.read(ByteRange{0, block_size});
    EXPECT_EQ(flatten(rope, 0, block_size), std::string(block_size, 'F'));
}
