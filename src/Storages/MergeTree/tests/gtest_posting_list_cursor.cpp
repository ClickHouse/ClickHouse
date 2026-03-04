#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListData.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>

#include <turbopfor.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <random>
#include <set>
#include <vector>

using namespace DB;
namespace fs = std::filesystem;

namespace
{

/// Helper: build a TokenPostingsInfo with an embedded Roaring bitmap from a sorted vector of doc IDs.
TokenPostingsInfo makeEmbeddedInfo(const std::vector<uint32_t> & doc_ids)
{
    TokenPostingsInfo info;
    info.cardinality = static_cast<UInt32>(doc_ids.size());

    auto bitmap = std::make_shared<roaring::Roaring>();
    for (auto id : doc_ids)
        bitmap->add(id);
    info.embedded_postings = bitmap;

    if (!doc_ids.empty())
    {
        info.ranges.emplace_back(doc_ids.front(), doc_ids.back());
        info.offsets.emplace_back(); // dummy, not used for embedded
    }

    return info;
}

/// Helper: create a PostingListCursor for an embedded posting list.
PostingListCursorPtr makeEmbeddedCursor(const TokenPostingsInfo & info)
{
    return std::make_shared<PostingListCursor>(info);
}

/// Helper: generate a sequence of doc IDs: {start, start+step, start+2*step, ...}
std::vector<uint32_t> generateRange(uint32_t start, uint32_t count, uint32_t step = 1)
{
    std::vector<uint32_t> result;
    result.reserve(count);
    for (uint32_t i = 0; i < count; ++i)
        result.push_back(start + i * step);
    return result;
}

/// Helper: collect all remaining doc IDs from a cursor using next().
/// For non-embedded cursors, seek() must be called first to decode the first packed block.
std::vector<uint32_t> drainCursor(PostingListCursorPtr cursor)
{
    std::vector<uint32_t> result;
    while (cursor->valid())
    {
        result.push_back(cursor->value());
        cursor->next();
    }
    return result;
}

/// Helper: seek to the first doc, then drain.
/// Used for multi-block (non-embedded) cursors where construction only loads the
/// Index Section but does not decode any packed block.
std::vector<uint32_t> seekAndDrainCursor(PostingListCursorPtr cursor, uint32_t first_doc)
{
    cursor->seek(first_doc);
    return drainCursor(cursor);
}

/// Helper: do linearOr into a buffer and return the set bits as doc IDs.
std::vector<uint32_t> linearOrToDocIds(PostingListCursorPtr cursor, size_t row_offset, size_t num_rows)
{
    std::vector<UInt8> buf(num_rows, 0);
    cursor->linearOr(buf.data(), row_offset, num_rows);
    std::vector<uint32_t> result;
    for (size_t i = 0; i < num_rows; ++i)
        if (buf[i])
            result.push_back(static_cast<uint32_t>(row_offset + i));
    return result;
}

/// Helper: perform intersection using lazyIntersectPostingLists and return doc IDs.
std::vector<uint32_t> intersectAndCollect(
    PostingListCursorMap & postings,
    const std::vector<String> & tokens,
    size_t row_offset,
    size_t num_rows,
    bool brute_force,
    float density_threshold = 0.5f)
{
    auto col = ColumnUInt8::create(num_rows, UInt8(0));
    lazyIntersectPostingLists(*col, postings, tokens, 0, row_offset, num_rows, brute_force, density_threshold);
    const auto & data = col->getData();
    std::vector<uint32_t> result;
    for (size_t i = 0; i < num_rows; ++i)
        if (data[i])
            result.push_back(static_cast<uint32_t>(row_offset + i));
    return result;
}

/// Helper: perform union using lazyUnionPostingLists and return doc IDs.
std::vector<uint32_t> unionAndCollect(
    PostingListCursorMap & postings,
    const std::vector<String> & tokens,
    size_t row_offset,
    size_t num_rows)
{
    auto col = ColumnUInt8::create(num_rows, UInt8(0));
    lazyUnionPostingLists(*col, postings, tokens, 0, row_offset, num_rows, false, 0.5f);
    const auto & data = col->getData();
    std::vector<uint32_t> result;
    for (size_t i = 0; i < num_rows; ++i)
        if (data[i])
            result.push_back(static_cast<uint32_t>(row_offset + i));
    return result;
}

/// Helper: write a PrefixVarUInt32 to a byte vector (mirrors PostingListData.cpp VarInt namespace).
void writePrefixVarUInt32(UInt32 x, std::vector<char> & out)
{
    static constexpr UInt32 ONE_BYTE_MAX = 176;
    static constexpr UInt32 TWO_BYTE_MAX = 16560;
    static constexpr UInt32 THREE_BYTE_MAX = 540848;
    static constexpr UInt32 FOUR_BYTE_MAX = 16777215;

    if (x <= ONE_BYTE_MAX)
    {
        out.push_back(static_cast<char>(x));
        return;
    }
    if (x <= TWO_BYTE_MAX)
    {
        UInt32 adjusted = x - 177;
        out.push_back(static_cast<char>(177 + (adjusted >> 8)));
        out.push_back(static_cast<char>(adjusted & 0xFF));
        return;
    }
    if (x <= THREE_BYTE_MAX)
    {
        UInt32 adjusted = x - 16561;
        out.push_back(static_cast<char>(241 + (adjusted >> 16)));
        out.push_back(static_cast<char>((adjusted >> 8) & 0xFF));
        out.push_back(static_cast<char>(adjusted & 0xFF));
        return;
    }
    if (x <= FOUR_BYTE_MAX)
    {
        out.push_back(static_cast<char>(249));
        out.push_back(static_cast<char>((x >> 16) & 0xFF));
        out.push_back(static_cast<char>((x >> 8) & 0xFF));
        out.push_back(static_cast<char>(x & 0xFF));
        return;
    }
    out.push_back(static_cast<char>(250));
    out.push_back(static_cast<char>((x >> 24) & 0xFF));
    out.push_back(static_cast<char>((x >> 16) & 0xFF));
    out.push_back(static_cast<char>((x >> 8) & 0xFF));
    out.push_back(static_cast<char>(x & 0xFF));
}

/// Helper: write a VarUInt64 (LEB128) to a byte vector.
void writeVarUInt64(UInt64 x, std::vector<char> & out)
{
    while (x > 0x7F)
    {
        out.push_back(static_cast<char>(0x80 | (x & 0x7F)));
        x >>= 7;
    }
    out.push_back(static_cast<char>(x));
}

/// Result of building multi-block test data.
struct MultiBlockTestData
{
    std::vector<char> buffer;      /// The contiguous .lpst-like data buffer
    TokenPostingsInfo info;        /// Populated TokenPostingsInfo
    std::vector<uint32_t> all_docs; /// All doc IDs in order (for verification)
};

/// Build a multi-large-block TokenPostingsInfo and data buffer for testing.
///
/// @param blocks  Vector of sorted doc ID vectors, one per large block.
///                blocks[0] must include the first_doc_id; the first doc of blocks[0]
///                will be treated as first_doc_id (stored separately, not encoded in TurboPFor).
///                Subsequent blocks encode ALL their docs in TurboPFor.
///
/// Returns a MultiBlockTestData with the binary buffer, TokenPostingsInfo, and flattened doc list.
MultiBlockTestData makeMultiBlockData(const std::vector<std::vector<uint32_t>> & blocks)
{
    MultiBlockTestData result;
    auto & buf = result.buffer;
    auto & info = result.info;

    if (blocks.empty())
        return result;

    uint32_t first_doc_id = blocks[0].front();

    /// Flatten all doc IDs for verification
    for (const auto & block_docs : blocks)
        result.all_docs.insert(result.all_docs.end(), block_docs.begin(), block_docs.end());

    UInt32 total_cardinality = static_cast<UInt32>(result.all_docs.size());
    info.cardinality = total_cardinality;

    alignas(16) uint8_t packed_buffer[512 * 2]; /// Enough for p4Enc output

    for (size_t blk_idx = 0; blk_idx < blocks.size(); ++blk_idx)
    {
        const auto & block_docs = blocks[blk_idx];

        /// For large block 0: first_doc_id is stored separately, so we encode
        /// (block_docs.size() - 1) docs starting from block_docs[1].
        /// For subsequent blocks: encode all docs.
        std::vector<uint32_t> docs_to_encode;
        uint32_t delta_base;

        if (blk_idx == 0)
        {
            docs_to_encode.assign(block_docs.begin() + 1, block_docs.end());
            delta_base = first_doc_id;
        }
        else
        {
            docs_to_encode.assign(block_docs.begin(), block_docs.end());
            delta_base = block_docs.front() - 1;
        }

        UInt32 large_block_doc_count = static_cast<UInt32>(docs_to_encode.size());
        UInt32 num_full_blocks = large_block_doc_count / TURBOPFOR_BLOCK_SIZE;
        UInt32 tail_count = large_block_doc_count % TURBOPFOR_BLOCK_SIZE;
        UInt32 num_packed_blocks = num_full_blocks + (tail_count > 0 ? 1 : 0);

        /// Data Section: encode packed blocks
        UInt64 data_section_offset = buf.size();
        std::vector<UInt32> packed_block_last_doc_ids;
        std::vector<UInt64> packed_block_offsets;

        UInt32 prev_doc = delta_base;
        size_t doc_pos = 0;

        for (UInt32 pb = 0; pb < num_packed_blocks; ++pb)
        {
            UInt32 count = (pb == num_packed_blocks - 1 && tail_count > 0) ? tail_count : static_cast<UInt32>(TURBOPFOR_BLOCK_SIZE);

            /// Compute delta-1 array
            std::vector<UInt32> deltas(count);
            for (UInt32 d = 0; d < count; ++d)
            {
                deltas[d] = docs_to_encode[doc_pos + d] - prev_doc - 1;
                prev_doc = docs_to_encode[doc_pos + d];
            }

            /// TurboPFor encode
            uint8_t * encoded_end;
            if (count == TURBOPFOR_BLOCK_SIZE)
                encoded_end = turbopfor::p4Enc128v32(deltas.data(), TURBOPFOR_BLOCK_SIZE, packed_buffer);
            else
                encoded_end = turbopfor::p4Enc32(deltas.data(), count, packed_buffer);

            UInt32 compressed_bytes = static_cast<UInt32>(encoded_end - packed_buffer);

            packed_block_last_doc_ids.push_back(docs_to_encode[doc_pos + count - 1]);
            packed_block_offsets.push_back(static_cast<UInt64>(buf.size()));

            /// Write: [PrefixVarInt: compressed_bytes][data]
            writePrefixVarUInt32(compressed_bytes, buf);
            buf.insert(buf.end(), reinterpret_cast<char *>(packed_buffer),
                       reinterpret_cast<char *>(packed_buffer) + compressed_bytes);

            doc_pos += count;
        }

        /// Index Section
        UInt64 index_section_offset = buf.size();

        /// [PrefixVarInt: num_packed_blocks]
        writePrefixVarUInt32(num_packed_blocks, buf);

        /// N × [PrefixVarInt: last_doc_id]
        for (auto id : packed_block_last_doc_ids)
            writePrefixVarUInt32(id, buf);

        /// N × [VarUInt64: absolute_offset]
        for (auto off : packed_block_offsets)
            writeVarUInt64(off, buf);

        /// Build LargePostingBlockMeta
        LargePostingBlockMeta meta;
        meta.last_doc_id = block_docs.back();
        meta.block_doc_count = large_block_doc_count;
        meta.offset = data_section_offset;
        meta.index_offset = index_section_offset;
        info.offsets.push_back(meta);

        /// Build RowsRange
        info.ranges.emplace_back(block_docs.front(), block_docs.back());
    }

    return result;
}

/// Helper: create a PostingListCursor for multi-block test data.
/// Writes the binary buffer to a temporary .lpst file, constructs a real
/// LargePostingListReaderStream via DiskLocal + DataPartStorageOnDiskFull,
/// and uses the production PostingListCursor constructor — no modifications needed.
PostingListCursorPtr makeMultiBlockCursor(const MultiBlockTestData & data)
{
    /// Create a unique temp directory for this test invocation
    auto tmp_dir = fs::temp_directory_path() / ("gtest_plc_" + std::to_string(reinterpret_cast<uintptr_t>(&data)));
    fs::create_directories(tmp_dir / "part");

    /// Write the binary buffer to a .lpst file
    {
        auto out_path = tmp_dir / "part" / "stream.lpst";
        std::ofstream ofs(out_path, std::ios::binary);
        ofs.write(data.buffer.data(), static_cast<std::streamsize>(data.buffer.size()));
        ofs.close();
    }

    /// Construct the stream infrastructure
    auto disk = std::make_shared<DiskLocal>("test_disk", tmp_dir.string() + "/");
    auto volume = std::make_shared<SingleDiskVolume>("test_vol", disk);
    auto storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "part");

    auto settings = MergeTreeReaderSettings::createFromSettings();
    settings.is_compressed = false;

    static constexpr size_t marks_count = 1;
    auto stream = std::make_shared<LargePostingListReaderStream>(
        /*merged_part_offsets=*/nullptr,
        /*part_index=*/0,
        /*part_starting_offset=*/0,
        storage,
        "stream",
        ".lpst",
        marks_count,
        MarkRanges{{0, marks_count}},
        settings,
        /*uncompressed_cache=*/nullptr,
        data.buffer.size(),
        /*marks_loader=*/nullptr,
        ReadBufferFromFileBase::ProfileCallback{},
        CLOCK_MONOTONIC_COARSE);

    return std::make_shared<PostingListCursor>(std::move(stream), data.info);
}

} // anonymous namespace


// ===========================================================================================
// Section 1: Basic cursor operations (embedded path)
// ===========================================================================================

TEST(PostingListCursorTest, EmptyEmbeddedCursor)
{
    auto info = makeEmbeddedInfo({});
    auto cursor = makeEmbeddedCursor(info);
    /// Empty embedded bitmap should still create a cursor, but it should be invalid
    /// because there are no doc_ids to iterate.
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, SingleDocEmbedded)
{
    auto info = makeEmbeddedInfo({42});
    auto cursor = makeEmbeddedCursor(info);

    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 42u);
    EXPECT_EQ(cursor->cardinality(), 1u);

    cursor->next();
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, MultipleDocsSequentialIteration)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto drained = drainCursor(cursor);
    EXPECT_EQ(drained, docs);
}

TEST(PostingListCursorTest, LargeEmbeddedCursor)
{
    auto docs = generateRange(0, 6);
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto drained = drainCursor(cursor);
    EXPECT_EQ(drained, docs);
}

TEST(PostingListCursorTest, SparseEmbeddedCursor)
{
    auto docs = generateRange(100, 5, 100); // 100,200,300,400,500
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto drained = drainCursor(cursor);
    EXPECT_EQ(drained, docs);
}

TEST(PostingListCursorTest, NextAfterInvalidIsNoop)
{
    auto info = makeEmbeddedInfo({5});
    auto cursor = makeEmbeddedCursor(info);
    cursor->next();
    EXPECT_FALSE(cursor->valid());
    cursor->next(); // Should not crash
    EXPECT_FALSE(cursor->valid());
    cursor->next(); // Multiple calls after invalid
    EXPECT_FALSE(cursor->valid());
}


// ===========================================================================================
// Section 2: Seek operations
// ===========================================================================================

TEST(PostingListCursorTest, SeekToExactValue)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(30);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 30u);
}

TEST(PostingListCursorTest, SeekToNonExistentGoesToNext)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(25);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 30u);
}

TEST(PostingListCursorTest, SeekBeyondLastInvalidates)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(31);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, SeekToFirstDoc)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(10);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 10u);
}

TEST(PostingListCursorTest, SeekToLastDoc)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(30);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 30u);

    cursor->next();
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, SeekToZero)
{
    std::vector<uint32_t> docs = {0, 5, 10};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(0);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 0u);
}

TEST(PostingListCursorTest, SeekBeforeFirst)
{
    std::vector<uint32_t> docs = {100, 200, 300};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(50);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 100u);
}

TEST(PostingListCursorTest, SeekProgressivelyForward)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50, 60};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(25);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 30u);

    cursor->seek(55);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 60u);

    cursor->seek(61);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, SeekThenNext)
{
    std::vector<uint32_t> docs = {5, 10, 15, 20, 25, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(15);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 15u);

    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 20u);

    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 25u);
}


// ===========================================================================================
// Section 3: Density and cardinality
// ===========================================================================================

TEST(PostingListCursorTest, CardinalityReflectsDocCount)
{
    auto info = makeEmbeddedInfo({1, 2, 3, 4, 5});
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_EQ(cursor->cardinality(), 5u);
}

TEST(PostingListCursorTest, DensityPerfectlyDense)
{
    auto docs = generateRange(0, 5);
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_DOUBLE_EQ(cursor->density(), 1.0);
}

TEST(PostingListCursorTest, DensitySparse)
{
    std::vector<uint32_t> docs = {0, 100};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_NEAR(cursor->density(), 2.0 / 101.0, 1e-6);
}

TEST(PostingListCursorTest, DensitySingleDoc)
{
    auto info = makeEmbeddedInfo({42});
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_DOUBLE_EQ(cursor->density(), 1.0);
}


// ===========================================================================================
// Section 4: linearOr (set bits for all doc IDs in range)
// ===========================================================================================

TEST(PostingListCursorTest, LinearOrFullRange)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 0, 60);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, LinearOrPartialRange)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 15, 30); // rows [15, 45)
    std::vector<uint32_t> expected = {20, 30, 40};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, LinearOrNoOverlap)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 100, 50);
    EXPECT_TRUE(result.empty());
}

TEST(PostingListCursorTest, LinearOrSingleRowMatch)
{
    std::vector<uint32_t> docs = {42};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 42, 1);
    EXPECT_EQ(result, std::vector<uint32_t>{42});
}

TEST(PostingListCursorTest, LinearOrDenseRange)
{
    auto docs = generateRange(100, 6);
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 0, 110);
    EXPECT_EQ(result, docs);
}


// ===========================================================================================
// Section 5: linearAnd (increment counts for matching rows)
// ===========================================================================================

TEST(PostingListCursorTest, LinearAndFullRange)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    std::vector<UInt8> buf(60, 0);
    cursor->linearAnd(buf.data(), 0, 60);

    for (auto d : docs)
        EXPECT_EQ(buf[d], 1u) << "Expected buf[" << d << "] == 1";
    EXPECT_EQ(buf[0], 0u);
    EXPECT_EQ(buf[15], 0u);
}

TEST(PostingListCursorTest, LinearAndIncrementsExisting)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    std::vector<UInt8> buf(40, 1);
    cursor->linearAnd(buf.data(), 0, 40);

    EXPECT_EQ(buf[10], 2u);
    EXPECT_EQ(buf[20], 2u);
    EXPECT_EQ(buf[30], 2u);
    EXPECT_EQ(buf[0], 1u);
    EXPECT_EQ(buf[5], 1u);
}


// ===========================================================================================
// Section 6: Two-cursor intersection (intersectTwo)
// ===========================================================================================

TEST(PostingListCursorTest, IntersectTwoIdentical)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info1 = makeEmbeddedInfo(docs);
    auto info2 = makeEmbeddedInfo(docs);
    auto c1 = makeEmbeddedCursor(info1);
    auto c2 = makeEmbeddedCursor(info2);

    PostingListCursorMap postings;
    postings["a"] = c1;
    postings["b"] = c2;

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 60, false, 100.0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, IntersectTwoDisjoint)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30});
    auto info2 = makeEmbeddedInfo({15, 25, 35});
    auto c1 = makeEmbeddedCursor(info1);
    auto c2 = makeEmbeddedCursor(info2);

    PostingListCursorMap postings;
    postings["a"] = c1;
    postings["b"] = c2;

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 50, false, 100.0);
    EXPECT_TRUE(result.empty());
}

TEST(PostingListCursorTest, IntersectTwoPartialOverlap)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30, 40, 50});
    auto info2 = makeEmbeddedInfo({20, 30, 60, 70});
    auto c1 = makeEmbeddedCursor(info1);
    auto c2 = makeEmbeddedCursor(info2);

    PostingListCursorMap postings;
    postings["a"] = c1;
    postings["b"] = c2;

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 80, false, 100.0);
    std::vector<uint32_t> expected = {20, 30};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectTwoDenseVsSparse)
{
    auto dense_docs = generateRange(0, 1000);
    auto sparse_docs = generateRange(0, 100, 10); // 0,10,20,...,990

    auto dataD = makeMultiBlockData({dense_docs});
    auto dataS = makeMultiBlockData({sparse_docs});

    PostingListCursorMap postings;
    postings["dense"] = makeMultiBlockCursor(dataD);
    postings["sparse"] = makeMultiBlockCursor(dataS);

    auto result = intersectAndCollect(postings, {"dense", "sparse"}, 0, 1000, false, 100.0);
    EXPECT_EQ(result, sparse_docs);
}

TEST(PostingListCursorTest, IntersectTwoSingleCommon)
{
    auto info1 = makeEmbeddedInfo({1, 50, 100});
    auto info2 = makeEmbeddedInfo({49, 50, 51});
    auto c1 = makeEmbeddedCursor(info1);
    auto c2 = makeEmbeddedCursor(info2);

    PostingListCursorMap postings;
    postings["a"] = c1;
    postings["b"] = c2;

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 150, false, 100.0);
    EXPECT_EQ(result, std::vector<uint32_t>{50});
}


// ===========================================================================================
// Section 7: Three-cursor intersection (intersectThree)
// ===========================================================================================

TEST(PostingListCursorTest, IntersectThreeAllMatch)
{
    auto docs = generateRange(0, 5, 10); // 0,10,20,30,40
    auto info1 = makeEmbeddedInfo(docs);
    auto info2 = makeEmbeddedInfo(docs);
    auto info3 = makeEmbeddedInfo(docs);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);

    auto result = intersectAndCollect(postings, {"a", "b", "c"}, 0, 50, false, 100.0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, IntersectThreePartialOverlap)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30, 40, 50, 60});
    auto info2 = makeEmbeddedInfo({15, 20, 30, 45, 60});
    auto info3 = makeEmbeddedInfo({20, 25, 30, 55, 60});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);

    auto result = intersectAndCollect(postings, {"a", "b", "c"}, 0, 70, false, 100.0);
    std::vector<uint32_t> expected = {20, 30, 60};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectThreeNoCommon)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30});
    auto info2 = makeEmbeddedInfo({11, 21, 31});
    auto info3 = makeEmbeddedInfo({12, 22, 32});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);

    auto result = intersectAndCollect(postings, {"a", "b", "c"}, 0, 50, false, 100.0);
    EXPECT_TRUE(result.empty());
}


// ===========================================================================================
// Section 8: Four-cursor intersection (intersectFour)
// ===========================================================================================

TEST(PostingListCursorTest, IntersectFourAllOverlap)
{
    auto docs = generateRange(0, 5, 10); // 0,10,20,30,40
    auto info1 = makeEmbeddedInfo(docs);
    auto info2 = makeEmbeddedInfo(docs);
    auto info3 = makeEmbeddedInfo(docs);
    auto info4 = makeEmbeddedInfo(docs);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);
    postings["d"] = makeEmbeddedCursor(info4);

    auto result = intersectAndCollect(postings, {"a", "b", "c", "d"}, 0, 50, false, 100.0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, IntersectFourMixedSelectivity)
{
    // Dense: every 2nd
    auto docs1 = generateRange(0, 500, 2);
    // Sparse: every 3rd
    auto docs2 = generateRange(0, 334, 3);
    // Sparser: every 5th
    auto docs3 = generateRange(0, 200, 5);
    // Sparsest: every 7th
    auto docs4 = generateRange(0, 143, 7);

    auto data1 = makeMultiBlockData({docs1});
    auto data2 = makeMultiBlockData({docs2});
    auto data3 = makeMultiBlockData({docs3});
    auto data4 = makeMultiBlockData({docs4});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(data1);
    postings["b"] = makeMultiBlockCursor(data2);
    postings["c"] = makeMultiBlockCursor(data3);
    postings["d"] = makeMultiBlockCursor(data4);

    auto result = intersectAndCollect(postings, {"a", "b", "c", "d"}, 0, 1000, false, 100.0);

    // LCM(2,3,5,7) = 210
    std::vector<uint32_t> expected;
    for (uint32_t i = 0; i < 1000; i += 210)
        expected.push_back(i);
    EXPECT_EQ(result, expected);
}


// ===========================================================================================
// Section 9: Five+ cursor intersection (intersectLeapfrogLinear, n=5..8)
// ===========================================================================================

TEST(PostingListCursorTest, IntersectFiveCursors)
{
    // Every i-th prime: 2,3,5,7,11 -> LCM=2310
    auto docs1 = generateRange(0, 500, 2);
    auto docs2 = generateRange(0, 334, 3);
    auto docs3 = generateRange(0, 200, 5);
    auto docs4 = generateRange(0, 143, 7);
    auto docs5 = generateRange(0, 91, 11);

    auto data1 = makeMultiBlockData({docs1});
    auto data2 = makeMultiBlockData({docs2});
    auto data3 = makeMultiBlockData({docs3});
    auto data4 = makeMultiBlockData({docs4});
    auto data5 = makeMultiBlockData({docs5});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(data1);
    postings["b"] = makeMultiBlockCursor(data2);
    postings["c"] = makeMultiBlockCursor(data3);
    postings["d"] = makeMultiBlockCursor(data4);
    postings["e"] = makeMultiBlockCursor(data5);

    auto result = intersectAndCollect(postings, {"a", "b", "c", "d", "e"}, 0, 1000, false, 100.0);
    // Only 0 is common (LCM=2310, next would be 2310 which is >= 1000)
    EXPECT_EQ(result, std::vector<uint32_t>{0});
}

TEST(PostingListCursorTest, IntersectEightCursors)
{
    auto docs = generateRange(0, 6, 10); // 0,10,20,30,40,50
    std::vector<MultiBlockTestData> datas(8);
    PostingListCursorMap postings;
    std::vector<String> tokens;

    for (int i = 0; i < 8; ++i)
    {
        datas[i] = makeMultiBlockData({docs});
        String name = "t" + std::to_string(i);
        tokens.push_back(name);
    }
    for (int i = 0; i < 8; ++i)
        postings[tokens[i]] = makeMultiBlockCursor(datas[i]);

    auto result = intersectAndCollect(postings, tokens, 0, 60, false, 100.0);
    EXPECT_EQ(result, docs);
}


// ===========================================================================================
// Section 10: Nine+ cursor intersection (intersectLeapfrogHeap)
// ===========================================================================================

TEST(PostingListCursorTest, IntersectNineCursorsHeap)
{
    auto docs = generateRange(0, 3, 20); // 0,20,40
    std::vector<TokenPostingsInfo> infos(9);
    PostingListCursorMap postings;
    std::vector<String> tokens;

    for (int i = 0; i < 9; ++i)
    {
        infos[i] = makeEmbeddedInfo(docs);
        String name = "t" + std::to_string(i);
        tokens.push_back(name);
    }
    for (int i = 0; i < 9; ++i)
        postings[tokens[i]] = makeEmbeddedCursor(infos[i]);

    auto result = intersectAndCollect(postings, tokens, 0, 50, false, 100.0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, IntersectTenCursorsWithVaryingDocs)
{
    std::vector<MultiBlockTestData> datas(10);
    PostingListCursorMap postings;
    std::vector<String> tokens;

    for (int i = 0; i < 10; ++i)
    {
        String name = "t" + std::to_string(i);
        tokens.push_back(name);
    }

    for (int i = 0; i < 10; ++i)
    {
        std::vector<uint32_t> docs;
        for (uint32_t d = 0; d < 1000; d += (i + 1))
            docs.push_back(d);
        datas[i] = makeMultiBlockData({docs});
        postings[tokens[i]] = makeMultiBlockCursor(datas[i]);
    }

    auto result = intersectAndCollect(postings, tokens, 0, 1000, false, 100.0);
    // LCM(1,2,...,10) = 2520, so only 0 within [0,1000)
    EXPECT_EQ(result, std::vector<uint32_t>{0});
}


// ===========================================================================================
// Section 11: Brute-force intersection (intersectBruteForce)
// ===========================================================================================

TEST(PostingListCursorTest, BruteForceIntersectTwo)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30, 40, 50});
    auto info2 = makeEmbeddedInfo({20, 30, 60});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 70, true);
    std::vector<uint32_t> expected = {20, 30};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, BruteForceIntersectThree)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30, 40, 50});
    auto info2 = makeEmbeddedInfo({20, 30, 50, 60});
    auto info3 = makeEmbeddedInfo({5, 20, 30, 45, 50, 70});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);

    auto result = intersectAndCollect(postings, {"a", "b", "c"}, 0, 80, true);
    std::vector<uint32_t> expected = {20, 30, 50};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, BruteForceVsLeapfrogConsistency)
{
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint32_t> dist(0, 999);

    for (int trial = 0; trial < 10; ++trial)
    {
        std::vector<MultiBlockTestData> datas(3);
        std::vector<std::vector<uint32_t>> all_docs(3);

        for (int i = 0; i < 3; ++i)
        {
            std::set<uint32_t> s;
            size_t count = 50 + trial * 20;
            while (s.size() < count)
                s.insert(dist(rng));
            all_docs[i].assign(s.begin(), s.end());
            datas[i] = makeMultiBlockData({all_docs[i]});
        }

        // Brute force
        PostingListCursorMap postings_bf;
        postings_bf["a"] = makeMultiBlockCursor(datas[0]);
        postings_bf["b"] = makeMultiBlockCursor(datas[1]);
        postings_bf["c"] = makeMultiBlockCursor(datas[2]);
        auto bf_result = intersectAndCollect(postings_bf, {"a", "b", "c"}, 0, 1000, true);

        // Leapfrog (low density threshold to force leapfrog)
        PostingListCursorMap postings_lf;
        postings_lf["a"] = makeMultiBlockCursor(datas[0]);
        postings_lf["b"] = makeMultiBlockCursor(datas[1]);
        postings_lf["c"] = makeMultiBlockCursor(datas[2]);
        auto lf_result = intersectAndCollect(postings_lf, {"a", "b", "c"}, 0, 1000, false, 100.0);

        EXPECT_EQ(bf_result, lf_result) << "Brute-force vs leapfrog mismatch at trial " << trial;
    }
}


// ===========================================================================================
// Section 12: Union (lazyUnionPostingLists)
// ===========================================================================================

TEST(PostingListCursorTest, UnionSingleCursor)
{
    std::vector<uint32_t> docs = {5, 15, 25};
    auto info = makeEmbeddedInfo(docs);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info);

    auto result = unionAndCollect(postings, {"a"}, 0, 30);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, UnionTwoCursorsNoOverlap)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30});
    auto info2 = makeEmbeddedInfo({15, 25, 35});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = unionAndCollect(postings, {"a", "b"}, 0, 40);
    std::vector<uint32_t> expected = {10, 15, 20, 25, 30, 35};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, UnionTwoCursorsWithOverlap)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30, 40});
    auto info2 = makeEmbeddedInfo({20, 30, 50});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = unionAndCollect(postings, {"a", "b"}, 0, 60);
    std::vector<uint32_t> expected = {10, 20, 30, 40, 50};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, UnionMultipleCursors)
{
    auto info1 = makeEmbeddedInfo({1, 4, 7});
    auto info2 = makeEmbeddedInfo({2, 5, 8});
    auto info3 = makeEmbeddedInfo({3, 6, 9});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);

    auto result = unionAndCollect(postings, {"a", "b", "c"}, 0, 10);
    std::vector<uint32_t> expected = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    EXPECT_EQ(result, expected);
}


// ===========================================================================================
// Section 13: Intersection with row_offset (windowed intersection)
// ===========================================================================================

TEST(PostingListCursorTest, IntersectWithRowOffset)
{
    auto info1 = makeEmbeddedInfo({100, 200, 300, 400, 500});
    auto info2 = makeEmbeddedInfo({100, 200, 300, 400, 500});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    // Only look at rows [200, 400)
    auto result = intersectAndCollect(postings, {"a", "b"}, 200, 200, false, 100.0);
    std::vector<uint32_t> expected = {200, 300};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectWindowExcludesAll)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30});
    auto info2 = makeEmbeddedInfo({10, 20, 30});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = intersectAndCollect(postings, {"a", "b"}, 100, 50, false, 100.0);
    EXPECT_TRUE(result.empty());
}


// ===========================================================================================
// Section 14: Union with row_offset (windowed union)
// ===========================================================================================

TEST(PostingListCursorTest, UnionWithRowOffset)
{
    auto info1 = makeEmbeddedInfo({50, 100, 150, 200});
    auto info2 = makeEmbeddedInfo({75, 125, 175});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    // Look at rows [100, 200) -> 100 rows
    auto result = unionAndCollect(postings, {"a", "b"}, 100, 100);
    std::vector<uint32_t> expected = {100, 125, 150, 175};
    EXPECT_EQ(result, expected);
}


// ===========================================================================================
// Section 15: Single cursor intersection (degenerate case, acts like OR)
// ===========================================================================================

TEST(PostingListCursorTest, IntersectSingleCursor)
{
    auto info = makeEmbeddedInfo({5, 10, 15, 20, 25});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info);

    auto result = intersectAndCollect(postings, {"a"}, 0, 30, false);
    std::vector<uint32_t> expected = {5, 10, 15, 20, 25};
    EXPECT_EQ(result, expected);
}


// ===========================================================================================
// Section 16: Missing tokens in postings map
// ===========================================================================================

TEST(PostingListCursorTest, IntersectMissingToken)
{
    auto info = makeEmbeddedInfo({1, 2, 3});

    PostingListCursorMap postings;
    postings["exists"] = makeEmbeddedCursor(info);

    // "missing" token doesn't exist in map → should act as if 0 cursors intersect
    auto result = intersectAndCollect(postings, {"exists", "missing"}, 0, 10, false, 100.0);
    // Only "exists" cursor is used, intersect with nothing → depends on implementation
    // Since only 1 cursor found, it should just do linearOr
    // But actually if only 1 cursor found out of 2 tokens, n=1 path is taken
    std::vector<uint32_t> expected = {1, 2, 3};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, UnionMissingToken)
{
    auto info = makeEmbeddedInfo({10, 20, 30});

    PostingListCursorMap postings;
    postings["exists"] = makeEmbeddedCursor(info);

    auto result = unionAndCollect(postings, {"exists", "missing"}, 0, 40);
    std::vector<uint32_t> expected = {10, 20, 30};
    EXPECT_EQ(result, expected);
}


// ===========================================================================================
// Section 17: Empty intersection/union
// ===========================================================================================

TEST(PostingListCursorTest, IntersectZeroCursors)
{
    PostingListCursorMap postings;
    auto result = intersectAndCollect(postings, {}, 0, 100, false);
    EXPECT_TRUE(result.empty());
}

TEST(PostingListCursorTest, UnionZeroCursors)
{
    PostingListCursorMap postings;
    auto result = unionAndCollect(postings, {}, 0, 100);
    EXPECT_TRUE(result.empty());
}


// ===========================================================================================
// Section 18: Stress tests — large random intersections
// ===========================================================================================

TEST(PostingListCursorTest, StressRandomIntersectTwo)
{
    std::mt19937 rng(12345);

    for (int trial = 0; trial < 20; ++trial)
    {
        std::uniform_int_distribution<uint32_t> dist(0, 9999);
        std::set<uint32_t> s1, s2;

        size_t sz1 = 200 + trial * 50;
        size_t sz2 = 100 + trial * 30;
        while (s1.size() < sz1) s1.insert(dist(rng));
        while (s2.size() < sz2) s2.insert(dist(rng));

        std::vector<uint32_t> v1(s1.begin(), s1.end());
        std::vector<uint32_t> v2(s2.begin(), s2.end());

        // Compute expected intersection
        std::vector<uint32_t> expected;
        std::set_intersection(s1.begin(), s1.end(), s2.begin(), s2.end(), std::back_inserter(expected));

        auto data1 = makeMultiBlockData({v1});
        auto data2 = makeMultiBlockData({v2});

        PostingListCursorMap postings;
        postings["a"] = makeMultiBlockCursor(data1);
        postings["b"] = makeMultiBlockCursor(data2);

        auto result = intersectAndCollect(postings, {"a", "b"}, 0, 10000, false, 100.0);
        EXPECT_EQ(result, expected) << "Mismatch at trial " << trial;
    }
}

TEST(PostingListCursorTest, StressRandomIntersectFour)
{
    std::mt19937 rng(54321);

    for (int trial = 0; trial < 10; ++trial)
    {
        std::uniform_int_distribution<uint32_t> dist(0, 4999);

        std::vector<std::set<uint32_t>> sets(4);
        std::vector<MultiBlockTestData> datas(4);
        PostingListCursorMap postings;
        std::vector<String> tokens;

        for (int i = 0; i < 4; ++i)
        {
            size_t sz = 100 + trial * 30 + i * 20;
            while (sets[i].size() < sz) sets[i].insert(dist(rng));

            std::vector<uint32_t> v(sets[i].begin(), sets[i].end());
            datas[i] = makeMultiBlockData({v});

            String name = "t" + std::to_string(i);
            tokens.push_back(name);
        }
        for (int i = 0; i < 4; ++i)
            postings[tokens[i]] = makeMultiBlockCursor(datas[i]);

        // Compute expected 4-way intersection
        std::vector<uint32_t> tmp1, tmp2, expected;
        std::set_intersection(sets[0].begin(), sets[0].end(), sets[1].begin(), sets[1].end(), std::back_inserter(tmp1));
        std::set_intersection(sets[2].begin(), sets[2].end(), sets[3].begin(), sets[3].end(), std::back_inserter(tmp2));
        std::set_intersection(tmp1.begin(), tmp1.end(), tmp2.begin(), tmp2.end(), std::back_inserter(expected));

        auto result = intersectAndCollect(postings, tokens, 0, 5000, false, 100.0);
        EXPECT_EQ(result, expected) << "Mismatch at trial " << trial;
    }
}

TEST(PostingListCursorTest, StressRandomUnion)
{
    std::mt19937 rng(99999);

    for (int trial = 0; trial < 10; ++trial)
    {
        std::uniform_int_distribution<uint32_t> dist(0, 999);

        std::set<uint32_t> all;
        std::vector<MultiBlockTestData> datas(3);
        PostingListCursorMap postings;
        std::vector<String> tokens;

        for (int i = 0; i < 3; ++i)
        {
            std::set<uint32_t> s;
            size_t sz = 50 + trial * 10;
            while (s.size() < sz) s.insert(dist(rng));
            all.insert(s.begin(), s.end());

            std::vector<uint32_t> v(s.begin(), s.end());
            datas[i] = makeMultiBlockData({v});

            String name = "t" + std::to_string(i);
            tokens.push_back(name);
        }
        for (int i = 0; i < 3; ++i)
            postings[tokens[i]] = makeMultiBlockCursor(datas[i]);

        std::vector<uint32_t> expected(all.begin(), all.end());
        auto result = unionAndCollect(postings, tokens, 0, 1000);
        EXPECT_EQ(result, expected) << "Mismatch at trial " << trial;
    }
}


// ===========================================================================================
// Section 19: Edge cases for intersection algorithms
// ===========================================================================================

TEST(PostingListCursorTest, IntersectOneCursorEndsEarly)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30});
    auto docs2 = generateRange(10, 6, 10); // 10,20,30,40,50,60
    auto info2 = makeEmbeddedInfo(docs2);

    PostingListCursorMap postings;
    postings["short"] = makeEmbeddedCursor(info1);
    postings["long"] = makeEmbeddedCursor(info2);

    auto result = intersectAndCollect(postings, {"short", "long"}, 0, 70, false, 100.0);
    std::vector<uint32_t> expected = {10, 20, 30};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectAllDocsAtMaxValue)
{
    uint32_t big = 1000000;
    auto info1 = makeEmbeddedInfo({big - 2, big - 1, big});
    auto info2 = makeEmbeddedInfo({big - 2, big - 1, big});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = intersectAndCollect(postings, {"a", "b"}, big - 2, 3, false, 100.0);
    std::vector<uint32_t> expected = {big - 2, big - 1, big};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectLargeGapBetweenMatches)
{
    auto info1 = makeEmbeddedInfo({0, 10000, 20000});
    auto info2 = makeEmbeddedInfo({0, 10000, 20000});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 30000, false, 100.0);
    std::vector<uint32_t> expected = {0, 10000, 20000};
    EXPECT_EQ(result, expected);
}


// ===========================================================================================
// Section 20: Brute-force with high cardinality
// ===========================================================================================

TEST(PostingListCursorTest, BruteForceDenseLargeRange)
{
    auto docs1 = generateRange(0, 5000);
    auto docs2 = generateRange(0, 2500, 2); // 0,2,4,...,4998

    auto data1 = makeMultiBlockData({docs1});
    auto data2 = makeMultiBlockData({docs2});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(data1);
    postings["b"] = makeMultiBlockCursor(data2);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 5000, true);
    EXPECT_EQ(result, docs2);
}


// ===========================================================================================
// Section 21: Cursor-level seek+next interleaving
// ===========================================================================================

TEST(PostingListCursorTest, AlternatingSeekAndNext)
{
    auto docs = generateRange(0, 6, 3); // 0,3,6,9,12,15
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    // Start: value should be 0
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 0u);

    // Seek to 2 -> should land on 3 (first multiple of 3 >= 2)
    cursor->seek(2);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 3u);

    // next() -> 6
    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 6u);

    // Seek to 10 -> should land on 12
    cursor->seek(10);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 12u);

    // next() -> 15
    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 15u);

    // next() -> invalid
    cursor->next();
    EXPECT_FALSE(cursor->valid());
}


// ===========================================================================================
// Section 22: Regression test for the two bugs that were fixed
// ===========================================================================================

/// The next() bug: when a cursor exhausts all packed blocks in one large block,
/// it should advance to the next large block (not just invalidate).
/// For embedded cursors, all data is in one "block", so this is implicitly tested
/// through the draining tests above. This test verifies the cursor properly exhausts
/// all elements.
TEST(PostingListCursorTest, NextExhaustsAllElementsProperly)
{
    auto docs = generateRange(0, 6);
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    size_t count = 0;
    while (cursor->valid())
    {
        EXPECT_EQ(cursor->value(), static_cast<uint32_t>(count));
        cursor->next();
        ++count;
    }
    EXPECT_EQ(count, 6u);
}

/// The seek() bug: seek on the slow path should start from the next large block,
/// not retry the current one. For embedded cursors, this manifests as: after seek
/// fails (target beyond all data), cursor should be invalid.
TEST(PostingListCursorTest, SeekBeyondEndMakesInvalid)
{
    auto info = makeEmbeddedInfo({10, 20, 30});
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(31);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, SeekExactLastThenNextInvalidates)
{
    auto info = makeEmbeddedInfo({10, 20, 30});
    auto cursor = makeEmbeddedCursor(info);

    cursor->seek(30);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 30u);

    cursor->next();
    EXPECT_FALSE(cursor->valid());
}


// ===========================================================================================
// Section 23: Mixed algorithm paths — brute-force threshold switching
// ===========================================================================================

TEST(PostingListCursorTest, DensityThresholdSwitchesAlgorithm)
{
    // Two dense cursors: density ~= 1.0
    auto docs1 = generateRange(0, 100);
    auto docs2 = generateRange(0, 100);
    auto data1 = makeMultiBlockData({docs1});
    auto data2 = makeMultiBlockData({docs2});

    // With high threshold → leapfrog
    {
        PostingListCursorMap postings;
        postings["a"] = makeMultiBlockCursor(data1);
        postings["b"] = makeMultiBlockCursor(data2);
        auto result = intersectAndCollect(postings, {"a", "b"}, 0, 100, false, 100.0);
        EXPECT_EQ(result, docs1);
    }

    // With low threshold → brute-force (density 1.0 >= 0.1)
    {
        PostingListCursorMap postings;
        postings["a"] = makeMultiBlockCursor(data1);
        postings["b"] = makeMultiBlockCursor(data2);
        auto result = intersectAndCollect(postings, {"a", "b"}, 0, 100, false, 0.1f);
        EXPECT_EQ(result, docs1);
    }
}


// ===========================================================================================
// Section 24: Interleaving of cursors with very different cardinalities
// ===========================================================================================

TEST(PostingListCursorTest, HighlyAsymmetricCardinalities)
{
    // Cursor A: single doc at 500
    auto info1 = makeEmbeddedInfo({500});
    // Cursor B: 5000 docs
    auto docs2 = generateRange(0, 5000);
    auto data2 = makeMultiBlockData({docs2});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeMultiBlockCursor(data2);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 5000, false, 100.0);
    EXPECT_EQ(result, std::vector<uint32_t>{500});
}

TEST(PostingListCursorTest, OneEmptyOneFullIntersection)
{
    auto info1 = makeEmbeddedInfo({});
    auto docs2 = generateRange(0, 100);
    auto data2 = makeMultiBlockData({docs2});

    PostingListCursorMap postings;
    postings["empty"] = makeEmbeddedCursor(info1);
    postings["full"] = makeMultiBlockCursor(data2);

    // The empty cursor is invalid from the start, so intersect should return empty.
    // But since the cursor is invalid after seek(0), intersect returns immediately.
    auto result = intersectAndCollect(postings, {"empty", "full"}, 0, 100, false, 100.0);
    EXPECT_TRUE(result.empty());
}


// ===========================================================================================
// Section 25: Consecutive doc IDs
// ===========================================================================================

TEST(PostingListCursorTest, ConsecutiveDocIds)
{
    auto docs = generateRange(1000, 6);
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = drainCursor(cursor);
    EXPECT_EQ(result.size(), 6u);
    EXPECT_EQ(result.front(), 1000u);
    EXPECT_EQ(result.back(), 1005u);
}


// ===========================================================================================
// Section 26: Intersection with window at exactly the boundary of matching docs
// ===========================================================================================

TEST(PostingListCursorTest, WindowExactlyOnFirstMatch)
{
    auto info1 = makeEmbeddedInfo({50, 100, 150});
    auto info2 = makeEmbeddedInfo({50, 100, 150});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    // Window [50, 51) — exactly covers only doc 50
    auto result = intersectAndCollect(postings, {"a", "b"}, 50, 1, false, 100.0);
    EXPECT_EQ(result, std::vector<uint32_t>{50});
}

TEST(PostingListCursorTest, WindowJustMissesMatch)
{
    auto info1 = makeEmbeddedInfo({50, 100, 150});
    auto info2 = makeEmbeddedInfo({50, 100, 150});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    // Window [51, 100) — misses 50, captures nothing (100 is at end boundary)
    auto result = intersectAndCollect(postings, {"a", "b"}, 51, 49, false, 100.0);
    EXPECT_TRUE(result.empty());
}


// ===========================================================================================
// Section 27: Multi-large-block — basic next() iteration
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockSingleBlockSmall)
{
    /// Single large block with fewer than 128 docs (tail block only).
    std::vector<uint32_t> docs = generateRange(0, 50); // 0..49
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, data.all_docs.front());
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, MultiBlockSingleBlockExact128)
{
    /// Single large block with exactly 128+1 = 129 docs total.
    /// first_doc_id (doc 0) is stored separately, 128 docs encoded in one full packed block.
    std::vector<uint32_t> docs = generateRange(0, 129); // 0..128
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, data.all_docs.front());
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, MultiBlockSingleBlockWithTail)
{
    /// 200 docs total: first_doc_id + 199 encoded = 1 full block (128) + tail (71).
    std::vector<uint32_t> docs = generateRange(0, 200);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, data.all_docs.front());
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, MultiBlockTwoBlocks)
{
    /// Two large blocks.
    /// Block 0: docs 0..199 (first_doc_id=0, 199 encoded)
    /// Block 1: docs 300..499 (200 docs encoded)
    std::vector<uint32_t> block0 = generateRange(0, 200);
    std::vector<uint32_t> block1 = generateRange(300, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, data.all_docs.front());
    EXPECT_EQ(result, data.all_docs);
}

TEST(PostingListCursorTest, MultiBlockThreeBlocks)
{
    /// Three large blocks with gaps.
    std::vector<uint32_t> block0 = generateRange(0, 150);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    std::vector<uint32_t> block2 = generateRange(1000, 100);
    auto data = makeMultiBlockData({block0, block1, block2});
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, data.all_docs.front());
    EXPECT_EQ(result, data.all_docs);
}

TEST(PostingListCursorTest, MultiBlockSparseDocsInBlocks)
{
    /// Sparse doc IDs within each block.
    std::vector<uint32_t> block0 = generateRange(0, 150, 3); // 0,3,6,...,447
    std::vector<uint32_t> block1 = generateRange(1000, 150, 5); // 1000,1005,...,1745
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, data.all_docs.front());
    EXPECT_EQ(result, data.all_docs);
}


// ===========================================================================================
// Section 28: Multi-large-block — seek operations
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockSeekWithinFirstBlock)
{
    std::vector<uint32_t> block0 = generateRange(0, 200);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    cursor->seek(100);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 100u);
}

TEST(PostingListCursorTest, MultiBlockSeekToSecondBlock)
{
    std::vector<uint32_t> block0 = generateRange(0, 200);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    cursor->seek(500);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 500u);
}

TEST(PostingListCursorTest, MultiBlockSeekToGapBetweenBlocks)
{
    std::vector<uint32_t> block0 = generateRange(0, 200); // 0..199
    std::vector<uint32_t> block1 = generateRange(500, 200); // 500..699
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    /// Seek to 300 — beyond block 0, should land on block 1's first doc (500)
    cursor->seek(300);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 500u);
}

TEST(PostingListCursorTest, MultiBlockSeekBeyondAll)
{
    std::vector<uint32_t> block0 = generateRange(0, 200);
    std::vector<uint32_t> block1 = generateRange(500, 200); // ..699
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    cursor->seek(700);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, MultiBlockSeekProgressivelyAcrossBlocks)
{
    std::vector<uint32_t> block0 = generateRange(0, 150);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    std::vector<uint32_t> block2 = generateRange(1000, 100);
    auto data = makeMultiBlockData({block0, block1, block2});
    auto cursor = makeMultiBlockCursor(data);

    cursor->seek(50);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 50u);

    cursor->seek(600);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 600u);

    cursor->seek(1050);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 1050u);

    cursor->seek(1100);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, MultiBlockSeekThenNext)
{
    std::vector<uint32_t> block0 = generateRange(0, 150);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    cursor->seek(148);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 148u);

    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 149u);

    /// next() should cross to block 1
    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 500u);

    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 501u);
}


// ===========================================================================================
// Section 29: Multi-large-block — linearOr
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockLinearOrFullRange)
{
    std::vector<uint32_t> block0 = generateRange(0, 150);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    auto result = linearOrToDocIds(cursor, 0, 700);
    EXPECT_EQ(result, data.all_docs);
}

TEST(PostingListCursorTest, MultiBlockLinearOrPartialFirstBlock)
{
    std::vector<uint32_t> block0 = generateRange(0, 200);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    auto result = linearOrToDocIds(cursor, 50, 100); // [50, 150)
    auto expected = generateRange(50, 100); // 50..149
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, MultiBlockLinearOrSecondBlockOnly)
{
    std::vector<uint32_t> block0 = generateRange(0, 150);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    auto result = linearOrToDocIds(cursor, 500, 200); // [500, 700)
    auto expected = generateRange(500, 200);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, MultiBlockLinearOrSpansBothBlocks)
{
    std::vector<uint32_t> block0 = generateRange(0, 150);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    auto result = linearOrToDocIds(cursor, 100, 550); // [100, 650)
    std::vector<uint32_t> expected;
    for (uint32_t d = 100; d < 150; ++d)
        expected.push_back(d);
    for (uint32_t d = 500; d < 650; ++d)
        expected.push_back(d);
    EXPECT_EQ(result, expected);
}


// ===========================================================================================
// Section 30: Multi-large-block — linearAnd
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockLinearAndFullRange)
{
    std::vector<uint32_t> block0 = generateRange(0, 150);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    std::vector<UInt8> buf(700, 0);
    cursor->linearAnd(buf.data(), 0, 700);

    for (auto d : data.all_docs)
        EXPECT_EQ(buf[d], 1u) << "Expected buf[" << d << "] == 1";

    /// Check some docs in the gap are 0
    EXPECT_EQ(buf[200], 0u);
    EXPECT_EQ(buf[400], 0u);
}

TEST(PostingListCursorTest, MultiBlockLinearAndIncrementsExisting)
{
    std::vector<uint32_t> block0 = generateRange(0, 150);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    std::vector<UInt8> buf(700, 1);
    cursor->linearAnd(buf.data(), 0, 700);

    for (auto d : data.all_docs)
        EXPECT_EQ(buf[d], 2u) << "Expected buf[" << d << "] == 2";
    EXPECT_EQ(buf[200], 1u);
}


// ===========================================================================================
// Section 31: Multi-large-block — packed block index binary search (seekImpl)
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockSeekToMiddleOfPackedBlocks)
{
    /// Large block with 400 docs → 399 encoded → 3 full packed blocks + tail of 15.
    /// Seek should binary-search packed_block_last_doc_ids to find the right block.
    std::vector<uint32_t> docs = generateRange(0, 400);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    /// Seek to doc 200 → should be in packed block 1 (docs 129..256)
    cursor->seek(200);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 200u);

    /// Seek forward to 350 → should be in packed block 2
    cursor->seek(350);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 350u);
}

TEST(PostingListCursorTest, MultiBlockSeekExactPackedBlockBoundary)
{
    /// 257 docs → first_doc_id + 256 encoded → 2 full packed blocks.
    /// packed_block_last_doc_ids should be [128, 256].
    std::vector<uint32_t> docs = generateRange(0, 257);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    /// Seek to exactly doc 128 (last doc of first packed block)
    cursor->seek(128);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 128u);

    /// Seek to doc 129 (first doc of second packed block)
    cursor->seek(129);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 129u);
}


// ===========================================================================================
// Section 32: Multi-large-block — next() crossing packed block and large block boundaries
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockNextCrossesPackedBlockBoundary)
{
    /// 260 docs → first_doc_id + 259 encoded → 2 full blocks + tail of 3.
    std::vector<uint32_t> docs = generateRange(0, 260);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    /// Seek to last element in first packed block (doc 128)
    cursor->seek(128);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 128u);

    /// next() should cross to second packed block
    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 129u);
}

TEST(PostingListCursorTest, MultiBlockNextCrossesLargeBlockBoundary)
{
    std::vector<uint32_t> block0 = generateRange(0, 130); // 0..129
    std::vector<uint32_t> block1 = generateRange(500, 130); // 500..629
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    /// Drain to end of block 0
    cursor->seek(129);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 129u);

    /// next() should cross to block 1
    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 500u);
}


// ===========================================================================================
// Section 33: Multi-large-block — tail block handling
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockTailBlockOnly)
{
    /// 10 docs: first_doc_id + 9 encoded → single tail block (9 docs).
    std::vector<uint32_t> docs = generateRange(100, 10);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, data.all_docs.front());
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, MultiBlockExactlyOneFullBlock)
{
    /// 129 docs: first_doc_id + 128 encoded → exactly one full packed block (128).
    std::vector<uint32_t> docs = generateRange(0, 129);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, data.all_docs.front());
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, MultiBlockTailInSecondLargeBlock)
{
    /// Block 0: 129 docs (1 full packed block)
    /// Block 1: 50 docs (tail block only)
    std::vector<uint32_t> block0 = generateRange(0, 129);
    std::vector<uint32_t> block1 = generateRange(500, 50);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, data.all_docs.front());
    EXPECT_EQ(result, data.all_docs);
}


// ===========================================================================================
// Section 34: Multi-large-block — intersection with multi-block cursors
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockIntersectTwoMultiBlockCursors)
{
    /// Cursor A: block0=[0..149], block1=[500..699]
    /// Cursor B: block0=[100..299], block1=[600..799]
    /// Intersection: [100..149] ∪ [600..699]
    auto dataA = makeMultiBlockData({generateRange(0, 150), generateRange(500, 200)});
    auto dataB = makeMultiBlockData({generateRange(100, 200), generateRange(600, 200)});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 800, false, 100.0);

    std::vector<uint32_t> expected;
    for (uint32_t d = 100; d < 150; ++d)
        expected.push_back(d);
    for (uint32_t d = 600; d < 700; ++d)
        expected.push_back(d);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, MultiBlockIntersectMultiBlockWithEmbedded)
{
    /// Multi-block cursor A: [0..199], [500..699]
    /// Embedded cursor B: {50, 150, 550, 650, 800}
    /// Intersection: {50, 150, 550, 650}
    auto dataA = makeMultiBlockData({generateRange(0, 200), generateRange(500, 200)});
    auto infoB = makeEmbeddedInfo({50, 150, 550, 650, 800});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeEmbeddedCursor(infoB);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 1000, false, 100.0);
    std::vector<uint32_t> expected = {50, 150, 550, 650};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, MultiBlockIntersectDisjoint)
{
    auto dataA = makeMultiBlockData({generateRange(0, 150)});
    auto dataB = makeMultiBlockData({generateRange(500, 150)});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 700, false, 100.0);
    EXPECT_TRUE(result.empty());
}


// ===========================================================================================
// Section 35: Multi-large-block — union with multi-block cursors
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockUnionTwoCursors)
{
    auto dataA = makeMultiBlockData({generateRange(0, 150)});
    auto dataB = makeMultiBlockData({generateRange(500, 150)});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);

    auto result = unionAndCollect(postings, {"a", "b"}, 0, 700);
    std::vector<uint32_t> expected;
    for (uint32_t d = 0; d < 150; ++d) expected.push_back(d);
    for (uint32_t d = 500; d < 650; ++d) expected.push_back(d);
    EXPECT_EQ(result, expected);
}


// ===========================================================================================
// Section 36: Multi-large-block — brute-force intersection
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockBruteForceIntersect)
{
    auto dataA = makeMultiBlockData({generateRange(0, 200), generateRange(500, 200)});
    auto dataB = makeMultiBlockData({generateRange(100, 200), generateRange(600, 200)});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 800, true);

    std::vector<uint32_t> expected;
    for (uint32_t d = 100; d < 200; ++d)
        expected.push_back(d);
    for (uint32_t d = 600; d < 700; ++d)
        expected.push_back(d);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, MultiBlockBruteForceVsLeapfrogConsistency)
{
    auto dataA = makeMultiBlockData({generateRange(0, 200), generateRange(500, 200)});
    auto dataB = makeMultiBlockData({generateRange(50, 250), generateRange(550, 250)});

    // Brute force
    {
        PostingListCursorMap postings;
        postings["a"] = makeMultiBlockCursor(dataA);
        postings["b"] = makeMultiBlockCursor(dataB);
        auto bf = intersectAndCollect(postings, {"a", "b"}, 0, 800, true);

        PostingListCursorMap postings2;
        postings2["a"] = makeMultiBlockCursor(dataA);
        postings2["b"] = makeMultiBlockCursor(dataB);
        auto lf = intersectAndCollect(postings2, {"a", "b"}, 0, 800, false, 100.0);

        EXPECT_EQ(bf, lf);
    }
}


// ===========================================================================================
// Section 37: Multi-large-block — large doc count stress test
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockLargeDocCount)
{
    /// 1000 docs in one block: first_doc_id + 999 encoded.
    /// 999 / 128 = 7 full blocks + tail of 103.
    std::vector<uint32_t> docs = generateRange(0, 1000);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, data.all_docs.front());
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, MultiBlockManyLargeBlocks)
{
    /// 5 large blocks of 130 docs each, with gaps.
    std::vector<std::vector<uint32_t>> blocks;
    for (int i = 0; i < 5; ++i)
        blocks.push_back(generateRange(static_cast<uint32_t>(i * 500), 130));

    auto data = makeMultiBlockData(blocks);
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, data.all_docs.front());
    EXPECT_EQ(result, data.all_docs);
}

TEST(PostingListCursorTest, MultiBlockSeekAcrossManyBlocks)
{
    std::vector<std::vector<uint32_t>> blocks;
    for (int i = 0; i < 5; ++i)
        blocks.push_back(generateRange(static_cast<uint32_t>(i * 500), 130));

    auto data = makeMultiBlockData(blocks);
    auto cursor = makeMultiBlockCursor(data);

    /// Seek to each block's midpoint
    for (int i = 0; i < 5; ++i)
    {
        uint32_t target = static_cast<uint32_t>(i * 500 + 65);
        cursor->seek(target);
        ASSERT_TRUE(cursor->valid()) << "Block " << i << " seek failed";
        EXPECT_EQ(cursor->value(), target) << "Block " << i;
    }

    /// Seek beyond last block
    cursor->seek(5000);
    EXPECT_FALSE(cursor->valid());
}


// ===========================================================================================
// Section 38: Multi-large-block — cardinality and density
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockCardinality)
{
    std::vector<uint32_t> block0 = generateRange(0, 150);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    EXPECT_EQ(cursor->cardinality(), 350u);
}


// ===========================================================================================
// Section 39: Multi-block — seek to middle packed block, then next() across large block boundary
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockSeekToMiddlePackedBlockThenDrainAcrossLargeBlock)
{
    /// Block 0: 400 docs (0..399) → first_doc_id + 399 encoded → 3 full packed blocks + tail.
    /// Block 1: 200 docs (1000..1199).
    /// Seek to doc 200 (middle of packed block 1), then next() all the way through
    /// block 0 and across into block 1.
    std::vector<uint32_t> block0 = generateRange(0, 400);
    std::vector<uint32_t> block1 = generateRange(1000, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    cursor->seek(200);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 200u);

    /// Drain the rest and verify
    std::vector<uint32_t> result;
    while (cursor->valid())
    {
        result.push_back(cursor->value());
        cursor->next();
    }

    std::vector<uint32_t> expected;
    for (uint32_t d = 200; d < 400; ++d)
        expected.push_back(d);
    for (uint32_t d = 1000; d < 1200; ++d)
        expected.push_back(d);
    EXPECT_EQ(result, expected);
}


// ===========================================================================================
// Section 40: Multi-block — seek on sparse doc IDs (gap values)
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockSparseSeekToGapValue)
{
    /// Sparse docs: 0,3,6,...,447 in block 0; 1000,1005,...,1745 in block 1.
    /// Seek to values that fall in gaps between actual doc IDs.
    std::vector<uint32_t> block0 = generateRange(0, 150, 3);  // 0,3,6,...,447
    std::vector<uint32_t> block1 = generateRange(1000, 150, 5); // 1000,1005,...,1745
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    /// Seek to 4 (between 3 and 6) → should land on 6
    cursor->seek(4);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 6u);

    /// Seek to 100 (between 99 and 102) → should land on 102
    cursor->seek(100);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 102u);

    /// Seek to 500 (in gap between blocks) → should land on 1000
    cursor->seek(500);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 1000u);

    /// Seek to 1001 (between 1000 and 1005) → should land on 1005
    cursor->seek(1001);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 1005u);

    /// Seek to 1746 (beyond last doc) → invalid
    cursor->seek(1746);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, MultiBlockSparseSeekToFirstDocOfEachBlock)
{
    std::vector<uint32_t> block0 = generateRange(10, 150, 3);  // 10,13,...,457
    std::vector<uint32_t> block1 = generateRange(1000, 150, 5); // 1000,1005,...,1745
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    /// Seek to exact first doc of block 0
    cursor->seek(10);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 10u);

    /// Seek to exact first doc of block 1
    cursor->seek(1000);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 1000u);
}


// ===========================================================================================
// Section 41: Multi-block — linearOr/linearAnd when range misses all large blocks
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockLinearOrRangeBeforeAllBlocks)
{
    /// Block 0: 500..649, Block 1: 1000..1149
    /// Range [0, 100) is entirely before block 0.
    std::vector<uint32_t> block0 = generateRange(500, 150);
    std::vector<uint32_t> block1 = generateRange(1000, 150);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    auto result = linearOrToDocIds(cursor, 0, 100);
    EXPECT_TRUE(result.empty());
}

TEST(PostingListCursorTest, MultiBlockLinearOrRangeAfterAllBlocks)
{
    /// Block 0: 0..149, Block 1: 500..699
    /// Range [2000, 100) is entirely after all blocks.
    std::vector<uint32_t> block0 = generateRange(0, 150);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    auto result = linearOrToDocIds(cursor, 2000, 100);
    EXPECT_TRUE(result.empty());
}

TEST(PostingListCursorTest, MultiBlockLinearOrRangeInGapBetweenBlocks)
{
    /// Block 0: 0..149, Block 1: 500..699
    /// Range [200, 200) = [200, 400) is in the gap.
    std::vector<uint32_t> block0 = generateRange(0, 150);
    std::vector<uint32_t> block1 = generateRange(500, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    auto result = linearOrToDocIds(cursor, 200, 200);
    EXPECT_TRUE(result.empty());
}

TEST(PostingListCursorTest, MultiBlockLinearAndRangeBeforeAllBlocks)
{
    std::vector<uint32_t> block0 = generateRange(500, 150);
    std::vector<uint32_t> block1 = generateRange(1000, 150);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    std::vector<UInt8> buf(100, 1);
    cursor->linearAnd(buf.data(), 0, 100);

    /// All values should remain 1 (no increments)
    for (size_t i = 0; i < 100; ++i)
        EXPECT_EQ(buf[i], 1u) << "Unexpected increment at offset " << i;
}


// ===========================================================================================
// Section 42: Multi-block — density computation verification
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockDensityPerfectlyDenseBlock0)
{
    /// Block 0: consecutive docs 0..199 → 200 total docs.
    /// large_block_doc_count = 199 (first_doc_id excluded from count).
    /// range = (0, 199), range_span = 200.
    /// density = (199 + 1) / 200 = 1.0
    std::vector<uint32_t> docs = generateRange(0, 200);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    EXPECT_DOUBLE_EQ(cursor->density(), 1.0);
}

TEST(PostingListCursorTest, MultiBlockDensitySparseBlock0)
{
    /// Block 0: sparse docs 0,3,6,...,447 → 150 docs total.
    /// large_block_doc_count = 149.
    /// range = (0, 447), range_span = 448.
    /// density = (149 + 1) / 448 = 150/448
    std::vector<uint32_t> docs = generateRange(0, 150, 3);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    double expected_density = 150.0 / 448.0;
    EXPECT_NEAR(cursor->density(), expected_density, 1e-6);
}

TEST(PostingListCursorTest, MultiBlockDensityAfterSeekToSecondLargeBlock)
{
    /// Block 0: 0..199 (dense, 200 docs).
    /// Block 1: 500,502,504,...,698 → 100 docs, step 2.
    /// Global density = cardinality / (global_end - global_begin + 1) = 300 / 699.
    /// Density is computed once at construction and does not change after seek.
    std::vector<uint32_t> block0 = generateRange(0, 200);
    std::vector<uint32_t> block1 = generateRange(500, 100, 2); // 500,502,...,698
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    double expected_density = 300.0 / 699.0;
    EXPECT_NEAR(cursor->density(), expected_density, 1e-6);

    /// Seek to block 1 — density stays the same (global).
    cursor->seek(500);
    ASSERT_TRUE(cursor->valid());
    EXPECT_NEAR(cursor->density(), expected_density, 1e-6);
}


// ===========================================================================================
// Section 43: Multi-block — 3-way and 4-way intersection with multi-block cursors
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockIntersectThreeMultiBlockCursors)
{
    /// Cursor A: [0..199], [500..699]
    /// Cursor B: [50..249], [550..749]
    /// Cursor C: [100..299], [600..799]
    /// Intersection: [100..199] ∪ [600..699]
    auto dataA = makeMultiBlockData({generateRange(0, 200), generateRange(500, 200)});
    auto dataB = makeMultiBlockData({generateRange(50, 200), generateRange(550, 200)});
    auto dataC = makeMultiBlockData({generateRange(100, 200), generateRange(600, 200)});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);
    postings["c"] = makeMultiBlockCursor(dataC);

    auto result = intersectAndCollect(postings, {"a", "b", "c"}, 0, 800, false, 100.0);

    std::vector<uint32_t> expected;
    for (uint32_t d = 100; d < 200; ++d) expected.push_back(d);
    for (uint32_t d = 600; d < 700; ++d) expected.push_back(d);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, MultiBlockIntersectFourMultiBlockCursors)
{
    /// Cursor A: [0..199]
    /// Cursor B: [50..249]
    /// Cursor C: [100..299]
    /// Cursor D: [150..349]
    /// Intersection: [150..199]
    auto dataA = makeMultiBlockData({generateRange(0, 200)});
    auto dataB = makeMultiBlockData({generateRange(50, 200)});
    auto dataC = makeMultiBlockData({generateRange(100, 200)});
    auto dataD = makeMultiBlockData({generateRange(150, 200)});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);
    postings["c"] = makeMultiBlockCursor(dataC);
    postings["d"] = makeMultiBlockCursor(dataD);

    auto result = intersectAndCollect(postings, {"a", "b", "c", "d"}, 0, 400, false, 100.0);

    std::vector<uint32_t> expected;
    for (uint32_t d = 150; d < 200; ++d) expected.push_back(d);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, MultiBlockIntersectFiveMultiBlockLeapfrogLinear)
{
    /// 5 cursors → dispatches to intersectLeapfrogLinear.
    /// All share [100..149].
    auto dataA = makeMultiBlockData({generateRange(0, 200)});
    auto dataB = makeMultiBlockData({generateRange(50, 200)});
    auto dataC = makeMultiBlockData({generateRange(100, 200)});
    auto dataD = makeMultiBlockData({generateRange(100, 150)});
    auto dataE = makeMultiBlockData({generateRange(100, 130)});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);
    postings["c"] = makeMultiBlockCursor(dataC);
    postings["d"] = makeMultiBlockCursor(dataD);
    postings["e"] = makeMultiBlockCursor(dataE);

    auto result = intersectAndCollect(postings, {"a", "b", "c", "d", "e"}, 0, 400, false, 100.0);

    /// Intersection is [100..229] ∩ each cursor. Cursor E is [100..229] (130 docs).
    /// Cursor D is [100..249]. Cursor C is [100..299]. Cursor B is [50..249]. Cursor A is [0..199].
    /// So intersection = [100..199] ∩ [100..229] = [100..199]
    std::vector<uint32_t> expected;
    for (uint32_t d = 100; d < 200; ++d) expected.push_back(d);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, MultiBlockIntersectThreeDisjoint)
{
    auto dataA = makeMultiBlockData({generateRange(0, 150)});
    auto dataB = makeMultiBlockData({generateRange(200, 150)});
    auto dataC = makeMultiBlockData({generateRange(400, 150)});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);
    postings["c"] = makeMultiBlockCursor(dataC);

    auto result = intersectAndCollect(postings, {"a", "b", "c"}, 0, 600, false, 100.0);
    EXPECT_TRUE(result.empty());
}


// ===========================================================================================
// Section 44: Multi-block — seek to first_doc_id with non-zero start
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockSeekToFirstDocIdNonZero)
{
    /// first_doc_id = 1000 (ranges[0].begin), stored separately.
    /// Seek to exactly 1000 should find it via the prepend_first_doc_id logic.
    std::vector<uint32_t> docs = generateRange(1000, 200); // 1000..1199
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    cursor->seek(1000);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 1000u);

    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 1001u);
}

TEST(PostingListCursorTest, MultiBlockSeekBeforeFirstDocIdNonZero)
{
    /// first_doc_id = 500. Seek to 400 (before range) should land on 500.
    std::vector<uint32_t> docs = generateRange(500, 200); // 500..699
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    cursor->seek(400);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 500u);
}

TEST(PostingListCursorTest, MultiBlockDrainFromFirstDocIdNonZero)
{
    /// Verify all docs including first_doc_id are returned when draining.
    std::vector<uint32_t> docs = generateRange(5000, 300); // 5000..5299
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, 5000);
    EXPECT_EQ(result, docs);
}


// ===========================================================================================
// Section 45: Multi-block — need_seek_before_decode state transitions
//   Seek sets need_seek=true, decodeNextBlock clears it, subsequent next()
//   should do sequential reads (no redundant seek).
//   We verify indirectly: seek to a packed block, then next() through
//   the remaining packed blocks and into the next large block.
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockSeekThenSequentialNextThroughMultiplePackedBlocks)
{
    /// 500 docs in block 0 → first_doc_id + 499 encoded → 3 full blocks + tail.
    /// 200 docs in block 1.
    /// Seek to doc 200 (packed block 1), then next() all the way.
    std::vector<uint32_t> block0 = generateRange(0, 500);
    std::vector<uint32_t> block1 = generateRange(1000, 200);
    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    cursor->seek(200);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 200u);

    /// Walk through remaining packed blocks in block 0
    uint32_t prev = 200;
    while (cursor->valid() && cursor->value() < 500)
    {
        EXPECT_GE(cursor->value(), prev);
        prev = cursor->value();
        cursor->next();
    }

    /// Should now be at block 1
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 1000u);

    /// Walk through block 1
    prev = 1000;
    while (cursor->valid())
    {
        EXPECT_GE(cursor->value(), prev);
        prev = cursor->value();
        cursor->next();
    }
    EXPECT_EQ(prev, 1199u);
}


// ===========================================================================================
// Section 46: Multi-block — single doc in block (doc_count == 1 equivalent)
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockMinimalBlock)
{
    /// Block 0 with only 2 docs: first_doc_id + 1 encoded doc.
    /// This is the minimum for the large block path (1 doc would be embedded).
    std::vector<uint32_t> docs = {100, 200};
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, 100);
    EXPECT_EQ(result, docs);
}


// ===========================================================================================
// Section 47: Multi-block — many large blocks (10+) seek stress test
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockTenLargeBlocksSeekStress)
{
    /// 10 large blocks, each 130 docs, spaced 500 apart.
    std::vector<std::vector<uint32_t>> blocks;
    for (int i = 0; i < 10; ++i)
        blocks.push_back(generateRange(static_cast<uint32_t>(i * 500), 130));

    auto data = makeMultiBlockData(blocks);
    auto cursor = makeMultiBlockCursor(data);

    /// Forward seek to every block's last doc
    for (int i = 0; i < 10; ++i)
    {
        uint32_t last_doc = static_cast<uint32_t>(i * 500 + 129);
        cursor->seek(last_doc);
        ASSERT_TRUE(cursor->valid()) << "Block " << i << " last doc seek failed";
        EXPECT_EQ(cursor->value(), last_doc) << "Block " << i;
    }

    /// Seek beyond all
    cursor->seek(5000);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, MultiBlockTenLargeBlocksFullDrain)
{
    std::vector<std::vector<uint32_t>> blocks;
    for (int i = 0; i < 10; ++i)
        blocks.push_back(generateRange(static_cast<uint32_t>(i * 500), 130));

    auto data = makeMultiBlockData(blocks);
    auto cursor = makeMultiBlockCursor(data);

    auto result = seekAndDrainCursor(cursor, data.all_docs.front());
    EXPECT_EQ(result, data.all_docs);
}

TEST(PostingListCursorTest, MultiBlockTwentyLargeBlocksSeekSkip)
{
    /// 20 blocks — seek to every other block to test skipping.
    std::vector<std::vector<uint32_t>> blocks;
    for (int i = 0; i < 20; ++i)
        blocks.push_back(generateRange(static_cast<uint32_t>(i * 300), 130));

    auto data = makeMultiBlockData(blocks);
    auto cursor = makeMultiBlockCursor(data);

    /// Seek to every even-numbered block
    for (int i = 0; i < 20; i += 2)
    {
        uint32_t target = static_cast<uint32_t>(i * 300 + 50);
        cursor->seek(target);
        ASSERT_TRUE(cursor->valid()) << "Block " << i;
        EXPECT_EQ(cursor->value(), target) << "Block " << i;
    }
}


// ===========================================================================================
// Section 48: Multi-block + Embedded mixed — brute-force path
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockWithEmbeddedBruteForceIntersect)
{
    /// Multi-block cursor A: [0..199], [500..699]
    /// Embedded cursor B: all even numbers 0,2,4,...,698
    /// Brute-force intersection: even numbers in [0..199] ∪ even numbers in [500..699]
    auto dataA = makeMultiBlockData({generateRange(0, 200), generateRange(500, 200)});
    auto dataB = makeMultiBlockData({generateRange(0, 350, 2)}); // 0,2,4,...,698

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 700, true);

    std::vector<uint32_t> expected;
    for (uint32_t d = 0; d < 200; d += 2) expected.push_back(d);
    for (uint32_t d = 500; d < 700; d += 2) expected.push_back(d);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, MultiBlockWithEmbeddedBruteForceThreeWay)
{
    /// 3-way brute-force: multi-block A, multi-block B, embedded C.
    auto dataA = makeMultiBlockData({generateRange(0, 300)});
    auto dataB = makeMultiBlockData({generateRange(100, 300)});
    auto dataC = makeMultiBlockData({generateRange(150, 50)}); // 150..199

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);
    postings["c"] = makeMultiBlockCursor(dataC);

    auto result = intersectAndCollect(postings, {"a", "b", "c"}, 0, 500, true);

    /// A=[0..299], B=[100..399], C=[150..199]
    /// Intersection = [150..199]
    std::vector<uint32_t> expected = generateRange(150, 50);
    EXPECT_EQ(result, expected);
}


// ===========================================================================================
// Section 49: Multi-block — linearOrImpl/linearAndImpl packed block skip/early-return branches
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockLinearOrPackedBlockSkipBeforeRange)
{
    /// 400 docs: first_doc_id=0, docs 0..399. Packed blocks:
    ///   block 0: docs 0..128, block 1: docs 129..256, block 2: docs 257..384, tail: 385..399
    /// linearOr with row_offset=300 should skip blocks 0 and 1 (back() < 300),
    /// then process blocks 2 and tail.
    std::vector<uint32_t> docs = generateRange(0, 400);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto result = linearOrToDocIds(cursor, 300, 100); // [300, 400)
    std::vector<uint32_t> expected = generateRange(300, 100);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, MultiBlockLinearOrPackedBlockEarlyReturnAfterRange)
{
    /// Same 400 docs. linearOr with range [50, 150) should process block 0 and
    /// part of block 1, then early-return when block 2's front > 150.
    std::vector<uint32_t> docs = generateRange(0, 400);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto result = linearOrToDocIds(cursor, 50, 100); // [50, 150)
    std::vector<uint32_t> expected = generateRange(50, 100);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, MultiBlockLinearAndPackedBlockSkipAndEarlyReturn)
{
    /// Verify that linearAnd also correctly skips blocks before range and
    /// stops early after range.
    std::vector<uint32_t> docs = generateRange(0, 500);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    std::vector<UInt8> buf(100, 0);
    cursor->linearAnd(buf.data(), 200, 100); // [200, 300)

    for (size_t i = 0; i < 100; ++i)
        EXPECT_EQ(buf[i], 1u) << "Expected buf[" << i << "] == 1 (doc " << (200 + i) << ")";
}


// ===========================================================================================
// Section 50: Multi-block — union with multiple multi-block cursors
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockUnionThreeMultiBlockCursors)
{
    auto dataA = makeMultiBlockData({generateRange(0, 150)});   // 0..149
    auto dataB = makeMultiBlockData({generateRange(200, 150)});  // 200..349
    auto dataC = makeMultiBlockData({generateRange(400, 150)});  // 400..549

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);
    postings["c"] = makeMultiBlockCursor(dataC);

    auto result = unionAndCollect(postings, {"a", "b", "c"}, 0, 600);

    std::vector<uint32_t> expected;
    for (uint32_t d = 0; d < 150; ++d) expected.push_back(d);
    for (uint32_t d = 200; d < 350; ++d) expected.push_back(d);
    for (uint32_t d = 400; d < 550; ++d) expected.push_back(d);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, MultiBlockUnionOverlapping)
{
    /// Two multi-block cursors with overlapping doc ranges.
    auto dataA = makeMultiBlockData({generateRange(0, 200)});   // 0..199
    auto dataB = makeMultiBlockData({generateRange(100, 200)});  // 100..299

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);

    auto result = unionAndCollect(postings, {"a", "b"}, 0, 300);

    /// Union should be 0..299
    std::vector<uint32_t> expected = generateRange(0, 300);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, MultiBlockUnionWithWindow)
{
    auto dataA = makeMultiBlockData({generateRange(0, 200), generateRange(500, 200)});
    auto dataB = makeMultiBlockData({generateRange(300, 200)});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);

    /// Window [250, 600) should capture: A's [500..599], B's [300..499]
    auto result = unionAndCollect(postings, {"a", "b"}, 250, 350);

    std::vector<uint32_t> expected;
    for (uint32_t d = 300; d < 500; ++d) expected.push_back(d);
    for (uint32_t d = 500; d < 600; ++d) expected.push_back(d);
    EXPECT_EQ(result, expected);
}


// ===========================================================================================
// Section 51: Multi-block intersection — brute-force vs leapfrog consistency (3+ cursors)
// ===========================================================================================

TEST(PostingListCursorTest, MultiBlockBruteForceVsLeapfrogThreeWay)
{
    auto dataA = makeMultiBlockData({generateRange(0, 300), generateRange(500, 200)});
    auto dataB = makeMultiBlockData({generateRange(50, 300), generateRange(550, 200)});
    auto dataC = makeMultiBlockData({generateRange(100, 200), generateRange(600, 150)});

    // Brute force
    PostingListCursorMap postings_bf;
    postings_bf["a"] = makeMultiBlockCursor(dataA);
    postings_bf["b"] = makeMultiBlockCursor(dataB);
    postings_bf["c"] = makeMultiBlockCursor(dataC);
    auto bf = intersectAndCollect(postings_bf, {"a", "b", "c"}, 0, 800, true);

    // Leapfrog
    PostingListCursorMap postings_lf;
    postings_lf["a"] = makeMultiBlockCursor(dataA);
    postings_lf["b"] = makeMultiBlockCursor(dataB);
    postings_lf["c"] = makeMultiBlockCursor(dataC);
    auto lf = intersectAndCollect(postings_lf, {"a", "b", "c"}, 0, 800, false, 100.0);

    EXPECT_EQ(bf, lf);

    /// Also verify expected result:
    /// A∩B∩C block0: [100..299] ∩ [50..349] ∩ [0..299] = [100..299]
    /// A∩B∩C block1: [500..699] ∩ [550..749] ∩ [600..749] = [600..699]
    std::vector<uint32_t> expected;
    for (uint32_t d = 100; d < 300; ++d) expected.push_back(d);
    for (uint32_t d = 600; d < 700; ++d) expected.push_back(d);
    EXPECT_EQ(bf, expected);
}

TEST(PostingListCursorTest, MultiBlockBruteForceVsLeapfrogFourWay)
{
    auto dataA = makeMultiBlockData({generateRange(0, 250)});
    auto dataB = makeMultiBlockData({generateRange(50, 250)});
    auto dataC = makeMultiBlockData({generateRange(100, 250)});
    auto dataD = makeMultiBlockData({generateRange(150, 250)});

    PostingListCursorMap postings_bf;
    postings_bf["a"] = makeMultiBlockCursor(dataA);
    postings_bf["b"] = makeMultiBlockCursor(dataB);
    postings_bf["c"] = makeMultiBlockCursor(dataC);
    postings_bf["d"] = makeMultiBlockCursor(dataD);
    auto bf = intersectAndCollect(postings_bf, {"a", "b", "c", "d"}, 0, 500, true);

    PostingListCursorMap postings_lf;
    postings_lf["a"] = makeMultiBlockCursor(dataA);
    postings_lf["b"] = makeMultiBlockCursor(dataB);
    postings_lf["c"] = makeMultiBlockCursor(dataC);
    postings_lf["d"] = makeMultiBlockCursor(dataD);
    auto lf = intersectAndCollect(postings_lf, {"a", "b", "c", "d"}, 0, 500, false, 100.0);

    EXPECT_EQ(bf, lf);

    std::vector<uint32_t> expected;
    for (uint32_t d = 150; d < 250; ++d) expected.push_back(d);
    EXPECT_EQ(bf, expected);
}


// ===========================================================================================
// Section: Arithmetic block skip optimization tests
//
// These tests verify that the TurboPFor constant/zero-delta block fast-skip
// optimization produces identical results to full decompression.  Key scenarios:
//   - Zero-delta blocks (consecutive doc_ids, step=1)
//   - Constant-delta blocks (uniform step > 1)
//   - Mixed blocks (some arithmetic, some not)
//   - seek/next/linearOr/linearAnd/intersection through arithmetic blocks
//   - Tail blocks (< 128 elements) that are arithmetic
//   - Large block 0 packed block 0 is excluded (prepend_first_doc_id)
//   - Transitions between arithmetic and non-arithmetic blocks
// ===========================================================================================

/// Zero-delta block: 257 consecutive docs → packed block 0 (128, prepend, not arithmetic),
/// packed block 1 (128, zero-delta → arithmetic).  Seek into the arithmetic block.
TEST(PostingListCursorTest, ArithmeticZeroDeltaSeekDrain)
{
    auto docs = generateRange(0, 257);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    /// Seek to a doc_id in the second packed block (block 1, which is arithmetic).
    cursor->seek(200);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 200u);

    /// Drain the rest and verify.
    auto remaining = drainCursor(cursor);
    std::vector<uint32_t> expected;
    for (uint32_t d = 200; d <= 256; ++d)
        expected.push_back(d);
    EXPECT_EQ(remaining, expected);
}

/// Zero-delta: full drain via seek(0) + next.
TEST(PostingListCursorTest, ArithmeticZeroDeltaFullDrain)
{
    auto docs = generateRange(0, 257);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto all = seekAndDrainCursor(cursor, 0);
    EXPECT_EQ(all, docs);
}

/// Zero-delta: linearOr over the full range.
TEST(PostingListCursorTest, ArithmeticZeroDeltaLinearOr)
{
    auto docs = generateRange(0, 257);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto result = linearOrToDocIds(cursor, 0, 260);
    EXPECT_EQ(result, docs);
}

/// Zero-delta: linearOr with a partial range that clips into the arithmetic block.
TEST(PostingListCursorTest, ArithmeticZeroDeltaLinearOrPartialRange)
{
    auto docs = generateRange(0, 257);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    /// Request range [150, 230) — starts in packed block 1 (arithmetic).
    auto result = linearOrToDocIds(cursor, 150, 80);
    std::vector<uint32_t> expected;
    for (uint32_t d = 150; d < 230; ++d)
        expected.push_back(d);
    EXPECT_EQ(result, expected);
}

/// Zero-delta: linearAnd over full range.
TEST(PostingListCursorTest, ArithmeticZeroDeltaLinearAnd)
{
    auto docs = generateRange(0, 257);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    std::vector<UInt8> buf(260, 0);
    cursor->linearAnd(buf.data(), 0, 260);
    /// linearAnd increments counters; check that the arithmetic block docs are counted.
    for (uint32_t d = 0; d <= 256; ++d)
        EXPECT_EQ(buf[d], 1u) << "at doc_id " << d;
    for (uint32_t d = 257; d < 260; ++d)
        EXPECT_EQ(buf[d], 0u);
}

/// Constant-delta block: step=3 (constant_value=2).
/// 257 docs with step 3: block 1 is constant-delta arithmetic.
TEST(PostingListCursorTest, ArithmeticConstantDeltaSeekDrain)
{
    auto docs = generateRange(0, 257, 3);  // 0, 3, 6, 9, ..., 768
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    /// Seek to something in the second packed block (arithmetic, step=3).
    /// Block 1 starts at docs[129] = 129*3 = 387.
    cursor->seek(400);
    ASSERT_TRUE(cursor->valid());
    /// 400 is not a multiple of 3 from 0, so we expect the next multiple: ceil((400-387)/3)*3 + 387
    /// docs in block 1: 387, 390, 393, ...; first >= 400 is 402.
    EXPECT_EQ(cursor->value(), 402u);

    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 405u);
}

/// Constant-delta: full drain.
TEST(PostingListCursorTest, ArithmeticConstantDeltaFullDrain)
{
    auto docs = generateRange(0, 257, 3);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto all = seekAndDrainCursor(cursor, 0);
    EXPECT_EQ(all, docs);
}

/// Constant-delta: linearOr.
TEST(PostingListCursorTest, ArithmeticConstantDeltaLinearOr)
{
    auto docs = generateRange(0, 257, 3);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto result = linearOrToDocIds(cursor, 0, 770);
    EXPECT_EQ(result, docs);
}

/// Constant-delta: seek to exact arithmetic value.
TEST(PostingListCursorTest, ArithmeticConstantDeltaSeekExact)
{
    auto docs = generateRange(10, 257, 5);  // 10, 15, 20, ..., 10 + 256*5 = 1290
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    /// Seek to 650 (which is 10 + 128*5 = 650, first doc of block 1).
    cursor->seek(650);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 650u);
}

/// Constant-delta: seek to first and last doc of arithmetic block.
TEST(PostingListCursorTest, ArithmeticSeekFirstAndLastOfBlock)
{
    auto docs = generateRange(0, 257, 2);  // 0, 2, 4, ..., 512
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    /// Block 1 (arithmetic): docs[129]=258 to docs[256]=512.
    cursor->seek(258);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 258u);

    /// Re-create cursor, seek to last doc of block 1.
    auto cursor2 = makeMultiBlockCursor(data);
    cursor2->seek(512);
    ASSERT_TRUE(cursor2->valid());
    EXPECT_EQ(cursor2->value(), 512u);

    cursor2->next();
    EXPECT_FALSE(cursor2->valid());
}

/// Seek within arithmetic block then next() transitions to next block.
TEST(PostingListCursorTest, ArithmeticNextTransitionsToNextBlock)
{
    /// 385 consecutive docs → docs_to_encode = 384 → 3 packed blocks of 128.
    /// Blocks 1 and 2 are zero-delta arithmetic.
    auto docs = generateRange(0, 385);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    /// Seek to last element of block 1: docs[256]=256.
    cursor->seek(256);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 256u);

    /// next() should transition to block 2 (also arithmetic).
    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 257u);
}

/// Mixed: first block is non-arithmetic, second block is arithmetic.
TEST(PostingListCursorTest, ArithmeticMixedNonArithThenArith)
{
    std::vector<uint32_t> docs;
    docs.push_back(0);  // first_doc_id

    /// Block 0: 128 docs with variable gaps (non-constant delta).
    uint32_t prev = 0;
    std::mt19937 rng(42);
    for (int i = 0; i < 128; ++i)
    {
        prev += 1 + (rng() % 5);  // gap 1-5 (variable → non-constant delta)
        docs.push_back(prev);
    }

    /// Block 1: 128 consecutive docs after prev (step=1 → zero-delta).
    for (int i = 0; i < 128; ++i)
    {
        ++prev;
        docs.push_back(prev);
    }

    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto all = seekAndDrainCursor(cursor, 0);
    EXPECT_EQ(all, docs);
}

/// Seek across a mixed sequence: seek from non-arithmetic block into arithmetic block.
TEST(PostingListCursorTest, ArithmeticSeekFromNonArithToArith)
{
    std::vector<uint32_t> docs;
    docs.push_back(0);

    uint32_t prev = 0;
    std::mt19937 rng(123);
    for (int i = 0; i < 128; ++i)
    {
        prev += 1 + (rng() % 10);
        docs.push_back(prev);
    }

    /// Block 1: 128 docs with step=1 starting from prev+1.
    uint32_t block1_start = prev + 1;
    for (int i = 0; i < 128; ++i)
        docs.push_back(block1_start + static_cast<uint32_t>(i));

    auto data = makeMultiBlockData({docs});

    /// Seek from block 0 into block 1.
    auto cursor = makeMultiBlockCursor(data);
    cursor->seek(block1_start + 50);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), block1_start + 50);
}

/// Tail block (< 128 elements) that is arithmetic.
TEST(PostingListCursorTest, ArithmeticTailBlock)
{
    /// 179 docs consecutive → docs_to_encode=178 → packed block 0 (128) + tail block (50, zero-delta arithmetic).
    auto docs = generateRange(0, 179);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    /// Seek into the tail block.
    cursor->seek(140);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 140u);

    auto remaining = drainCursor(cursor);
    std::vector<uint32_t> expected;
    for (uint32_t d = 140; d <= 178; ++d)
        expected.push_back(d);
    EXPECT_EQ(remaining, expected);
}

/// Tail block with constant delta > 1.
TEST(PostingListCursorTest, ArithmeticTailBlockConstantDelta)
{
    /// 179 docs with step 4.
    auto docs = generateRange(0, 179, 4);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    /// Tail block starts at docs[129] = 129 * 4 = 516.
    cursor->seek(516);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 516u);

    /// Seek to non-exact value in tail.
    auto cursor2 = makeMultiBlockCursor(data);
    cursor2->seek(520);
    ASSERT_TRUE(cursor2->valid());
    EXPECT_EQ(cursor2->value(), 520u);

    auto cursor3 = makeMultiBlockCursor(data);
    cursor3->seek(519);
    ASSERT_TRUE(cursor3->valid());
    EXPECT_EQ(cursor3->value(), 520u);
}

/// Block 0 of large block 0 is NOT arithmetic (prepend_first_doc_id).
/// Verify that the optimization does not apply to it.
TEST(PostingListCursorTest, ArithmeticBlock0NotOptimized)
{
    /// 129 consecutive docs: docs_to_encode has 128 (one full packed block).
    /// This packed block 0 has prepend_first_doc_id, so it should NOT be arithmetic.
    auto docs = generateRange(0, 129);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    /// Should still work correctly — full decode path.
    auto all = seekAndDrainCursor(cursor, 0);
    EXPECT_EQ(all, docs);
}

/// Intersection (leapfrog) with arithmetic blocks.
TEST(PostingListCursorTest, ArithmeticIntersectTwoLeapfrog)
{
    /// Cursor A: 257 consecutive docs [0..256], block 1 is arithmetic.
    auto docsA = generateRange(0, 257);
    auto dataA = makeMultiBlockData({docsA});

    /// Cursor B: 257 docs with step 2 [0, 2, 4, ..., 512], block 1 is arithmetic.
    auto docsB = generateRange(0, 257, 2);
    auto dataB = makeMultiBlockData({docsB});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);
    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 520, false, 100.0);

    /// Intersection: even numbers from 0 to 256.
    std::vector<uint32_t> expected;
    for (uint32_t d = 0; d <= 256; d += 2)
        expected.push_back(d);
    EXPECT_EQ(result, expected);
}

/// Intersection (brute force) with arithmetic blocks.
TEST(PostingListCursorTest, ArithmeticIntersectTwoBruteForce)
{
    auto docsA = generateRange(0, 257);
    auto dataA = makeMultiBlockData({docsA});

    auto docsB = generateRange(0, 257, 2);
    auto dataB = makeMultiBlockData({docsB});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);
    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 520, true);

    std::vector<uint32_t> expected;
    for (uint32_t d = 0; d <= 256; d += 2)
        expected.push_back(d);
    EXPECT_EQ(result, expected);
}

/// Union with arithmetic blocks.
TEST(PostingListCursorTest, ArithmeticUnionTwo)
{
    auto docsA = generateRange(0, 257);
    auto dataA = makeMultiBlockData({docsA});

    auto docsB = generateRange(300, 130, 2);  // 300, 302, ..., 558
    auto dataB = makeMultiBlockData({docsB});

    PostingListCursorMap postings;
    postings["a"] = makeMultiBlockCursor(dataA);
    postings["b"] = makeMultiBlockCursor(dataB);
    auto result = unionAndCollect(postings, {"a", "b"}, 0, 600);

    /// Build expected: union of both sets.
    std::set<uint32_t> expected_set(docsA.begin(), docsA.end());
    expected_set.insert(docsB.begin(), docsB.end());
    std::vector<uint32_t> expected(expected_set.begin(), expected_set.end());
    EXPECT_EQ(result, expected);
}

/// Multiple packed blocks, all arithmetic (zero-delta), drain through all.
TEST(PostingListCursorTest, ArithmeticMultipleBlocksAllZeroDelta)
{
    /// 513 consecutive docs → docs_to_encode has 512 → 4 packed blocks of 128,
    /// blocks 1,2,3 are all zero-delta arithmetic.
    auto docs = generateRange(0, 513);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto all = seekAndDrainCursor(cursor, 0);
    EXPECT_EQ(all, docs);
}

/// Multiple packed blocks, all arithmetic, linearOr.
TEST(PostingListCursorTest, ArithmeticMultipleBlocksLinearOr)
{
    auto docs = generateRange(0, 513);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto result = linearOrToDocIds(cursor, 0, 520);
    EXPECT_EQ(result, docs);
}

/// Seek to every doc_id in an arithmetic block to verify correctness.
TEST(PostingListCursorTest, ArithmeticSeekEveryDocInBlock)
{
    auto docs = generateRange(0, 257, 3);
    auto data = makeMultiBlockData({docs});

    /// Block 1 contains docs[129]=387 to docs[256]=768.
    for (uint32_t target = 387; target <= 768; target += 3)
    {
        auto cursor = makeMultiBlockCursor(data);
        cursor->seek(target);
        ASSERT_TRUE(cursor->valid()) << "target=" << target;
        EXPECT_EQ(cursor->value(), target) << "target=" << target;
    }
}

/// Seek to values between arithmetic doc_ids (non-exact).
TEST(PostingListCursorTest, ArithmeticSeekBetweenDocs)
{
    auto docs = generateRange(0, 257, 5);  // 0, 5, 10, ..., 1280
    auto data = makeMultiBlockData({docs});

    /// Block 1 starts at docs[129] = 645.
    for (uint32_t target = 646; target <= 660; ++target)
    {
        if (target % 5 == 0) continue;  // skip exact matches
        auto cursor = makeMultiBlockCursor(data);
        cursor->seek(target);
        ASSERT_TRUE(cursor->valid()) << "target=" << target;
        /// Should land on the next multiple of 5 >= target.
        uint32_t expected_val = ((target + 4) / 5) * 5;
        EXPECT_EQ(cursor->value(), expected_val) << "target=" << target;
    }
}

/// Seek beyond the last doc of an arithmetic block.
TEST(PostingListCursorTest, ArithmeticSeekBeyondBlock)
{
    auto docs = generateRange(0, 257);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    cursor->seek(300);
    EXPECT_FALSE(cursor->valid());
}

/// Brute force vs leapfrog consistency with arithmetic blocks.
TEST(PostingListCursorTest, ArithmeticBruteForceVsLeapfrogConsistency)
{
    auto docsA = generateRange(0, 385);
    auto dataA = makeMultiBlockData({docsA});

    auto docsB = generateRange(0, 257, 2);
    auto dataB = makeMultiBlockData({docsB});

    auto docsC = generateRange(100, 200);
    auto dataC = makeMultiBlockData({docsC});

    PostingListCursorMap postings_bf;
    postings_bf["a"] = makeMultiBlockCursor(dataA);
    postings_bf["b"] = makeMultiBlockCursor(dataB);
    postings_bf["c"] = makeMultiBlockCursor(dataC);
    auto bf = intersectAndCollect(postings_bf, {"a", "b", "c"}, 0, 520, true);

    PostingListCursorMap postings_lf;
    postings_lf["a"] = makeMultiBlockCursor(dataA);
    postings_lf["b"] = makeMultiBlockCursor(dataB);
    postings_lf["c"] = makeMultiBlockCursor(dataC);
    auto lf = intersectAndCollect(postings_lf, {"a", "b", "c"}, 0, 520, false, 100.0);

    EXPECT_EQ(bf, lf);
}

/// Two large blocks where the second large block has arithmetic blocks.
TEST(PostingListCursorTest, ArithmeticSecondLargeBlock)
{
    auto block0 = generateRange(0, 150);     // 1 packed block + 22 tail
    auto block1 = generateRange(200, 257);   // 2 packed blocks, block 1 is arithmetic

    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    /// Seek into the arithmetic block of the second large block.
    /// block1 docs_to_encode starts from docs[0]=200 with delta_base=199.
    /// Packed block 0 of large block 1: docs[0..127] = 200..327.
    /// Packed block 1 of large block 1: docs[128..256] = 328..456 — arithmetic.
    cursor->seek(350);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 350u);

    auto remaining = drainCursor(cursor);
    std::vector<uint32_t> expected;
    for (uint32_t d = 350; d <= 456; ++d)
        expected.push_back(d);
    EXPECT_EQ(remaining, expected);
}

/// linearOr spanning two large blocks, second has arithmetic blocks.
TEST(PostingListCursorTest, ArithmeticSecondLargeBlockLinearOr)
{
    auto block0 = generateRange(0, 150);
    auto block1 = generateRange(200, 257);

    auto data = makeMultiBlockData({block0, block1});
    auto cursor = makeMultiBlockCursor(data);

    auto result = linearOrToDocIds(cursor, 0, 460);

    std::set<uint32_t> expected_set;
    for (auto d : block0) expected_set.insert(d);
    for (auto d : block1) expected_set.insert(d);
    std::vector<uint32_t> expected(expected_set.begin(), expected_set.end());
    EXPECT_EQ(result, expected);
}

/// Large constant-delta value (step=100).
TEST(PostingListCursorTest, ArithmeticLargeStep)
{
    auto docs = generateRange(0, 257, 100);  // 0, 100, 200, ..., 25600
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    auto all = seekAndDrainCursor(cursor, 0);
    EXPECT_EQ(all, docs);

    /// linearOr
    auto cursor2 = makeMultiBlockCursor(data);
    auto result = linearOrToDocIds(cursor2, 0, 25700);
    EXPECT_EQ(result, docs);
}

/// Seek repeatedly within the same arithmetic block (cache hit path).
TEST(PostingListCursorTest, ArithmeticRepeatedSeekSameBlock)
{
    auto docs = generateRange(0, 257);
    auto data = makeMultiBlockData({docs});
    auto cursor = makeMultiBlockCursor(data);

    /// Seek multiple times within block 1 (arithmetic).
    cursor->seek(130);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 130u);

    cursor->seek(150);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 150u);

    cursor->seek(200);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 200u);

    cursor->seek(256);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 256u);
}
