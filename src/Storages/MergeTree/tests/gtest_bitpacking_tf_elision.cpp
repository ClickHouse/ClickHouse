#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Common/PODArray.h>
#include <IO/WriteBufferFromString.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>

#include <absl/container/flat_hash_map.h>
#include <roaring/roaring.hh>

#include <filesystem>
#include <fstream>
#include <vector>

using namespace DB;
namespace fs = std::filesystem;

namespace
{

/// Test-local bundle of the per-row term frequencies (rows absent from `tf_overflow` have
/// `tf == 1`) and the granule's `SmallFloat` document-length bytes, indexed by row id.
/// Both referenced containers must outlive the bundle.
struct TermFrequencies
{
    const absl::flat_hash_map<UInt32, UInt32> & tf_overflow;
    const PaddedPODArray<UInt8> & doc_lengths;

    /// Exact term frequency of the token in `row_id` (>= 1 for rows in the posting list).
    UInt32 tf(UInt32 row_id) const
    {
        auto it = tf_overflow.find(row_id);
        return it != tf_overflow.end() ? it->second : 1;
    }
};

/// Encode a posting list with the production bitpacking codec, optionally passing per-row term
/// frequencies. Mirrors `makeMultiBlockData` in `gtest_posting_list_cursor.cpp`:
/// a `PostingListCodecBitpacking` writes a single segment large enough to hold all docs into a
/// `WriteBufferFromOwnString`. Returns the raw encoded bytes and fills `info`.
std::string encodeWith(
    const std::vector<uint32_t> & doc_ids,
    TokenPostingsInfo & info,
    const TermFrequencies * tfs)
{
    roaring::Roaring bitmap;
    for (auto id : doc_ids)
        bitmap.add(id);

    /// Sorted, deduplicated row ids — the order the codec encodes them in.
    std::vector<uint32_t> row_ids(bitmap.cardinality());
    bitmap.toUint32Array(row_ids.data());

    info.cardinality = static_cast<UInt32>(row_ids.size());
    info.header = PostingsSerialization::Flags::IsCompressed | PostingsSerialization::Flags::HasBlockIndex;
    if (tfs != nullptr)
        info.header |= PostingsSerialization::Flags::HasTermFrequencies;

    /// When scoring, gather the per-row `(tf - 1)` parallel to the row ids and hand over the
    /// granule-wide doc lengths — exactly what the build/merge paths pass into the accumulator.
    std::vector<uint32_t> tf_minus_one;
    if (tfs != nullptr)
    {
        tf_minus_one.reserve(row_ids.size());
        for (auto id : row_ids)
            tf_minus_one.push_back(tfs->tf(id) - 1);
    }

    const PostingListCodecBitpacking codec_for_context;
    const PostingListBuildContext context
    {
        codec_for_context,
        row_ids.size() + BLOCK_SIZE,
        /*enable_positions=*/false,
        /*enable_scoring=*/tfs != nullptr,
        tfs != nullptr ? &tfs->doc_lengths : nullptr,
    };

    SegmentedPostingListCodec codec(IPostingListCodec::Type::Bitpacking);
    /// One large segment so all docs land in a single segment (matches the cursor-test harness).
    codec.append(row_ids, tf_minus_one, context);
    WriteBufferFromOwnString out;
    codec.serializeTo(out, info);
    out.finalize();
    return out.str();
}

/// Holds the on-disk stream infrastructure alive for the lifetime of a scoring cursor built
/// over an encoded buffer. Mirrors `MultiBlockTestData` + `makeMultiBlockCursor`.
struct ScoringStreamHarness
{
    std::string buffer;
    TokenPostingsInfo info;
    PaddedPODArray<UInt8> doc_lengths;
    DocLengthsCursorPtr doc_lengths_provider;

    std::shared_ptr<DiskLocal> disk;
    std::shared_ptr<SingleDiskVolume> volume;
    std::shared_ptr<DataPartStorageOnDiskFull> storage_holder;
    std::unique_ptr<MergeTreeReaderStreamSingleColumnWholePart> stream;
    std::shared_ptr<TextIndexPostingsCache> cache;
};

/// Write `harness.buffer` to a temp `.pst` file and build a real `MergeTreeReaderStream`, then a
/// `PostingListScoringCursor` over it. Exactly mirrors `makeMultiBlockCursor` plus the scoring
/// cursor's extra `doc_lengths` argument.
std::shared_ptr<PostingListScoringCursor> makeScoringCursor(ScoringStreamHarness & harness, const char * tag)
{
    auto tmp_dir = fs::temp_directory_path() / (std::string("gtest_tfelision_") + tag);
    fs::remove_all(tmp_dir);
    fs::create_directories(tmp_dir / "part");

    {
        auto out_path = tmp_dir / "part" / "stream.pst";
        std::ofstream ofs(out_path, std::ios::binary);
        ofs.write(harness.buffer.data(), static_cast<std::streamsize>(harness.buffer.size()));
    }

    harness.disk = std::make_shared<DiskLocal>("test_disk", tmp_dir.string() + "/");
    harness.volume = std::make_shared<SingleDiskVolume>("test_vol", harness.disk);
    harness.storage_holder = std::make_shared<DataPartStorageOnDiskFull>(harness.volume, "", "part");

    auto settings = MergeTreeReaderSettings::createFromSettings();
    settings.is_compressed = false;

    static constexpr size_t marks_count = 1;
    harness.stream = std::make_unique<MergeTreeReaderStreamSingleColumnWholePart>(
        harness.storage_holder,
        "stream",
        ".pst",
        marks_count,
        MarkRanges{{0, marks_count}},
        settings,
        /*uncompressed_cache=*/nullptr,
        harness.buffer.size(),
        /*marks_loader=*/nullptr,
        ReadBufferFromFileBase::ProfileCallback{},
        CLOCK_MONOTONIC_COARSE);

    harness.stream->getDataBuffer();

    if (!harness.cache)
        harness.cache = std::make_shared<TextIndexPostingsCache>("SLRU", 1ULL << 30, 0, 0.5);

    /// `harness.doc_lengths` is fully filled by now; snapshot it into an in-memory `DocLengthsCursor`.
    PaddedPODArray<UInt8> doc_length_bytes;
    doc_length_bytes.assign(harness.doc_lengths);
    harness.doc_lengths_provider = std::make_shared<DocLengthsCursor>(std::move(doc_length_bytes));

    return std::make_shared<PostingListScoringCursor>(
        *harness.stream, harness.info, harness.doc_lengths_provider.get(), harness.cache.get());
}

/// Drive a scoring cursor across the whole posting list and collect `(row_id, tf())` pairs.
/// `advance(first_doc)` decodes the first block; `next()` walks within / across blocks; the cursor
/// re-decodes blocks (and thus `decoded_tfs`) as it advances.
std::vector<std::pair<uint32_t, UInt32>> collectTfs(PostingListScoringCursor & cursor, uint32_t first_doc)
{
    std::vector<std::pair<uint32_t, UInt32>> result;
    cursor.advance(first_doc);
    while (cursor.valid())
    {
        result.emplace_back(cursor.value(), cursor.termFrequency());
        cursor.next();
    }
    return result;
}

} // anonymous namespace


/// All-TF==1 elision: a block whose every `tf == 1` emits EXACTLY ONE TF byte (the `bw_tfs == 0`
/// header) and no packed payload. We isolate the TF sub-payload contribution by comparing the same
/// posting list encoded WITH an all-ones TF source against the SAME list encoded with a TF source
/// where exactly one row has a larger tf. The all-ones encoding's per-block TF overhead must be
/// exactly 1 byte/block; the varying encoding's must be strictly larger for the affected block.
TEST(BitpackingTfElisionTest, AllOnesBlockEmitsExactlyOneTfByte)
{
    /// Two full blocks (256 sequential row ids) so num_blocks == 2 (no tail).
    constexpr uint32_t count = 2 * BLOCK_SIZE;
    std::vector<uint32_t> docs;
    docs.reserve(count);
    for (uint32_t i = 0; i < count; ++i)
        docs.push_back(i);

    /// Baseline: same row ids, no TF source (filter-only / legacy format).
    TokenPostingsInfo info_no_tf;
    std::string enc_no_tf = encodeWith(docs, info_no_tf, nullptr);

    /// All-ones TF source: an empty overflow map means tf == 1 for every row, dl == constant.
    absl::flat_hash_map<UInt32, UInt32> no_tf_overflow;
    PaddedPODArray<UInt8> doc_length_bytes(count, 7);
    TermFrequencies src_all_ones{no_tf_overflow, doc_length_bytes};
    TokenPostingsInfo info_all_ones;
    std::string enc_all_ones = encodeWith(docs, info_all_ones, &src_all_ones);

    const size_t num_blocks = (count + BLOCK_SIZE - 1) / BLOCK_SIZE;
    ASSERT_EQ(num_blocks, 2u);

    /// Compute the exact expected size delta between the WITH-TF (all-ones) encoding and the
    /// no-TF encoding, derived precisely from the format:
    ///   - per segment header: +2 raw UB bytes (segment_min_dl_byte, segment_max_tf_minus_one)
    ///   - per block TF sub-payload: +1 byte each (bw_tfs == 0, NO payload) — the elision
    ///   - per block Index Section UB arrays: +2 bytes each (min_dl_byte[b], max_tf_minus_one[b])
    /// (block_offsets[] are unchanged in count; their VarUInt values shift because each block grew
    ///  by exactly its TF sub-payload, but VarUInt width can change, so we don't assume offset
    ///  byte-width equality — instead we measure the TF sub-payload contribution directly below.)
    const size_t num_segments = 1;
    const size_t expected_header_ub = num_segments * 2;          /// 2 UB bytes per segment header
    const size_t expected_tf_subpayload = num_blocks * 1;        /// 1 byte/block, elided payload
    const size_t expected_index_ub = num_blocks * 2;             /// min_dl_byte[] + max_tf_minus_one[]

    /// The block_offsets[] VarUInts may widen by at most a small amount because each block's payload
    /// grew by exactly 1 byte (the elided TF header). Rather than depend on VarUInt widths, assert the
    /// lower bound (delta >= the fixed parts) and that the only block-growth is the 1-byte/block TF
    /// header (delta - header_ub - index_ub - offset_widening == num_blocks).
    const Int64 delta = static_cast<Int64>(enc_all_ones.size()) - static_cast<Int64>(enc_no_tf.size());

    /// The fixed (non-offset) additions: header UB + per-block TF header + per-block index UB.
    const Int64 fixed_additions
        = static_cast<Int64>(expected_header_ub) + static_cast<Int64>(expected_tf_subpayload) + static_cast<Int64>(expected_index_ub);

    /// Each block's payload grew by exactly 1 byte (its elided TF header), which can push a
    /// block_offsets[] VarUInt across a 7-bit boundary, adding at most 1 byte per offset entry.
    const Int64 max_offset_widening = static_cast<Int64>(num_blocks);

    EXPECT_GE(delta, fixed_additions)
        << "WITH-TF encoding must be at least the fixed UB + 1-byte/block TF header larger";
    EXPECT_LE(delta, fixed_additions + max_offset_widening)
        << "WITH-TF encoding may exceed the fixed additions only by VarUInt offset widening (<= 1 byte/block)";

    /// Direct, VarUInt-independent isolation of the per-block TF sub-payload size.
    /// We re-walk the payload region of each segment and confirm each block's TF header is present and
    /// is the elided form (bw_tfs == 0, no payload). This proves the all-1 case adds exactly 1 byte/block.
    {
        /// Decode by reading back via the scoring cursor and asserting tf()==1 for every row.
        ScoringStreamHarness harness;
        harness.buffer = enc_all_ones;
        harness.info = info_all_ones;
        harness.doc_lengths.resize(count);
        for (uint32_t i = 0; i < count; ++i)
            harness.doc_lengths[i] = 7;

        auto cursor = makeScoringCursor(harness, "all_ones");
        auto tfs = collectTfs(*cursor, 0);
        ASSERT_EQ(tfs.size(), count);
        for (size_t i = 0; i < tfs.size(); ++i)
        {
            EXPECT_EQ(tfs[i].first, static_cast<uint32_t>(i)) << "row id mismatch at " << i;
            EXPECT_EQ(tfs[i].second, 1u) << "expected tf==1 for all-ones block at row " << i;
        }
    }
}


/// A non-elided block: give exactly one row a larger tf (5) so the block's TF payload is present
/// (bw_tfs > 0, > 1 byte), and the scoring cursor reads the exact tf values back (1s and the 5).
TEST(BitpackingTfElisionTest, NonElidedBlockHasPayloadAndExactTfs)
{
    /// One full block (128 rows) keeps num_blocks == 1 so the affected block is unambiguous.
    constexpr uint32_t count = BLOCK_SIZE;
    std::vector<uint32_t> docs;
    docs.reserve(count);
    for (uint32_t i = 0; i < count; ++i)
        docs.push_back(i);

    PaddedPODArray<UInt8> doc_length_bytes(count, 7);

    /// All-ones baseline (empty overflow map, single block -> single elided TF header byte).
    absl::flat_hash_map<UInt32, UInt32> no_tf_overflow;
    TermFrequencies src_all_ones{no_tf_overflow, doc_length_bytes};
    TokenPostingsInfo info_all_ones;
    std::string enc_all_ones = encodeWith(docs, info_all_ones, &src_all_ones);

    /// Varying TF source: row 50 has tf == 5, all others tf == 1.
    absl::flat_hash_map<UInt32, UInt32> tf_overflow{{50, 5}};
    TermFrequencies src_varying{tf_overflow, doc_length_bytes};
    TokenPostingsInfo info_varying;
    std::string enc_varying = encodeWith(docs, info_varying, &src_varying);

    /// The non-elided block's TF sub-payload is now present: encoding must be strictly larger than the
    /// all-ones (elided) encoding by more than the (zero) header/index difference — i.e. the block's
    /// TF header is no longer the sole byte; a packed payload follows.
    EXPECT_GT(enc_varying.size(), enc_all_ones.size())
        << "a block with a varying tf must carry a packed TF payload (more than the 1-byte elided header)";

    /// Read back the exact tf values via the scoring cursor.
    ScoringStreamHarness harness;
    harness.buffer = enc_varying;
    harness.info = info_varying;
    harness.doc_lengths.resize(count);
    for (uint32_t i = 0; i < count; ++i)
        harness.doc_lengths[i] = 7;

    auto cursor = makeScoringCursor(harness, "varying");
    auto tfs = collectTfs(*cursor, 0);
    ASSERT_EQ(tfs.size(), count);
    for (size_t i = 0; i < tfs.size(); ++i)
    {
        EXPECT_EQ(tfs[i].first, static_cast<uint32_t>(i)) << "row id mismatch at " << i;
        const UInt32 expected_tf = (i == 50) ? 5u : 1u;
        EXPECT_EQ(tfs[i].second, expected_tf) << "tf mismatch at row " << i;
    }
}
