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
#include <Storages/MergeTree/PostingListSegment.h>
#include <Storages/MergeTree/BM25Kernel.h>
#include <Common/PODArray.h>
#include <IO/WriteBufferFromString.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>

#include <absl/container/flat_hash_map.h>
#include <roaring/roaring.hh>

#include <algorithm>
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

    /// `SmallFloat` document-length byte of `row_id`.
    UInt8 dlByte(UInt32 row_id) const
    {
        chassert(row_id < doc_lengths.size());
        return doc_lengths[row_id];
    }
};

/// Encode a posting list with the production bitpacking codec. `max_rowids_in_segment` controls the
/// segment split (rounded up to a BLOCK_SIZE multiple internally). Mirrors `makeMultiBlockData`.
std::string encodeWith(
    const std::vector<uint32_t> & doc_ids,
    size_t max_rowids_in_segment,
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
        max_rowids_in_segment,
        /*enable_positions=*/false,
        /*enable_scoring=*/tfs != nullptr,
        tfs != nullptr ? &tfs->doc_lengths : nullptr,
    };

    SegmentedPostingListCodec codec(IPostingListCodec::Type::Bitpacking);
    codec.append(row_ids, tf_minus_one, context);
    WriteBufferFromOwnString out;
    codec.serializeTo(out, info);
    out.finalize();
    return out.str();
}

/// On-disk stream infrastructure kept alive for the lifetime of a cursor. Mirrors `MultiBlockTestData`.
struct StreamHarness
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

    void writeAndBuildStream(const char * tag)
    {
        auto tmp_dir = fs::temp_directory_path() / (std::string("gtest_blockmax_") + tag);
        fs::remove_all(tmp_dir);
        fs::create_directories(tmp_dir / "part");

        {
            auto out_path = tmp_dir / "part" / "stream.pst";
            std::ofstream ofs(out_path, std::ios::binary);
            ofs.write(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        }

        disk = std::make_shared<DiskLocal>("test_disk", tmp_dir.string() + "/");
        volume = std::make_shared<SingleDiskVolume>("test_vol", disk);
        storage_holder = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "part");

        auto settings = MergeTreeReaderSettings::createFromSettings();
        settings.is_compressed = false;

        static constexpr size_t marks_count = 1;
        stream = std::make_unique<MergeTreeReaderStreamSingleColumnWholePart>(
            storage_holder,
            "stream",
            ".pst",
            marks_count,
            MarkRanges{{0, marks_count}},
            settings,
            /*uncompressed_cache=*/nullptr,
            buffer.size(),
            /*marks_loader=*/nullptr,
            ReadBufferFromFileBase::ProfileCallback{},
            CLOCK_MONOTONIC_COARSE);

        stream->getDataBuffer();

        if (!cache)
            cache = std::make_shared<TextIndexPostingsCache>("SLRU", 1ULL << 30, 0, 0.5);

        /// `doc_lengths` is fully filled by now; snapshot it into an in-memory `DocLengthsCursor`.
        PaddedPODArray<UInt8> doc_length_bytes;
        doc_length_bytes.assign(doc_lengths);
        doc_lengths_provider = std::make_shared<DocLengthsCursor>(std::move(doc_length_bytes));
    }

    std::shared_ptr<PostingListScoringCursor> makeScoringCursor()
    {
        return std::make_shared<PostingListScoringCursor>(*stream, info, doc_lengths_provider.get(), cache.get());
    }

    std::shared_ptr<PostingListCursor> makePlainCursor()
    {
        return std::make_shared<PostingListCursor>(*stream, info, cache.get());
    }
};

/// Expected per-block reduce over a contiguous range of doc ids.
struct BlockExpect
{
    UInt8 min_dl = 0xFF;
    UInt8 max_tf_minus_one = 0;   /// saturating to 255
};

} // anonymous namespace


/// Round-trip the per-block and per-segment block-max UB inputs at both granularities:
///   - per block: minDocumentLengthByte(b) == min dlByte over the block; maxTermFrequencyMinusOne(b) == min(255, max(tf-1)).
///   - per segment: segmentMinDocumentLengthByte() == min over the segment's blocks; segmentMaxTermFrequencyMinusOne()
///     == max over the segment's blocks (the reduce).
/// Includes one block forced to tf >= 256 so its max_tf_minus_one saturates to 255.
TEST(BlockMaxScoreCodecTest, RoundTripPerBlockAndPerSegmentUb)
{
    /// 5 full blocks of 128 sequential row ids => 640 docs. With max_rowids_in_segment = 256
    /// (2 blocks/segment) the split is: segment 0 -> blocks {0,1}, segment 1 -> blocks {2,3},
    /// segment 2 -> block {4}. So 3 segments, blocks of sizes [128,128,128,128,128].
    constexpr uint32_t num_full_blocks = 5;
    constexpr uint32_t count = num_full_blocks * BLOCK_SIZE; /// 640
    std::vector<uint32_t> docs;
    docs.reserve(count);
    for (uint32_t i = 0; i < count; ++i)
        docs.push_back(i);

    /// Build a TF/dl source that varies per block.
    /// Block b covers global rows [b*128, (b+1)*128).
    ///   - dl: base byte that decreases with b (so min_dl varies per block).
    ///   - tf: one "spike" row per block with an increasing tf; block 3 gets a spike >= 256 so its
    ///     max_tf_minus_one saturates to 255.
    /// Rows with `tf > 1` live in the overflow map (absent rows have tf == 1); the dense array holds
    /// the `SmallFloat` doc-length byte per row id — the shape the build and merge paths produce.
    absl::flat_hash_map<UInt32, UInt32> tf_overflow;
    PaddedPODArray<UInt8> doc_length_bytes(count, 100);
    TermFrequencies src{tf_overflow, doc_length_bytes};

    std::vector<BlockExpect> expect(num_full_blocks);
    /// Per-block dl bytes and tf spikes (chosen so each block's min_dl and max_tf differ).
    /// dl_base[b] is the dl for most rows in block b; one row gets a smaller dl to make the min distinct.
    const UInt8 dl_base[num_full_blocks]   = {120, 110, 90, 80, 60};
    const UInt8 dl_min_row[num_full_blocks]= {115, 105, 85, 70, 55};
    /// tf spike per block (exact tf of one row). Block 3 spikes to 1000 (>= 256) -> saturates.
    const UInt32 tf_spike[num_full_blocks] = {3,    7,   1,  1000, 42};

    for (uint32_t b = 0; b < num_full_blocks; ++b)
    {
        const uint32_t block_begin = b * BLOCK_SIZE;
        /// Default dl for the whole block.
        for (uint32_t r = block_begin; r < block_begin + BLOCK_SIZE; ++r)
            doc_length_bytes[r] = dl_base[b];
        /// One row with the smaller dl (the per-block min).
        doc_length_bytes[block_begin + 10] = dl_min_row[b];
        /// One row with the tf spike (the per-block max tf).
        tf_overflow[block_begin + 20] = tf_spike[b];

        /// Compute the expected per-block reduce directly from the source.
        UInt8 min_dl = 0xFF;
        UInt32 max_tfm1 = 0;
        for (uint32_t r = block_begin; r < block_begin + BLOCK_SIZE; ++r)
        {
            min_dl = std::min(min_dl, src.dlByte(r));
            max_tfm1 = std::max(max_tfm1, src.tf(r) - 1u);
        }
        expect[b].min_dl = min_dl;
        expect[b].max_tf_minus_one = static_cast<UInt8>(std::min<UInt32>(255u, max_tfm1));
    }

    /// Sanity: block 3's spike of 1000 must saturate to 255.
    ASSERT_EQ(expect[3].max_tf_minus_one, 255u);

    TokenPostingsInfo info;
    std::string enc = encodeWith(docs, /*max_rowids_in_segment=*/2 * BLOCK_SIZE, info, &src);

    StreamHarness harness;
    harness.buffer = enc;
    harness.info = info;
    harness.doc_lengths.resize(count);
    for (uint32_t r = 0; r < count; ++r)
        harness.doc_lengths[r] = src.dlByte(r);
    harness.writeAndBuildStream("roundtrip");

    /// We expect 3 segments with the block partition described above.
    ASSERT_EQ(info.offsets.size(), 3u) << "expected 3 segments for 640 docs at 256 docs/segment";

    /// Map global block index -> (segment_idx, local_block_idx) for the 2-blocks-per-segment split.
    struct SegBlock { size_t seg; size_t local; uint32_t first_doc; size_t local_block_count; };
    const SegBlock layout[num_full_blocks] = {
        {0, 0, 0 * BLOCK_SIZE, 2},
        {0, 1, 1 * BLOCK_SIZE, 2},
        {1, 0, 2 * BLOCK_SIZE, 2},
        {1, 1, 3 * BLOCK_SIZE, 2},
        {2, 0, 4 * BLOCK_SIZE, 1},
    };

    /// Build a BM25Weight with a simple length-norm cache.
    const BM25Params params; /// k1 = 1.2, b = 0.75
    const BM25LengthNormCache lnc(/*avgdl=*/100.0, params);
    const BM25Weight w(/*idf=*/2.5, params, &lnc);

    auto cursor = harness.makeScoringCursor();

    /// Per-segment expected reduce (min of min_dl, max of max_tf_minus_one over the segment's blocks).
    UInt8 seg_min_dl[3] = {0xFF, 0xFF, 0xFF};
    UInt8 seg_max_tfm1[3] = {0, 0, 0};
    for (uint32_t b = 0; b < num_full_blocks; ++b)
    {
        const size_t s = layout[b].seg;
        seg_min_dl[s] = std::min(seg_min_dl[s], expect[b].min_dl);
        seg_max_tfm1[s] = std::max(seg_max_tfm1[s], expect[b].max_tf_minus_one);
    }

    for (uint32_t b = 0; b < num_full_blocks; ++b)
    {
        const auto & lb = layout[b];

        /// Position the cursor on the segment containing this block (advance to the block's first doc).
        cursor->advance(lb.first_doc);
        ASSERT_TRUE(cursor->valid()) << "cursor must be valid at doc " << lb.first_doc;
        ASSERT_EQ(cursor->value(), lb.first_doc);
        ASSERT_EQ(cursor->currentBlockIndex(), lb.local)
            << "advance must position at local block " << lb.local << " for global block " << b;

        /// Per-block UB round-trip.
        EXPECT_EQ(cursor->minDocumentLengthByte(lb.local), expect[b].min_dl)
            << "minDocumentLengthByte mismatch at global block " << b;
        EXPECT_EQ(cursor->maxTermFrequencyMinusOne(lb.local), expect[b].max_tf_minus_one)
            << "maxTermFrequencyMinusOne mismatch at global block " << b;

        /// Per-segment UB reduce.
        EXPECT_EQ(cursor->segmentMinDocumentLengthByte(), seg_min_dl[lb.seg])
            << "segmentMinDocumentLengthByte mismatch at segment " << lb.seg;
        EXPECT_EQ(cursor->segmentMaxTermFrequencyMinusOne(), seg_max_tfm1[lb.seg])
            << "segmentMaxTermFrequencyMinusOne mismatch at segment " << lb.seg;

        /// blockMaxScore: w.weight when the block saturates (max_tf byte == 255), else
        /// w.contribution(maxTermFrequencyMinusOne + 1, minDocumentLengthByte).
        const Float32 got = cursor->blockMaxScore(w);
        Float32 expected_score = 0;
        if (expect[b].max_tf_minus_one == 255)
            expected_score = w.weight;
        else
            expected_score = w.contribution(static_cast<UInt32>(expect[b].max_tf_minus_one) + 1u, expect[b].min_dl);
        EXPECT_FLOAT_EQ(got, expected_score) << "blockMaxScore mismatch at global block " << b;
    }

    /// The saturated block (global block 3) must return exactly w.weight. `advance` is forward-only,
    /// so use a fresh cursor and advance forward to the block instead of re-advancing backward.
    {
        auto cursor2 = harness.makeScoringCursor();
        cursor2->advance(layout[3].first_doc);
        ASSERT_TRUE(cursor2->valid());
        ASSERT_EQ(cursor2->currentBlockIndex(), layout[3].local);
        EXPECT_FLOAT_EQ(cursor2->blockMaxScore(w), w.weight)
            << "saturated block (max_tf byte == 255) must return the per-term weight";
    }
}


/// `seekBlock` shallow-positions on the block containing a doc id without decoding the body; the
/// UB accessors and blockMaxScore must then reflect that block.
TEST(BlockMaxScoreCodecTest, SeekBlockShallowPositioning)
{
    constexpr uint32_t num_full_blocks = 3;
    constexpr uint32_t count = num_full_blocks * BLOCK_SIZE; /// 384
    std::vector<uint32_t> docs;
    docs.reserve(count);
    for (uint32_t i = 0; i < count; ++i)
        docs.push_back(i);

    absl::flat_hash_map<UInt32, UInt32> tf_overflow;
    PaddedPODArray<UInt8> doc_length_bytes(count, 50);
    TermFrequencies src{tf_overflow, doc_length_bytes};

    std::vector<BlockExpect> expect(num_full_blocks);
    const UInt8 dl_min_row[num_full_blocks] = {40, 30, 20};
    const UInt32 tf_spike[num_full_blocks] = {2, 9, 4};
    for (uint32_t b = 0; b < num_full_blocks; ++b)
    {
        const uint32_t block_begin = b * BLOCK_SIZE;
        doc_length_bytes[block_begin + 5] = dl_min_row[b];
        tf_overflow[block_begin + 7] = tf_spike[b];

        UInt8 min_dl = 0xFF;
        UInt32 max_tfm1 = 0;
        for (uint32_t r = block_begin; r < block_begin + BLOCK_SIZE; ++r)
        {
            min_dl = std::min(min_dl, src.dlByte(r));
            max_tfm1 = std::max(max_tfm1, src.tf(r) - 1u);
        }
        expect[b].min_dl = min_dl;
        expect[b].max_tf_minus_one = static_cast<UInt8>(std::min<UInt32>(255u, max_tfm1));
    }

    TokenPostingsInfo info;
    /// One big segment so all 3 blocks share a segment and seekBlock can hop between them.
    std::string enc = encodeWith(docs, /*max_rowids_in_segment=*/count + BLOCK_SIZE, info, &src);
    ASSERT_EQ(info.offsets.size(), 1u);

    StreamHarness harness;
    harness.buffer = enc;
    harness.info = info;
    harness.doc_lengths.resize(count);
    for (uint32_t r = 0; r < count; ++r)
        harness.doc_lengths[r] = src.dlByte(r);
    harness.writeAndBuildStream("seekblock");

    const BM25Params params;
    const BM25LengthNormCache lnc(60.0, params);
    const BM25Weight w(1.7, params, &lnc);

    auto cursor = harness.makeScoringCursor();

    /// Prepare the segment by advancing to the first doc, then seekBlock across the blocks.
    cursor->advance(0);
    ASSERT_TRUE(cursor->valid());

    for (uint32_t b = 0; b < num_full_blocks; ++b)
    {
        const uint32_t mid_doc = b * BLOCK_SIZE + 50; /// somewhere inside block b
        cursor->seekBlock(mid_doc);
        EXPECT_EQ(cursor->currentBlockIndex(), b) << "seekBlock must position at block " << b;
        EXPECT_EQ(cursor->minDocumentLengthByte(b), expect[b].min_dl);
        EXPECT_EQ(cursor->maxTermFrequencyMinusOne(b), expect[b].max_tf_minus_one);

        const Float32 got = cursor->blockMaxScore(w);
        const Float32 expected_score = (expect[b].max_tf_minus_one == 255)
            ? w.weight
            : w.contribution(static_cast<UInt32>(expect[b].max_tf_minus_one) + 1u, expect[b].min_dl);
        EXPECT_FLOAT_EQ(got, expected_score) << "blockMaxScore after seekBlock mismatch at block " << b;
    }
}


/// A NO-source (filter-only) encode read by a plain `PostingListCursor` behaves as before: the
/// UB arrays are ABSENT (the header lacks `HasTermFrequencies`) and the row ids round-trip exactly.
/// We also confirm the encoded bytes are byte-identical to a separate no-TF encode.
TEST(BlockMaxScoreCodecTest, NoSourceEncodeHasNoUbAndRoundTrips)
{
    constexpr uint32_t count = 3 * BLOCK_SIZE;
    std::vector<uint32_t> docs;
    docs.reserve(count);
    for (uint32_t i = 0; i < count; ++i)
        docs.push_back(i * 2); /// strided so deltas are non-trivial

    TokenPostingsInfo info_no_tf;
    std::string enc_no_tf = encodeWith(docs, /*max_rowids_in_segment=*/count + BLOCK_SIZE, info_no_tf, nullptr);

    /// The no-TF header must not carry HasTermFrequencies.
    EXPECT_EQ(info_no_tf.header & PostingsSerialization::Flags::HasTermFrequencies, 0u)
        << "filter-only encode must not set HasTermFrequencies";

    /// A second no-TF encode must be byte-identical (deterministic, no UB drift).
    TokenPostingsInfo info_no_tf2;
    std::string enc_no_tf2 = encodeWith(docs, /*max_rowids_in_segment=*/count + BLOCK_SIZE, info_no_tf2, nullptr);
    EXPECT_EQ(enc_no_tf, enc_no_tf2) << "no-source encode must be deterministic and byte-identical";

    /// Read back via a plain PostingListCursor and verify exact row-id round-trip.
    StreamHarness harness;
    harness.buffer = enc_no_tf;
    harness.info = info_no_tf;
    harness.writeAndBuildStream("nosource");

    auto cursor = harness.makePlainCursor();
    cursor->advance(docs.front());
    std::vector<uint32_t> drained;
    while (cursor->valid())
    {
        drained.push_back(cursor->value());
        cursor->next();
    }
    EXPECT_EQ(drained, docs) << "filter-only cursor must round-trip the row ids exactly";

    /// A WITH-TF encode of the same docs must be strictly larger (it carries the UB bytes + TF headers),
    /// confirming the no-TF path omitted them.
    /// The row ids are strided (`i * 2`), so the dl array must span up to the largest row id.
    absl::flat_hash_map<UInt32, UInt32> no_tf_overflow;
    PaddedPODArray<UInt8> doc_length_bytes(docs.back() + 1, 7);
    TermFrequencies src{no_tf_overflow, doc_length_bytes};
    TokenPostingsInfo info_tf;
    std::string enc_tf = encodeWith(docs, /*max_rowids_in_segment=*/count + BLOCK_SIZE, info_tf, &src);
    EXPECT_GT(enc_tf.size(), enc_no_tf.size())
        << "WITH-TF encode must be larger than the filter-only encode (UB arrays + per-block TF headers present)";
}
