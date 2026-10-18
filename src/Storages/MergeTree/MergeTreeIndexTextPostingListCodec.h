#pragma once

#include <Compression/ICompressionCodec.h>
#include <Common/PODArray.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>

#include <limits>
#include <memory>
#include <span>
#include <string_view>

namespace DB
{

struct TokenPostingsInfo;
struct PostingListBuildContext;
class WriteBuffer;
class ReadBuffer;
using PostingList = roaring::Roaring;

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

/// Segment + block + delta framework for serializing a posting list in a compact block-compressed format.
///
/// Values are delta-compressed, then each fixed-size block (physical chunk, controlled by BLOCK_SIZE) is encoded
/// by a per-block payload codec (IPostingListBlockCodec). The block payload is the only
/// codec-specific part; the segment / block / Index Section layout below is shared by all block codecs.
///
/// Posting lists are additionally split into "segments" (logical chunks, controlled by postings_list_block_size)
/// to simplify metadata and to support multiple ranges per token (min/max row id per segment).
///
/// With scoring every block is followed by the per-row weights of its row ids, encoded by the weights codec
/// that the scoring kind determines (see `createWeightsCodec`), and the segment header and the Index Section
/// carry the block-max inputs of the scoring (see `PackedBlockMeta`).
///
/// Assumes that input row ids are strictly increasing.
class SegmentedPostingListCodec
{
    static constexpr size_t BLOCK_SIZE = IPostingListBlockCodec::BLOCK_SIZE;

    /// Header written at the beginning of each segment before the payload.
    struct Header
    {
        Header() = default;

        Header(size_t payload_bytes_, uint32_t cardinality_, uint32_t base_value_)
            : payload_bytes(payload_bytes_)
            , cardinality(cardinality_)
            , first_row_id(base_value_)
        {
        }

        void write(WriteBuffer & out, IPostingListCodec::Type codec_type_, bool has_weights) const
        {
            writeVarUInt(static_cast<uint8_t>(codec_type_), out);
            writeVarUInt(payload_bytes, out);
            writeVarUInt(cardinality, out);
            writeVarUInt(first_row_id, out);

            if (has_weights)
            {
                writeBinaryLittleEndian(segment_min_dl_byte, out);
                writeBinaryLittleEndian(segment_max_weight_minus_one, out);
            }
        }

        static uint32_t readUInt32(ReadBuffer & in, std::string_view field_name)
        {
            UInt64 v = 0;
            readVarUInt(v, in);
            if (v > std::numeric_limits<uint32_t>::max())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: posting list segment {} {} exceeds UInt32 max", field_name, v);
            return static_cast<uint32_t>(v);
        }

        void read(ReadBuffer & in, bool has_weights)
        {
            UInt64 v = 0;
            readVarUInt(v, in);
            if (!isValidPostingListBlockCodecType(v))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: unknown posting list block codec type {}", v);
            codec_type = static_cast<IPostingListCodec::Type>(v);

            readVarUInt(v, in);
            payload_bytes = static_cast<uint64_t>(v);

            cardinality = readUInt32(in, "cardinality");
            first_row_id = readUInt32(in, "first row id");

            if (has_weights)
            {
                readBinaryLittleEndian(segment_min_dl_byte, in);
                readBinaryLittleEndian(segment_max_weight_minus_one, in);
            }
        }

        /// Block codec used for this segment's payload. Filled by read.
        IPostingListCodec::Type codec_type = IPostingListCodec::Type::Bitpacking;
        /// Number of compressed bytes (per segment) following this header
        uint64_t payload_bytes = 0;
        /// Number of postings (row ids) in this segment
        uint32_t cardinality = 0;
        /// The first row id in the segment (used as a base value to restore from deltas)
        uint32_t first_row_id = 0;
        /// Min `SmallFloat` doc-length byte across the segment. Written only with weights.
        UInt8 segment_min_dl_byte = 0xFF;
        /// Max saturating `(weight - 1)` across the segment (`255` is the "max weight >= 256" sentinel). Written only with weights.
        UInt8 segment_max_weight_minus_one = 0;
    };

    /// A segment header together with its payload, which points either into the read
    /// buffer or into the scratch buffer passed to `readSegment`.
    struct SegmentData
    {
        Header header;
        std::span<const std::byte> payload;
    };

    /// In-memory descriptor of one segment inside `compressed_data`.
    struct SegmentDescriptor
    {
        /// Number of postings in this segment
        uint32_t cardinality = 0;
        /// Start offset in `compressed_data`
        size_t compressed_data_offset = 0;
        /// Payload size in bytes (excluding header)
        size_t compressed_data_size = 0;
        /// Row range covered by this segment.
        uint32_t row_id_begin = 0;
        uint32_t row_id_end = 0;
    };

    /// Per-block metadata collected during encoding for V2 Index Section.
    struct PackedBlockMeta
    {
        /// Last row_id in this packed block
        UInt32 last_row_id = 0;
        /// Offset within segment payload (from segment data start)
        UInt64 relative_offset = 0;
        /// Min `SmallFloat` doc-length byte in this block.
        UInt8 min_dl_byte = 0xFF;
        /// Max saturating `(weight - 1)` in this block (`255` is the "max weight >= 256" sentinel).
        UInt8 max_weight_minus_one = 0;
    };

    /// Per-segment list of packed block metadata.
    struct SegmentBlockMetas
    {
        std::vector<PackedBlockMeta> metas;
    };

public:
    /// Constructs a codec for decoding only: `append` requires the encoding constructor below.
    /// `scoring_` is the scoring kind of the weights stored in the posting list (`None` if it stores no weights).
    explicit SegmentedPostingListCodec(TextIndexScoringKind scoring_);

    /// Constructs a codec for encoding the posting lists of an index with the scoring kind `scoring_`.
    /// The requested `segment_size_` is rounded up to a multiple of BLOCK_SIZE.
    SegmentedPostingListCodec(IPostingListCodec::Type block_codec_type_, size_t segment_size_, TextIndexScoringKind scoring_);

    /// Encodes a batch of sorted unique row ids (increasing across calls), appending
    /// to the open segment and starting a new one every `segment_size` row ids.
    /// `context.scoring` must be the scoring kind the codec was constructed with.
    /// With scoring a non-empty `weights`, parallel to `row_ids`, carries the per-row
    /// weights (empty if every weight is 1), and the doc lengths of the rows come from `context.doc_lengths`.
    void append(
        std::span<const UInt32> row_ids,
        std::span<const UInt32> weights,
        const PostingListBuildContext & context);

    /// Write all segments to output and fill TokenPostingsInfo:
    /// - offsets: byte offsets in output where each segment begins
    /// - ranges: [row_begin, row_end] row range for each segment
    void serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const;

    /// Total number of row ids added so far.
    size_t cardinality() const { return total_row_ids; }

    /// True if the posting list stores weights for scoring.
    bool hasScoring() const;

    /// Deserialize a postings list from input `in` into `out`.
    ///
    /// Format per segment:
    ///   Header + [compressed bytes]
    ///
    /// Decompression restores delta values and then performs an inclusive scan
    /// to reconstruct absolute row ids.
    ///
    /// `max_cardinality` bounds the sizes claimed by the segment header (see `readSegmentData`).
    /// With scoring every block is followed by its weights, which are skipped.
    void decode(ReadBuffer & in, UInt64 max_cardinality, PostingList & postings, PaddedPODArray<char> & buffer);

    /// The same, but appends the decoded row ids to the plain array,
    /// decoding blocks directly into the array without a roaring bitmap.
    void decode(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<char> & buffer);

    /// Deserializes a postings list with the exact per-row weights.
    ///
    /// Works like the above, but the weights payload of each block is decoded.
    /// Every decoded weight is appended to `weights`, parallel to the decoded row ids.
    void decodeWithWeights(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & weights, PaddedPODArray<char> & buffer);

private:
    /// Encodes one block of up to BLOCK_SIZE row ids as deltas and appends it to `compressed_data`.
    ///
    /// Block layout:
    ///   [1 byte bits][row ids payload][weights payload]
    ///
    /// - bits: max bit-width among deltas in this block
    /// - row ids payload: Codec::encode(...) bitpacked bytes
    /// - weights payload: the per-row weights encoded by the weights codec, only with scoring
    ///
    /// Also updates current segment metadata (cardinality, payload size).
    ///
    /// When scoring is enabled, the block-max metadata takes the doc lengths from `doc_lengths[row_id - doc_lengths_first_row_id]`.
    void encodeBlock(
        std::span<const UInt32> block_row_ids,
        std::span<const UInt32> weights,
        std::span<const UInt8> doc_lengths,
        UInt32 doc_lengths_first_row_id);

    /// Encodes one block of `count` weights with the weights codec and appends it to `compressed_data`, after its deltas.
    /// An empty `weights` means that every weight is 1.
    void encodeWeights(std::span<const UInt32> weights, size_t count);

    /// Decodes one compressed block of `out.size()` row ids into `out` and reconstructs absolute row ids.
    ///
    /// - Delegates the block payload to `block_codec` (bitpacking reads a bits-width byte), which fills `out` with delta values
    /// - inclusive_scan converts deltas to row ids using `prev_row_id` as initial prefix
    /// - Updates prev_row_id to the last decoded row id
    void decodeBlock(std::span<const std::byte> & in, std::span<uint32_t> out);

    /// Reads a segment header and returns it together with the segment payload.
    /// Throws CORRUPTED_DATA if the header claims more than `max_cardinality` row ids or more payload bytes than they can take.
    SegmentData readSegmentData(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<char> & buffer);

    /// Skips one block's weights.
    void skipWeights(std::span<const std::byte> & in, size_t count);

    /// Decodes one block's weights and appends the exact weight for each row to `weights`.
    void decodeWeights(std::span<const std::byte> & in, size_t count, PaddedPODArray<UInt32> & weights);

    /// Number of row ids per segment.
    size_t segment_size = 0;
    /// All segments. Filled on encode only: decode reads the payload from the buffer passed to it.
    PODArray<char> compressed_data;
    /// Last encoded/decoded row id
    uint32_t prev_row_id = 0;
    /// Number of row ids in the open segment.
    size_t row_ids_in_current_segment = 0;
    /// Scratch buffer for one block: the deltas being encoded, or the row ids being decoded
    std::vector<UInt32> block_values;
    /// Each segment has an in-memory descriptor
    std::vector<SegmentDescriptor> segment_descriptors;
    /// Per-segment packed block metadata for V2 Index Section
    std::vector<SegmentBlockMetas> segment_block_metas;
    /// Total number of postings added across all segments.
    size_t total_row_ids = 0;
    /// Scoring kind of the weights stored with the row ids, `None` without weights.
    /// With scoring `serializeTo` also emits the per-segment header block-max bytes and the per-block
    /// `min_dl_byte[]`/`max_weight_minus_one[]` arrays.
    TextIndexScoringKind scoring;
    /// Per-block payload codec (bitpacking). On encode it is fixed by the constructor; on decode it
    /// is created from the segment header. One instance is reused across all blocks of a single encode/decode call.
    std::unique_ptr<IPostingListBlockCodec> block_codec;
    /// Per-block codec of the weights, determined by the scoring kind and the codec of the row ids (see `createWeightsCodec`).
    /// Null without weights. Like `block_codec`, on encode it is created by the constructor; on decode, from the segment header.
    std::unique_ptr<IPostingListWeightsCodec> weights_codec;
};

/// Accumulator for block-compressed codecs (see SegmentedPostingListCodec).
/// Wraps SegmentedPostingListCodec, which encodes each added segment into
/// blocks held in memory; the compressed bytes are flushed on `finalize`.
class SegmentedPostingListEncoder final : public IPostingListEncoder
{
public:
    SegmentedPostingListEncoder(IPostingListCodec::Type block_codec_type_, size_t segment_size, TextIndexScoringKind scoring)
        : impl(block_codec_type_, segment_size, scoring)
    {
    }

    void append(std::span<const UInt32> row_ids, std::span<const UInt32> weights, const PostingListBuildContext & context) override
    {
        impl.append(row_ids, weights, context);
    }

    void finalize(WriteBuffer & out, TokenPostingsInfo & info) override;

    size_t cardinality() const override { return impl.cardinality(); }

private:
    SegmentedPostingListCodec impl;
};

/// Codec for serializing a postings list to/from a binary stream in a compact block-compressed format.
///
/// Values are delta-compressed within fixed-size blocks (physical chunks of `IPostingListBlockCodec::BLOCK_SIZE` row ids),
/// and each block payload is produced by an IPostingListBlockCodec chosen by `getType`.
///
/// Posting lists are additionally split into "segments" (logical chunks, controlled by postings_list_block_size)
/// to simplify metadata and to support multiple ranges per token (min/max row id per segment).
///
/// The framing is codec-independent, so `decode` is driven by the codec type in each segment header.
///
/// Assumes that input row ids are strictly increasing.
class SegmentedPostingListCodecBase : public IPostingListCodec
{
public:
    explicit SegmentedPostingListCodecBase(Type type_) : IPostingListCodec(type_) {}

    /// Creates a SegmentedPostingListEncoder whose block payloads are produced by this codec's block codec (see `getType`).
    std::unique_ptr<IPostingListEncoder> createEncoder(size_t segment_size, TextIndexScoringKind scoring) const override;

    void decode(ReadBuffer & in, UInt64 max_cardinality, PostingList & postings, TextIndexScoringKind scoring, PaddedPODArray<char> & buffer) const override;
    void decode(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, TextIndexScoringKind scoring, PaddedPODArray<char> & buffer) const override;
    void decodeWithWeights(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & weights, TextIndexScoringKind scoring, PaddedPODArray<char> & buffer) const override;
};

/// Each block is stored as [1 byte: bits-width][bit-packed payload], at the block's maximum delta width.
class PostingListCodecBitpacking : public SegmentedPostingListCodecBase
{
public:
    static const char * getName() { return "bitpacking"; }

    PostingListCodecBitpacking() : SegmentedPostingListCodecBase(Type::Bitpacking) {}
};

/// Bit-packed at a size-minimising base width; outliers become patched exceptions, all-equal deltas a constant.
class PostingListCodecPFor : public SegmentedPostingListCodecBase
{
public:
    static const char * getName() { return "pfor"; }

    PostingListCodecPFor() : SegmentedPostingListCodecBase(Type::PFor) {}
};

/// Accumulator for the None codec.
/// Each added segment is stored as a Roaring bitmap and serialized on `finalize`
/// as a portable Roaring bitmap prefixed by its size in bytes.
class PostingListEncoderNone final : public IPostingListEncoder
{
public:
    explicit PostingListEncoderNone(size_t segment_size_) : segment_size(segment_size_) {}

    void append(
        std::span<const UInt32> row_ids,
        std::span<const UInt32> weights,
        const PostingListBuildContext & context) override;
    void finalize(WriteBuffer & out, TokenPostingsInfo & info) override;

    size_t cardinality() const override { return total_row_ids; }

private:
    void finishSegment();

    const size_t segment_size;
    PostingList current_segment;
    std::vector<PostingList> segments;
    size_t rows_in_current_segment = 0;
    size_t total_row_ids = 0;
};

/// A codec that applies no compression: a posting list segment is stored as
/// [VarUInt: number of bytes][portable serialization of a roaring bitmap].
class PostingListCodecNone : public IPostingListCodec
{
public:
    static const char * getName() { return "none"; }

    PostingListCodecNone() : IPostingListCodec(Type::None) {}

    std::unique_ptr<IPostingListEncoder> createEncoder(size_t segment_size, TextIndexScoringKind scoring) const override;
    void decode(ReadBuffer & in, UInt64 max_cardinality, PostingList & postings, TextIndexScoringKind scoring, PaddedPODArray<char> & buffer) const override;
    void decode(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, TextIndexScoringKind scoring, PaddedPODArray<char> & buffer) const override;
    void decodeWithWeights(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & weights, TextIndexScoringKind scoring, PaddedPODArray<char> & buffer) const override;
};

}
