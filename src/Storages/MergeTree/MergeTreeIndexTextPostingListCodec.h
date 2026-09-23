#pragma once

#include <Compression/ICompressionCodec.h>
#include <Common/PODArray.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>

#include <memory>
#include <span>

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
/// With BM25 scoring every block is followed by the per-row `(tf - 1)` of its row ids, encoded by the same block codec,
/// and the segment header and the Index Section carry the block-max inputs of the scoring (see `PackedBlockMeta`).
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

        void write(WriteBuffer & out, IPostingListCodec::Type codec_type_, bool has_term_frequencies) const
        {
            writeVarUInt(static_cast<uint8_t>(codec_type_), out);
            writeVarUInt(payload_bytes, out);
            writeVarUInt(cardinality, out);
            writeVarUInt(first_row_id, out);

            if (has_term_frequencies)
            {
                writeBinaryLittleEndian(segment_min_dl_byte, out);
                writeBinaryLittleEndian(segment_max_tf_minus_one, out);
            }
        }

        void read(ReadBuffer & in, bool has_term_frequencies)
        {
            UInt64 v = 0;
            readVarUInt(v, in);
            if (!isValidPostingListBlockCodecType(v))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: unknown posting list block codec type {}", v);
            codec_type = static_cast<IPostingListCodec::Type>(v);

            readVarUInt(v, in);
            payload_bytes = static_cast<uint64_t>(v);

            readVarUInt(v, in);
            cardinality = static_cast<uint32_t>(v);

            readVarUInt(v, in);
            first_row_id = static_cast<uint32_t>(v);

            if (has_term_frequencies)
            {
                readBinaryLittleEndian(segment_min_dl_byte, in);
                readBinaryLittleEndian(segment_max_tf_minus_one, in);
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
        /// Min `SmallFloat` doc-length byte across the segment. Written only with term frequencies.
        UInt8 segment_min_dl_byte = 0xFF;
        /// Max saturating `(tf - 1)` across the segment (`255` is the "max tf >= 256" sentinel). Written only with term frequencies.
        UInt8 segment_max_tf_minus_one = 0;
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
        /// Max saturating `(tf - 1)` in this block (`255` is the "max tf >= 256" sentinel).
        UInt8 max_tf_minus_one = 0;
    };

    /// Per-segment list of packed block metadata.
    struct SegmentBlockMetas
    {
        std::vector<PackedBlockMeta> metas;
    };

public:
    /// Constructs a codec for decoding only: `append` requires the encoding constructor below.
    SegmentedPostingListCodec() = default;

    /// Constructs a codec for encoding.
    /// The requested `segment_size_` is rounded up to a multiple of BLOCK_SIZE.
    SegmentedPostingListCodec(IPostingListCodec::Type block_codec_type_, size_t segment_size_);

    /// Encodes a batch of sorted unique row ids (increasing across calls), appending
    /// to the open segment and starting a new one every `segment_size` row ids.
    /// On the BM25 scoring path (`context.enable_scoring`) a non-empty `tf_minus_one`, parallel to `row_ids`,
    /// carries the per-row term frequencies, and the doc lengths of the rows come from `context.doc_lengths`.
    void append(
        std::span<const UInt32> row_ids,
        std::span<const UInt32> tf_minus_one,
        const PostingListBuildContext & context);

    /// Write all segments to output and fill TokenPostingsInfo:
    /// - offsets: byte offsets in output where each segment begins
    /// - ranges: [row_begin, row_end] row range for each segment
    void serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const;

    /// Total number of row ids added so far.
    size_t cardinality() const { return total_row_ids; }

    /// True once an `append` carried scoring (i.e. BM25 scoring is enabled for the index).
    bool hasScoring() const { return has_scoring; }

    /// Deserialize a postings list from input `in` into `out`.
    ///
    /// Format per segment:
    ///   Header + [compressed bytes]
    ///
    /// Decompression restores delta values and then performs an inclusive scan
    /// to reconstruct absolute row ids.
    ///
    /// `max_cardinality` bounds the sizes claimed by the segment header (see `readSegmentData`).
    /// With `has_term_frequencies` every block is followed by its term frequencies, which are skipped.
    void decode(ReadBuffer & in, UInt64 max_cardinality, PostingList & postings, bool has_term_frequencies, PaddedPODArray<char> & buffer);

    /// The same, but appends the decoded row ids to the plain array,
    /// decoding blocks directly into the array without a roaring bitmap.
    void decode(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, bool has_term_frequencies, PaddedPODArray<char> & buffer);

    /// Deserializes a postings list with the exact per-row term frequencies.
    ///
    /// Works like the above, but the term-frequency payload of each block is decoded.
    /// Every decoded `tf` is appended to `tfs`, parallel to the decoded row ids.
    void decodeWithTermFrequencies(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & tfs, PaddedPODArray<char> & buffer);

private:
    /// Encodes one block of up to BLOCK_SIZE row ids as deltas and appends it to `compressed_data`.
    ///
    /// Block layout:
    ///   [1 byte bits][row ids payload][term frequencies payload]
    ///
    /// - bits: max bit-width among deltas in this block
    /// - row ids payload: Codec::encode(...) bitpacked bytes
    /// - term frequencies payload: the per-row `(tf - 1)` encoded by the same block codec, only with scoring
    ///
    /// Also updates current segment metadata (cardinality, payload size).
    ///
    /// When scoring is enabled, the block-max metadata takes the doc lengths from `doc_lengths[row_id - doc_lengths_first_row_id]`.
    void encodeBlock(
        std::span<const UInt32> block_row_ids,
        std::span<const UInt32> term_frequencies,
        std::span<const UInt8> doc_lengths,
        UInt32 doc_lengths_first_row_id);

    /// Appends one block's `count` term frequencies to `compressed_data`, after its deltas.
    void encodeTermFrequencies(std::span<const UInt32> term_frequencies, size_t count);

    /// Decodes one compressed block of `out.size()` row ids into `out` and reconstructs absolute row ids.
    ///
    /// - Delegates the block payload to `block_codec` (bitpacking reads a bits-width byte), which fills `out` with delta values
    /// - inclusive_scan converts deltas to row ids using `prev_row_id` as initial prefix
    /// - Updates prev_row_id to the last decoded row id
    void decodeBlock(std::span<const std::byte> & in, std::span<uint32_t> out);

    /// Reads a segment header and returns it together with the segment payload.
    /// Throws CORRUPTED_DATA if the header claims more than `max_cardinality` row ids or more payload bytes than they can take.
    SegmentData readSegmentData(ReadBuffer & in, UInt64 max_cardinality, bool has_term_frequencies, PaddedPODArray<char> & buffer);

    /// Skips one block's term frequencies.
    void skipTermFrequencies(std::span<const std::byte> & in, size_t count);

    /// Decodes one block's term frequencies and appends the exact `tf` for each row to `tfs`.
    void decodeTermFrequencies(std::span<const std::byte> & in, size_t count, PaddedPODArray<UInt32> & tfs);

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
    /// Set true once an `append` was made with scoring enabled; observed by `serializeTo`, which has no
    /// build context, to emit the per-segment header block-max bytes and the per-block
    /// `min_dl_byte[]`/`max_tf_minus_one[]` arrays.
    bool has_scoring = false;
    /// Per-block payload codec (bitpacking). On encode it is fixed by the constructor; on decode it
    /// is created from the segment header. One instance is reused across all blocks of a single encode/decode call.
    std::unique_ptr<IPostingListBlockCodec> block_codec;
};

/// Accumulator for block-compressed codecs (see SegmentedPostingListCodec).
/// Wraps SegmentedPostingListCodec, which encodes each added segment into
/// blocks held in memory; the compressed bytes are flushed on `finalize`.
class SegmentedPostingListEncoder final : public IPostingListEncoder
{
public:
    SegmentedPostingListEncoder(IPostingListCodec::Type block_codec_type_, size_t segment_size)
        : impl(block_codec_type_, segment_size)
    {
    }

    void append(
        std::span<const UInt32> row_ids,
        std::span<const UInt32> tf_minus_one,
        const PostingListBuildContext & context) override
    {
        impl.append(row_ids, tf_minus_one, context);
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
    std::unique_ptr<IPostingListEncoder> createEncoder(size_t segment_size) const override;

    void decode(ReadBuffer & in, UInt64 max_cardinality, PostingList & postings, bool has_term_frequencies, PaddedPODArray<char> & buffer) const override;
    void decode(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, bool has_term_frequencies, PaddedPODArray<char> & buffer) const override;
    void decodeWithTermFrequencies(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & tfs, PaddedPODArray<char> & buffer) const override;
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
        std::span<const UInt32> tf_minus_one,
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

    std::unique_ptr<IPostingListEncoder> createEncoder(size_t segment_size) const override;
    void decode(ReadBuffer & in, UInt64 max_cardinality, PostingList & postings, bool has_term_frequencies, PaddedPODArray<char> & buffer) const override;
    void decode(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, bool has_term_frequencies, PaddedPODArray<char> & buffer) const override;
    void decodeWithTermFrequencies(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & tfs, PaddedPODArray<char> & buffer) const override;
};

}
