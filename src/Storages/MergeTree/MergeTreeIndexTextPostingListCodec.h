#pragma once

#include <Compression/ICompressionCodec.h>
#include <Common/PODArray_fwd.h>
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
/// by a per-block payload codec (IPostingListBlockCodec). The block payload is the only codec-specific part.
/// The segment / block / Index Section layout is shared by all block codecs.
///
/// Posting lists are additionally split into "segments" (logical chunks, controlled by postings_list_block_size)
/// to support multiple reading partial ranges per token (min/max row id per segment).
///
/// Assumes that input row ids are strictly increasing.
class SegmentedPostingListCodec
{
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

        void write(WriteBuffer & out, IPostingListCodec::Type codec_type_, bool enabled_scoring) const
        {
            writeVarUInt(static_cast<uint8_t>(codec_type_), out);
            writeVarUInt(payload_bytes, out);
            writeVarUInt(cardinality, out);
            writeVarUInt(first_row_id, out);

            if (enabled_scoring)
            {
                writeBinaryLittleEndian(segment_min_dl_byte, out);
                writeBinaryLittleEndian(segment_max_tf_minus_one, out);
            }
        }

        void read(ReadBuffer & in, bool enabled_scoring)
        {
            UInt64 v = 0;
            readVarUInt(v, in);
            if (v != static_cast<uint8_t>(IPostingListCodec::Type::Bitpacking))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected codec type Bitpacking, got {}", v);
            codec_type = static_cast<IPostingListCodec::Type>(v);

            readVarUInt(v, in);
            payload_bytes = static_cast<uint64_t>(v);

            readVarUInt(v, in);
            cardinality = static_cast<uint32_t>(v);

            readVarUInt(v, in);
            first_row_id = static_cast<uint32_t>(v);

            if (enabled_scoring)
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
        /// Min `SmallFloat` doc-length byte across the segment.
        UInt8 segment_min_dl_byte = 0xFF;
        /// Max saturating `(tf - 1)` across the segment (`255` is the "max tf >= 256" sentinel).
        UInt8 segment_max_tf_minus_one = 0;
    };

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
    SegmentedPostingListCodec() = default;
    explicit SegmentedPostingListCodec(IPostingListCodec::Type block_codec_type_);

    /// Encodes a batch of sorted unique row ids (increasing across calls).
    /// Appends to the open segment and starts a new one every `context.segment_size` row ids.
    void append(
        std::span<const UInt32> row_ids,
        std::span<const UInt32> tf_minus_one,
        const PostingListBuildContext & context);

    /// Writes all buffered segments to `out` and fills `info` with:
    /// - offsets: byte offsets in `out` where each segment begins
    /// - ranges: [row_begin, row_end] row range for each segment
    void serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const;

    /// Total number of row ids added so far.
    size_t cardinality() const { return total_row_ids; }

    /// True once an `append` carried scoring (i.e. BM25 scoring is enabled for the index).
    bool hasScoring() const { return has_scoring; }

    /// Deserializes a postings list from input `in` into `out`.
    ///
    /// Format per segment:
    ///   Header + [row ids compressed bytes] + [term frequencies compressed bytes]
    ///
    /// Decompression restores delta values and then performs an inclusive scan to reconstruct absolute row ids.
    /// When `enabled_scoring` is set, scoring payload is written after row ids and is skipped.
    void decode(ReadBuffer & in, PostingList & postings, bool enabled_scoring, PaddedPODArray<char> & buffer);

    /// The same, but appends the decoded row ids to the plain array,
    /// decoding blocks directly into the array without a roaring bitmap.
    void decode(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, bool enabled_scoring, PaddedPODArray<char> & buffer);

    /// Deserializes a postings list with the exact per-row term frequencies.
    ///
    /// Works like above`, but the term-frequency payload of each block is decoded.
    /// Every decoded `tf` is appended to `tfs`, parallel to the decoded row ids.
    void decodeWithTermFrequencies(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & tfs, PaddedPODArray<char> & buffer);

private:
    /// Encodes one block of up to BLOCK_SIZE row ids as deltas and appends it to `compressed_data`.
    ///
    /// Block layout:
    ///   [1 byte bits][row ids payload][term frequencies payload]
    ///
    /// - bits: max bit-width among deltas in this block
    /// - row ids payload: Codec::encode(...) bitpacked bytes
    /// - term frequencies payload: Codec::encode(...) bitpacked bytes
    ///
    /// Also updates current segment metadata (cardinality, payload size).
    ///
    /// When scoring is enabled, a term-frequency block is appended after the deltas, and the block-max
    /// metadata takes the doc lengths from `doc_lengths[row_id - doc_lengths_first_row_id]`.
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
    SegmentData readSegmentData(ReadBuffer & in, bool enabled_scoring, PaddedPODArray<char> & buffer);

    /// Skips one block's term frequencies
    void skipTermFrequencies(std::span<const std::byte> & in, size_t count);

    /// Decodes one block's term frequencies and appends the exact `tf` for each row to `tfs`.
    void decodeTermFrequencies(std::span<const std::byte> & in, size_t count, PaddedPODArray<UInt32> & tfs);

    /// All segments. Filled on encode only: decode reads the payload from the buffer passed to it.
    std::string compressed_data;
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
    explicit SegmentedPostingListEncoder(IPostingListCodec::Type block_codec_type_) : impl(block_codec_type_) {}

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

/// Codec for serializing/deserializing a postings list to/from a binary stream.
/// A codec for a postings list stored in a compact block-compressed format.
///
/// Values are first delta-compressed then bigpacked, each within fixed-size blocks (physical chunks, controlled by BLOCK_SIZE).
/// Each compressed block is stored as: [1 byte: bits-width][payload].
///
/// Posting lists are additionally split into "segments" (logical chunks, controlled by postings_list_block_size)
/// to simplify metadata and to support multiple ranges per token (min/max row id per segment).
///
/// Assumes that input row ids are strictly increasing.
class PostingListCodecBitpacking : public IPostingListCodec
{
public:
    static const char * getName() { return "bitpacking"; }

    PostingListCodecBitpacking() : IPostingListCodec(Type::Bitpacking) {}

    /// Normalizes the requested segment size to a multiple of BLOCK_SIZE, because the SIMD
    /// bit-packing implementation expects block-aligned sizes for efficient processing.
    size_t getSegmentSize(size_t posting_list_block_size) const override;
    std::unique_ptr<IPostingListEncoder> createEncoder() const override;

    void decode(ReadBuffer & in, PostingList & postings, bool enabled_scoring, PaddedPODArray<char> & buffer) const override;
    void decode(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, bool enabled_scoring, PaddedPODArray<char> & buffer) const override;
    void decodeWithTermFrequencies(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & tfs, PaddedPODArray<char> & buffer) const override;
};

/// Accumulator for the None codec.
/// Each added segment is stored as a Roaring bitmap and serialized on `finalize`
/// as a portable Roaring bitmap prefixed by its size in bytes.
class PostingListEncoderNone final : public IPostingListEncoder
{
public:
    void append(
        std::span<const UInt32> row_ids,
        std::span<const UInt32> tf_minus_one,
        const PostingListBuildContext & context) override;
    void finalize(WriteBuffer & out, TokenPostingsInfo & info) override;

    size_t cardinality() const override { return total_row_ids; }

private:
    void finishSegment();

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

    std::unique_ptr<IPostingListEncoder> createEncoder() const override;
    void decode(ReadBuffer & in, PostingList & postings, bool enabled_scoring, PaddedPODArray<char> & buffer) const override;
    void decode(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, bool enabled_scoring, PaddedPODArray<char> & buffer) const override;
    void decodeWithTermFrequencies(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & tfs, PaddedPODArray<char> & buffer) const override;
};

}

