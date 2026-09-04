#pragma once

#include <Compression/ICompressionCodec.h>
#include <Common/PODArray_fwd.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>

#include <memory>

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
/// by a per-block payload codec (IPostingListBlockCodec — currently bitpacking). The block payload is the only
/// codec-specific part; the segment / block / Index Section layout below is shared by all block codecs.
///
/// Posting lists are additionally split into "segments" (logical chunks, controlled by postings_list_block_size)
/// to simplify metadata and to support multiple ranges per token (min/max row id per segment).
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

        void write(WriteBuffer & out, IPostingListCodec::Type codec_type_) const
        {
            writeVarUInt(static_cast<uint8_t>(codec_type_), out);
            writeVarUInt(payload_bytes, out);
            writeVarUInt(cardinality, out);
            writeVarUInt(first_row_id, out);
        }

        void read(ReadBuffer & in)
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
        }

        /// Block codec used for this segment's payload. Filled by read.
        IPostingListCodec::Type codec_type = IPostingListCodec::Type::Bitpacking;
        /// Number of compressed bytes (per segment) following this header
        uint64_t payload_bytes = 0;
        /// Number of postings (row ids) in this segment
        uint32_t cardinality = 0;
        /// The first row id in the segment (used as a base value to restore from deltas)
        uint32_t first_row_id = 0;
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
        uint32_t last_row_id;
        /// Offset within segment payload (from segment data start)
        uint64_t relative_offset;
    };

    /// Per-segment list of packed block metadata.
    struct SegmentBlockMetas
    {
        std::vector<PackedBlockMeta> metas;
    };

public:
    SegmentedPostingListCodec() = default;
    explicit SegmentedPostingListCodec(IPostingListCodec::Type block_codec_type_);

    /// Encode a batch of sorted unique row ids (increasing across calls), appending
    /// to the open segment and starting a new one every `segment_size` row ids.
    /// Values are converted to deltas (gaps) and compressed in blocks of BLOCK_SIZE
    /// values into `compressed_data`. Every call, except the final one, must contain
    /// a multiple of BLOCK_SIZE row ids, so that only the very last block is partial.
    void append(std::span<const UInt32> row_ids, size_t segment_size);

    /// Serialize all buffered postings to `out` and update TokenPostingsInfo.
    /// Writes per-segment headers followed by the segment payload bytes.
    void encode(WriteBuffer & out, TokenPostingsInfo & info) const
    {
        serializeTo(out, info);
    }

    /// Total number of row ids added so far.
    size_t cardinality() const { return total_row_ids; }

    /// Heap memory held by the in-memory encoded representation.
    size_t memoryUsageBytes() const
    {
        size_t block_metas_bytes = 0;
        for (const auto & segment : segment_block_metas)
            block_metas_bytes += segment.metas.capacity() * sizeof(PackedBlockMeta);

        return compressed_data.capacity()
            + block_values.capacity() * sizeof(UInt32)
            + segment_descriptors.capacity() * sizeof(SegmentDescriptor)
            + segment_block_metas.capacity() * sizeof(SegmentBlockMetas)
            + block_metas_bytes;
    }

    /// Deserialize a postings list from input `in` into `out`.
    ///
    /// Format per segment:
    ///   Header + [compressed bytes]
    ///
    /// Decompression restores delta values and then performs an inclusive scan
    /// to reconstruct absolute row ids.
    void decode(ReadBuffer & in, PostingList & postings, PaddedPODArray<char> & buffer);

    /// The same, but appends the decoded row ids to the plain array,
    /// decoding blocks directly into the array without a roaring bitmap.
    void decode(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<char> & buffer);

private:
    /// Write all segments to output and fill TokenPostingsInfo:
    /// - offsets: byte offsets in output where each segment begins
    /// - ranges: [row_begin, row_end] row range for each segment
    void serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const;

    /// Encodes one block of up to BLOCK_SIZE row ids as deltas and appends it to `compressed_data`.
    ///
    /// Block layout:
    ///   [1 byte bits][row ids payload]
    ///
    /// - bits: max bit-width among deltas in this block
    /// - row ids payload: Codec::encode(...) bitpacked bytes
    ///
    /// Also updates current segment metadata (cardinality, payload size).
    void encodeBlock(std::span<const UInt32> block_row_ids);

    /// Decodes one compressed block of `out.size()` row ids into `out` and reconstructs absolute row ids.
    ///
    /// - Delegates the block payload to `block_codec` (bitpacking reads a bits-width byte), which fills `out` with delta values
    /// - inclusive_scan converts deltas to row ids using `prev_row_id` as initial prefix
    /// - Updates prev_row_id to the last decoded row id
    void decodeBlock(std::span<const std::byte> & in, std::span<uint32_t> out);

    /// Reads a segment header and returns it together with the segment payload.
    SegmentData readSegmentData(ReadBuffer & in, PaddedPODArray<char> & buffer);

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

    void append(std::span<const UInt32> row_ids, size_t segment_size) override { impl.append(row_ids, segment_size); }
    void finalize(WriteBuffer & out, TokenPostingsInfo & info) override;

    size_t cardinality() const override { return impl.cardinality(); }
    size_t memoryUsageBytes() const override { return impl.memoryUsageBytes(); }

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

    void decode(ReadBuffer & in, PostingList & postings, PaddedPODArray<char> & buffer) const override;
    void decode(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<char> & buffer) const override;
};

/// Accumulator for the None codec.
/// Each added segment is stored as a Roaring bitmap and serialized on `finalize`
/// as a portable Roaring bitmap prefixed by its size in bytes.
class PostingListEncoderNone final : public IPostingListEncoder
{
public:
    void append(std::span<const UInt32> row_ids, size_t segment_size) override;
    void finalize(WriteBuffer & out, TokenPostingsInfo & info) override;

    size_t cardinality() const override { return total_row_ids; }
    size_t memoryUsageBytes() const override;

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
    void decode(ReadBuffer & in, PostingList & postings, PaddedPODArray<char> & buffer) const override;
    void decode(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<char> & buffer) const override;
};

}

