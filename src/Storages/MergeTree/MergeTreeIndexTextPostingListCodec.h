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
        uint32_t last_row_id;       /// Last row_id in this packed block
        uint64_t relative_offset;   /// Offset within segment payload (from segment data start)
    };

    /// Per-segment list of packed block metadata.
    struct SegmentBlockMetas
    {
        std::vector<PackedBlockMeta> metas;
    };

public:
    SegmentedPostingListCodec() = default;
    explicit SegmentedPostingListCodec(size_t postings_list_block_size, IPostingListCodec::Type block_codec_type_);

    /// Add a single increasing row id.
    ///
    /// Internally we store deltas (gaps) in `current_segment` until reaching BLOCK_SIZE,
    /// then compress the full block into `compressed_data`.
    /// When the segment reaches `max_rowids_in_segment`, flush it.
    void insert(uint32_t row_id);

    /// Add a block of BLOCK_SIZE-many row ids.
    ///
    /// Assumes:
    /// - row_ids.size() == BLOCK_SIZE
    /// - total is aligned by BLOCK_SIZE
    ///
    /// It computes deltas in-place using adjacent_difference for better throughput.
    void insert(std::span<uint32_t> row_ids);

    /// Serialize all buffered postings to `out` and update TokenPostingsInfo.
    ///
    /// Flushes any pending partial block and writes per-segment headers
    /// followed by the segment payload bytes.
    void encode(WriteBuffer & out, TokenPostingsInfo & info)
    {
        if (!current_segment.empty())
            encodeBlock(current_segment);

        serializeTo(out, info);
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
    void reset()
    {
        total_row_ids = 0;
        compressed_data.clear();
        segment_descriptors.clear();
        segment_block_metas.clear();

        resetCurrentSegment();
    }

    void resetCurrentSegment()
    {
        current_segment.clear();
        row_ids_in_current_segment = 0;
        prev_row_id = 0;
    }

    /// Flush current segment:
    /// - compress pending partial block
    /// - reset block state so a new segment can start
    void flushCurrentSegment()
    {
       chassert(row_ids_in_current_segment <= max_rowids_in_segment);

       if (!current_segment.empty())
           encodeBlock(current_segment);

        resetCurrentSegment();
    }

    /// Write all segments to output and fill TokenPostingsInfo:
    /// - offsets: byte offsets in output where each segment begins
    /// - ranges: [row_begin, row_end] row range for each segment
    void serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const;

    /// Encode one block of delta values and append it to `compressed_data`.
    ///
    /// Block layout:
    ///   [1 byte bits][payload]
    ///
    /// - bits: max bit-width among deltas in this block
    /// - payload: Codec::encode(...) bitpacked bytes
    ///
    /// Also updates current segment metadata (count, max, payload size).
    void encodeBlock(std::span<uint32_t> segment);

    /// Decode one compressed block of `out.size()` row ids into `out` and reconstruct absolute row ids.
    ///
    /// - Delegates the block payload to `block_codec` (bitpacking reads a bits-width byte), which fills
    ///   `out` with delta values
    /// - inclusive_scan converts deltas -> row ids using `prev_row_id` as initial prefix
    /// - Updates prev_row_id to the last decoded row id
    void decodeBlock(std::span<const std::byte> & in, std::span<uint32_t> out);

    /// Reads a segment header and returns it together with the segment payload.
    SegmentData readSegmentData(ReadBuffer & in, PaddedPODArray<char> & buffer);

    /// All segments. Filled on encode only: decode reads the payload from the buffer passed to it.
    std::string compressed_data;
    /// Last encoded/decoded row id
    uint32_t prev_row_id = 0;
    /// Row ids in the current segment
    std::vector<uint32_t> current_segment;
    /// Each segment has an in-memory descriptor
    std::vector<SegmentDescriptor> segment_descriptors;
    /// Per-segment packed block metadata for V2 Index Section
    std::vector<SegmentBlockMetas> segment_block_metas;
    /// Total number of postings added across all segments.
    size_t total_row_ids = 0;
    /// Number of values added in the current segment.
    size_t row_ids_in_current_segment = 0;
    /// Segment size
    const size_t max_rowids_in_segment = 1024 * 1024;
    /// Per-block payload codec (bitpacking). On encode it is fixed by the constructor; on decode it
    /// is created from the segment header. One instance is reused across all blocks of a single encode/decode call.
    std::unique_ptr<IPostingListBlockCodec> block_codec;
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
class PostingListCodecBitpacking : public  IPostingListCodec
{
public:
    static const char * getName() { return "bitpacking"; }

    PostingListCodecBitpacking() : IPostingListCodec(Type::Bitpacking) {}

    void encode(const PostingList & postings, size_t max_rowids_in_segment, TokenPostingsInfo & info, WriteBuffer & out) const override;
    void decode(ReadBuffer & in, PostingList & postings, PaddedPODArray<char> & buffer) const override;
    void decode(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<char> & buffer) const override;
};

/// A codec that applies no compression: a posting list block is stored as
/// [VarUInt: number of bytes][portable serialization of a roaring bitmap].
///
/// Encoding is done by `PostingsSerialization`, which splits the posting list into blocks
/// without copying the underlying roaring containers, so `encode` is not implemented here.
class PostingListCodecNone : public IPostingListCodec
{
public:
    static const char * getName() { return "none"; }

    PostingListCodecNone() : IPostingListCodec(Type::None) {}

    void encode(const PostingList &, size_t, TokenPostingsInfo &, WriteBuffer &) const override {}
    void decode(ReadBuffer & in, PostingList & postings, PaddedPODArray<char> & buffer) const override;
    void decode(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<char> & buffer) const override;
};

}

