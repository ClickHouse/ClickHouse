#pragma once

#include <Compression/ICompressionCodec.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IPostingListCodec.h>

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

/// A codec for a postings list stored in a compact block-compressed format.
///
/// Values are first delta-compressed then bigpacked, each within fixed-size blocks (physical chunks, controlled by BLOCK_SIZE).
/// Each compressed block is stored as: [1 byte: bits-width][payload].
///
/// Posting lists are additionally split into "segments" (logical chunks, controlled by postings_list_block_size)
/// to simplify metadata and to support multiple ranges per token (min/max row id per segment).
///
/// Assumes that input row ids are strictly increasing.
class PostingListCodecBitpackingImpl
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

        void write(WriteBuffer & out) const
        {
            /// At the moment, bitpacking is the only supported codec, could add more codecs in future
            writeVarUInt(static_cast<uint8_t>(IPostingListCodec::Type::Bitpacking), out);
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

            readVarUInt(v, in);
            payload_bytes = static_cast<uint64_t>(v);

            readVarUInt(v, in);
            cardinality = static_cast<uint32_t>(v);

            readVarUInt(v, in);
            first_row_id = static_cast<uint32_t>(v);
        }

        /// Number of compressed bytes (per segment) following this header
        uint64_t payload_bytes = 0;
        /// Number of postings (row ids) in this segment
        uint32_t cardinality = 0;
        /// The first row id in the segment (used as a base value to restore from deltas)
        uint32_t first_row_id = 0;
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

public:
    PostingListCodecBitpackingImpl() = default;
    explicit PostingListCodecBitpackingImpl(size_t postings_list_block_size);

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
    void decode(ReadBuffer & in, PostingList & postings);

private:
    void reset()
    {
        total_row_ids = 0;
        compressed_data.clear();
        segment_descriptors.clear();

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

    /// Decode one compressed block into `current_segment` and reconstruct absolute row ids.
    ///
    /// - Reads bits-width byte
    /// - Codec::decode fills `current_segment` with delta values
    /// - inclusive_scan converts deltas -> row ids using `prev_row_id` as initial prefix
    /// - Updates prev_row_id to the last decoded row id
    static void decodeBlock(std::span<const std::byte> & in, size_t count, uint32_t & prev_row_id, std::vector<uint32_t> & current_segment);

    /// All segments
    std::string compressed_data;
    /// Last encoded/decoded row id
    uint32_t prev_row_id = 0;
    /// Row ids in the current segment
    std::vector<uint32_t> current_segment;
    /// Each segment has an in-memory descriptor
    std::vector<SegmentDescriptor> segment_descriptors;
    /// Total number of postings added across all segments.
    size_t total_row_ids = 0;
    /// Number of values added in the current segment.
    size_t row_ids_in_current_segment = 0;
    /// Segment size
    const size_t max_rowids_in_segment = 1024 * 1024;
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
    void decode(ReadBuffer & in, PostingList & postings) const override;
};

/// A posting list codec that doesn't compress (no-op).
class PostingListCodecNone : public IPostingListCodec
{
public:
    static const char * getName() { return "none"; }

    PostingListCodecNone() : IPostingListCodec(Type::None) {}

    void encode(const PostingList &, size_t, TokenPostingsInfo &, WriteBuffer &) const override {}
    void decode(ReadBuffer &, PostingList &) const override {}
};

}

