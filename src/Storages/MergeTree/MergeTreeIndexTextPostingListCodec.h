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
/// Values are first delta-compressed then bitpacked, each within fixed-size blocks
/// (physical chunks, controlled by POSTING_LIST_UNIT_SIZE).
/// Each compressed block is stored as: [1 byte: bits-width][4 bytes: row_begin][payload].
///
/// Posting lists are additionally split into "segments" (logical chunks, controlled by
/// postings_list_block_size) to simplify metadata and to support multiple ranges per
/// token (min/max row id per segment).
///
/// Assumes that input row ids are strictly increasing.
///
/// Storage Layout
/// ==============
///
/// Hierarchy:
///   Posting List
///   └── Segments (logical chunks, size controlled by posting_list_block_size, default 1M)
///       └── Blocks (physical chunks, fixed size POSTING_LIST_UNIT_SIZE = 128)
///
/// Segment Layout:
/// ┌─────────────────────────────────────────────────────────────────────┐
/// │ Header (VarUInt)  │ Block Skip Index (optional) │ Compressed Blocks │
/// └─────────────────────────────────────────────────────────────────────┘
///
/// Header fields (VarUInt encoded):
///   - codec_type (uint8): encoding type (Bitpacking=1)
///   - payload_bytes (uint64): compressed data size
///   - cardinality (uint32): number of row IDs in segment
///   - first_row_id (uint32): base value for delta decoding
///   - has_block_skip_index (uint8): whether block index exists
///
/// Block Skip Index (only when posting_list_apply_mode=lazy):
///   - block_row_ends[]: max row ID of each block (for binary search)
///   - block_offsets[]: byte offset of each block in payload
///
/// Block Layout:
/// ┌──────────────────────────────────────────────────────────────┐
/// │ max_bits (1B) │ row_begin (4B) │ bitpacked deltas (variable) │
/// └──────────────────────────────────────────────────────────────┘
///
class PostingListCodecBitpackingImpl
{
    friend class PostingListCursor;
    /// Header written at the beginning of each segment before the payload.
    struct Header
    {
        Header() = default;

        Header(size_t payload_bytes_, uint32_t cardinality_, uint32_t base_value_, bool has_block_skip_index_)
            : payload_bytes(payload_bytes_)
            , cardinality(cardinality_)
            , first_row_id(base_value_)
            , has_block_skip_index(has_block_skip_index_)
        {
        }

        void write(WriteBuffer & out) const
        {
            writeVarUInt(static_cast<uint8_t>(IPostingListCodec::Type::Bitpacking), out);
            writeVarUInt(payload_bytes, out);
            writeVarUInt(cardinality, out);
            writeVarUInt(first_row_id, out);
            writeVarUInt(has_block_skip_index, out);
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

            readVarUInt(v, in);
            has_block_skip_index = static_cast<uint8_t>(v);
        }

        /// Number of compressed bytes (per segment) following this header
        uint64_t payload_bytes = 0;
        /// Number of postings (row ids) in this segment
        uint32_t cardinality = 0;
        /// The first row id in the segment (used as a base value to restore from deltas)
        uint32_t first_row_id = 0;
        uint8_t has_block_skip_index = 0;
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

        size_t block_count = 0;

        /// The following two fields are only needed when posting_list_apply_mode=lazy.
        /// They enable block-level binary search for efficient seek/linearOr/linearAnd operations.
        /// Max row ID of each block (for binary search to skip blocks)
        std::vector<uint32_t> block_row_ends;
        /// Byte offset of each block within compressed_data (for direct block access)
        std::vector<size_t> block_offsets;
    };

public:
    PostingListCodecBitpackingImpl() = default;

    explicit PostingListCodecBitpackingImpl(size_t postings_list_block_size_, bool has_block_skip_index_);

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
    void clear()
    {
        resetCurrentSegment();

        total_row_ids = 0;
        compressed_data.clear();
        segment_descriptors.clear();
    }

    void resetCurrentSegment()
    {
        current_segment.clear();
        row_ids_in_current_segment = 0;
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
    static void decodeBlock(std::span<const std::byte> & in, size_t count, std::vector<uint32_t> & current_segment);

    /// All segments
    std::string compressed_data;
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
    /// If true, write block_row_ends and block_offsets for lazy apply mode (seek/skip support).
    /// If false, skip writing block index to save space when only materialize mode is used.
    bool has_block_skip_index = false;
};

/// Codec for serializing/deserializing a postings list to/from a binary stream.
/// See PostingListCodecBitpackingImpl for storage layout details.
class PostingListCodecBitpacking : public  IPostingListCodec
{
public:
    static const char * getName() { return "bitpacking"; }

    PostingListCodecBitpacking() : IPostingListCodec(Type::Bitpacking) {}

    void encode(const PostingListBuilder & postings, size_t max_rowids_in_segment, bool has_block_skip_index, TokenPostingsInfo & info, WriteBuffer & out) const override;
    void decode(ReadBuffer & in, PostingList & postings) const override;
};

/// A posting list codec that doesn't compress (no-op).
class PostingListCodecNone : public IPostingListCodec
{
public:
    static const char * getName() { return "none"; }

    PostingListCodecNone() : IPostingListCodec(Type::None) {}

    void encode(const PostingListBuilder &, size_t, bool, TokenPostingsInfo &, WriteBuffer &) const override {}
    void decode(ReadBuffer &, PostingList &) const override {}
};

struct CodecUtil
{
static void encodeU8(uint8_t x, std::span<char> & out)
{
    out[0] = static_cast<char>(x);
    out = out.subspan(1);
}

static uint8_t decodeU8(std::span<const std::byte> & in)
{
    auto v = static_cast<uint8_t>(in[0]);
    in = in.subspan(1);
    return v;
}

static void encodeU32(uint32_t x, std::span<char> & out)
{
    out[0] = static_cast<char>(x & 0xFF);
    out[1] = static_cast<char>((x >> 8) & 0xFF);
    out[2] = static_cast<char>((x >> 16) & 0xFF);
    out[3] = static_cast<char>((x >> 24) & 0xFF);
    out = out.subspan(4);
}

static uint32_t decodeU32(std::span<const std::byte> & in)
{
    uint32_t v =
    (static_cast<uint32_t>(static_cast<uint8_t>(in[0]))) |
    (static_cast<uint32_t>(static_cast<uint8_t>(in[1])) <<  8) |
    (static_cast<uint32_t>(static_cast<uint8_t>(in[2])) << 16) |
    (static_cast<uint32_t>(static_cast<uint8_t>(in[3])) << 24);

    in = in.subspan(4);
    return v;
}

template<typename T>
static void writeArrayU32(const std::vector<T> & arr, WriteBuffer & wb)
{
    writeVarUInt(arr.size(), wb);
    for (size_t i = 0; i < arr.size(); ++i)
        writeVarUInt(arr[i], wb);
}

template<typename T>
static void readArrayU32(ReadBuffer & rb, std::vector<T> & arr)
{
    UInt64 x = 0;
    readVarUInt(x, rb);
    arr.resize(x);
    for (size_t i = 0; i < x; ++i)
        readVarUInt(arr[i], rb);
}
};

}

