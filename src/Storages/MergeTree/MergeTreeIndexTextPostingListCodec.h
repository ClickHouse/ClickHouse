#pragma once

#include <Compression/ICompressionCodec.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <Storages/MergeTree/IPostingListCodec.h>

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
    PostingListCodecBitpackingImpl() = default;

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
        return compressed_data.capacity()
            + current_segment.capacity() * sizeof(uint32_t)
            + segment_descriptors.capacity() * sizeof(SegmentDescriptor)
            + segment_block_metas.capacity() * sizeof(SegmentBlockMetas);
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
    /// Number of row ids in the open segment.
    size_t row_ids_in_current_segment = 0;
    /// Scratch buffer for deltas of the block being encoded (also reused as the decode buffer)
    std::vector<uint32_t> current_segment;
    /// Each segment has an in-memory descriptor
    std::vector<SegmentDescriptor> segment_descriptors;
    /// Per-segment packed block metadata for V2 Index Section
    std::vector<SegmentBlockMetas> segment_block_metas;
    /// Total number of postings added across all segments.
    size_t total_row_ids = 0;
};

/// Accumulator for the Bitpacking codec.
/// Wraps PostingListCodecBitpackingImpl, which encodes each added segment into
/// bit-packed blocks held in memory; the compressed bytes are flushed on `finalize`.
class PostingListAccumulatorBitpacking final : public IPostingListAccumulator
{
public:
    void append(std::span<const UInt32> row_ids, size_t segment_size) override { impl.append(row_ids, segment_size); }
    void finalize(WriteBuffer & out, TokenPostingsInfo & info) override;

    size_t cardinality() const override { return impl.cardinality(); }
    size_t memoryUsageBytes() const override { return impl.memoryUsageBytes(); }

private:
    PostingListCodecBitpackingImpl impl;
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

    /// Normalizes the requested segment size to a multiple of BLOCK_SIZE, because the SIMD
    /// bit-packing implementation expects block-aligned sizes for efficient processing.
    size_t getSegmentSize(size_t posting_list_block_size) const override;

    std::unique_ptr<IPostingListAccumulator> createAccumulator() const override
    {
        return std::make_unique<PostingListAccumulatorBitpacking>();
    }

    void decode(ReadBuffer & in, PostingList & postings) const override;
};

/// Accumulator for the None codec.
/// Each added segment is stored as a Roaring bitmap and serialized on `finalize`
/// as a portable Roaring bitmap prefixed by its size in bytes.
class PostingListAccumulatorNone final : public IPostingListAccumulator
{
public:
    void append(std::span<const UInt32> row_ids, size_t segment_size) override;
    void finalize(WriteBuffer & out, TokenPostingsInfo & info) override;

    size_t cardinality() const override { return total_row_ids; }
    size_t memoryUsageBytes() const override;

private:
    /// Seals the open segment and starts a new one.
    void sealSegment();

    PostingList current_segment;
    std::vector<PostingList> segments;
    size_t rows_in_current_segment = 0;
    size_t total_row_ids = 0;
};

/// A posting list codec that doesn't compress.
/// Each segment is serialized as a portable Roaring bitmap with a leading VarUInt size.
class PostingListCodecNone : public IPostingListCodec
{
public:
    static const char * getName() { return "none"; }

    PostingListCodecNone() : IPostingListCodec(Type::None) {}

    std::unique_ptr<IPostingListAccumulator> createAccumulator() const override
    {
        return std::make_unique<PostingListAccumulatorNone>();
    }

    void decode(ReadBuffer & in, PostingList & postings) const override;
};

}

