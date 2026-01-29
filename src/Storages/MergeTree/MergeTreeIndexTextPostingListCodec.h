#pragma once

#include <config.h>
#include <Compression/ICompressionCodec.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>

#if USE_FASTPFOR
#include <Storages/MergeTree/FastPForBlockCodec.h>
#endif

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

/// A generic codec implementation for postings list using pluggable block compression.
///
/// This template class provides the common logic for:
/// - Delta encoding of row IDs
/// - Segment-based organization (for metadata and range queries)
/// - Block-level compression using the provided BlockCodec
///
/// Template parameters:
/// - BlockCodec: The underlying block compression algorithm (e.g., BitpackingBlockCodec, SIMDFastPForBlockCodec)
/// - codec_type: The IPostingListCodec::Type enum value for header identification
///
/// Block format (for codecs that store bit-width like Bitpacking):
///   [1 byte: bits-width][payload]
///
/// Segment format:
///   [Header: codec_type | payload_bytes | cardinality | first_row_id] (all VarUInt)
///   [Payload: compressed blocks]
///
/// Assumes that input row ids are strictly increasing.
template <typename BlockCodec, IPostingListCodec::Type codec_type>
class PostingListCodecBlockImpl
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
            writeVarUInt(static_cast<uint8_t>(codec_type), out);
            writeVarUInt(payload_bytes, out);
            writeVarUInt(cardinality, out);
            writeVarUInt(first_row_id, out);
        }

        void read(ReadBuffer & in)
        {
            UInt64 v = 0;
            readVarUInt(v, in);
            if (v != static_cast<uint8_t>(codec_type))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected codec type {}, got {}", static_cast<uint8_t>(codec_type), v);

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
    PostingListCodecBlockImpl() = default;
    explicit PostingListCodecBlockImpl(size_t postings_list_block_size);

    /// Add a single increasing row id.
    ///
    /// Internally we store deltas (gaps) in `current_segment` until reaching BLOCK_SIZE,
    /// then compress the full block into `compressed_data`.
    /// When the segment reaches `max_rowids_in_segment`, flush it.
    void insert(uint32_t row_id);

    /// Add a block of BLOCK_SIZE-many row ids (batch insert).
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

    /// Deserialize a postings list from input `in` into `postings`.
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
    /// - payload: BlockCodec::encode(...) compressed bytes
    ///
    /// Also updates current segment metadata (count, max, payload size).
    void encodeBlock(std::span<uint32_t> segment);

    /// Decode one compressed block into `current_segment` and reconstruct absolute row ids.
    ///
    /// - Reads bits-width byte
    /// - BlockCodec::decode fills `current_segment` with delta values
    /// - inclusive_scan converts deltas -> row ids using `prev_row_id` as initial prefix
    /// - Updates prev_row_id to the last decoded row id
    static void decodeBlock(std::span<const std::byte> & in, size_t count, uint32_t & prev_row_id, std::vector<uint32_t> & current_segment);

    /// All segments' compressed data
    std::string compressed_data;
    /// Last encoded/decoded row id
    uint32_t prev_row_id = 0;
    /// Row ids (as deltas) in the current segment
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

/// Generic codec wrapper that delegates to the underlying PostingListCodecBlockImpl.
///
/// This template class provides the IPostingListCodec interface implementation
/// for any BlockCodec type, eliminating code duplication between different codec types.
///
/// A codec for a postings list stored in a compact block-compressed format.
///
/// Values are first delta-compressed then block-packed, each within fixed-size blocks
/// (physical chunks, controlled by BLOCK_SIZE).
/// Each compressed block is stored as: [1 byte: bits-width][payload].
///
/// Posting lists are additionally split into "segments" (logical chunks, controlled by postings_list_block_size)
/// to simplify metadata and to support multiple ranges per token (min/max row id per segment).
///
/// Assumes that input row ids are strictly increasing.
///
/// Template parameters:
/// - BlockCodec: The underlying block compression algorithm (e.g., BitpackingBlockCodec, SIMDFastPForBlockCodec)
/// - codec_type: The IPostingListCodec::Type enum value
template <typename BlockCodec, IPostingListCodec::Type codec_type>
class PostingListCodecGeneric : public IPostingListCodec
{
    using Impl = PostingListCodecBlockImpl<BlockCodec, codec_type>;

public:
    static const char * getName() { return BlockCodec::name(); }

    PostingListCodecGeneric() : IPostingListCodec(codec_type) {}

    void encode(const PostingList & postings, size_t max_rowids_in_segment, TokenPostingsInfo & info, WriteBuffer & out) const override
    {
        Impl impl(max_rowids_in_segment);
        std::vector<uint32_t> rowids;
        rowids.resize(postings.cardinality());
        postings.toUint32Array(rowids.data());

        std::span<uint32_t> rowids_view(rowids.data(), rowids.size());
        while (rowids_view.size() >= BLOCK_SIZE)
        {
            auto front = rowids_view.first(BLOCK_SIZE);
            impl.insert(front);
            rowids_view = rowids_view.subspan(BLOCK_SIZE);
        }

        if (!rowids_view.empty())
        {
            for (auto rowid : rowids_view)
                impl.insert(rowid);
        }
        impl.encode(out, info);
    }

    void decode(ReadBuffer & in, PostingList & postings) const override
    {
        Impl impl;
        impl.decode(in, postings);
    }
};

/// Codec for serializing/deserializing a postings list using Bitpacking compression.
///
/// Bitpacking is the default codec that provides good compression ratio with fast decode speed.
/// It packs integers using the minimum number of bits required for the maximum value in each block.
using PostingListCodecBitpacking = PostingListCodecGeneric<BitpackingBlockCodec, IPostingListCodec::Type::Bitpacking>;

/// A posting list codec that doesn't compress (no-op).
class PostingListCodecNone : public IPostingListCodec
{
public:
    static const char * getName() { return "none"; }

    PostingListCodecNone() : IPostingListCodec(Type::None) {}

    void encode(const PostingList &, size_t, TokenPostingsInfo &, WriteBuffer &) const override {}
    void decode(ReadBuffer &, PostingList &) const override {}
};

#if USE_FASTPFOR

/// FastPFor (Fast Patched Frame-of-Reference) codec for posting list compression.
///
/// A high-performance integer compression algorithm that combines:
/// - Frame-of-Reference (FOR): stores values as offsets from a reference
/// - Patching: handles outliers separately to maintain compression efficiency
/// - SIMD acceleration: uses SSE/AVX on x86_64, SIMDE on ARM64
///
/// Characteristics:
/// - Compression ratio: High (typically 3-5x for posting lists)
/// - Encode speed: Fast
/// - Decode speed: Very fast (SIMD-optimized)
/// - Best for: General-purpose posting lists with varying delta distributions
///
/// Uses CompositeCodec with VariableByte for remainder handling (values not fitting in a full block).
using PostingListCodecFastPFor = PostingListCodecGeneric<SIMDFastPForBlockCodec, IPostingListCodec::Type::FastPFor>;

/// SIMD Binary Packing codec for posting list compression.
///
/// A simple but extremely fast bit-packing algorithm with SIMD optimization.
/// Packs integers using the minimum bits needed for the maximum value in each block.
///
/// Characteristics:
/// - Compression ratio: Medium (depends heavily on value distribution)
/// - Encode speed: Very fast
/// - Decode speed: Fastest among all codecs (simple bit unpacking with SIMD)
/// - Best for: Speed-critical query scenarios where decode latency matters most
///
/// Trade-off: Sacrifices some compression ratio for maximum decode throughput.
using PostingListCodecBinaryPacking = PostingListCodecGeneric<SIMDBinaryPackingBlockCodec, IPostingListCodec::Type::BinaryPacking>;

/// Simple8b codec for posting list compression.
///
/// Packs multiple small integers into 64-bit words using a selector-based scheme.
/// Each 64-bit word contains a 4-bit selector (indicating how many integers and their bit-width)
/// followed by 60 bits of packed data.
///
/// Characteristics:
/// - Compression ratio: High for small deltas (can pack up to 60 1-bit values per word)
/// - Encode speed: Medium (selector computation overhead)
/// - Decode speed: Medium (selector-based branching)
/// - Best for: Posting lists with many consecutive small deltas (dense documents)
///
/// Template parameter `true` enables RLE (Run-Length Encoding) optimization for repeated values.
using PostingListCodecSimple8b = PostingListCodecGeneric<Simple8bBlockCodec, IPostingListCodec::Type::Simple8b>;

/// StreamVByte codec for posting list compression.
///
/// A byte-aligned variable-byte encoding with SIMD-accelerated decoding.
/// Separates control bytes (describing value lengths) from data bytes,
/// enabling vectorized parallel decoding of multiple integers.
///
/// Characteristics:
/// - Compression ratio: Lower than bit-packing methods (byte-aligned)
/// - Encode speed: Fast
/// - Decode speed: Very fast with good cache behavior (sequential memory access)
/// - Best for: Streaming scenarios, random access patterns, or when byte-alignment is preferred
///
/// Advantage: Predictable performance regardless of data distribution.
using PostingListCodecStreamVByte = PostingListCodecGeneric<StreamVByteBlockCodec, IPostingListCodec::Type::StreamVByte>;

/// OptPFor (Optimized Patched Frame-of-Reference) codec for posting list compression.
///
/// An enhanced version of PFor that optimizes exception (outlier) handling.
/// Uses a more sophisticated patching scheme to achieve better compression
/// while maintaining fast decode speed.
///
/// Characteristics:
/// - Compression ratio: Highest among all variants (best for storage efficiency)
/// - Encode speed: Slower (more complex optimization during encoding)
/// - Decode speed: Fast (slightly slower than FastPFor due to patch handling)
/// - Best for: Storage-constrained scenarios where compression ratio is paramount
///
/// Trade-off: Better compression at the cost of slower encoding and slightly slower decoding.
using PostingListCodecOptPFor = PostingListCodecGeneric<SIMDOptPForBlockCodec, IPostingListCodec::Type::OptPFor>;

#endif // USE_FASTPFOR

}

