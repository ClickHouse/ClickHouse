#pragma once

#include <Compression/ICompressionCodec.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Storages/MergeTree/IPostingListCodec.h>

#include <absl/container/inlined_vector.h>

namespace DB
{
struct TokenPostingsInfo;
class WriteBuffer;
class ReadBuffer;
using PostingList = roaring::Roaring;

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
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

    /// Per-segment list of packed block metadata. Tokens with up to 128 rows (the majority of
    /// promoted tokens) have exactly one packed block, stored inline at no size cost
    /// (the inline element fits in the heap representation's footprint).
    struct SegmentBlockMetas
    {
        absl::InlinedVector<PackedBlockMeta, 1> metas;
    };

public:
    PostingListCodecBitpackingImpl() = default;

    /// Add a single increasing row id.
    ///
    /// Internally we store deltas (gaps) in `current_segment` until reaching BLOCK_SIZE,
    /// then compress the full block into `compressed_data`.
    /// When the segment reaches `max_rowids_in_segment`, flush it. The segment size is passed in
    /// (rather than stored) because it is a per-index constant, the same for every token.
    /// Inline because this is the per-row hot path of the text index build; everything it touches
    /// per row sits on the first cache line of this object (see the field order below).
    ALWAYS_INLINE void insert(uint32_t row_id, size_t max_rowids_in_segment)
    {
        if (row_ids_in_current_segment == 0)
        {
            startSegment(row_id);
            return;
        }

        current_segment.emplace_back(row_id - prev_row_id);
        prev_row_id = row_id;
        ++row_ids_in_current_segment;
        ++total_row_ids;

        if (current_segment.size() == BLOCK_SIZE)
        {
            encodeBlock(current_segment);
            current_segment.clear();
        }

        if (row_ids_in_current_segment == max_rowids_in_segment)
            flushCurrentSegment();
    }

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

private:
    /// Starts a new segment with its first row id: appends a fresh segment descriptor and
    /// block-metas entry and reserves the block buffer. Cold path (once per segment).
    void startSegment(uint32_t row_id);

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

    /// Fields read and written by `insert` on every row come first, so the per-row hot path
    /// touches a single cache line of this object.
    /// Last encoded/decoded row id
    uint32_t prev_row_id = 0;
    /// Number of values added in the current segment.
    size_t row_ids_in_current_segment = 0;
    /// Total number of postings added across all segments.
    size_t total_row_ids = 0;
    /// Deltas of the current partially filled block (also reused as the decode buffer)
    std::vector<uint32_t> current_segment;
    /// All segments
    std::string compressed_data;
    /// Each segment has an in-memory descriptor. With the default `posting_list_block_size`
    /// almost every token has exactly one segment, so one element is stored inline: no heap
    /// allocation, and `back()` in `encodeBlock` reads this object instead of chasing a pointer.
    absl::InlinedVector<SegmentDescriptor, 1> segment_descriptors;
    /// Per-segment packed block metadata for V2 Index Section (one element inline, see above)
    absl::InlinedVector<SegmentBlockMetas, 1> segment_block_metas;
};

/// Streaming accumulator for the Bitpacking codec.
///
/// Wraps PostingListCodecBitpackingImpl, which encodes row ids into bit-packed 128-row blocks
/// on the fly (compressing each block as soon as it is full) and starts a new segment every
/// `posting_list_block_size` row ids. The compressed bytes are held in memory and flushed on `finalize`.
/// Marked `final` so that callers holding a concrete pointer can devirtualize `insert`
/// (the per-row hot path during the build).
class PostingListAccumulatorBitpacking final : public IPostingListAccumulator
{
public:
    /// Per-row hot state owned by the caller (see `IPostingListAccumulator` for the contract).
    /// The bitpacking codec streams deltas directly into the impl and needs none.
    struct InsertState
    {
    };

    ALWAYS_INLINE void insert(UInt32 row_id, size_t posting_list_block_size, InsertState &) { impl.insert(row_id, posting_list_block_size); }
    void finalize(WriteBuffer & out, TokenPostingsInfo & info) override;

    UInt32 cardinality() const override { return static_cast<UInt32>(impl.cardinality()); }
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

    std::unique_ptr<IPostingListAccumulator> createAccumulator() const override
    {
        return std::make_unique<PostingListAccumulatorBitpacking>();
    }

    void decode(ReadBuffer & in, PostingList & postings) const override;
};

/// Streaming accumulator for the None codec.
///
/// Accumulates row ids directly into Roaring bitmaps, starting a new segment every
/// `posting_list_block_size` row ids. Each segment is serialized on `finalize` as a portable
/// Roaring bitmap prefixed by its size in bytes (`writeVarUInt(num_bytes) + portable bytes`).
///
/// Marked `final` so that callers holding a concrete pointer can devirtualize `insert`.
class PostingListAccumulatorNone final : public IPostingListAccumulator
{
public:
    /// Per-row hot state owned by the caller (see `IPostingListAccumulator` for the contract):
    /// `bulk_context` caches the current bitmap's container, `rows_in_segment` counts row ids toward
    /// the segment boundary. Both are reset at every segment boundary. Keeping them in the caller's
    /// storage means the per-row path below touches no field of this (likely cache-cold) object:
    /// the Roaring bulk insert goes straight to the cached container.
    struct InsertState
    {
        roaring::BulkContext bulk_context;
        size_t rows_in_segment = 0;
    };

    /// The segment size is passed in (rather than stored) because it is a per-index constant.
    ALWAYS_INLINE void insert(UInt32 row_id, size_t posting_list_block_size, InsertState & state)
    {
        current_segment.addBulk(state.bulk_context, row_id);

        if (++state.rows_in_segment == posting_list_block_size)
            sealSegment(state);
    }

    void finalize(WriteBuffer & out, TokenPostingsInfo & info) override;

    /// Computed from the bitmaps (cold path) so the per-row insert needs no extra counter.
    UInt32 cardinality() const override
    {
        size_t total = current_segment.cardinality();

        for (const auto & segment : segments)
            total += segment.cardinality();

        return static_cast<UInt32>(total);
    }

    size_t memoryUsageBytes() const override;

private:
    /// Seals the current (full) segment and starts a new one. Resets the caller-owned state
    /// because the new segment is a fresh bitmap with no cached container. Cold path.
    void sealSegment(InsertState & state);

    PostingList current_segment;
    std::vector<PostingList> segments;
};

/// A posting list codec that doesn't compress.
///
/// Encoding accumulates row ids into Roaring bitmaps (see PostingListAccumulatorNone) split into
/// segments of `posting_list_block_size` row ids; each segment is serialized as a portable Roaring bitmap.
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

/// Dispatches on the codec type to the concrete accumulator type, invoking `f` with a
/// (null) pointer of that concrete type as a compile-time tag. This lets the per-row hot path
/// devirtualize `IPostingListAccumulator::insert` once the codec type is known for the whole build.
///
///     dispatchByPostingCodec(type, [&](auto * tag)
///     {
///         using Accumulator = std::remove_pointer_t<decltype(tag)>;
///         ...
///     });
template <typename F>
void dispatchByPostingCodec(IPostingListCodec::Type type, F && f)
{
    switch (type)
    {
        case IPostingListCodec::Type::None:
            f(static_cast<PostingListAccumulatorNone *>(nullptr));
            return;
        case IPostingListCodec::Type::Bitpacking:
            f(static_cast<PostingListAccumulatorBitpacking *>(nullptr));
            return;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown posting list codec type {}", static_cast<int>(type));
}

}

