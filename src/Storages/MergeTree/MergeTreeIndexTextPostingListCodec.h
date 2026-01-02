#pragma once

#include <config.h>

#if USE_SIMDCOMP

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <roaring/roaring.hh>

extern "C"
{
#include <simdcomp.h>
}
namespace DB
{
struct TokenPostingsInfo;
class WriteBuffer;
class ReadBuffer;
using PostingList = roaring::Roaring;
struct PostingListBuilder;


/// PostingListCodecImpl
///
/// A serializer/deserializer for a postings list (sorted docIDs) stored in a compact
/// block-compressed format.
///
/// High-level idea:
/// - Input docIDs are strictly increasing.
/// - Values are encoded as deltas (gaps) and compressed in fixed-size blocks (BLOCK_SIZE).
/// - Each compressed block is stored as: [1 byte bits-width][bitpacked payload].
/// - A posting list may be split into "segments" (logical chunks, controlled by postings_list_block_size)
///   to simplify metadata and to support multiple ranges per token (min/max docID per segment).
///
class PostingListCodecImpl
{
    /// Per-segment header written before each segment payload.
    ///
    /// - bytes: number of compressed bytes following this header (segment payload size)
    /// - cardinality: number of postings (docIDs) in this segment
    /// - base_value: the first docID in the segment (also used to restore deltas)
    struct Header
    {
        Header() = default;
        explicit Header(uint16_t codec_type_, size_t bytes_, uint32_t cardinality_, uint32_t base_value_)
            : codec_type(codec_type_)
            , bytes(bytes_)
            , cardinality(cardinality_)
            , base_value(base_value_)
        {
        }

        void write(WriteBuffer & out) const
        {
            writeVarUInt(codec_type, out);
            writeVarUInt(bytes, out);
            writeVarUInt(cardinality, out);
            writeVarUInt(base_value, out);
        }

        void read(ReadBuffer & in)
        {
            UInt64 v = 0;
            readVarUInt(v, in);
            codec_type = static_cast<uint16_t>(v);

            readVarUInt(v, in);
            bytes = static_cast<size_t>(v);

            readVarUInt(v, in);
            cardinality = static_cast<uint32_t>(v);

            readVarUInt(v, in);
            base_value = static_cast<uint32_t>(v);
        }

        uint8_t codec_type = 0;
        size_t bytes = 0;
        uint32_t cardinality = 0;
        uint32_t base_value = 0;
    };

    /// In-memory descriptor of one segment inside `compressed_data`.
    ///
    /// - cardinality: number of postings in this segment
    /// - compressed_data_offset: start offset in `compressed_data`
    /// - compressed_data_size: payload size in bytes (excluding Header)
    /// - min/max: rowid range covered by this segment.
    struct SegmentDescriptor
    {
        uint32_t cardinality = 0;
        size_t compressed_data_offset = 0;
        size_t compressed_data_size = 0;
        uint32_t row_offset_begin = 0;
        uint32_t row_offset_end = 0;

        SegmentDescriptor() = default;
    };

    /// BlockCodec used by PostingListCodecImpl to compress/decompress arrays of
    /// unsigned integers (typically delta/gap values).
    struct BlockCodec
    {
        static constexpr size_t BLOCK_SIZE = 128;

        /// Returns {compressed_bytes, bits} where bits is the max bit-width required
        /// to represent all values in [0..n).
        ALWAYS_INLINE static std::pair<size_t, size_t> evaluateSizeAndMaxBits(std::span<uint32_t> & data)
        {
            size_t n = data.size();
            auto bits = maxbits_length(data.data(), n);
            auto bytes = simdpack_compressedbytes(n, bits);
            return {bytes, bits};
        }

        ALWAYS_INLINE static uint32_t encode(std::span<uint32_t> & in, uint32_t bits, std::span<char> & out)
        {
            /// simdcomp expects __m128i* output pointer; we compute consumed bytes
            /// from the returned end pointer (in units of 16-byte vectors).
            auto * m128_out = reinterpret_cast<__m128i *>(out.data());
            auto * m128_out_end = simdpack_length(in.data(), in.size(), m128_out, bits);
            auto used = static_cast<size_t>(m128_out_end - m128_out) * sizeof(__m128i);
            out = out.subspan(used);
            return used;
        }

        ALWAYS_INLINE static std::size_t decode(std::span<const std::byte> & in, std::size_t n, uint32_t bits, std::span<uint32_t> & out)
        {
            /// simdcomp expects __m128i* input pointer; we compute consumed bytes
            /// from the returned end pointer (in units of 16-byte vectors).
            auto * m128i_in = reinterpret_cast<const __m128i *>(in.data());
            auto * m128i_in_end = simdunpack_length(m128i_in, n, out.data(), bits);
            auto used = static_cast<size_t>(m128i_in_end - m128i_in) * sizeof(__m128);
            in = in.subspan(used);
            return used;
        }
    };
public:
    static constexpr size_t BLOCK_SIZE = 128;

    PostingListCodecImpl() = default;

    /// Normalize the requested block size to a multiple of BLOCK_SIZE.
    /// We encode/decode posting lists in fixed-size blocks, and the SIMD bit-packing
    /// implementation expects block-aligned sizes for efficient processing.
    explicit PostingListCodecImpl(size_t postings_list_block_size)
        : posting_list_block_size((postings_list_block_size + BLOCK_SIZE - 1) & ~(BLOCK_SIZE - 1))
    {
        compressed_data.reserve(BLOCK_SIZE);
        current.reserve(BLOCK_SIZE);
    }

    size_t size() const { return cardinality; }
    bool empty() const { return size(); }


    /// Add a single increasing docID.
    ///
    /// Internally we store deltas (gaps) in `current` until reaching BLOCK_SIZE,
    /// then compress the full block into `compressed_data`.
    /// When the segment reaches `posting_list_block_size`, flush it.
    void insert(uint32_t value);

    /// Add exactly one full block of BLOCK_SIZE values.
    ///
    /// This path assumes:
    /// - values.size() == BLOCK_SIZE
    /// - total is aligned by BLOCK_SIZE
    ///
    /// It computes deltas in-place using adjacent_difference for better throughput.
    void insert(std::span<uint32_t> values);

    /// Serialize all buffered postings to `out` and update TokenPostingsInfo.
    ///
    /// This flushes any pending partial block and writes per-segment headers
    /// followed by the segment payload bytes.
    void serialize(WriteBuffer & out, TokenPostingsInfo & info)
    {
        if (!current.empty())
            compressBlock(current);

        serializeTo(out, info);
    }

    /// Deserialize a postings list from input `in` into `out`.
    ///
    /// Format per segment:
    ///   Header + [compressed bytes]
    ///
    /// Decompression restores delta values and then performs an inclusive scan
    /// to reconstruct absolute docIDs.
    void deserialize(ReadBuffer & in, PostingList & out);
    void clear()
    {
        reset();
        cardinality = 0;
        compressed_data.clear();
        segments.clear();
    }
    size_t getSizeInBytes() const { return compressed_data.size(); }
private:
    void reset()
    {
        current.clear();
        total_in_current_segment = 0;
        prev_value = {};
    }

    /// Flush current segment:
    /// - compress pending partial block
    /// - reset block state so a new segment can start
    void flushSegment()
    {
       chassert(total_in_current_segment <= posting_list_block_size);
       if (!current.empty())
           compressBlock(current);

        reset();
    }

    /// Write all segments to output and fill TokenPostingsInfo:
    /// - offsets: byte offsets in output where each segment begins
    /// - ranges: [row_offset_begin,row_offset_end] rowid range for each segment
    void serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const;
    /// Compress one block of delta values and append it to `compressed_data`.
    ///
    /// Block layout:
    ///   [1 byte bits][payload]
    ///
    /// - bits: max bit-width among deltas in this block
    /// - payload: Codec::encode(...) bitpacked bytes
    ///
    /// Also updates current segment metadata (count, max, payload size).
    void compressBlock(std::span<uint32_t> segment);

    /// Decode one compressed block into `current` and reconstruct absolute docIDs.
    ///
    /// - Reads bits-width byte
    /// - Codec::decode fills `current` with delta values
    /// - inclusive_scan converts deltas -> docIDs using `prev_value` as initial prefix
    /// - Updates prev_value to the last decoded docID
    static void decodeOneBlock(std::span<const std::byte> & in, size_t count, uint32_t & prev_value, std::vector<uint32_t> & current);

    ALWAYS_INLINE static void encodeU8(uint8_t x, std::span<char> & out)
    {
        out[0] = static_cast<char>(x);
        out = out.subspan(1);
    }

    ALWAYS_INLINE static uint8_t decodeU8(std::span<const std::byte> & in)
    {
        auto v = static_cast<uint8_t>(in[0]);
        in = in.subspan(1);
        return v;
    }

    std::string compressed_data;
    uint32_t prev_value = {};

    /// Number of values added in the current segment.
    size_t total_in_current_segment = 0;
    std::vector<uint32_t> current;
    size_t posting_list_block_size = 1024 * 1024;
    std::vector<SegmentDescriptor> segments;

    /// Total number of postings added across all segments.
    size_t cardinality = 0;
    /// Used as the globally unique identifier for a codec, and it is defined in IPostingListCodec.
    IPostingListCodec::Type codec_type = IPostingListCodec::Type::Bitpacking;
};

/// Codec for serializing/deserializing a single postings list to/from a binary stream.
struct PostingListCodecSIMDComp : public  IPostingListCodec
{
    static const char * getName() { return "bitpacking"; }

    PostingListCodecSIMDComp();

    void encode(const PostingListBuilder & builder, size_t posting_list_block_size, TokenPostingsInfo & info, WriteBuffer & wb) const override;
    void decode(ReadBuffer & rb, PostingList & posting_list) const override;
};

}
#endif
