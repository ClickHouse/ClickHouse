#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/IntegerCodecTrait.h>

namespace DB
{
/// PostingListCodecImpl
///
/// A serializer/deserializer for a postings list (sorted docIDs) stored in a compact
/// block-compressed format.
///
/// High-level idea:
/// - Input docIDs are strictly increasing.
/// - Values are encoded as deltas (gaps) and compressed in fixed-size blocks (kBlockSize).
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
    struct ContainerHeader
    {
        ContainerHeader() = default;
        explicit ContainerHeader(size_t bytes_, uint32_t cardinality_, uint32_t base_value_)
            : bytes(bytes_)
            , cardinality(cardinality_)
            , base_value(base_value_)
        {
        }

        template<typename Out>
        void encodeTo(Out & out) const
        {
            writeVarUInt(bytes, out);
            writeVarUInt(cardinality, out);
            writeVarUInt(base_value, out);
        }

        template<typename In>
        void decodeFrom(In & in)
        {
            readOneField(bytes, in);
            readOneField(cardinality, in);
            readOneField(base_value, in);
        }

        template <typename F, typename In>
        static void readOneField(F & out, In & in)
        {
            UInt64 v = 0;
            readVarUInt(v, in);
            out = static_cast<F>(v);
        }

        size_t bytes = 0;
        uint32_t cardinality = 0;
        uint32_t base_value = 0;
    };

    /// In-memory descriptor of one segment inside `compressed_data`.
    ///
    /// - count: number of postings in this segment
    /// - compressed_data_offset: start offset in `compressed_data`
    /// - compressed_data_size: payload size in bytes (excluding ContainerHeader)
    /// - min/max: docID range covered by this segment.
    struct SegmentDesc
    {
        uint32_t count = 0;
        size_t compressed_data_offset = 0;
        size_t compressed_data_size = 0;
        uint32_t min;
        uint32_t max;
        SegmentDesc() = default;
    };
public:
    static constexpr size_t kBlockSize = BlockCodec::kBlockSize;
    PostingListCodecImpl() = default;
    explicit PostingListCodecImpl(size_t postings_list_block_size)
        : posting_list_block_size((postings_list_block_size + kBlockSize - 1) & ~(kBlockSize - 1))
    {
        compressed_data.reserve(kBlockSize);
        current.reserve(kBlockSize);
    }

    size_t size() const { return cardinality; }
    bool empty() const { return cardinality == 0; }

    /// Add a single increasing docID.
    ///
    /// Internally we store deltas (gaps) in `current` until reaching kBlockSize,
    /// then compress the full block into `compressed_data`.
    /// When the segment reaches `posting_list_block_size`, flush it.
    void add(uint32_t value)
    {
        if (total_in_current_segment == 0)
        {
            segments.emplace_back();
            segments.back().min = value;
            segments.back().compressed_data_offset = compressed_data.size();

            prev_value = value;
            current.emplace_back(value - prev_value);
            ++total_in_current_segment;
            ++cardinality;
            return;
        }

        current.emplace_back(value - prev_value);
        prev_value = value;
        ++total_in_current_segment;
        ++cardinality;

        if (current.size() == kBlockSize)
            compressBlock(current);

        if (total_in_current_segment == posting_list_block_size)
            flushSegment();
    }

    /// Add exactly one full block of kBlockSize values.
    ///
    /// This path assumes:
    /// - values.size() == kBlockSize
    /// - total is aligned by kBlockSize
    ///
    /// It computes deltas in-place using adjacent_difference for better throughput.
    void addBatch(std::span<uint32_t> values)
    {
        chassert(values.size() == kBlockSize && total_in_current_segment % kBlockSize == 0);
        if (total_in_current_segment == 0)
        {
            segments.emplace_back();
            segments.back().min = values.front();
            segments.back().compressed_data_offset = compressed_data.size();
            prev_value = values.front();
            total_in_current_segment += kBlockSize;
            cardinality += kBlockSize;
        }

        auto last = values.back();
        std::adjacent_difference(values.begin(), values.end(), values.begin());
        values[0] -= prev_value;
        prev_value = last;

        compressBlock(values);

        if (total_in_current_segment == posting_list_block_size)
            flushSegment();
    }

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
    ///   ContainerHeader + [compressed bytes]
    ///
    /// Decompression restores delta values and then performs an inclusive scan
    /// to reconstruct absolute docIDs.
    void deserialize(ReadBuffer & in, PostingList & out)
    {
        ContainerHeader header;
        header.decodeFrom(in);
        prev_value = header.base_value;

        uint32_t tail_block_size = header.cardinality % kBlockSize;
        uint32_t full_block_count = header.cardinality / kBlockSize;
        current.reserve(kBlockSize);
        if (header.bytes > (compressed_data.capacity() - compressed_data.size()))
            compressed_data.reserve(compressed_data.size() + header.bytes);
        compressed_data.resize(header.bytes);
        in.readStrict(compressed_data.data(), header.bytes);

        auto * p = reinterpret_cast<unsigned char *>(compressed_data.data());
        for (uint32_t i = 0; i < full_block_count; i++)
        {
            decodeOneBlock(p, kBlockSize, prev_value, current);
            out.addMany(current.size(), current.data());
        }
        if (tail_block_size)
        {
            decodeOneBlock(p, tail_block_size, prev_value, current);
            out.addMany(current.size(), current.data());
        }
    }
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
    /// - ranges: [min,max] docID range for each segment
    void serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const
    {
        for (const auto & desc : segments)
        {
            info.offsets.emplace_back(out.count());
            info.ranges.emplace_back(desc.min, desc.max);
            ContainerHeader header(desc.compressed_data_size, desc.count, desc.min);
            header.encodeTo(out);
            out.write(compressed_data.data() + desc.compressed_data_offset, desc.compressed_data_size);
        }
    }
    /// Compress one block of delta values and append it to `compressed_data`.
    ///
    /// Block layout:
    ///   [1 byte bits][payload]
    ///
    /// - bits: max bit-width among deltas in this block
    /// - payload: Codec::encode(...) bitpacked bytes
    ///
    /// Also updates current segment metadata (count, max, payload size).
    void compressBlock(std::span<uint32_t> segment)
    {
        segments.back().count += segment.size();
        segments.back().max = prev_value;

        auto [cap, bits] = BlockCodec::evaluateSizeAndMaxBits(segment.data(), segment.size());
        size_t memory_gap = compressed_data.capacity() - compressed_data.size();
        if (cap + 1 > memory_gap)
        {
            auto min_need = cap + 1 - memory_gap;
            compressed_data.reserve(compressed_data.size() + 2 * min_need);
        }
        /// Block Layout: [1byte(bits)][payload]
        size_t offset = compressed_data.size();
        compressed_data.resize(compressed_data.size() + cap + 1);
        auto * p = reinterpret_cast<unsigned char *>(compressed_data.data() + offset);

        encodeU8(bits, p);
        auto used = BlockCodec::encode(segment.data(), segment.size(), bits, p);
        chassert(used == cap);

        segments.back().compressed_data_size = compressed_data.size() - segments.back().compressed_data_offset;
        current.clear();
    }

    /// Decode one compressed block into `current` and reconstruct absolute docIDs.
    ///
    /// - Reads bits-width byte
    /// - Codec::decode fills `current` with delta values
    /// - inclusive_scan converts deltas -> docIDs using `prev_value` as initial prefix
    /// - Updates prev_value to the last decoded docID
    static void decodeOneBlock(unsigned char * & in, size_t count, uint32_t & prev_value, std::vector<uint32_t> & current)
    {
        uint8_t bits = decodeU8(in);
        current.resize(count);

        /// Decode postings to buffer named temp.
        BlockCodec::decode(in, count, bits, current.data());

        /// Restore the original array from the decompressed delta values.
        std::inclusive_scan(current.begin(), current.end(), current.begin(), std::plus<uint32_t>{}, prev_value);
        prev_value = current.empty() ? prev_value : current.back();
    }

    ALWAYS_INLINE static void encodeU8(uint8_t x, unsigned char * & out)
    {
        *out++ = static_cast<char>(x);
    }

    ALWAYS_INLINE static uint8_t decodeU8(unsigned char * & in)
    {
        return *in++;
    }

    std::string compressed_data;
    uint32_t prev_value = {};

    /// Number of values added in the current segment.
    size_t total_in_current_segment = 0;
    std::vector<uint32_t> current;
    size_t posting_list_block_size = 1024 * 1024;
    std::vector<SegmentDesc> segments;

    /// Total number of postings added across all segments.
    size_t cardinality = 0;
};

void PostingListCodec::decode(ReadBuffer & in, PostingList & postings)
{
    PostingListCodecImpl impl;
    impl.deserialize(in, postings);
}

void PostingListCodec::encode(const PostingListBuilder & postings, size_t posting_list_block_size, TokenPostingsInfo & info, WriteBuffer & out)
{
    PostingListCodecImpl impl(posting_list_block_size);
    if (postings.isSmall())
    {
        const auto & small = postings.getSmall();
        size_t small_size = postings.size();
        for (size_t i = 0; i < small_size; ++i)
            impl.add(small[i]);
    }
    else
    {
        std::vector<uint32_t> posting_values;
        posting_values.resize(postings.size());
        const auto & large = postings.getLarge();
        large.toUint32Array(posting_values.data());
        std::span<uint32_t> values(posting_values.data(), posting_values.size());

        auto block_size = PostingListCodecImpl::kBlockSize;
        while (values.size() >= block_size)
        {
            auto front = values.first(block_size);
            impl.addBatch(front);
            values = values.subspan(block_size);
        }
        if (!values.empty())
        {
            for (auto v : values)
                impl.add(v);
        }
    }
    impl.serialize(out, info);
}
}
