#pragma once
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/IntegerCodecTrait.h>
#include <Storages/MergeTree/MergeTreeIndexTextCommon.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <roaring/roaring.hh>


namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

/// A compact, block-based container for storing monotonically increasing
/// postings (document IDs or row offsets) with TurboPFor compression.
///
/// This templated class supports both 32-bit and 64-bit integer types.
/// Postings are accumulated in memory and compressed in fixed-size blocks
/// (`kBlockSize = 128`) using TurboPFor’s PForDelta codec
/// (`p4nd1enc32` / `p4nd1enc64`). Each compressed block is prefixed by a
/// small variable-length header that stores the number of elements and the
/// compressed byte length, both encoded as VarUInt values.
///
/// Data Layout
///
/// The serialized byte stream layout is as follows:
///
///   +--------------------------------------------------------------------+
///   | VarUInt(block_count) | VarUInt(total_elems) | VarUInt(bytes_total) |
///   | [BlockHeader][CompressedBlock] ... [BlockHeader][CompressedBlock]  |
///   +--------------------------------------------------------------------+
///
/// Each `[BlockHeader]` is encoded using VarUInt values:
///   - VarUInt(n)      : number of elements in the block
///   - VarUInt(bytes)  : number of compressed bytes that follow
///
/// The `[CompressedBlock]` contains TurboPFor-encoded integer deltas.
///
/// Compression
///
/// When `kBlockSize` values have been collected, the block is delta-encoded
/// (using the “delta − 1” scheme for strictly increasing sequences) and
/// compressed with the TurboPFor codec. Remaining values in `current` are
/// automatically flushed during serialization.


/// PostingsContainer — Writable postings container for in-memory build mode.
/// Accepts monotonically increasing uint32_t values, compresses them in
/// fixed-size blocks, and supports serialization and lazy iteration.
/// Used during index building or in-memory caching.
template<typename T>
class PostingsContainerImpl
{
    static_assert(std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>, "PostingsContainer only supports uint32_t and uint64_t");
    static constexpr size_t kBlockSize = CodecTraits<T>::kBlockSize;
    struct BlockHeader
    {
        BlockHeader() = default;
        explicit  BlockHeader(uint16_t count_, uint16_t max_bits_, uint32_t bytes_)
            : count(count_)
            , max_bits(max_bits_)
            , bytes(bytes_)
        {
        }
        uint16_t count = 0;
        uint16_t max_bits = 0;
        uint32_t bytes = 0;
    };
    struct ContainerHeader
    {
        ContainerHeader() = default;
        explicit ContainerHeader(T base_value_, uint32_t block_count_)
            : base_value(base_value_)
            , block_count(block_count_)
        {
        }
        T base_value {};
        uint32_t block_count = 0;
    };
    struct SegmentDesc
    {
        ContainerHeader header;
        size_t compressed_data_offset = 0;
        size_t compressed_data_size = 0;
        T last_value;
        SegmentDesc() = default;
    };
public:
    PostingsContainerImpl() = default;
    explicit PostingsContainerImpl(size_t postings_list_block_size)
        : postings_list_segment_size(postings_list_block_size)
    {
        current.reserve(kBlockSize);
    }

    size_t size() const { return cardinality; }
    bool empty() const { return cardinality == 0; }

    ALWAYS_INLINE void add(T value)
    {
        if (total == postings_list_segment_size)
        {
            flushSegment();
        }
        if (total == 0)
        {
            segments.emplace_back();
            segments.back().header.base_value = value;
            segments.back().compressed_data_offset = compressed_data.size();

            prev_value = value;
            current.emplace_back(value - prev_value);
            ++total;
            ++cardinality;
            return;
        }
        if (current.size() == kBlockSize)
        {
            compressBlock(current, temp_compression_data_buffer);
            current.clear();
        }
        /// Delta computation is intentionally deferred
        /// and will be applied later as part of the block compression step.
        current.emplace_back(value - prev_value);
        prev_value = value;
        ++total;
        ++cardinality;
    }

    /// Serializes posting list to a WriteBuffer-like output.
    template<typename Out>
    size_t serialize(Out & out, TokenPostingsInfo & info)
    {
        if (!current.empty())
            compressBlock(current, temp_compression_data_buffer);

        return serializeTo(out, info);
    }

    /// Reads postings data back from an Input buffer (ReadBuffer).
    template<typename In, typename Container>
    void deserialize(In & in, Container & out)
    {
        ContainerHeader header;
        deserializeFrom(in, header);
        prev_value = header.base_value;
        std::string temp_buffer;
        std::vector<T> temp_compress_buffer;
        temp_compress_buffer.reserve(kBlockSize);
        for (size_t i = 0; i < static_cast<size_t>(header.block_count); ++i)
            decompressBlock(in, temp_buffer, temp_compress_buffer, [&out] (auto & temp) { out.addMany(temp.size(), temp.data()); });
    }

    size_t getSizeInBytes() const { return compressed_data.size(); }
private:
    void reset()
    {
        current.clear();
        total = 0;
        prev_value = {};
    }

    void flushSegment()
    {
       chassert(total <= postings_list_segment_size);
       if (!current.empty())
           compressBlock(current, temp_compression_data_buffer);
        reset();
    }
    template<typename Out>
    size_t serializeTo(Out & out, TokenPostingsInfo & info) const
    {
        auto offset = out.count();
        for (auto & segment_desc : segments)
        {
            info.offsets.emplace_back(out.count());
            info.ranges.emplace_back(segment_desc.header.base_value, segment_desc.last_value);
            writeContainerHeader(segment_desc.header, out);
            out.write(compressed_data.data() + segment_desc.compressed_data_offset, segment_desc.compressed_data_size);
        }
        return out.count() - offset;
    }

    template<typename In>
    void deserializeFrom(In & in, ContainerHeader & header)
    {
        readContainerHeader(header, in);
    }

    void compressBlock(std::vector<T> & segment, std::string & temp_compression_data)
    {
        ++segments.back().header.block_count;
        segments.back().last_value = prev_value;

        /// Delta-encode this segment with a running base (prev_value),
        /// so it can be compressed efficiently and chained across segments.
        /// std::adjacent_difference(segment.begin(), segment.end(), segment.begin());
        /// segment.front() -= prev_value;
        /// prev_value = segment.back();
        auto [cap, bits] = CodecTraits<T>::evaluateSizeAndMaxBits(segment.data(), segment.size());
        temp_compression_data.resize(cap);
        auto bytes = CodecTraits<T>::encode(segment.data(), segment.size(), bits, reinterpret_cast<unsigned char*>(temp_compression_data.data()));

        auto f1 = compressed_data.size();
        ///	Write the BlockHeader followed by the compressed posting list data.
        BlockHeader block_header { static_cast<uint16_t>(segment.size()), static_cast<uint16_t>(bits), static_cast<uint32_t>(bytes) };
        WriteBufferFromString compressed_buffer(compressed_data, AppendModeTag {});
        writeBlockHeader(block_header, compressed_buffer);
        compressed_buffer.write(temp_compression_data.data(), bytes);
        compressed_buffer.finalize();
        auto f2 = compressed_data.size();
        segments.back().compressed_data_size = compressed_data.size() - segments.back().compressed_data_offset;
        (void) f1;
        (void) f2;
    }

    template<typename Out>
    static void writeBlockHeader(BlockHeader header, Out & out)
    {
        writeVarUInt(header.count, out);
        writeVarUInt(header.bytes, out);
        writeVarUInt(header.max_bits, out);
    }

    template <typename F, typename In>
    static void readOneField(F & out, In & in)
    {
        UInt64 v = 0;
        readVarUInt(v, in);
        out = static_cast<F>(v);
    }

    template<typename In>
    static void readBlockHeader(BlockHeader & header, In & in)
    {
        readOneField(header.count, in);
        readOneField(header.bytes, in);
        readOneField(header.max_bits, in);
    }

    template<typename Out>
    static void writeContainerHeader(ContainerHeader header, Out & out)
    {
        writeVarUInt(header.block_count, out);
        writeVarUInt(header.base_value, out);
    }

    template<typename In>
    static void readContainerHeader(ContainerHeader & header, In & in)
    {
       readOneField(header.block_count, in);
       readOneField(header.base_value, in);
    }

    static void decodeBlock(unsigned char *src, uint16_t n, uint32_t max_bits, std::vector<T> & out, uint32_t bytes_expected)
    {
        out.resize(n);
        size_t used = CodecTraits<T>::decode(src, n, max_bits, out.data());
        if (used != bytes_expected)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "compressed/decompressed mismatch");
    }

    template<typename In, typename Consumer>
    void decompressBlock(In & in, std::string & temp_buffer, std::vector<T> & temp, Consumer &&consumer)
    {
        /// Decode block header and read the compressed posting list data.
        BlockHeader block_header;
        readBlockHeader(block_header, in);
        temp.resize(block_header.count);
        temp_buffer.resize(block_header.bytes);
        in.readStrict(temp_buffer.data(), block_header.bytes);

        /// Decode postings to buffer named temp.
        unsigned char * p = reinterpret_cast<unsigned char *>(temp_buffer.data());
        auto used = CodecTraits<T>::decode(p, block_header.count, block_header.max_bits, temp.data());
        if (used != block_header.bytes)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Compressed and decompressed byte counts do not match. compressed = {}, decompressed = {}", block_header.bytes, used);
        chassert(block_header.count == temp.size());

        /// Restore the original array from the decompressed delta values.
        std::inclusive_scan(temp.begin(), temp.end(), temp.begin(), std::plus<T>{}, prev_value);
        prev_value = temp.empty() ? prev_value : temp.back();
        consumer(temp);
    }
    std::string compressed_data;
    std::string temp_compression_data_buffer;
    T prev_value = {};
    size_t total = 0;
    std::vector<T> current;
    size_t postings_list_segment_size = 0;
    std::vector<SegmentDesc> segments;
    size_t cardinality = 0;
};

using PostingsContainer32 = PostingsContainerImpl<uint32_t>;
using PostingList = roaring::Roaring;

template<typename In>
static void deserializePostings(In & in, PostingList & postings)
{
    PostingsContainer32 pc;
    pc.deserialize(in, postings);
}

template<typename Out>
size_t serializePostings(Out & out, const PostingList & in)
{
    std::vector<uint32_t> postings_data;
    postings_data.resize(in.cardinality());
    in.toUint32Array(postings_data.data());
    PostingsContainer32 pc;
    return pc.serialize(postings_data, out);
}

template<typename Out, size_t N>
size_t serializePostings(Out & out, const std::array<uint32_t, N> & small, size_t size)
{
    chassert(size <= N);
    std::vector<uint32_t> postings_data(small.begin(), small.begin() + size);
    PostingsContainer32 pc;
    return pc.serialize(postings_data, out);
}
}
