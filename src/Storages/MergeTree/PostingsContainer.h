#pragma once
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/IntegerCodecTrait.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <roaring.hh>

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
    static_assert(std::is_same_v<T, uint32_t>, "PostingsContainer only supports uint32_t");
    static constexpr size_t kBlockSize = CodecTraits<T>::kBlockSize;
    struct BlockHeader
    {
        uint16_t count = 0;
        uint16_t max_bits = 0;
        uint32_t bytes = 0;
    };
    struct ContainerHeader
    {
        T base_value {};
        uint32_t block_count = 0;
        uint32_t bytes = 0;
        uint32_t size = 0;
    };
public:
    explicit PostingsContainerImpl() = default;

    size_t size() const { return header.size; }
    bool empty() const { return header.size == 0; }

    void add(T value)
    {
        if (current.size() == kBlockSize)
        {
            compressBlock(current, temp_compression_data_buffer);
            current.reserve(kBlockSize);
            current.clear();
        }
        /// Delta computation is intentionally deferred
        /// and will be applied later as part of the block compression step.
        current.emplace_back(value);
        ++header.size;
    }

    /// Serializes posting list to a WriteBuffer-like output.
    template<typename Out>
    size_t serialize(Out & out)
    {
        if (!current.empty())
            compressBlock(current, temp_compression_data_buffer);
        header.bytes = compressed_data.size();
        return serializeTo(out);
    }

    /// Reads postings data back from an Input buffer (ReadBuffer).
    template<typename In, typename Container>
    void deserialize(In & in, Container & out)
    {
        deserializeFrom(in);
        prev_value = header.base_value;
        std::string temp_buffer;
        std::vector<T> temp_compress_buffer;
        temp_compress_buffer.reserve(PostingsContainerImpl<T>::kBlockSize);
        ReadBufferFromMemory data_buffer(compressed_data);
        for (size_t i = 0; i < static_cast<size_t>(header.block_count); ++i)
            decompressBlock(data_buffer, temp_buffer, temp_compress_buffer, [&out] (std::vector<int32_t> & temp) { out.addMany(temp.size(), temp.data()); });
    }

    size_t getSizeInBytes() const { return compressed_data.size(); }
private:
    template<typename Out>
    size_t serializeTo(Out & out) const
    {
        auto offset = out.offset();
        writeContainerHeader(header, out);
        out.write(compressed_data.data(), compressed_data.size());
        return out.offset() - offset;
    }

    template<typename In>
    void deserializeFrom(In & in)
    {
        readContainerHeader(header, in);
        compressed_data.resize(header.bytes);
        in.readStrict(compressed_data.data(), header.bytes);
    }

    void compressBlock(std::vector<T> & segment, std::string & temp_compression_data)
    {
        if (header.block_count == 0)
        {
            header.base_value = segment.front();
            prev_value = header.base_value;
        }
        ++header.block_count;
        /// delta[0] = v[0] - prev_value
        /// delta[i] = v[i] - v[i-1]（i>=1）
        std::adjacent_difference(segment.begin(), segment.end(), segment.begin());
        segment.front() -= prev_value;
        prev_value = segment.back();
        auto [cap, bits] = CodecTraits<T>::evaluateSizeAndMaxBits(segment.data(), segment.size());
        temp_compression_data.resize(cap);
        auto bytes = CodecTraits<T>::encode(segment.data(), segment.size(), bits, reinterpret_cast<unsigned char*>(temp_compression_data.data()));
        BlockHeader block_header { static_cast<uint16_t>(segment.size()), static_cast<uint16_t>(bits), static_cast<uint32_t>(bytes) };
        WriteBufferFromString compressed_buffer(compressed_data, AppendModeTag {});
        writeBlockHeader(block_header, compressed_buffer);
        compressed_buffer.write(temp_compression_data.data(), bytes);
        compressed_buffer.finalize();
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
        writeVarUInt(header.bytes, out);
        writeVarUInt(header.size, out);
        writeVarUInt(header.base_value, out);
    }

    template<typename In>
    static void readContainerHeader(ContainerHeader & header, In & in)
    {
       readOneField(header.block_count, in);
       readOneField(header.bytes, in);
       readOneField(header.size, in);
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
        /// Decode block header.
        BlockHeader block_header;
        readBlockHeader(block_header, in);
        temp.resize(block_header.count);
        temp_buffer.resize(block_header.bytes);
        in.readStrict(temp_buffer.data(), block_header.bytes);
        /// Decode postings
        unsigned char * p = reinterpret_cast<unsigned char *>(temp_buffer.data());
        auto used = CodecTraits<T>::decode(p, block_header.count, block_header.max_bits, temp.data());
        if (used != block_header.bytes)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Compressed and decompressed byte counts do not match. compressed = {}, decompressed = {}", block_header.bytes, used);
        chassert(block_header.count == temp.size());
        std::inclusive_scan(temp.begin(), temp.end(), temp.begin(), std::plus<T>{}, prev_value);
        prev_value = temp.empty() ? prev_value : temp.back();
        consumer(temp);
    }
    ContainerHeader header;
    std::string compressed_data;
    std::string temp_compression_data_buffer;
    T prev_value;
    std::vector<T> current;
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
