#pragma once
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/IntegerCodecTrait.h>
#include <IO/ReadBufferFromMemory.h>
#include <roaring.hh>

#pragma clang optimize off
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
        uint16_t count;
        uint16_t max_bits;
        uint32_t bytes;
    };
public:
    explicit PostingsContainerImpl() = default;

    size_t size() const { return total; }
    bool empty() const { return total == 0; }

    void add(T value)
    {
        if (current.size() == kBlockSize)
        {
            std::span<const T> segment(current.begin(), current.end());
            compressBlock(segment, temp_compression_data_buffer);
            current.reserve(kBlockSize);
            current.clear();
        }
        current.emplace_back(value);
    }
    template<typename Out>
    size_t serialize(Out & out)
    {
        if (!current.empty())
            compressBlock(std::span<const T> (current.begin(), current.end()), temp_compression_data_buffer);
        return serializeTo(out);
    }
    /// Serializes the container to a WriteBuffer-like output.
    template<typename Container, typename Out>
    size_t serialize(Container & in, Out & out)
    {
        chassert(std::is_sorted(in.begin(), in.end()));
        std::span<const T> container(in.data(), in.size());
        while (!container.empty())
        {
            size_t segment_length = std::min<size_t>(container.size(), kBlockSize);
            std::span<const T> segment = container.first(segment_length);
            compressBlock(segment, temp_compression_data_buffer);
            container = container.subspan(segment_length);
        }
        return serializeTo(out);
    }

    /// Reads postings data back from an Input buffer (ReadBuffer).
    template<typename In, typename Container>
    void deserialize(In & in, Container & out)
    {
        deserializeFrom(in);
        std::string temp_buffer;
        std::vector<T> temp_compress_buffer;
        temp_compress_buffer.reserve(PostingsContainerImpl<T>::kBlockSize);
        ReadBufferFromMemory data_buffer(compressed_buffer);
        for (size_t i = 0; i < block_count; ++i)
            decompressBlock(data_buffer, temp_buffer, temp_compress_buffer, [&out] (std::vector<uint32_t> & temp) { out.addMany(temp.size(), temp.data()); });
    }

    size_t getSizeInBytes() const { return compressed_buffer.size(); }
private:
    template<typename Out>
    size_t serializeTo(Out & out) const
    {
        auto offset = out.offset();
        writeVarUInt(block_count, out);
        writeVarUInt(total, out);
        writeVarUInt(compressed_buffer.size(), out);
        out.write(compressed_buffer.data(), compressed_buffer.size());
        return out.offset() - offset;
    }

    template<typename  In>
    void deserializeFrom(In & in)
    {
        reset();
        readVarUInt(block_count, in);
        readVarUInt(total, in);
        size_t bytes = 0;
        readVarUInt(bytes, in);
        compressed_buffer.resize(bytes);
        in.readStrict(compressed_buffer.data(), bytes);
    }

    void reset()
    {
        compressed_buffer.clear();
        block_count = 0;
        total = 0;
    }
    void compressBlock(std::span<const T> segment, std::string & temp_compression_data)
    {
        auto [cap, bits] = CodecTraits<T>::evaluateSizeAndMaxBits(segment.data(), segment.size());
        temp_compression_data.resize(cap);
        auto bytes = CodecTraits<T>::encode(segment.data(), segment.size(), bits, reinterpret_cast<unsigned char*>(temp_compression_data.data()));
        WriteBufferFromString wb(compressed_buffer, AppendModeTag{});
        BlockHeader header { static_cast<uint16_t>(segment.size()), static_cast<uint16_t>(bits), static_cast<uint32_t>(bytes) };
        writeHeader(header, wb);
        wb.write(temp_compression_data.data(), bytes);
        block_count++;
        total += segment.size();
    }
    template<typename Out>
    static void writeHeader(BlockHeader header, Out & out)
    {
        writeVarUInt(header.count, out);
        writeVarUInt(header.bytes, out);
        writeVarUInt(header.max_bits, out);
    }

    template <typename F, typename In>
    static void readOneField(F & out, In & in)
    {
        uint64_t v = 0;
        readVarUInt(v, in);
        out = static_cast<F>(v);
    }

    template<typename In>
    static void readHeader(BlockHeader & header, In & in)
    {
        readOneField(header.count, in);
        readOneField(header.bytes, in);
        readOneField(header.max_bits, in);
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
        BlockHeader header;
        readHeader(header, in);
        temp.resize(header.count);
        temp_buffer.resize(header.bytes);
        in.readStrict(temp_buffer.data(), header.bytes);
        /// Decode postings
        unsigned char * p = reinterpret_cast<unsigned char *>(temp_buffer.data());
        auto used = CodecTraits<T>::decode(p, header.count, header.max_bits, temp.data());
        if (used != header.bytes)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Compressed and decompressed byte counts do not match. compressed = {}, decompressed = {}", header.bytes, used);
        chassert(header.count == temp.size());
        consumer(temp);
    }
    std::string compressed_buffer;
    std::string temp_compression_data_buffer;
    std::vector<T> current;
    size_t block_count = 0;
    size_t total = 0;
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
