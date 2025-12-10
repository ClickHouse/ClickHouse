#pragma once
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/IntegerCodecTrait.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
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


/// Codec — Low-level encoding/decoding utility for postings blocks.
/// Provides static helpers to write/read block headers and perform
/// TurboPFor compression/decompression (p4nd1enc32 / p4nd1dec32).
/// Used by both in-memory and streaming postings sources.
struct Codec
{
    template<typename Out>
    static void writeHeader(uint16_t n, uint32_t bytes, uint32_t max_bits, Out & out)
    {
        writeVarUInt(n, out);
        writeVarUInt(bytes, out);
        writeVarUInt(max_bits, out);
    }

    template<typename Input>
    static void readHeader(uint16_t & n, uint32_t & bytes, uint32_t & max_bits, Input & in)
    {
        uint64_t v = 0;
        readVarUInt(v, in);
        n = static_cast<uint16_t>(v);

        v = 0;
        readVarUInt(v, in);
        bytes = static_cast<uint32_t>(v);

        v = 0;
        readVarUInt(v, in);
        max_bits = static_cast<uint32_t>(v);
    }
    template<typename T>
    static void decodeBlock(unsigned char *src, uint16_t n, uint32_t max_bits, std::vector<T> & out, uint32_t bytes_expected)
    {
        out.resize(n);
        size_t used = CodecTraits<T>::decode(src, n, max_bits, out.data());
        if (used != bytes_expected)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "compressed/decompressed mismatch");
    }
    template<typename T, typename Input, typename Consumer>
    static void decompressCurrent(Input & in, std::string & temp_buffer, std::vector<T> & temp, Consumer &&consumer)
    {
        /// Decode block header.
        uint16_t n = 0;
        uint32_t bytes = 0;
        uint32_t max_bits = 0;
        Codec::readHeader(n, bytes, max_bits, in);
        temp.resize(n);
        temp_buffer.resize(bytes);
        in.readStrict(temp_buffer.data(), bytes);
        /// Decode postings
        unsigned char * p = reinterpret_cast<unsigned char *>(temp_buffer.data());
        auto used = CodecTraits<T>::decode(p, n, max_bits, temp.data());
        if (used != bytes)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                            "Compressed and decompressed byte counts do not match. compressed = {}, decompressed = {}",
                            bytes, used);
        assert(n == temp.size());
        /// Fill data to container.
        consumer(temp);
    }
};

/// PostingsContainer — Writable postings container for in-memory build mode.
/// Accepts monotonically increasing uint32_t values, compresses them in
/// fixed-size blocks, and supports serialization and lazy iteration.
/// Used during index building or in-memory caching.
template<typename T>
class PostingsContainerImpl
{
    static_assert(std::is_same_v<T, uint32_t>, "PostingsContainer only supports uint32_t");
    static constexpr UInt8  kBlockSizeShift = 7;
    static constexpr size_t kBlockSize = 1 << kBlockSizeShift;
    static constexpr size_t kBlockSizeMask = kBlockSize - 1;
    template<typename In, typename OutContainer, typename U>
    friend void deserializePostings(In & in, OutContainer & out);
    template<typename Out, typename U>
    friend size_t serializePostingsImpl(Out & out, const std::vector<U> & array);
public:
    using ValueType = T;
    explicit PostingsContainerImpl() = default;

    /// Adds a new posting (document ID / row offset).
    /// The values must be strictly increasing. When the current block
    /// reaches kBlockSize elements, it is compressed and appended to `data`.
    void add(T v)
    {
        if (!current.empty() && v <= current.back())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The current value must be greater than the previous value. prev = {}, current = {}", current.back(), v);
        current.emplace_back(v);
        total++;
        if (current.size() == kBlockSize)
            compressCurrent();
    }

    size_t size() const { return total; }

    bool empty() const { return total == 0; }

    // Serializes the container to a WriteBuffer-like output.
    /// Writes VarUInt(block_count, total), VarUInt(data size), and raw bytes.
    /// Any remaining uncompressed postings are flushed before serialization.
    template<typename Output>
    size_t serialize(Output & out) const
    {
        if (!current.empty())
            const_cast<PostingsContainerImpl*>(this)->compressCurrent();
        assert(current.empty());
        auto offset = out.offset();
        writeVarUInt(block_count, out);
        writeVarUInt(total, out);
        size_t data_size = data.size();
        writeVarUInt(data.size(), out);
        out.write(data.data(), data.size());
        (void) data_size;
        return out.offset() - offset;
    }

    void flush()
    {
        if (!current.empty())
        {
            compressCurrent();
        }
    }

    /// Reads postings data back from an Input buffer (ReadBuffer).
    /// This overwrites any existing data.
    template<typename  Input>
    void deserialize(Input & in)
    {
        reset();
        readVarUInt(block_count, in);
        readVarUInt(total, in);
        size_t bytes = 0;
        readVarUInt(bytes, in);
        data.resize(bytes);
        in.readStrict(data.data(), bytes);
    }

    /// Swaps content with another PostingsContainer (no copy).
    void swap(PostingsContainerImpl & o) noexcept
    {
        using std::swap;
        swap(data, o.data);
        swap(block_count, o.block_count);
        swap(total, o.total);
        swap(current, o.current);
    }

    friend void swap(PostingsContainerImpl & lhs, PostingsContainerImpl & rhs) noexcept
    {
        lhs.swap(rhs);
    }

private:
    using InputType = ReadBufferFromString;
    struct IteratorData
    {
        InputType rb;
        const std::vector<T> & current;
        explicit IteratorData(const std::string & data_, const std::vector<T> & current_) : rb(data_), current(current_) {}
        bool empty() { return rb.eof() && current.empty(); }
    };
    void reset()
    {
        data.clear();
        current.clear();
        block_count = 0;
        total = 0;
    }
    void compressCurrent()
    {
        auto [cap, bits] = CodecTraits<T>::evaluateSizeAndMaxBits(current);
        temp_compression_data.resize(cap);
        auto bytes = CodecTraits<T>::encode(current.data(), current.size(), bits, reinterpret_cast<unsigned char*>(temp_compression_data.data()));
        WriteBufferFromString wb(data, AppendModeTag{});
        Codec::writeHeader(current.size(), bytes, bits, wb);
        wb.write(temp_compression_data.data(), bytes);
        block_count++;
        current.clear();
    }
    std::string data;
    std::vector<T> current;
    std::string temp_compression_data;
    size_t block_count = 0;
    size_t total = 0;
};

using PostingsContainer32 = PostingsContainerImpl<uint32_t>;
using PostingList = roaring::Roaring;
/// Reads, decompresses and emits postings directly to an output sink (PostingsList).
/// Used for full decode in one pass, without exposing intermediate state.
template<typename In, typename OutContainer, typename T>
static void deserializePostings(In & in, OutContainer & out)
{
    PostingsContainerImpl<T> self;
    self.deserialize(in);
    std::string temp_buffer;
    std::vector<T> temp_compress_buffer;
    temp_compress_buffer.reserve(PostingsContainerImpl<T>::kBlockSize);
    ReadBufferFromMemory data_buffer(self.data);
    for (size_t i = 0; i < self.block_count; ++i)
        Codec::decompressCurrent<T>(data_buffer, temp_buffer, temp_compress_buffer, [&out] (std::vector<uint32_t> & temp) { out.addMany(temp.size(), temp.data()); });
    if (!self.current.empty())
        out.addMany(self.current.size(), self.current.data());
}

template<typename Out, typename T>
size_t serializePostingsImpl(Out & out, const std::vector<T> & array)
{
    chassert(std::is_sorted(array.begin(), array.end()));
    PostingsContainerImpl<T> self;
    size_t i = 0;
    size_t many_size = array.size();
    while (i < many_size)
    {
        size_t write_pos = self.current.size();
        size_t can_fill = std::min(PostingsContainerImpl<T>::kBlockSize - write_pos, many_size - i);
        self.current.resize(write_pos + can_fill);
        std::copy_n(array.data() + i, can_fill, self.current.data() + write_pos);
        i += can_fill;
        self.total += can_fill;
        if (self.current.size() == PostingsContainerImpl<T>::kBlockSize)
            self.compressCurrent();
    }
    return self.serialize(out);
}

template<typename Out>
size_t serializePostings(Out & out, const PostingList & in)
{
    std::vector<uint32_t> postings_array;
    postings_array.resize(in.cardinality());
    in.toUint32Array(postings_array.data());
    return serializePostingsImpl<Out, uint32_t>(out, postings_array);
}

template<typename Out, size_t N>
size_t serializePostings(Out & out, const std::array<uint32_t, N> & small, size_t size)
{
    chassert(size <= N);
    std::vector<uint32_t> postings_array(small.begin(), small.begin() + size);
    return serializePostingsImpl<Out, uint32_t>(out, postings_array);
}
}
