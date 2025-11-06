#pragma once
#include <Common/Exception.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
extern "C" {
#include <ic.h>
}

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
namespace internal
{
template <typename T>
struct CodecTraits;

template <>
struct CodecTraits<uint32_t>
{
    ALWAYS_INLINE static uint32_t bound(std::size_t n)
    {
        return p4nbound256v32(static_cast<uint32_t>(n));
    }

    ALWAYS_INLINE static uint32_t encode(uint32_t * p, std::size_t n, unsigned char *out)
    {
        return p4nd1enc32(p, n, out);
    }

    ALWAYS_INLINE static std::size_t decode(unsigned char * p, std::size_t n, uint32_t *out)
    {
        return p4nd1dec32(p, n, out);
    }
};

template <>
struct CodecTraits<uint64_t>
{
    ALWAYS_INLINE static uint32_t bound(std::size_t n)
    {
        return p4nbound256v32(static_cast<uint32_t>(n));
    }

    ALWAYS_INLINE static uint64_t encode(uint64_t * p, std::size_t n, unsigned char *out)
    {
        return p4nd1enc64(p, n, out);
    }

    ALWAYS_INLINE static std::size_t decode(unsigned char * p, std::size_t n, uint64_t *out)
    {
        return p4nd1dec64(p, n, out);
    }
};

/// Codec — Low-level encoding/decoding utility for postings blocks.
/// Provides static helpers to write/read block headers and perform
/// TurboPFor compression/decompression (p4nd1enc32 / p4nd1dec32).
/// Used by both in-memory and streaming postings sources.
struct Codec
{
    template<typename Out>
    static void writeHeader(uint16_t n, uint32_t bytes, Out & out)
    {
        writeVarUInt(n, out);
        writeVarUInt(bytes, out);
    }

    template<typename Input>
    static void readHeader(uint16_t & n, uint32_t & bytes, Input & in)
    {
        uint64_t v = 0;
        readVarUInt(v, in);
        n = static_cast<uint16_t>(v);

        v = 0;
        readVarUInt(v, in);
        bytes = static_cast<uint32_t>(v);
    }
    template<typename T>
    static void decodeBlock(unsigned char *src, uint16_t n, std::vector<T> & out, uint32_t bytes_expected)
    {
        out.resize(n);
        size_t used = CodecTraits<T>::decode(src, n, out.data());
        if (used != bytes_expected)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "compressed/decompressed mismatch");
    }
    template<typename T, typename Input, typename Consumer>
    static void decompressCurrent(Input & in, std::string & temp_buffer, std::vector<T> & temp, Consumer &&consumer)
    {
        /// Decode block header.
        uint16_t n = 0;
        uint32_t bytes = 0;
        Codec::readHeader(n, bytes, in);
        temp.resize(n);
        temp_buffer.resize(bytes);
        in.readStrict(temp_buffer.data(), bytes);
        /// Decode postings
        unsigned char *p = reinterpret_cast<unsigned char *>(temp_buffer.data());
        auto used = CodecTraits<T>::decode(p, n, temp.data());
        if (used != bytes)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                            "Compressed and decompressed byte counts do not match. compressed = {}, decompressed = {}",
                            bytes, used);
        assert(n == temp.size());
        /// Fill data to container.
        consumer(temp);
    }
};

/// MemorySource — Implementation of IBlockSource for in-memory postings.
/// Reads compressed blocks directly from a contiguous memory buffer and
/// lazily decodes them one by one.
template<typename T>
struct Source
{
    template<typename Input>
    static bool decodeOneBlock(Input & in, std::vector<T> & decoded, size_t & current_block, size_t & total_blocks, std::string & raw)
    {
        if (in.eof())
            return false;
        if (current_block >= total_blocks)
            return false;
        uint16_t n = 0;
        uint32_t bytes = 0;
        Codec::readHeader(n, bytes, in);
        raw.resize(bytes);
        in.readStrict(raw.data(), bytes);
        auto p = reinterpret_cast<unsigned char*>(raw.data());
        Codec::decodeBlock<T>(p, n, decoded, bytes);
        ++ current_block;
        return true;
    }
};
template<typename T>
struct MemorySourceImpl : Source<T>
{
    template<typename Input>
    static bool decodeNext(Input & rb, std::vector<T> & decoded, size_t & current_block, size_t & total_blocks, const std::vector<T> & current, std::string & raw)
    {
        if (current_block < total_blocks)
        {
            if (Source<T>::decodeOneBlock(rb, decoded, current_block, total_blocks, raw))
                return true;
        }
        if (current_block == total_blocks && !current.empty())
        {
            ++current_block;
            decoded.assign(current.begin(), current.end());
            return true;
        }
        return false;
    }
};

template<typename T>
struct StreamSourceImpl : Source<T>
{
    template<typename Input>
    static bool decodeNext(Input & rb, std::vector<T> & decoded, size_t & current_block, size_t & total_blocks, const std::vector<T> &, std::string & raw)
    {
        if (current_block < total_blocks)
        {
            if (Source<T>::decodeOneBlock(rb, decoded, current_block, total_blocks, raw))
                return true;
        }
        return false;
    }
};

template<typename T> struct MemorySourceTraits;
template<> struct MemorySourceTraits<uint32_t> : MemorySourceImpl<uint32_t> {};
template<> struct MemorySourceTraits<uint64_t> : MemorySourceImpl<uint64_t> {};

template<typename T> struct StreamSourceTraits;
template<> struct StreamSourceTraits<uint32_t> : StreamSourceImpl<uint32_t> {};
template<> struct StreamSourceTraits<uint64_t> : StreamSourceImpl<uint64_t> {};

template<typename Out, typename IteratorLeft, typename IteratorRight>
void mergePostingsTwo(Out & out, IteratorLeft left_begin, const IteratorLeft left_end, IteratorRight right_begin, const IteratorRight right_end)
{
    using ValueType = Out::ValueType;
    bool has_left = (left_begin != left_end);
    bool has_right = (right_begin != right_end);
    ValueType left_val = 0;
    ValueType right_val = 0;
    if (has_left)
        left_val = *left_begin;
    if (has_right)
        right_val = *right_begin;

    while (has_left && has_right)
    {
        if (left_val < right_val)
        {
            out.add(left_val);
            ++left_begin;
            has_left = left_begin != left_end;
            if (has_left)
                left_val = *left_begin;
        }
        else if (right_val < left_val)
        {
            out.add(right_val);
            ++right_begin;
            has_right = right_begin != right_end;
            if (has_right)
                right_val = *right_begin;
        }
        else
        {
            out.add(left_val);
            ++left_begin;
            ++right_begin;
            has_left = left_begin != left_end;
            has_right = right_begin != right_end;
            if (has_left)
                left_val = *left_begin;
            if (has_right)
                right_val = *right_begin;
        }
    }

    while (has_left)
    {
        out.add(left_val);
        ++left_begin;
        has_left = left_begin != left_end;
        if (has_left)
            left_val = *left_begin;
    }

    while (has_right)
    {
        out.add(right_val);
        ++right_begin;
        has_right = right_begin != right_end;
        if (has_right)
            right_val = *right_begin;
    }
}

template <typename It>
struct PostingRange
{
    using ValueType = It::ValueType;
    It cur;
    It end;
    ValueType value = 0;
    bool valid = false;

    PostingRange() = default;

    PostingRange(It b, It e) : cur(b), end(e)
    {
        if (cur != end)
        {
            value = static_cast<ValueType>(*cur);
            valid = true;
        }
    }

    bool next()
    {
        if (!valid)
            return false;
        ++cur;
        if (cur == end)
        {
            valid = false;
            return false;
        }
        value = static_cast<uint32_t>(*cur);
        return true;
    }
};

template<typename ValueType>
struct PQItem
{
    ValueType value;
    std::size_t idx;
    [[maybe_unused]] bool operator<(const PQItem & other) const noexcept
    {
        return value > other.value;
    }
};

template <typename Out, typename... Pairs>
void mergePostingsVariadic(Out & out, Pairs&&... pairs)
{
    using ValueType = Out::ValueType;
    auto ranges = std::make_tuple(PostingRange<typename std::decay_t<Pairs>::first_type> (std::forward<Pairs>(pairs).first, std::forward<Pairs>(pairs).second )...);
    using TupleType = decltype(ranges);
    constexpr std::size_t K = std::tuple_size<TupleType>::value;

    std::priority_queue<PQItem<ValueType>> pq;
    [&]<std::size_t... I>(std::index_sequence<I...>)
    {
        ((std::get<I>(ranges).valid ? pq.push(PQItem{std::get<I>(ranges).value, I}) : void()), ...);
    } (std::make_index_sequence<K>{});

    if (pq.empty())
        return;

    uint32_t last_written = 0;
    bool has_last = false;

    while (!pq.empty())
    {
        auto top = pq.top();
        pq.pop();

        const uint32_t v = top.value;
        const std::size_t idx = top.idx;

        if (!has_last || v != last_written)
        {
            out.add(v);
            last_written = v;
            has_last = true;
        }

        auto & rng = std::get<idx>(ranges);
        if (rng.next())
        {
            pq.push(PQItem{rng.value, idx});
        }
    }
}
}

struct ContainerBase
{
/// Generic forward iterator that consumes data
/// from an IBlockSource. Decodes one block at a time and exposes individual
/// posting values in ascending order. Shared by memory and stream containers.
template<typename Input, typename T, typename Traits, typename Data>
class Iterator
{
public:
    using ValueType = T;
    explicit Iterator(std::shared_ptr<Data> data_, size_t total_blocks_)
        : data(data_)
        , total_blocks(total_blocks_)
    {
        if (data->empty())
        {
            is_end = true;
            current_offset = std::numeric_limits<size_t>::max();
        }
        if (!is_end)
            decodeNextBlock();
    }
    explicit Iterator()
        : current_offset(std::numeric_limits<size_t>::max())
        , is_end(true)
    {
    }

    T operator*() const { return current_value; }
    Iterator & operator++()
    {
        advance();
        return *this;
    }
    bool operator==(const Iterator & rhs) const
    {
        return is_end == rhs.is_end && current_offset == rhs.current_offset;
    }
    bool operator!=(const Iterator & rhs) const { return !(*this == rhs); }

private:
    void advance()
    {
        if (is_end)
            return;
        ++current_index;
        ++current_offset;
        if (current_index >= decoded.size())
            decodeNextBlock();
        if (!is_end)
            current_value = decoded[current_index];
    }

    void decodeNextBlock()
    {
        if (!Traits::decodeNext(data->rb, decoded, current_block, total_blocks, data->current, raw))
        {
            is_end = true;
            current_offset = std::numeric_limits<size_t>::max();
            return;
        }
        current_index = 0;
        current_value = decoded[0];
    }

    std::string raw;
    std::shared_ptr<Data> data {};
    std::vector<T> decoded;
    size_t current_index = 0;
    T current_value = 0;

    size_t current_offset = 0;

    size_t total_blocks = 0;
    size_t current_block = 0;
    bool is_end = false;
};
};

/// PostingsContainer — Writable postings container for in-memory build mode.
/// Accepts monotonically increasing uint32_t values, compresses them in
/// fixed-size blocks, and supports serialization and lazy iteration.
/// Used during index building or in-memory caching.
template<typename T>
class PostingsContainerImpl : public ContainerBase
{
    static_assert(std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>, "PostingsContainer only supports uint32_t / uint64_t");
    static constexpr UInt8  kBlockSizeShift = 7;
    static constexpr size_t kBlockSize = 1 << kBlockSizeShift;
    static constexpr size_t kBlockSizeMask = kBlockSize - 1;
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
        writeVarUInt(data.size(), out);
        out.write(data.data(), data.size());
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

    /// Reads, decompresses and emits postings directly to an output sink (PostingsList).
    /// Used for full decode in one pass, without exposing intermediate state.
    template<typename In, typename Out>
    void decodeTo(In & in, Out & out)
    {
        deserialize(in);
        std::string temp_buffer;
        std::vector<T> temp_compress_buffer;
        temp_compress_buffer.reserve(kBlockSize);
        for (size_t i = 0; i < block_count; ++i)
            internal::Codec::decompressCurrent<T>(in, temp_buffer, temp_compress_buffer, [&out] (std::vector<uint32_t> & temp) { out.addMany(temp.size(), temp.data()); });
        if (!current.empty())
            out.addMany(current.size(), current.data());
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

    /// Returns an iterator over decompressed postings (lazy block decoding).
    auto begin() const
    {
        return Iterator<InputType, ValueType, internal::MemorySourceTraits<ValueType>, IteratorData>(std::make_shared<IteratorData>(data, current), block_count);
    }
    auto end() const { return Iterator<InputType, ValueType, internal::MemorySourceTraits<ValueType>, IteratorData>(); }
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
        const uint32_t n = current.size();
        const uint32_t cap = internal::CodecTraits<T>::bound(n);
        temp_compression_data.resize(cap);
        auto bytes = internal::CodecTraits<T>::encode(current.data(), n, reinterpret_cast<unsigned char*>(temp_compression_data.data()));
        WriteBufferFromString wb(data, AppendModeTag{});
        internal::Codec::writeHeader(n, bytes, wb);
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
/// PostingsContainerStreamView — Read-only streaming view over serialized
/// postings data stored in a ReadBuffer. Supports lazy, block-by-block
/// decoding without loading the entire postings into memory.
/// Typically used for on-disk postings iteration when merging parts.
template<typename T>
class PostingsContainerStreamViewImpl : public ContainerBase
{
public:
    using ValueType = T;
    /// Constructs a streaming postings view backed by the given ReadBuffer.
    /// Reads the header (block_count, total, data_size) but does not decompress any postings.
    explicit PostingsContainerStreamViewImpl(ReadBuffer & rb) : read_buffer(rb)
    {
        readVarUInt(block_count, read_buffer);
        readVarUInt(total, read_buffer);
        size_t bytes = 0;
        readVarUInt(bytes, read_buffer);
    }

    /// Returns a forward iterator that lazily decodes postings
    /// directly from the underlying ReadBuffer as needed.
    /// The iterator consumes the buffer in a single forward pass.
    auto begin() const
    {
        return Iterator<InputType, ValueType, internal::StreamSourceTraits<ValueType>, IteratorData>(std::make_shared<IteratorData>(read_buffer), block_count);
    }
    auto end() const { return Iterator<InputType, ValueType, internal::StreamSourceTraits<ValueType>, IteratorData>(); }
    bool empty() const { return total == 0; }
    size_t size() const { return total; }
private:
    using InputType = ReadBuffer;
    struct IteratorData
    {
        InputType & rb;
        std::vector<T> current;
        explicit IteratorData(ReadBuffer & rb_) : rb(rb_) {}
        bool empty() const { return rb.eof(); }
    };
    size_t block_count = 0;
    size_t total = 0;
    ReadBuffer & read_buffer;
};

using PostingsContainer32 = PostingsContainerImpl<uint32_t>;
using PostingsContainer64 = PostingsContainerImpl<uint64_t>;
using PostingsContainerStreamView32 = PostingsContainerStreamViewImpl<uint32_t>;
using PostingsContainerStreamView64 = PostingsContainerStreamViewImpl<uint64_t>;

/// Merges two or more postings containers (postings exposing begin()/end()
/// that iterate over ascending uint32_t values) into a single output sink.
///
/// The function has two code paths:
/// 1. When exactly two containers are provided, it uses a fast 2-way merge
///    (mergePostingsTwo), similar to merging two sorted lists.
/// 2. When more than two containers are provided, it falls back to a k-way
///    merge (mergePostingsVariadic) using a priority queue to keep the output
///    sorted and deduplicated.
/// Example:
/// ```cpp
/// PostingsContainer p1;
/// p1.add(1); p1.add(3); p1.add(7);
///
/// PostingsContainer p2;
/// p2.add(2); p2.add(3); p2.add(10);
///
/// PostingsContainer p3;
/// p3.add(4); p3.add(8);
///
/// PostingsContainer out;
///
/// // merge two
/// mergePostingsContainers(out, p1, p2);
/// // out now contains: 1,2,3,7,10 (3 appears once if mergePostingsTwo dedups)
///
/// // merge three
/// PostingsContainer out3;
/// mergePostingsContainers(out3, p1, p2, p3);
/// // out3 now contains: 1,2,3,4,7,8,10
/// ```
///
/// This is useful when merging inverted index postings from multiple parts into single one.
template <typename Out, typename C1, typename C2, typename... Rest>
static void mergePostingsContainers(Out & out, const C1 & c1, const C2 & c2, const Rest &... rest)
{
    if constexpr (sizeof...(rest) == 0)
        internal::mergePostingsTwo(out, c1.begin(), c1.end(), c2.begin(), c2.end());
    else
        internal::mergePostingsVariadic(out, std::pair{ c1.begin(), c1.end() }, std::pair{ c2.begin(), c2.end() }, std::pair{ rest.begin(), rest.end() }...);
}
}
