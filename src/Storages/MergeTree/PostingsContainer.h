#pragma once
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/IntegerCodecTrait.h>
#include <Storages/MergeTree/MergeTreeIndexTextCommon.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <roaring/roaring.hh>
#include <Storages/MergeTree/MergedPartOffsets.h>

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

namespace internal
{
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
template<typename ValueType>
struct ContainerHeader
{
    ContainerHeader() = default;
    explicit ContainerHeader(ValueType base_value_, uint32_t block_count_)
        : base_value(base_value_)
        , block_count(block_count_)
    {
    }
    ValueType base_value {};
    uint32_t block_count = 0;
};

template<typename ValueType>
struct SegmentDesc
{
    ContainerHeader<ValueType> header;
    size_t compressed_data_offset = 0;
    size_t compressed_data_size = 0;
    ValueType last_value;
    SegmentDesc() = default;
};

template<typename ValueType>
struct CodecUtils
{
    using BlockHeader = BlockHeader;
    using ContainerHeader = ContainerHeader<ValueType>;
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

    template<typename In>
    static void decodeOneBlock(In & in, ValueType & prev_value, std::vector<ValueType> & current, std::string & temp_buffer)
    {
        BlockHeader block_header;
        readBlockHeader(block_header, in);

        current.resize(block_header.count);
        temp_buffer.resize(block_header.bytes);
        in.readStrict(temp_buffer.data(), block_header.bytes);

        /// Decode postings to buffer named temp.
        unsigned char * p = reinterpret_cast<unsigned char *>(temp_buffer.data());
        auto used = CodecTraits<ValueType>::decode(p, block_header.count, block_header.max_bits, current.data());
        if (used != block_header.bytes)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Compressed and decompressed byte counts do not match. compressed = {}, decompressed = {}", block_header.bytes, used);
        chassert(block_header.count == current.size());

        /// Restore the original array from the decompressed delta values.
        std::inclusive_scan(current.begin(), current.end(), current.begin(), std::plus<ValueType>{}, prev_value);
        prev_value = current.empty() ? prev_value : current.back();
    }
};
}

/// PostingsContainer — Writable postings container for in-memory build mode.
/// Accepts monotonically increasing uint32_t values, compresses them in
/// fixed-size blocks, and supports serialization and lazy iteration.
/// Used during index building or in-memory caching.
template<typename T>
class PostingsContainerImpl
{
    static_assert(std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>, "PostingsContainer only supports uint32_t and uint64_t");
    using BlockHeader = internal::BlockHeader;
    using ContainerHeader = internal::ContainerHeader<T>;
    using SegmentDesc = internal::SegmentDesc<T>;
public:
    static constexpr size_t kBlockSize = CodecTraits<T>::kBlockSize;
    using ValueType = T;
    PostingsContainerImpl() = default;
    explicit PostingsContainerImpl(size_t postings_list_block_size)
        : postings_list_segment_size((postings_list_block_size + kBlockSize - 1) & ~(kBlockSize - 1))
    {
        current.reserve(kBlockSize);
    }

    size_t size() const { return cardinality; }
    bool empty() const { return cardinality == 0; }

    void add(T value)
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

    void add(std::vector<T> & values)
    {
        chassert(values.size() == kBlockSize);
        if (values.empty())
            return;
        if (total == postings_list_segment_size)
            flushSegment();
        if (total == 0)
        {
            segments.emplace_back();
            segments.back().header.base_value = values.front();
            segments.back().compressed_data_offset = compressed_data.size();
        }
        current.swap(values);
        auto new_prev_value = current.back();
        std::adjacent_difference(current.begin(), current.end(), current.begin());
        current[0] -= prev_value;
        prev_value = new_prev_value;
        total += current.size();
        cardinality += current.size();
        compressBlock(current, temp_compression_data_buffer);
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
    template<typename In>
    void deserialize(In & in, PostingList & out)
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
            internal::CodecUtils<ValueType>::writeContainerHeader(segment_desc.header, out);
            out.write(compressed_data.data() + segment_desc.compressed_data_offset, segment_desc.compressed_data_size);
        }
        return out.count() - offset;
    }

    template<typename In>
    void deserializeFrom(In & in, ContainerHeader & header)
    {
        internal::CodecUtils<ValueType>::readContainerHeader(header, in);
    }

    void compressBlock(std::vector<T> & segment, std::string & temp_compression_data)
    {
        ++segments.back().header.block_count;
        segments.back().last_value = prev_value;

        auto [cap, bits] = CodecTraits<T>::evaluateSizeAndMaxBits(segment.data(), segment.size());
        temp_compression_data.resize(cap);
        auto bytes = CodecTraits<T>::encode(segment.data(), segment.size(), bits, reinterpret_cast<unsigned char*>(temp_compression_data.data()));

        ///	Write the BlockHeader followed by the compressed posting list data.
        BlockHeader block_header { static_cast<uint16_t>(segment.size()), static_cast<uint16_t>(bits), static_cast<uint32_t>(bytes) };
        WriteBufferFromString compressed_buffer(compressed_data, AppendModeTag {});
        internal::CodecUtils<ValueType>::writeBlockHeader(block_header, compressed_buffer);
        compressed_buffer.write(temp_compression_data.data(), bytes);
        compressed_buffer.finalize();
        segments.back().compressed_data_size = compressed_data.size() - segments.back().compressed_data_offset;
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
        internal::CodecUtils<ValueType>::readBlockHeader(block_header, in);
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
    ValueType prev_value = {};
    size_t total = 0;
    std::vector<ValueType> current;
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

namespace internal
{
template<typename Stream, typename T>
class Iterator
{
public:
    using ValueType = T;
    using StreamType = Stream;
    using BlockHeader = internal::BlockHeader;
    using ContainerHeader = internal::ContainerHeader<ValueType>;
    explicit Iterator(StreamType & stream_, size_t index_, const TokenPostingsInfo & info_, bool is_end_)
        : stream(stream_)
        , info(info_)
        , is_end(is_end_)
        , index(index_)
    {
        total_containers = info.offsets.size();
        current_containers = 0;
        total_blocks = 0;

        auto * buffer = stream.getDataBuffer();
        if (buffer->eof() || total_containers == 0)
            is_end = true;

        if (!is_end)
            decodeNextBlock();
    }

    ValueType operator*() const
    {
        return current_value;
    }
    Iterator & operator++()
    {
        advance();
        return *this;
    }
    bool operator==(const Iterator & rhs) const
    {
        return is_end == rhs.is_end && index == rhs.index;
    }
    bool operator!=(const Iterator & rhs) const { return !(*this == rhs); }

    size_t getIndex() const { return index; }
private:
    void advance()
    {
        if (is_end)
            return;
        ++current_index;

        if (current_index == current.size() || current.empty())
            decodeNextBlock();
        if (!is_end)
            current_value = current[current_index];
    }

    void decodeNextBlock()
    {
        chassert(current_index == current.size() || current.empty());
        if (total_blocks == 0 && current_containers < total_containers)
        {
            auto offset_in_file = info.offsets[current_containers];
            stream.seekToMark({offset_in_file, 0});

            ContainerHeader header;
            CodecUtils<ValueType>::readContainerHeader(header, *stream.getDataBuffer());
            prev_value = header.base_value;

            total_blocks = header.block_count;
            ++current_containers;
            current_block = 0;
        }
        if (current_block < total_blocks)
        {
            current.clear();
            CodecUtils<ValueType>::decodeOneBlock(*stream.getDataBuffer(), prev_value, current, temp_buffer);
            ++current_block;
            if (current_block == total_blocks)
                total_blocks = 0;

            current_index = 0;
            if (current.empty())
                is_end = true;
            else
                current_value = current[current_index];
            return;
        }
        if (current_containers == total_containers)
            is_end = true;
    }

    std::string temp_buffer;
    ValueType prev_value;
    StreamType & stream;
    const TokenPostingsInfo & info;
    std::vector<ValueType> current;
    size_t current_index = 0;
    ValueType current_value {};

    size_t total_blocks = 0;
    size_t current_block = 0;

    size_t total_containers = 0;
    size_t current_containers = 0;

    bool is_end = false;
    size_t index = 0;
};

template<typename Out, typename IteratorLeft, typename IteratorRight>
void mergePostingsTwo(Out & out, IteratorLeft left_begin, const IteratorLeft left_end, IteratorRight right_begin, const IteratorRight right_end, const MergedPartOffsets & merged_part_offsets)
{
    using ValueType = Out::ValueType;
    bool has_left = (left_begin != left_end);
    bool has_right = (right_begin != right_end);
    ValueType left_val = 0;
    ValueType right_val = 0;
    size_t lindex = left_begin.getIndex();
    size_t rindex = right_begin.getIndex();
    if (has_left)
        left_val = merged_part_offsets[lindex, *left_begin];
    if (has_right)
        right_val = merged_part_offsets[rindex, *right_begin];

    while (has_left && has_right)
    {
        if (left_val < right_val)
        {
            out.add(left_val);
            ++left_begin;
            has_left = left_begin != left_end;
            if (has_left)
                left_val = merged_part_offsets[lindex, *left_begin];
        }
        else if (right_val < left_val)
        {
            out.add(right_val);
            ++right_begin;
            has_right = right_begin != right_end;
            if (has_right)
                right_val = merged_part_offsets[rindex, *right_begin];
        }
        else
        {
            out.add(left_val);
            ++left_begin;
            ++right_begin;
            has_left = left_begin != left_end;
            has_right = right_begin != right_end;
            if (has_left)
                left_val = merged_part_offsets[lindex, *left_begin];
            if (has_right)
                right_val = merged_part_offsets[rindex, *right_begin];
        }
    }

    while (has_left)
    {
        out.add(left_val);
        ++left_begin;
        has_left = left_begin != left_end;
        if (has_left)
            left_val = merged_part_offsets[lindex, *left_begin];
    }

    while (has_right)
    {
        out.add(right_val);
        ++right_begin;
        has_right = right_begin != right_end;
        if (has_right)
            right_val = merged_part_offsets[rindex, *right_begin];
    }
}

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

template<class Iterator>
struct PostingsRange
{
    using ValueType = Iterator::ValueType;
    Iterator cur;
    Iterator end;
    ValueType value = 0;
    bool valid = false;
    size_t index = 0;
    const MergedPartOffsets & merged_part_offsets;

    PostingsRange() = default;

    PostingsRange(Iterator b, Iterator e, const MergedPartOffsets & merged_part_offsets_)
        : cur(b)
        , end(e)
        , index(b.getIndex())
        , merged_part_offsets(merged_part_offsets_)
    {
        chassert(b.getIndex() == e.getIndex());
        if (cur != end)
        {
            value = merged_part_offsets[index, static_cast<ValueType>(*cur)];
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
        value = merged_part_offsets[index, static_cast<ValueType>(*cur)];
        return true;
    }
};

template <typename Out, typename PostingsContainer>
void mergePostingsVariadic(Out & out, const std::vector<PostingsContainer> & streams, const MergedPartOffsets & merged_part_offsets)
{
    using ValueType = typename Out::ValueType;
    using IteratorType = typename PostingsContainer::IteratorType;

    if (streams.empty())
        return;

    std::vector<PostingsRange<IteratorType>> ranges;
    ranges.reserve(streams.size());
    for (const auto & c : streams)
        ranges.emplace_back(std::begin(c), std::end(c), merged_part_offsets);

    std::priority_queue<PQItem<ValueType>> pq;
    for (std::size_t i = 0; i < ranges.size(); ++i)
    {
        if (ranges[i].valid)
            pq.push(PQItem<ValueType>{ranges[i].value, i});
    }

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

        auto & rng = ranges[idx];
        if (rng.next())
            pq.push(PQItem<ValueType>{rng.value, idx});
    }
}
}
/// PostingsContainerStreamView — Read-only streaming view over serialized
/// postings data stored in a ReadBuffer. Supports lazy, block-by-block
/// decoding without loading the entire postings into memory.
/// Typically used for on-disk postings iteration when merging parts.
template<typename StreamType, typename T>
class PostingsContainerStreamViewImpl
{
public:
    using ValueType = T;
    using IteratorType = internal::Iterator<StreamType, ValueType>;
    /// Constructs a streaming postings view backed by the given ReadBuffer.
    /// Reads the header (block_count, total, data_size) but does not decompress any postings.
    explicit PostingsContainerStreamViewImpl(StreamType & stream_, size_t index_, const TokenPostingsInfo & info_) : stream(stream_), index(index_), info(info_)
    {
    }

    /// Returns a forward iterator that lazily decodes postings
    /// directly from the underlying ReadBuffer as needed.
    /// The iterator consumes the buffer in a single forward pass.
    auto begin() const
    {
        return internal::Iterator<StreamType, ValueType>(stream, index, info, false);
    }
    auto end() const
    {
        return internal::Iterator<StreamType, ValueType>(stream, index, info, true);
    }

   void deserialize(PostingsContainerImpl<T> & target, const MergedPartOffsets & merged_part_offsets)
    {
        std::string temp_buffer;
        std::vector<ValueType> temp_compress_buffer;
        std::vector<ValueType> current;
        for (const auto & offset_in_file : info.offsets)
        {
            typename internal::CodecUtils<ValueType>::ContainerHeader header;
            auto & buffer = *stream.getDataBuffer();
            stream.seekToMark({offset_in_file, 0});
            internal::CodecUtils<ValueType>::readContainerHeader(header, buffer);

            ValueType prev_value = header.base_value;
            temp_compress_buffer.reserve(PostingsContainerImpl<ValueType>::kBlockSize);
            current.reserve(PostingsContainerImpl<ValueType>::kBlockSize);
            for (size_t i = 0; i < static_cast<size_t>(header.block_count); ++i)
            {
                internal::CodecUtils<ValueType>::decodeOneBlock(buffer, prev_value, current, temp_buffer);
                for (auto value : current)
                    target.add(merged_part_offsets[index, value]);
            }
        }
    }
private:
    StreamType & stream;
    size_t index = 0;
    const TokenPostingsInfo & info;
};

/// This is useful when merging inverted index postings from multiple parts into single one.
template <typename Out, typename StreamPostingsContainer>
static void mergePostingsContainers(Out & out, std::vector<StreamPostingsContainer> & containers, const MergedPartOffsets & merged_part_offsets)
{
    if (containers.size() == 2)
        internal::mergePostingsTwo(out, containers[0].begin(), containers[0].end(), containers[1].begin(), containers[1].end(), merged_part_offsets);
    else
        internal::mergePostingsVariadic(out, containers, merged_part_offsets);
}

template <typename Out, typename StreamPostingsContainer>
static void transformSinglePostingsContainer(Out & out, StreamPostingsContainer & container, const MergedPartOffsets & merged_part_offsets)
{
    container.deserialize(out, merged_part_offsets);
}
}
