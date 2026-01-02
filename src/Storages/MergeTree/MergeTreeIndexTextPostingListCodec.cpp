#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

#if USE_SIMDCOMP

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}


void PostingListCodecImpl::insert(uint32_t value)
{
    if (total_in_current_segment == 0)
    {
        segments.emplace_back();
        segments.back().row_offset_begin = value;
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

    if (current.size() == BLOCK_SIZE)
        compressBlock(current);

    if (total_in_current_segment == posting_list_block_size)
        flushSegment();
}

void PostingListCodecImpl::insert(std::span<uint32_t> values)
{
    chassert(values.size() == BLOCK_SIZE && total_in_current_segment % BLOCK_SIZE == 0);
    if (total_in_current_segment == 0)
    {
        segments.emplace_back();
        segments.back().row_offset_begin = values.front();
        segments.back().compressed_data_offset = compressed_data.size();
        prev_value = values.front();
        total_in_current_segment += BLOCK_SIZE;
        cardinality += BLOCK_SIZE;
    }

    auto last = values.back();
    std::adjacent_difference(values.begin(), values.end(), values.begin());
    values[0] -= prev_value;
    prev_value = last;

    compressBlock(values);

    if (total_in_current_segment == posting_list_block_size)
        flushSegment();
}

void PostingListCodecImpl::deserialize(ReadBuffer & in, PostingList & out)
{
    Header header;
    header.read(in);
    if (header.codec_type != static_cast<uint8_t>(codec_type))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected codec type {}, but got {}", codec_type, header.codec_type);

    prev_value = header.base_value;

    uint32_t tail_block_size = header.cardinality % BLOCK_SIZE;
    uint32_t full_block_count = header.cardinality / BLOCK_SIZE;
    current.reserve(BLOCK_SIZE);
    if (header.bytes > (compressed_data.capacity() - compressed_data.size()))
        compressed_data.reserve(compressed_data.size() + header.bytes);
    compressed_data.resize(header.bytes);
    in.readStrict(compressed_data.data(), header.bytes);

    //auto * p = reinterpret_cast<unsigned char *> (compressed_data.data());
    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte*>(compressed_data.data()), compressed_data.size());
    for (uint32_t i = 0; i < full_block_count; i++)
    {
        decodeOneBlock(compressed_data_span, BLOCK_SIZE, prev_value, current);
        out.addMany(current.size(), current.data());
    }
    if (tail_block_size)
    {
        decodeOneBlock(compressed_data_span, tail_block_size, prev_value, current);
        out.addMany(current.size(), current.data());
    }
}

void PostingListCodecImpl::serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const
{
    info.offsets.reserve(segments.size());
    info.ranges.reserve(segments.size());
    for (const auto & desc : segments)
    {
        info.offsets.emplace_back(out.count());
        info.ranges.emplace_back(desc.row_offset_begin, desc.row_offset_end);
        Header header(static_cast<uint8_t>(codec_type), desc.compressed_data_size, desc.cardinality, desc.row_offset_begin);
        header.write(out);
        out.write(compressed_data.data() + desc.compressed_data_offset, desc.compressed_data_size);
    }
}

void PostingListCodecImpl::compressBlock(std::span<uint32_t> segment)
{
    auto & last_segment = segments.back();
    last_segment.cardinality += segment.size();
    last_segment.row_offset_end = prev_value;

    auto [cap, bits] = BlockCodec::evaluateSizeAndMaxBits(segment);
    size_t memory_gap = compressed_data.capacity() - compressed_data.size();
    size_t need_bytes = cap + 1;
    if (memory_gap < need_bytes)
    {
        auto min_need = need_bytes - memory_gap;
        compressed_data.reserve(compressed_data.size() + 2 * min_need);
    }
    /// Block Layout: [1byte(bits)][payload]
    size_t offset = compressed_data.size();
    compressed_data.resize(compressed_data.size() + need_bytes);
    std::span<char> compressed_data_span(compressed_data.data() + offset, need_bytes);
    encodeU8(bits, compressed_data_span);
    auto used = BlockCodec::encode(segment, bits, compressed_data_span);
    chassert(used == cap && compressed_data_span.empty());

    last_segment.compressed_data_size = compressed_data.size() - last_segment.compressed_data_offset;
    current.clear();
}

void PostingListCodecImpl::decodeOneBlock(std::span<const std::byte> & in, size_t count, uint32_t & prev_value, std::vector<uint32_t> & current)
{
    if (in.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected at least {} bytes, but got {}", 1, in.size());

    uint8_t bits = decodeU8(in);
    current.resize(count);

    /// Decode postings to buffer named temp.
    std::span<uint32_t> current_span(current.data(), current.size());
    BlockCodec::decode(in, count, bits, current_span);

    /// Restore the original array from the decompressed delta values.
    std::inclusive_scan(current.begin(), current.end(), current.begin(), std::plus<uint32_t>{}, prev_value);
    prev_value = current.empty() ? prev_value : current.back();
}

PostingListCodecSIMDComp::PostingListCodecSIMDComp()
    : IPostingListCodec(Type::Bitpacking)
{
}

void PostingListCodecSIMDComp::decode(ReadBuffer & in, PostingList & postings) const
{
    PostingListCodecImpl impl;
    impl.deserialize(in, postings);
}

void PostingListCodecSIMDComp::encode(const PostingListBuilder & postings, size_t posting_list_block_size, TokenPostingsInfo & info, WriteBuffer & out) const
{
    PostingListCodecImpl impl(posting_list_block_size);
    if (postings.isSmall())
    {
        const auto & small_postings = postings.getSmall();
        size_t small_size = postings.size();
        for (size_t i = 0; i < small_size; ++i)
            impl.insert(small_postings[i]);
    }
    else
    {
        std::vector<uint32_t> rowids;
        rowids.resize(postings.size());
        const auto & large_postings = postings.getLarge();
        large_postings.toUint32Array(rowids.data());
        std::span<uint32_t> values(rowids.data(), rowids.size());

        auto block_size = PostingListCodecImpl::BLOCK_SIZE;
        while (values.size() >= block_size)
        {
            auto front = values.first(block_size);
            impl.insert(front);
            values = values.subspan(block_size);
        }
        if (!values.empty())
        {
            for (auto v : values)
                impl.insert(v);
        }
    }
    impl.serialize(out, info);
}
}
#endif
