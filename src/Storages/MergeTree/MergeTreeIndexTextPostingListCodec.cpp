#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

#if USE_SIMDCOMP

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}


void PostingListCodecImpl::insert(uint32_t row)
{
    if (rows_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_begin = row;
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        prev_row = row;
        current_segment.emplace_back(row - prev_row);
        ++rows_in_current_segment;
        ++total_rows;
        return;
    }

    current_segment.emplace_back(row - prev_row);
    prev_row = row;
    ++rows_in_current_segment;
    ++total_rows;

    if (current_segment.size() == BLOCK_SIZE)
        compressBlock(current_segment);

    if (rows_in_current_segment == posting_list_block_size)
        flushCurrentSegment();
}

void PostingListCodecImpl::insert(std::span<uint32_t> rows)
{
    chassert(rows.size() == BLOCK_SIZE && rows_in_current_segment % BLOCK_SIZE == 0);

    if (rows_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_begin = rows.front();
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        prev_row = rows.front();
        rows_in_current_segment += BLOCK_SIZE;
        total_rows += BLOCK_SIZE;
    }

    auto last_row = rows.back();
    std::adjacent_difference(rows.begin(), rows.end(), rows.begin());
    rows[0] -= prev_row;
    prev_row = last_row;

    compressBlock(rows);

    if (rows_in_current_segment == posting_list_block_size)
        flushCurrentSegment();
}

void PostingListCodecImpl::deserialize(ReadBuffer & in, PostingList & out)
{
    Header header;
    header.read(in);
    if (header.codec_type != static_cast<uint8_t>(codec_type))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected codec type {}, but got {}", codec_type, header.codec_type);

    prev_row = header.base_value;

    uint32_t tail_block_size = header.cardinality % BLOCK_SIZE;
    uint32_t full_block_count = header.cardinality / BLOCK_SIZE;
    current_segment.reserve(BLOCK_SIZE);
    if (header.bytes > (compressed_data.capacity() - compressed_data.size()))
        compressed_data.reserve(compressed_data.size() + header.bytes);
    compressed_data.resize(header.bytes);
    in.readStrict(compressed_data.data(), header.bytes);

    //auto * p = reinterpret_cast<unsigned char *> (compressed_data.data());
    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte*>(compressed_data.data()), compressed_data.size());
    for (uint32_t i = 0; i < full_block_count; i++)
    {
        decodeOneBlock(compressed_data_span, BLOCK_SIZE, prev_row, current_segment);
        out.addMany(current_segment.size(), current_segment.data());
    }
    if (tail_block_size)
    {
        decodeOneBlock(compressed_data_span, tail_block_size, prev_row, current_segment);
        out.addMany(current_segment.size(), current_segment.data());
    }
}

void PostingListCodecImpl::serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const
{
    info.offsets.reserve(segment_descriptors.size());
    info.ranges.reserve(segment_descriptors.size());

    for (const auto & descriptor : segment_descriptors)
    {
        info.offsets.emplace_back(out.count());
        info.ranges.emplace_back(descriptor.row_begin, descriptor.row_end);
        Header header(static_cast<uint8_t>(codec_type), descriptor.compressed_data_size, descriptor.cardinality, descriptor.row_begin);
        header.write(out);
        out.write(compressed_data.data() + descriptor.compressed_data_offset, descriptor.compressed_data_size);
    }
}

namespace
{
void encodeU8(uint8_t x, std::span<char> & out)
{
    out[0] = static_cast<char>(x);
    out = out.subspan(1);
}

uint8_t decodeU8(std::span<const std::byte> & in)
{
    auto v = static_cast<uint8_t>(in[0]);
    in = in.subspan(1);
    return v;
}
}

void PostingListCodecImpl::compressBlock(std::span<uint32_t> segment)
{
    auto & last_segment = segment_descriptors.back();
    last_segment.cardinality += segment.size();
    last_segment.row_end = prev_row;

    auto [needed_bytes, max_bits] = BlockCodec::calculateNeededBytesAndMaxBits(segment);
    size_t memory_gap = compressed_data.capacity() - compressed_data.size();
    size_t need_bytes = needed_bytes + 1;
    if (memory_gap < need_bytes)
    {
        auto min_need = need_bytes - memory_gap;
        compressed_data.reserve(compressed_data.size() + 2 * min_need);
    }
    /// Block Layout: [1byte(max_bits)][payload]
    size_t offset = compressed_data.size();
    compressed_data.resize(compressed_data.size() + need_bytes);
    std::span<char> compressed_data_span(compressed_data.data() + offset, need_bytes);
    encodeU8(max_bits, compressed_data_span);
    auto used = BlockCodec::encode(segment, max_bits, compressed_data_span);
    chassert(used == needed_bytes && compressed_data_span.empty());

    last_segment.compressed_data_size = compressed_data.size() - last_segment.compressed_data_offset;
    current_segment.clear();
}

void PostingListCodecImpl::decodeOneBlock(
        std::span<const std::byte> & in, size_t count, uint32_t & prev_row, std::vector<uint32_t> & current_segment)
{
    if (in.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected at least {} bytes, but got {}", 1, in.size());

    uint8_t bits = decodeU8(in);
    current_segment.resize(count);

    /// Decode postings to buffer named temp.
    std::span<uint32_t> current_span(current_segment.data(), current_segment.size());
    BlockCodec::decode(in, count, bits, current_span);

    /// Restore the original array from the decompressed delta values.
    std::inclusive_scan(current_segment.begin(), current_segment.end(), current_segment.begin(), std::plus<uint32_t>{}, prev_row);
    prev_row = current_segment.empty() ? prev_row : current_segment.back();
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

void PostingListCodecSIMDComp::encode(
        const PostingListBuilder & postings, size_t posting_list_block_size, TokenPostingsInfo & info, WriteBuffer & out) const
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
