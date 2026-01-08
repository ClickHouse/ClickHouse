#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/PostingListCompression.h>

#include <roaring/roaring.hh>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

/// Normalize the requested block size to a multiple of BLOCK_SIZE.
/// We encode/decode posting lists in fixed-size blocks, and the SIMD bit-packing
/// implementation expects block-aligned sizes for efficient processing.
PostingListCodecSIMDCompImpl::PostingListCodecSIMDCompImpl(size_t postings_list_block_size)
    : posting_list_block_size((postings_list_block_size + BLOCK_SIZE - 1) & ~(BLOCK_SIZE - 1))
{
    compressed_data.reserve(BLOCK_SIZE);
    current_segment.reserve(BLOCK_SIZE);
}

void PostingListCodecSIMDCompImpl::insert(uint32_t row_id)
{
    if (rows_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_id_begin = row_id;
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        prev_row_id = row_id;
        current_segment.emplace_back(row_id - prev_row_id);
        ++rows_in_current_segment;
        ++total_rows;
        return;
    }

    current_segment.emplace_back(row_id - prev_row_id);
    prev_row_id = row_id;
    ++rows_in_current_segment;
    ++total_rows;

    if (current_segment.size() == BLOCK_SIZE)
        compressBlock(current_segment);

    if (rows_in_current_segment == posting_list_block_size)
        flushCurrentSegment();
}

void PostingListCodecSIMDCompImpl::insert(std::span<uint32_t> row_ids)
{
    chassert(row_ids.size() == BLOCK_SIZE && rows_in_current_segment % BLOCK_SIZE == 0);

    if (rows_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_id_begin = row_ids.front();
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        prev_row_id = row_ids.front();
        rows_in_current_segment += BLOCK_SIZE;
        total_rows += BLOCK_SIZE;
    }

    auto last_row = row_ids.back();
    std::adjacent_difference(row_ids.begin(), row_ids.end(), row_ids.begin());
    row_ids[0] -= prev_row_id;
    prev_row_id = last_row;

    compressBlock(row_ids);

    if (rows_in_current_segment == posting_list_block_size)
        flushCurrentSegment();
}

void PostingListCodecSIMDCompImpl::decode(ReadBuffer & in, PostingList & postings)
{
    Header header;
    header.read(in);
    if (header.codec_type != static_cast<uint8_t>(codec_type))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected codec type {}, but got {}", codec_type, header.codec_type);

    prev_row_id = header.first_row_id;

    const size_t num_blocks = header.cardinality / BLOCK_SIZE;
    const size_t tail_size = header.cardinality % BLOCK_SIZE;

    current_segment.reserve(BLOCK_SIZE);
    if (header.payload_bytes > (compressed_data.capacity() - compressed_data.size()))
        compressed_data.reserve(compressed_data.size() + header.payload_bytes);
    compressed_data.resize(header.payload_bytes);

    in.readStrict(compressed_data.data(), header.payload_bytes);

    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte*>(compressed_data.data()), compressed_data.size());
    for (size_t i = 0; i < num_blocks; i++)
    {
        decodeBlock(compressed_data_span, BLOCK_SIZE, prev_row_id, current_segment);
        postings.addMany(current_segment.size(), current_segment.data());
    }
    if (tail_size)
    {
        decodeBlock(compressed_data_span, tail_size, prev_row_id, current_segment);
        postings.addMany(current_segment.size(), current_segment.data());
    }
}

void PostingListCodecSIMDCompImpl::serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const
{
    info.offsets.reserve(segment_descriptors.size());
    info.ranges.reserve(segment_descriptors.size());

    for (const auto & descriptor : segment_descriptors)
    {
        info.offsets.emplace_back(out.count());
        info.ranges.emplace_back(descriptor.row_id_begin, descriptor.row_id_end);
        Header header(static_cast<uint8_t>(codec_type), descriptor.compressed_data_size, descriptor.cardinality, descriptor.row_id_begin);
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

void PostingListCodecSIMDCompImpl::compressBlock(std::span<uint32_t> segment)
{
    auto & segment_descriptor = segment_descriptors.back();
    segment_descriptor.cardinality += segment.size();
    segment_descriptor.row_id_end = prev_row_id;

    auto [needed_bytes_without_header, max_bits] = BlockCodec::calculateNeededBytesAndMaxBits(segment);
    size_t remaining_memory = compressed_data.capacity() - compressed_data.size();
    size_t needed_bytes_with_header = needed_bytes_without_header + 1;
    if (remaining_memory < needed_bytes_with_header)
    {
        size_t min_need = needed_bytes_with_header - remaining_memory;
        compressed_data.reserve(compressed_data.size() + 2 * min_need);
    }
    /// Block Layout: [1byte(max_bits)][payload]
    size_t offset = compressed_data.size();
    compressed_data.resize(compressed_data.size() + needed_bytes_with_header);
    std::span<char> compressed_data_span(compressed_data.data() + offset, needed_bytes_with_header);
    encodeU8(max_bits, compressed_data_span);
    auto used_memory = BlockCodec::encode(segment, max_bits, compressed_data_span);
    chassert(used_memory == needed_bytes_without_header && compressed_data_span.empty());

    segment_descriptor.compressed_data_size = compressed_data.size() - segment_descriptor.compressed_data_offset;
    current_segment.clear();
}

void PostingListCodecSIMDCompImpl::decodeBlock(
        std::span<const std::byte> & in, size_t count, uint32_t & prev_row_id, std::vector<uint32_t> & current_segment)
{
    if (in.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected at least {} bytes, but got {}", 1, in.size());

    uint8_t bits = decodeU8(in);
    current_segment.resize(count);

    /// Decode postings to buffer named temp.
    std::span<uint32_t> current_span(current_segment.data(), current_segment.size());
    BlockCodec::decode(in, count, bits, current_span);

    /// Restore the original array from the decompressed delta values.
    std::inclusive_scan(current_segment.begin(), current_segment.end(), current_segment.begin(), std::plus<uint32_t>{}, prev_row_id);
    prev_row_id = current_segment.empty() ? prev_row_id : current_segment.back();
}

void PostingListCodecSIMDComp::decode(ReadBuffer & in, PostingList & postings) const
{
    PostingListCodecSIMDCompImpl impl;
    impl.decode(in, postings);
}

void PostingListCodecSIMDComp::encode(
        const PostingListBuilder & postings, size_t posting_list_block_size, TokenPostingsInfo & info, WriteBuffer & out) const
{
    PostingListCodecSIMDCompImpl impl(posting_list_block_size);

    if (postings.isSmall())
    {
        const auto & small_postings = postings.getSmall();
        size_t num_postings = postings.size();
        for (size_t i = 0; i < num_postings; ++i)
            impl.insert(small_postings[i]);
    }
    else
    {
        std::vector<uint32_t> rowids;
        rowids.resize(postings.size());
        const auto & large_postings = postings.getLarge();
        large_postings.toUint32Array(rowids.data());

        std::span<uint32_t> rowids_view(rowids.data(), rowids.size());
        while (rowids_view.size() >= BLOCK_SIZE)
        {
            auto front = rowids_view.first(BLOCK_SIZE);
            impl.insert(front);
            rowids_view = rowids_view.subspan(BLOCK_SIZE);
        }

        if (!rowids_view.empty())
        {
            for (auto rowid : rowids_view)
                impl.insert(rowid);
        }
    }

    impl.encode(out, info);
}
}

