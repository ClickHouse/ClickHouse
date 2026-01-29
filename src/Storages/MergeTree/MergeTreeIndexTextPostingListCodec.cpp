#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <roaring/roaring.hh>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

/// Normalize the requested block size to a multiple of POSTING_LIST_UNIT_SIZE.
/// We encode/decode posting lists in fixed-size blocks, and the SIMD bit-packing
/// implementation expects block-aligned sizes for efficient processing.
PostingListCodecBitpackingImpl::PostingListCodecBitpackingImpl(size_t postings_list_block_size, bool has_block_skip_index_)
    : max_rowids_in_segment((postings_list_block_size + POSTING_LIST_UNIT_SIZE - 1) & ~(POSTING_LIST_UNIT_SIZE - 1))
    , has_block_skip_index(has_block_skip_index_)
{
    compressed_data.reserve(POSTING_LIST_UNIT_SIZE);
    current_segment.reserve(POSTING_LIST_UNIT_SIZE);
}

void PostingListCodecBitpackingImpl::insert(uint32_t row_id)
{
    if (row_ids_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_id_begin = row_id;
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        current_segment.emplace_back(row_id);
        ++row_ids_in_current_segment;
        ++total_row_ids;
        return;
    }

    current_segment.emplace_back(row_id);
    ++row_ids_in_current_segment;
    ++total_row_ids;

    if (current_segment.size() == POSTING_LIST_UNIT_SIZE)
        encodeBlock(current_segment);

    if (row_ids_in_current_segment == max_rowids_in_segment)
        flushCurrentSegment();
}

void PostingListCodecBitpackingImpl::insert(std::span<uint32_t> row_ids)
{
    chassert(row_ids.size() == POSTING_LIST_UNIT_SIZE && row_ids_in_current_segment % POSTING_LIST_UNIT_SIZE == 0);

    if (row_ids_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_id_begin = row_ids.front();
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        total_row_ids += POSTING_LIST_UNIT_SIZE;
    }

    row_ids_in_current_segment += POSTING_LIST_UNIT_SIZE;
    encodeBlock(row_ids);

    if (row_ids_in_current_segment == max_rowids_in_segment)
        flushCurrentSegment();
}

void PostingListCodecBitpackingImpl::decode(ReadBuffer & in, PostingList & postings)
{
    Header header;
    header.read(in);
    has_block_skip_index = header.has_block_skip_index == 1;

    if (has_block_skip_index)
    {
        std::vector<uint32_t> block_row_ends;
        std::vector<size_t> offsets;
        CodecUtil::readArrayU32(in, block_row_ends);
        CodecUtil::readArrayU32(in, offsets);
    }

    const size_t num_blocks = header.cardinality / POSTING_LIST_UNIT_SIZE;
    const size_t tail_size = header.cardinality % POSTING_LIST_UNIT_SIZE;

    current_segment.reserve(POSTING_LIST_UNIT_SIZE);
    if (header.payload_bytes > (compressed_data.capacity() - compressed_data.size()))
        compressed_data.reserve(compressed_data.size() + header.payload_bytes);
    compressed_data.resize(header.payload_bytes);

    in.readStrict(compressed_data.data(), header.payload_bytes);

    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte*>(compressed_data.data()), compressed_data.size());
    for (size_t i = 0; i < num_blocks; i++)
    {
        decodeBlock(compressed_data_span, POSTING_LIST_UNIT_SIZE, current_segment);
        postings.addMany(current_segment.size(), current_segment.data());
    }
    if (tail_size)
    {
        decodeBlock(compressed_data_span, tail_size, current_segment);
        postings.addMany(current_segment.size(), current_segment.data());
    }
}

void PostingListCodecBitpackingImpl::serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const
{
    info.offsets.reserve(segment_descriptors.size());
    info.ranges.reserve(segment_descriptors.size());

    for (const auto & descriptor : segment_descriptors)
    {
        info.offsets.emplace_back(out.count());
        info.ranges.emplace_back(descriptor.row_id_begin, descriptor.row_id_end);
        Header header(descriptor.compressed_data_size, descriptor.cardinality, descriptor.row_id_begin, has_block_skip_index);
        header.write(out);

        if (has_block_skip_index)
        {
            CodecUtil::writeArrayU32(descriptor.block_row_ends, out);
            CodecUtil::writeArrayU32(descriptor.block_offsets, out);
        }
        out.write(compressed_data.data() + descriptor.compressed_data_offset, descriptor.compressed_data_size);
    }
}

void PostingListCodecBitpackingImpl::encodeBlock(std::span<uint32_t> segment)
{
    uint32_t row_begin = segment.front();
    uint32_t row_end = segment.back();

    auto & segment_descriptor = segment_descriptors.back();
    segment_descriptor.cardinality += segment.size();
    segment_descriptor.row_id_end = row_end;

    size_t offset = compressed_data.size();

    /// Record block index only when write_block_index is enabled (for lazy apply mode)
    if (has_block_skip_index)
    {
        segment_descriptor.block_row_ends.emplace_back(row_end);
        segment_descriptor.block_offsets.emplace_back(offset - segment_descriptor.compressed_data_offset);
    }

    ++segment_descriptor.block_count;

    std::adjacent_difference(segment.begin(), segment.end(), segment.begin());
    segment[0] -= row_begin;

    auto [needed_bytes_without_header, max_bits] = BitpackingBlockCodec::calculateNeededBytesAndMaxBits(segment);
    size_t remaining_memory = compressed_data.capacity() - compressed_data.size();
    size_t needed_bytes_with_header = needed_bytes_without_header + 1 + sizeof(uint32_t);
    if (remaining_memory < needed_bytes_with_header)
    {
        size_t min_need = needed_bytes_with_header - remaining_memory;
        compressed_data.reserve(compressed_data.size() + 2 * min_need);
    }
    /// Block Layout: [1byte(max_bits)][payload]
    compressed_data.resize(compressed_data.size() + needed_bytes_with_header);
    std::span<char> compressed_data_span(compressed_data.data() + offset, needed_bytes_with_header);
    CodecUtil::encodeU8(static_cast<uint8_t>(max_bits), compressed_data_span);
    CodecUtil::encodeU32(row_begin, compressed_data_span);
    auto used_memory = BitpackingBlockCodec::encode(segment, static_cast<uint32_t>(max_bits), compressed_data_span);
    chassert(used_memory == needed_bytes_without_header && compressed_data_span.empty());

    segment_descriptor.compressed_data_size = compressed_data.size() - segment_descriptor.compressed_data_offset;
    current_segment.clear();
}

void PostingListCodecBitpackingImpl::decodeBlock(
        std::span<const std::byte> & in, size_t count, std::vector<uint32_t> & current_segment)
{
    if (in.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected at least {} bytes, but got {}", 1, in.size());

    uint8_t bits = CodecUtil::decodeU8(in);
    uint32_t row_begin = CodecUtil::decodeU32(in);
    current_segment.clear();
    current_segment.resize(count);

    /// Decode postings to buffer named temp.
    std::span<uint32_t> current_span(current_segment.data(), current_segment.size());
    BitpackingBlockCodec::decode(in, count, bits, current_span);

    /// Restore the original array from the decompressed delta values.
    std::inclusive_scan(current_segment.begin(), current_segment.end(), current_segment.begin(), std::plus<uint32_t>{}, row_begin);
}

void PostingListCodecBitpacking::decode(ReadBuffer & in, PostingList & postings) const
{
    PostingListCodecBitpackingImpl impl;
    impl.decode(in, postings);
}

void PostingListCodecBitpacking::encode(
        const PostingListBuilder & postings, size_t max_rowids_in_segment, bool has_block_skip_index, TokenPostingsInfo & info, WriteBuffer & out) const
{
    PostingListCodecBitpackingImpl impl(max_rowids_in_segment, has_block_skip_index);
    if (postings.isLarge())
    {
        const auto & large_postings = postings.getLarge();
        std::vector<uint32_t> rowids;
        rowids.resize(large_postings.cardinality());
        large_postings.toUint32Array(rowids.data());

        std::span<uint32_t> rowids_view(rowids.data(), rowids.size());
        while (rowids_view.size() >= POSTING_LIST_UNIT_SIZE)
        {
            auto front = rowids_view.first(POSTING_LIST_UNIT_SIZE);
            impl.insert(front);
            rowids_view = rowids_view.subspan(POSTING_LIST_UNIT_SIZE);
        }

        if (!rowids_view.empty())
        {
            for (auto rowid: rowids_view)
                impl.insert(rowid);
        }
    }
    else
    {
        const auto & small_postings = postings.getSmall();
        for (size_t i = 0; i < postings.size(); ++i)
            impl.insert(small_postings[i]);
    }
    impl.encode(out, info);
}
}

