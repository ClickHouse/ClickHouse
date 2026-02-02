#include <config.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <roaring/roaring.hh>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

template <typename BlockCodec, IPostingListCodec::Type codec_type>
PostingListCodecBlockImpl<BlockCodec, codec_type>::PostingListCodecBlockImpl(size_t postings_list_block_size)
    : max_rowids_in_segment((postings_list_block_size + BlockCodec::BLOCK_SIZE - 1) & ~(BlockCodec::BLOCK_SIZE - 1))
{
    compressed_data.reserve(BlockCodec::BLOCK_SIZE);
    current_segment.reserve(BlockCodec::BLOCK_SIZE);
}

template <typename BlockCodec, IPostingListCodec::Type codec_type>
void PostingListCodecBlockImpl<BlockCodec, codec_type>::insert(uint32_t row_id)
{
    if (row_ids_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_id_begin = row_id;
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        prev_row_id = row_id;
        current_segment.emplace_back(row_id - prev_row_id);
        ++row_ids_in_current_segment;
        ++total_row_ids;
        return;
    }

    current_segment.emplace_back(row_id - prev_row_id);
    prev_row_id = row_id;
    ++row_ids_in_current_segment;
    ++total_row_ids;

    if (current_segment.size() == BlockCodec::BLOCK_SIZE)
    {
        encodeBlock(current_segment);
        current_segment.clear();
    }

    if (row_ids_in_current_segment == max_rowids_in_segment)
        flushCurrentSegment();
}

template <typename BlockCodec, IPostingListCodec::Type codec_type>
void PostingListCodecBlockImpl<BlockCodec, codec_type>::insert(std::span<uint32_t> row_ids)
{
    chassert(row_ids.size() == BlockCodec::BLOCK_SIZE && row_ids_in_current_segment % BlockCodec::BLOCK_SIZE == 0);

    if (row_ids_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_id_begin = row_ids.front();
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        prev_row_id = row_ids.front();
    }
    row_ids_in_current_segment += BlockCodec::BLOCK_SIZE;
    total_row_ids += BlockCodec::BLOCK_SIZE;

    auto last_row = row_ids.back();
    std::adjacent_difference(row_ids.begin(), row_ids.end(), row_ids.begin());
    row_ids[0] -= prev_row_id;
    prev_row_id = last_row;

    encodeBlock(row_ids);

    if (row_ids_in_current_segment == max_rowids_in_segment)
        flushCurrentSegment();
}

template <typename BlockCodec, IPostingListCodec::Type codec_type>
void PostingListCodecBlockImpl<BlockCodec, codec_type>::decode(ReadBuffer & in, PostingList & postings)
{
    Header header;
    header.read(in);

    prev_row_id = header.first_row_id;

    const size_t num_blocks = header.cardinality / BlockCodec::BLOCK_SIZE;
    const size_t tail_size = header.cardinality % BlockCodec::BLOCK_SIZE;

    current_segment.reserve(BlockCodec::BLOCK_SIZE);
    if (header.payload_bytes > (compressed_data.capacity() - compressed_data.size()))
        compressed_data.reserve(compressed_data.size() + header.payload_bytes);
    compressed_data.resize(header.payload_bytes);

    in.readStrict(compressed_data.data(), header.payload_bytes);

    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte*>(compressed_data.data()), compressed_data.size());
    for (size_t i = 0; i < num_blocks; i++)
    {
        decodeBlock(compressed_data_span, BlockCodec::BLOCK_SIZE, prev_row_id, current_segment);
        postings.addMany(current_segment.size(), current_segment.data());
    }
    if (tail_size)
    {
        decodeBlock(compressed_data_span, tail_size, prev_row_id, current_segment);
        postings.addMany(current_segment.size(), current_segment.data());
    }
}

template <typename BlockCodec, IPostingListCodec::Type codec_type>
void PostingListCodecBlockImpl<BlockCodec, codec_type>::serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const
{
    info.offsets.reserve(segment_descriptors.size());
    info.ranges.reserve(segment_descriptors.size());

    for (const auto & descriptor : segment_descriptors)
    {
        info.offsets.emplace_back(out.count());
        info.ranges.emplace_back(descriptor.row_id_begin, descriptor.row_id_end);
        Header header(descriptor.compressed_data_size, descriptor.cardinality, descriptor.row_id_begin);
        header.write(out);
        out.write(compressed_data.data() + descriptor.compressed_data_offset, descriptor.compressed_data_size);
    }
}

template <typename BlockCodec, IPostingListCodec::Type codec_type>
void PostingListCodecBlockImpl<BlockCodec, codec_type>::encodeBlock(std::span<uint32_t> segment)
{
    auto & segment_descriptor = segment_descriptors.back();
    segment_descriptor.cardinality += segment.size();
    segment_descriptor.row_id_end = prev_row_id;

    auto needed_bytes = BlockCodec::calculateNeededBytes(segment);
    size_t remaining_memory = compressed_data.capacity() - compressed_data.size();
    if (remaining_memory < needed_bytes)
    {
        size_t min_need = needed_bytes - remaining_memory;
        compressed_data.reserve(compressed_data.size() + 2 * min_need);
    }

    size_t offset = compressed_data.size();
    compressed_data.resize(compressed_data.size() + needed_bytes);
    std::span<char> compressed_data_span(compressed_data.data() + offset, needed_bytes);

    // Let BlockCodec handle its own encoding format (including any headers)
    auto used_memory = BlockCodec::encode(segment, compressed_data_span);

    /// For some BlockCodecs, actual size may differ from estimate - resize to actual.
    if (used_memory != needed_bytes)
    {
        compressed_data.resize(offset + used_memory);
    }

    segment_descriptor.compressed_data_size = compressed_data.size() - segment_descriptor.compressed_data_offset;
}

template <typename BlockCodec, IPostingListCodec::Type codec_type>
void PostingListCodecBlockImpl<BlockCodec, codec_type>::decodeBlock(
        std::span<const std::byte> & in, size_t count, uint32_t & prev_row_id, std::vector<uint32_t> & current_segment)
{
    chassert(count <= BlockCodec::BLOCK_SIZE);
    if (in.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected at least {} bytes, but got {}", 1, in.size());

    current_segment.resize(count);
    std::span<uint32_t> current_span(current_segment.data(), current_segment.size());

    // Let BlockCodec handle its own decoding format (including any headers)
    BlockCodec::decode(in, count, current_span);

    /// Restore the original array from the decompressed delta values.
    std::inclusive_scan(current_segment.begin(), current_segment.end(), current_segment.begin(), std::plus<uint32_t>{}, prev_row_id);
    prev_row_id = current_segment.empty() ? prev_row_id : current_segment.back();
}

/// Explicit template instantiations

/// Bitpacking instantiation
template class PostingListCodecBlockImpl<BitpackingBlockCodec, IPostingListCodec::Type::Bitpacking>;

#if USE_FASTPFOR
/// FastPFor instantiation
template class PostingListCodecBlockImpl<SIMDFastPForBlockCodec, IPostingListCodec::Type::FastPFor>;

/// BinaryPacking instantiation - fastest decode speed
template class PostingListCodecBlockImpl<SIMDBinaryPackingBlockCodec, IPostingListCodec::Type::BinaryPacking>;

/// OptPFor instantiation - highest compression ratio
template class PostingListCodecBlockImpl<SIMDOptPForBlockCodec, IPostingListCodec::Type::OptPFor>;
#endif

}

