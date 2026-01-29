#include <config.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <roaring/roaring.hh>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{
void writeByte(uint8_t x, std::span<char> & out)
{
    out[0] = static_cast<char>(x);
    out = out.subspan(1);
}

uint8_t readByte(std::span<const std::byte> & in)
{
    auto v = static_cast<uint8_t>(in[0]);
    in = in.subspan(1);
    return v;
}
}

template <typename BlockCodec, IPostingListCodec::Type codec_type>
PostingListCodecBlockImpl<BlockCodec, codec_type>::PostingListCodecBlockImpl(size_t postings_list_block_size)
    : max_rowids_in_segment((postings_list_block_size + BLOCK_SIZE - 1) & ~(BLOCK_SIZE - 1))
{
    compressed_data.reserve(BLOCK_SIZE);
    current_segment.reserve(BLOCK_SIZE);
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

    if (current_segment.size() == BLOCK_SIZE)
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
    chassert(row_ids.size() == BLOCK_SIZE && row_ids_in_current_segment % BLOCK_SIZE == 0);

    if (row_ids_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_id_begin = row_ids.front();
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        prev_row_id = row_ids.front();
    }
    row_ids_in_current_segment += BLOCK_SIZE;
    total_row_ids += BLOCK_SIZE;

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
    writeByte(static_cast<uint8_t>(max_bits), compressed_data_span);
    auto used_memory = BlockCodec::encode(segment, max_bits, compressed_data_span);

    /// For BlockCodecs like Bitpacking, we expect exact size match.
    /// For FastPFor, actual size may differ from estimate - resize to actual.
    size_t actual_total = 1 + used_memory;  // 1 byte header + payload
    if (actual_total != needed_bytes_with_header)
    {
        compressed_data.resize(offset + actual_total);
    }

    segment_descriptor.compressed_data_size = compressed_data.size() - segment_descriptor.compressed_data_offset;
}

template <typename BlockCodec, IPostingListCodec::Type codec_type>
void PostingListCodecBlockImpl<BlockCodec, codec_type>::decodeBlock(
        std::span<const std::byte> & in, size_t count, uint32_t & prev_row_id, std::vector<uint32_t> & current_segment)
{
    chassert(count <= BLOCK_SIZE);
    if (in.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected at least {} bytes, but got {}", 1, in.size());

    uint8_t bits = readByte(in);
    if (bits > 32)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected bits <= 32, but got {}", bits);
    current_segment.resize(count);

    size_t required_size = BlockCodec::bitpackingCompressedBytes(count, bits);
    if (in.size() < required_size)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected data size {}, but got {}", required_size, in.size());

    std::span<uint32_t> current_span(current_segment.data(), current_segment.size());
    BlockCodec::decode(in, count, bits, current_span);

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

/// Simple8b instantiation - high compression for small deltas
template class PostingListCodecBlockImpl<Simple8bBlockCodec, IPostingListCodec::Type::Simple8b>;

/// StreamVByte instantiation - fast streaming decode
template class PostingListCodecBlockImpl<StreamVByteBlockCodec, IPostingListCodec::Type::StreamVByte>;

/// OptPFor instantiation - highest compression ratio
template class PostingListCodecBlockImpl<SIMDOptPForBlockCodec, IPostingListCodec::Type::OptPFor>;
#endif

}

