#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>
#include <Common/PODArray.h>

#include <roaring/roaring.hh>

namespace DB
{

static_assert(IPostingListEncoder::append_granularity % BLOCK_SIZE == 0,
    "append_granularity must be a multiple of the physical block size of the segmented posting list codec");

/// Returns `num_bytes` contiguous bytes read from `in` and advances it past them.
/// Points into the buffer of `in` if the data is already there, into `buffer` otherwise.
static const char * readContiguousBytes(ReadBuffer & in, size_t num_bytes, PaddedPODArray<char> & buffer)
{
    if (in.position() && static_cast<size_t>(in.buffer().end() - in.position()) >= num_bytes)
    {
        const char * data = in.position();
        in.position() += num_bytes;
        return data;
    }

    buffer.resize(num_bytes);
    in.readStrict(buffer.data(), num_bytes);
    return buffer.data();
}

SegmentedPostingListCodec::SegmentedPostingListCodec(IPostingListCodec::Type block_codec_type_)
    : block_codec(createPostingListBlockCodec(block_codec_type_))
{
    compressed_data.reserve(BLOCK_SIZE);
    block_values.reserve(BLOCK_SIZE);
}

/// Previous appends must not have left a partial block in the open segment.
/// (see the contract in `IPostingListEncoder::append_granularity`).
void SegmentedPostingListCodec::append(std::span<const UInt32> row_ids, size_t segment_size)
{
    chassert(!row_ids.empty());
    chassert(row_ids_in_current_segment % BLOCK_SIZE == 0);

    total_row_ids += row_ids.size();

    /// Split the input into segments of `segment_size` row ids, and each segment into blocks of BLOCK_SIZE.
    for (size_t pos = 0; pos < row_ids.size();)
    {
        if (row_ids_in_current_segment == 0)
        {
            auto & descriptor = segment_descriptors.emplace_back();
            descriptor.row_id_begin = row_ids[pos];
            descriptor.compressed_data_offset = compressed_data.size();
            segment_block_metas.emplace_back();
            prev_row_id = row_ids[pos];
        }

        const size_t rows_in_chunk = std::min(segment_size - row_ids_in_current_segment, row_ids.size() - pos);

        for (size_t offset = 0; offset < rows_in_chunk; offset += BLOCK_SIZE)
        {
            const size_t block_size = std::min(BLOCK_SIZE, rows_in_chunk - offset);
            encodeBlock(row_ids.subspan(pos + offset, block_size));
        }

        pos += rows_in_chunk;
        row_ids_in_current_segment += rows_in_chunk;

        /// Seal the full segment: the next chunk (or the next call) starts a new one.
        if (row_ids_in_current_segment == segment_size)
            row_ids_in_current_segment = 0;
    }
}

SegmentedPostingListCodec::SegmentData SegmentedPostingListCodec::readSegmentData(ReadBuffer & in, PaddedPODArray<char> & buffer)
{
    SegmentData segment_data;
    segment_data.header.read(in);

    /// The segment header is self-describing: create the block codec it was written with.
    block_codec = createPostingListBlockCodec(segment_data.header.codec_type);
    prev_row_id = segment_data.header.first_row_id;

    const char * payload_data = readContiguousBytes(in, segment_data.header.payload_bytes, buffer);
    segment_data.payload = std::span(reinterpret_cast<const std::byte *>(payload_data), segment_data.header.payload_bytes);
    return segment_data;
}

void SegmentedPostingListCodec::decode(ReadBuffer & in, PostingList & postings, PaddedPODArray<char> & buffer)
{
    auto segment_data = readSegmentData(in, buffer);

    const size_t num_blocks = segment_data.header.cardinality / BLOCK_SIZE;
    const size_t tail_size = segment_data.header.cardinality % BLOCK_SIZE;

    block_values.resize(BLOCK_SIZE);

    for (size_t i = 0; i < num_blocks; i++)
    {
        decodeBlock(segment_data.payload, std::span(block_values.data(), BLOCK_SIZE));
        postings.addMany(BLOCK_SIZE, block_values.data());
    }
    if (tail_size)
    {
        decodeBlock(segment_data.payload, std::span(block_values.data(), tail_size));
        postings.addMany(tail_size, block_values.data());
    }
}

void SegmentedPostingListCodec::decode(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<char> & buffer)
{
    auto segment_data = readSegmentData(in, buffer);

    const size_t num_blocks = segment_data.header.cardinality / BLOCK_SIZE;
    const size_t tail_size = segment_data.header.cardinality % BLOCK_SIZE;

    size_t out_pos = row_ids.size();
    row_ids.resize(out_pos + segment_data.header.cardinality);

    for (size_t i = 0; i < num_blocks; i++)
    {
        decodeBlock(segment_data.payload, std::span(row_ids.data() + out_pos, BLOCK_SIZE));
        out_pos += BLOCK_SIZE;
    }
    if (tail_size)
        decodeBlock(segment_data.payload, std::span(row_ids.data() + out_pos, tail_size));
}

void SegmentedPostingListCodec::serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const
{
    info.offsets.reserve(segment_descriptors.size());
    info.ranges.reserve(segment_descriptors.size());

    for (size_t seg_idx = 0; seg_idx < segment_descriptors.size(); ++seg_idx)
    {
        const auto & descriptor = segment_descriptors[seg_idx];
        info.offsets.emplace_back(out.count());
        info.ranges.emplace_back(descriptor.row_id_begin, descriptor.row_id_end);

        const auto & block_metas = segment_block_metas[seg_idx].metas;
        Header header(descriptor.compressed_data_size, descriptor.cardinality, descriptor.row_id_begin);

        header.write(out, block_codec->type());
        out.write(compressed_data.data() + descriptor.compressed_data_offset, descriptor.compressed_data_size);

        /// Index Section: append per-block metadata after segment payload.
        /// This allows PostingListCursor to binary-search for blocks without decoding the entire segment.
        /// The cursor reads header + payload + Index Section sequentially, so no offset storage is needed in the dictionary.
        writeVarUInt(block_metas.size(), out);

        for (const auto & meta : block_metas)
            writeVarUInt(meta.last_row_id, out);

        for (const auto & meta : block_metas)
            writeVarUInt(meta.relative_offset, out);
    }
}

void SegmentedPostingListCodec::encodeBlock(std::span<const UInt32> block_row_ids)
{
    chassert(block_codec);
    chassert(!block_row_ids.empty() && block_row_ids.size() <= BLOCK_SIZE);

    /// Compute the deltas into the scratch buffer.
    /// The first element written by `adjacent_difference` is the value itself,
    /// so adjust it to the delta from the last row id of the previous block.
    block_values.resize(block_row_ids.size());
    std::adjacent_difference(block_row_ids.begin(), block_row_ids.end(), block_values.begin());
    block_values[0] = block_row_ids.front() - prev_row_id;
    prev_row_id = block_row_ids.back();

    auto & segment_descriptor = segment_descriptors.back();
    segment_descriptor.cardinality += block_row_ids.size();
    segment_descriptor.row_id_end = block_row_ids.back();

    /// Record packed block metadata for V2 Index Section.
    auto & block_meta = segment_block_metas.back().metas.emplace_back();
    block_meta.last_row_id = block_row_ids.back();
    block_meta.relative_offset = compressed_data.size() - segment_descriptor.compressed_data_offset;

    block_codec->encodeBlock(block_values, compressed_data);

    segment_descriptor.compressed_data_size = compressed_data.size() - segment_descriptor.compressed_data_offset;
}

void SegmentedPostingListCodec::decodeBlock(std::span<const std::byte> & in, std::span<uint32_t> out)
{
    chassert(!out.empty() && out.size() <= BLOCK_SIZE);
    chassert(block_codec);

    /// `in` is the remaining segment payload: a full block self-delimits, and the final tail block sees exactly
    /// its own bytes remaining (the Index Section is not part of this buffer). We only need `in` advanced past it.
    block_codec->decodeBlock(in, out.size(), out);

    /// Restore the original array from the decompressed delta values.
    std::inclusive_scan(out.begin(), out.end(), out.begin(), std::plus<uint32_t>{}, prev_row_id);
    prev_row_id = out.back();
}

void SegmentedPostingListEncoder::finalize(WriteBuffer & out, TokenPostingsInfo & info)
{
    using enum PostingsSerialization::Flags;

    impl.encode(out, info);

    info.header |= IsCompressed;
    info.header |= HasBlockIndex;

    if (info.offsets.size() == 1)
        info.header |= SingleBlock;
}

void PostingListCodecBitpacking::decode(ReadBuffer & in, PostingList & postings, PaddedPODArray<char> & buffer) const
{
    SegmentedPostingListCodec impl;
    impl.decode(in, postings, buffer);
}

void PostingListCodecBitpacking::decode(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<char> & buffer) const
{
    SegmentedPostingListCodec impl;
    impl.decode(in, row_ids, buffer);
}

size_t PostingListCodecBitpacking::getSegmentSize(size_t posting_list_block_size) const
{
    return (posting_list_block_size + BLOCK_SIZE - 1) & ~(BLOCK_SIZE - 1);
}

std::unique_ptr<IPostingListEncoder> PostingListCodecBitpacking::createEncoder() const
{
    return std::make_unique<SegmentedPostingListEncoder>(IPostingListCodec::Type::Bitpacking);
}

void PostingListEncoderNone::append(std::span<const UInt32> row_ids, size_t segment_size)
{
    chassert(!row_ids.empty());
    total_row_ids += row_ids.size();

    while (!row_ids.empty())
    {
        auto chunk = row_ids.first(std::min(segment_size - rows_in_current_segment, row_ids.size()));
        row_ids = row_ids.subspan(chunk.size());

        current_segment.addMany(chunk.size(), chunk.data());
        rows_in_current_segment += chunk.size();

        if (rows_in_current_segment == segment_size)
            finishSegment();
    }
}

void PostingListEncoderNone::finishSegment()
{
    /// Reduces the in-memory and serialized size of the bitmap by using run containers.
    current_segment.runOptimize();
    segments.push_back(std::move(current_segment));
    current_segment = PostingList{};
    rows_in_current_segment = 0;
}

void PostingListEncoderNone::finalize(WriteBuffer & out, TokenPostingsInfo & info)
{
    using enum PostingsSerialization::Flags;

    if (rows_in_current_segment != 0)
        finishSegment();

    /// Local buffer freed after this call: a per-accumulator member would keep one buffer
    /// alive per token until the whole granule is serialized, inflating peak memory.
    std::vector<char> serialize_buffer;

    for (const auto & segment : segments)
    {
        info.offsets.emplace_back(out.count());
        info.ranges.emplace_back(segment.minimum(), segment.maximum());

        size_t num_bytes = roaring::api::roaring_bitmap_portable_size_in_bytes(&segment.roaring);
        writeVarUInt(num_bytes, out);

        serialize_buffer.resize(num_bytes);
        roaring::api::roaring_bitmap_portable_serialize(&segment.roaring, serialize_buffer.data());
        out.write(serialize_buffer.data(), num_bytes);
    }

    if (info.offsets.size() == 1)
        info.header |= SingleBlock;
}

size_t PostingListEncoderNone::memoryUsageBytes() const
{
    size_t result = current_segment.getSizeInBytes();
    for (const auto & segment : segments)
        result += segment.getSizeInBytes();
    return result;
}

std::unique_ptr<IPostingListEncoder> PostingListCodecNone::createEncoder() const
{
    return std::make_unique<PostingListEncoderNone>();
}

void PostingListCodecNone::decode(ReadBuffer & in, PostingList & postings, PaddedPODArray<char> & buffer) const
{
    size_t num_bytes = 0;
    readVarUInt(num_bytes, in);
    postings = PostingList::readSafe(readContiguousBytes(in, num_bytes, buffer), num_bytes);
}

void PostingListCodecNone::decode(ReadBuffer & in, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<char> & buffer) const
{
    PostingList postings;
    decode(in, postings, buffer);

    size_t old_size = row_ids.size();
    row_ids.resize(old_size + postings.cardinality());
    postings.toUint32Array(row_ids.data() + old_size);
}

}

