#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>

#include <roaring/roaring.hh>

namespace DB
{

static_assert(IPostingListEncoder::append_granularity % BLOCK_SIZE == 0,
    "append_granularity must be a multiple of the physical block size of the segmented posting list codec");

SegmentedPostingListCodec::SegmentedPostingListCodec(IPostingListCodec::Type block_codec_type_)
    : block_codec(createPostingListBlockCodec(block_codec_type_))
{
    compressed_data.reserve(BLOCK_SIZE);
    current_segment.reserve(BLOCK_SIZE);
}

/// Previous appends must not have left a partial block in the open segment
/// (see the contract in IPostingListEncoder::append_granularity).
void SegmentedPostingListCodec::append(std::span<const UInt32> row_ids, size_t segment_size)
{
    chassert(!row_ids.empty());
    chassert(row_ids_in_current_segment % BLOCK_SIZE == 0);

    total_row_ids += row_ids.size();

    while (!row_ids.empty())
    {
        if (row_ids_in_current_segment == 0)
        {
            segment_descriptors.emplace_back();
            segment_descriptors.back().row_id_begin = row_ids.front();
            segment_descriptors.back().compressed_data_offset = compressed_data.size();
            segment_block_metas.emplace_back();

            /// The first row id of a segment is encoded as a zero delta; the base value
            /// is written into the segment header (`first_row_id`) on serialization.
            prev_row_id = row_ids.front();
        }

        auto chunk = row_ids.first(std::min(segment_size - row_ids_in_current_segment, row_ids.size()));
        row_ids = row_ids.subspan(chunk.size());
        row_ids_in_current_segment += chunk.size();

        for (size_t offset = 0; offset < chunk.size(); offset += BLOCK_SIZE)
        {
            auto block = chunk.subspan(offset, std::min(BLOCK_SIZE, chunk.size() - offset));

            /// Compute deltas into the scratch buffer. The first element written by
            /// adjacent_difference is the value itself, so adjust it to the delta from
            /// the last row id of the previous block (zero for the first block of a segment).
            current_segment.resize(block.size());
            std::adjacent_difference(block.begin(), block.end(), current_segment.begin());
            current_segment[0] = block.front() - prev_row_id;
            prev_row_id = block.back();

            encodeBlock(current_segment);
        }

        /// Seal the full segment: the next chunk (or call) starts a new one.
        if (row_ids_in_current_segment == segment_size)
            row_ids_in_current_segment = 0;
    }

    current_segment.clear();
}

void SegmentedPostingListCodec::decode(ReadBuffer & in, PostingList & postings)
{
    Header header;
    header.read(in);

    /// The segment header is self-describing: create the block codec it was written with.
    block_codec = createPostingListBlockCodec(header.codec_type);

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
        decodeBlock(compressed_data_span, BLOCK_SIZE);
        postings.addMany(current_segment.size(), current_segment.data());
    }
    if (tail_size)
    {
        decodeBlock(compressed_data_span, tail_size);
        postings.addMany(current_segment.size(), current_segment.data());
    }
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

        Header header(descriptor.compressed_data_size, descriptor.cardinality, descriptor.row_id_begin);
        header.write(out, block_codec->type());
        out.write(compressed_data.data() + descriptor.compressed_data_offset, descriptor.compressed_data_size);

        /// Index Section: append per-block metadata after segment payload.
        /// This allows PostingListCursor to binary-search for blocks without
        /// decoding the entire segment. The cursor reads header + payload + Index Section
        /// sequentially, so no offset storage is needed in the dictionary.
        const auto & block_metas = segment_block_metas[seg_idx].metas;
        writeVarUInt(block_metas.size(), out);

        for (const auto & meta : block_metas)
            writeVarUInt(meta.last_row_id, out);

        for (const auto & meta : block_metas)
            writeVarUInt(meta.relative_offset, out);
    }
}

void SegmentedPostingListCodec::encodeBlock(std::span<uint32_t> segment)
{
    chassert(block_codec);
    auto & segment_descriptor = segment_descriptors.back();
    segment_descriptor.cardinality += segment.size();
    segment_descriptor.row_id_end = prev_row_id;

    /// Record packed block metadata for V2 Index Section.
    /// relative_offset is relative to the segment's compressed_data_offset.
    auto & block_metas = segment_block_metas.back().metas;
    block_metas.push_back(
    {
        prev_row_id,
        compressed_data.size() - segment_descriptor.compressed_data_offset
    });

    block_codec->encodeBlock(segment, compressed_data);

    segment_descriptor.compressed_data_size = compressed_data.size() - segment_descriptor.compressed_data_offset;
}

void SegmentedPostingListCodec::decodeBlock(std::span<const std::byte> & in, size_t count)
{
    chassert(count <= BLOCK_SIZE);
    chassert(block_codec);
    current_segment.resize(count);
    std::span<uint32_t> current_span(current_segment.data(), current_segment.size());

    /// `in` is the remaining segment payload: a full block self-delimits, and the final tail block sees exactly
    /// its own bytes remaining (the Index Section is not part of this buffer). We only need `in` advanced past it.
    block_codec->decodeBlock(in, count, current_span);

    /// Restore the original array from the decompressed delta values.
    std::inclusive_scan(current_segment.begin(), current_segment.end(), current_segment.begin(), std::plus<uint32_t>{}, prev_row_id);
    prev_row_id = current_segment.empty() ? prev_row_id : current_segment.back();
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

void PostingListCodecBitpacking::decode(ReadBuffer & in, PostingList & postings) const
{
    SegmentedPostingListCodec impl;
    impl.decode(in, postings);
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
        info.header |= PostingsSerialization::Flags::SingleBlock;
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

void PostingListCodecNone::decode(ReadBuffer & in, PostingList & postings) const
{
    size_t num_bytes = 0;
    readVarUInt(num_bytes, in);

    /// If the posting list is completely in the buffer, avoid copying.
    if (in.position() && in.position() + num_bytes <= in.buffer().end())
    {
        postings = PostingList::read(in.position());
        in.position() += num_bytes;
        return;
    }

    std::vector<char> buffer(num_bytes);
    in.readStrict(buffer.data(), num_bytes);
    postings = PostingList::read(buffer.data());
}

}

