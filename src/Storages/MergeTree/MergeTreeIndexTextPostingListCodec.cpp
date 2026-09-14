#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>
#include <Common/PODArray.h>

#include <roaring/roaring.hh>

namespace DB
{

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

/// Normalize the requested block size to a multiple of BLOCK_SIZE.
/// We encode/decode posting lists in fixed-size blocks, and the SIMD bit-packing
/// implementation expects block-aligned sizes for efficient processing.
SegmentedPostingListCodec::SegmentedPostingListCodec(size_t postings_list_block_size, IPostingListCodec::Type block_codec_type_)
    : max_rowids_in_segment((postings_list_block_size + BLOCK_SIZE - 1) & ~(BLOCK_SIZE - 1))
    , block_codec(createPostingListBlockCodec(block_codec_type_))
{
    compressed_data.reserve(BLOCK_SIZE);
    current_segment.reserve(BLOCK_SIZE);
}

void SegmentedPostingListCodec::insert(uint32_t row_id)
{
    if (row_ids_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_id_begin = row_id;
        segment_descriptors.back().compressed_data_offset = compressed_data.size();
        segment_block_metas.emplace_back();

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

void SegmentedPostingListCodec::insert(std::span<uint32_t> row_ids)
{
    chassert(row_ids.size() == BLOCK_SIZE && row_ids_in_current_segment % BLOCK_SIZE == 0);

    if (row_ids_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_id_begin = row_ids.front();
        segment_descriptors.back().compressed_data_offset = compressed_data.size();
        segment_block_metas.emplace_back();

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

    current_segment.resize(BLOCK_SIZE);

    for (size_t i = 0; i < num_blocks; i++)
    {
        decodeBlock(segment_data.payload, std::span(current_segment.data(), BLOCK_SIZE));
        postings.addMany(BLOCK_SIZE, current_segment.data());
    }
    if (tail_size)
    {
        decodeBlock(segment_data.payload, std::span(current_segment.data(), tail_size));
        postings.addMany(tail_size, current_segment.data());
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

namespace
{

/// Shared block-feeding loop for all block codecs: full BLOCK_SIZE chunks via the bulk insert,
/// the remaining tail one row id at a time, then flush.
/// The on-disk segment/Index Section layout is identical; only `block_codec_type` selects the per-block payload format.
void encodePostingsInBlocks(
    const PostingList & postings,
    size_t max_rowids_in_segment,
    IPostingListCodec::Type block_codec_type,
    TokenPostingsInfo & info,
    WriteBuffer & out)
{
    SegmentedPostingListCodec impl(max_rowids_in_segment, block_codec_type);
    std::vector<uint32_t> rowids;
    rowids.resize(postings.cardinality());
    postings.toUint32Array(rowids.data());

    std::span<uint32_t> rowids_view(rowids.data(), rowids.size());
    while (rowids_view.size() >= BLOCK_SIZE)
    {
        auto front = rowids_view.first(BLOCK_SIZE);
        impl.insert(front);
        rowids_view = rowids_view.subspan(BLOCK_SIZE);
    }

    if (!rowids_view.empty())
    {
        for (auto rowid: rowids_view)
            impl.insert(rowid);
    }
    impl.encode(out, info);
}

}

void PostingListCodecBitpacking::encode(const PostingList & postings, size_t max_rowids_in_segment, TokenPostingsInfo & info, WriteBuffer & out) const
{
    encodePostingsInBlocks(postings, max_rowids_in_segment, IPostingListCodec::Type::Bitpacking, info, out);
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

