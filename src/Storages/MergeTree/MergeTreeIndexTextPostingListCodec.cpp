#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>

#include <roaring/roaring.hh>

namespace DB
{

/// Normalize the requested block size to a multiple of BLOCK_SIZE.
/// We encode/decode posting lists in fixed-size blocks, and the SIMD bit-packing
/// implementation expects block-aligned sizes for efficient processing.
SegmentedPostingListCodecImpl::SegmentedPostingListCodecImpl(size_t postings_list_block_size, IPostingListCodec::Type block_codec_type_)
    : max_rowids_in_segment((postings_list_block_size + BLOCK_SIZE - 1) & ~(BLOCK_SIZE - 1))
    , block_codec(createPostingListBlockCodec(block_codec_type_))
{
    compressed_data.reserve(BLOCK_SIZE);
    current_segment.reserve(BLOCK_SIZE);
}

void SegmentedPostingListCodecImpl::insert(uint32_t row_id)
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

void SegmentedPostingListCodecImpl::insert(std::span<uint32_t> row_ids)
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

void SegmentedPostingListCodecImpl::decode(ReadBuffer & in, PostingList & postings)
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

void SegmentedPostingListCodecImpl::serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const
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

void SegmentedPostingListCodecImpl::encodeBlock(std::span<uint32_t> segment)
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

void SegmentedPostingListCodecImpl::decodeBlock(std::span<const std::byte> & in, size_t count)
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

namespace
{
/// Shared block-feeding loop for both block codecs: full BLOCK_SIZE chunks via the bulk insert, the
/// remaining tail one row id at a time, then flush. The on-disk segment/Index Section layout is identical;
/// only `block_codec_type` selects the per-block payload format.
void encodePostingsInBlocks(
    const PostingList & postings,
    size_t max_rowids_in_segment,
    IPostingListCodec::Type block_codec_type,
    TokenPostingsInfo & info,
    WriteBuffer & out)
{
    SegmentedPostingListCodecImpl impl(max_rowids_in_segment, block_codec_type);
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

void PostingListCodecBitpacking::decode(ReadBuffer & in, PostingList & postings) const
{
    SegmentedPostingListCodecImpl impl;
    impl.decode(in, postings);
}

void PostingListCodecBitpacking::encode(
        const PostingList & postings, size_t max_rowids_in_segment, TokenPostingsInfo & info, WriteBuffer & out) const
{
    encodePostingsInBlocks(postings, max_rowids_in_segment, IPostingListCodec::Type::Bitpacking, info, out);
}

void PostingListCodecFastPFOR::decode(ReadBuffer & in, PostingList & postings) const
{
    /// The segment header records the codec, so the same impl decodes FastPFOR payloads.
    SegmentedPostingListCodecImpl impl;
    impl.decode(in, postings);
}

void PostingListCodecFastPFOR::encode(
        const PostingList & postings, size_t max_rowids_in_segment, TokenPostingsInfo & info, WriteBuffer & out) const
{
    encodePostingsInBlocks(postings, max_rowids_in_segment, IPostingListCodec::Type::FastPFOR, info, out);
}
}

