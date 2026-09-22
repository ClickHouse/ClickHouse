#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>
#include <Common/PODArray.h>

#include <roaring/roaring.hh>

#include <algorithm>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int SUPPORT_IS_DISABLED;
}

static_assert(IPostingListEncoder::append_granularity % IPostingListBlockCodec::BLOCK_SIZE == 0,
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

SegmentedPostingListCodec::SegmentedPostingListCodec(IPostingListCodec::Type block_codec_type_, size_t segment_size_)
    : segment_size((segment_size_ + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE)
    , block_codec(createPostingListBlockCodec(block_codec_type_))
{
    compressed_data.reserve(BLOCK_SIZE);
    block_values.reserve(BLOCK_SIZE);
}

/// Previous appends must not have left a partial block in the open segment.
/// (see the contract in `IPostingListEncoder::append_granularity`).
void SegmentedPostingListCodec::append(
    std::span<const UInt32> row_ids,
    std::span<const UInt32> tf_minus_one,
    const PostingListBuildContext & context)
{
    chassert(!row_ids.empty());
    chassert(segment_size != 0);
    chassert(row_ids_in_current_segment % BLOCK_SIZE == 0);
    chassert(tf_minus_one.empty() || (context.enable_scoring && tf_minus_one.size() == row_ids.size()));

    std::span<const UInt8> doc_lengths;
    if (context.enable_scoring)
    {
        has_scoring = true;
        chassert(context.doc_lengths && !context.doc_lengths->empty());
        doc_lengths = *context.doc_lengths;
    }

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
            const std::span<const UInt32> block_tf_minus_one = tf_minus_one.empty() ? tf_minus_one : tf_minus_one.subspan(pos + offset, block_size);

            encodeBlock(
                row_ids.subspan(pos + offset, block_size),
                block_tf_minus_one,
                doc_lengths,
                context.doc_lengths_first_row_id);
        }

        pos += rows_in_chunk;
        row_ids_in_current_segment += rows_in_chunk;

        /// Seal the full segment: the next chunk (or the next call) starts a new one.
        if (row_ids_in_current_segment == segment_size)
            row_ids_in_current_segment = 0;
    }
}

SegmentedPostingListCodec::SegmentData SegmentedPostingListCodec::readSegmentData(ReadBuffer & in, UInt64 max_cardinality, bool has_term_frequencies, PaddedPODArray<char> & buffer)
{
    SegmentData segment_data;
    auto & header = segment_data.header;
    header.read(in, has_term_frequencies);

    /// The segment header is self-describing: create the block codec it was written with.
    block_codec = createPostingListBlockCodec(header.codec_type);
    prev_row_id = header.first_row_id;

    /// The header comes from disk: bound the sizes it claims before growing any buffer to them.
    if (header.cardinality == 0 || header.cardinality > max_cardinality)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted posting list segment: cardinality {} is not in the range [1, {}] allowed by the token metadata",
            header.cardinality, max_cardinality);
    }

    /// With term frequencies every block is followed by a second block of the same codec.
    const UInt64 max_blocks = (header.cardinality + BLOCK_SIZE - 1) / BLOCK_SIZE;
    const UInt64 max_block_bytes = has_term_frequencies ? 2 * block_codec->maxBlockBytes() : block_codec->maxBlockBytes();
    const UInt64 max_payload_bytes = max_blocks * max_block_bytes;

    if (header.payload_bytes > max_payload_bytes)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted posting list segment: payload of {} bytes exceeds the upper bound of {} bytes for {} row ids",
            header.payload_bytes, max_payload_bytes, header.cardinality);
    }

    const char * payload_data = readContiguousBytes(in, header.payload_bytes, buffer);
    segment_data.payload = std::span(reinterpret_cast<const std::byte *>(payload_data), header.payload_bytes);
    return segment_data;
}

void SegmentedPostingListCodec::decode(ReadBuffer & in, UInt64 max_cardinality, PostingList & postings, bool has_term_frequencies, PaddedPODArray<char> & buffer)
{
    auto segment_data = readSegmentData(in, max_cardinality, has_term_frequencies, buffer);

    const size_t num_blocks = segment_data.header.cardinality / BLOCK_SIZE;
    const size_t tail_size = segment_data.header.cardinality % BLOCK_SIZE;

    block_values.resize(BLOCK_SIZE);

    for (size_t i = 0; i < num_blocks; i++)
    {
        decodeBlock(segment_data.payload, std::span(block_values.data(), BLOCK_SIZE));
        if (has_term_frequencies)
            skipTermFrequencies(segment_data.payload, BLOCK_SIZE);
        postings.addMany(BLOCK_SIZE, block_values.data());
    }
    if (tail_size)
    {
        decodeBlock(segment_data.payload, std::span(block_values.data(), tail_size));
        if (has_term_frequencies)
            skipTermFrequencies(segment_data.payload, tail_size);
        postings.addMany(tail_size, block_values.data());
    }
}

void SegmentedPostingListCodec::decode(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, bool has_term_frequencies, PaddedPODArray<char> & buffer)
{
    auto segment_data = readSegmentData(in, max_cardinality, has_term_frequencies, buffer);

    const size_t num_blocks = segment_data.header.cardinality / BLOCK_SIZE;
    const size_t tail_size = segment_data.header.cardinality % BLOCK_SIZE;

    size_t out_pos = row_ids.size();
    row_ids.resize(out_pos + segment_data.header.cardinality);

    for (size_t i = 0; i < num_blocks; i++)
    {
        decodeBlock(segment_data.payload, std::span(row_ids.data() + out_pos, BLOCK_SIZE));
        if (has_term_frequencies)
            skipTermFrequencies(segment_data.payload, BLOCK_SIZE);
        out_pos += BLOCK_SIZE;
    }
    if (tail_size)
    {
        decodeBlock(segment_data.payload, std::span(row_ids.data() + out_pos, tail_size));
        if (has_term_frequencies)
            skipTermFrequencies(segment_data.payload, tail_size);
    }
}

void SegmentedPostingListCodec::decodeWithTermFrequencies(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & tfs, PaddedPODArray<char> & buffer)
{
    /// This entry point is used only when the posting list carries term frequencies.
    auto segment_data = readSegmentData(in, max_cardinality, /*has_term_frequencies=*/true, buffer);

    const size_t num_blocks = segment_data.header.cardinality / BLOCK_SIZE;
    const size_t tail_size = segment_data.header.cardinality % BLOCK_SIZE;

    size_t out_pos = row_ids.size();
    row_ids.resize(out_pos + segment_data.header.cardinality);

    for (size_t i = 0; i < num_blocks; i++)
    {
        decodeBlock(segment_data.payload, std::span(row_ids.data() + out_pos, BLOCK_SIZE));
        decodeTermFrequencies(segment_data.payload, BLOCK_SIZE, tfs);
        out_pos += BLOCK_SIZE;
    }
    if (tail_size)
    {
        decodeBlock(segment_data.payload, std::span(row_ids.data() + out_pos, tail_size));
        decodeTermFrequencies(segment_data.payload, tail_size, tfs);
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

        const auto & block_metas = segment_block_metas[seg_idx].metas;
        Header header(descriptor.compressed_data_size, descriptor.cardinality, descriptor.row_id_begin);

        if (has_scoring)
        {
            /// Reduce per-block UB inputs into the per-segment pair stored in the header.
            header.segment_min_dl_byte = 0xFF;
            header.segment_max_tf_minus_one = 0;

            for (const auto & meta : block_metas)
            {
                header.segment_min_dl_byte = std::min(header.segment_min_dl_byte, meta.min_dl_byte);
                header.segment_max_tf_minus_one = std::max(header.segment_max_tf_minus_one, meta.max_tf_minus_one);
            }
        }

        header.write(out, block_codec->type(), has_scoring);
        out.write(compressed_data.data() + descriptor.compressed_data_offset, descriptor.compressed_data_size);

        /// Index Section: append per-block metadata after segment payload.
        /// This allows PostingListCursor to binary-search for blocks without decoding the entire segment.
        /// The cursor reads header + payload + Index Section sequentially, so no offset storage is needed in the dictionary.
        writeVarUInt(block_metas.size(), out);

        for (const auto & meta : block_metas)
            writeVarUInt(meta.last_row_id, out);

        for (const auto & meta : block_metas)
            writeVarUInt(meta.relative_offset, out);

        /// When scoring is on, the per-block block-max UB arrays follow `block_offsets[]`
        /// as two raw `UInt8` arrays: `min_dl_byte[]` then `max_tf_minus_one[]`.
        if (has_scoring)
        {
            for (const auto & meta : block_metas)
                writeBinaryLittleEndian(meta.min_dl_byte, out);

            for (const auto & meta : block_metas)
                writeBinaryLittleEndian(meta.max_tf_minus_one, out);
        }
    }
}

void SegmentedPostingListCodec::encodeBlock(
    std::span<const UInt32> block_row_ids,
    std::span<const UInt32> term_frequencies,
    std::span<const UInt8> doc_lengths,
    UInt32 doc_lengths_first_row_id)
{
    chassert(block_codec);
    chassert(!block_row_ids.empty() && block_row_ids.size() <= BLOCK_SIZE);
    chassert(term_frequencies.empty() || term_frequencies.size() == block_row_ids.size());
    chassert(has_scoring == !doc_lengths.empty());

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

    /// The scoring extras. The block-max UB inputs:
    /// - a min over the block's `SmallFloat` doc-length bytes
    /// - a max `(tf - 1)` saturated to `255`, where `255` means "max tf >= 256"
    if (has_scoring)
    {
        UInt8 min_dl_byte = 0xFF;

        for (UInt32 row_id : block_row_ids)
        {
            chassert(row_id >= doc_lengths_first_row_id);
            const size_t doc_lengths_index = row_id - doc_lengths_first_row_id;
            chassert(doc_lengths_index < doc_lengths.size());
            min_dl_byte = std::min(min_dl_byte, doc_lengths[doc_lengths_index]);
        }

        const UInt32 max_tf_minus_one = term_frequencies.empty() ? 0 : std::ranges::max(term_frequencies);

        block_meta.min_dl_byte = min_dl_byte;
        block_meta.max_tf_minus_one = static_cast<UInt8>(std::min<UInt32>(255, max_tf_minus_one));
        encodeTermFrequencies(term_frequencies, block_row_ids.size());
    }

    segment_descriptor.compressed_data_size = compressed_data.size() - segment_descriptor.compressed_data_offset;
}

void SegmentedPostingListCodec::encodeTermFrequencies(std::span<const UInt32> term_frequencies, size_t count)
{
    chassert(block_codec);
    chassert(count > 0 && count <= BLOCK_SIZE);
    chassert(term_frequencies.empty() || term_frequencies.size() == count);

    /// An empty `term_frequencies` means every `(tf - 1)` in the block is 0, which the codec encodes in its own all-zero form.
    if (term_frequencies.empty())
        block_codec->encodeZeros(count, compressed_data);
    else
        block_codec->encodeBlock(term_frequencies, compressed_data);
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

void SegmentedPostingListCodec::skipTermFrequencies(std::span<const std::byte> & in, size_t count)
{
    chassert(count <= BLOCK_SIZE);
    chassert(block_codec);
    block_codec->skipBlock(in, count);
}

void SegmentedPostingListCodec::decodeTermFrequencies(std::span<const std::byte> & in, size_t count, PaddedPODArray<UInt32> & tfs)
{
    chassert(count <= BLOCK_SIZE);
    chassert(block_codec);

    const size_t base = tfs.size();
    tfs.resize(base + count);
    block_codec->decodeBlock(in, count, std::span<uint32_t>(tfs.data() + base, count));

    /// The block stores `(tf - 1)`, so an all-zero block (every `tf == 1`) needs no special case here.
    for (size_t i = base; i < tfs.size(); ++i)
        tfs[i] += 1;
}

void SegmentedPostingListEncoder::finalize(WriteBuffer & out, TokenPostingsInfo & info)
{
    using enum PostingsSerialization::Flags;

    impl.serializeTo(out, info);

    info.header |= IsCompressed;
    info.header |= HasBlockIndex;

    if (impl.hasScoring())
        info.header |= HasTermFrequencies;

    if (info.offsets.size() == 1)
        info.header |= SingleBlock;
}

void SegmentedPostingListCodecBase::decode(ReadBuffer & in, UInt64 max_cardinality, PostingList & postings, bool has_term_frequencies, PaddedPODArray<char> & buffer) const
{
    SegmentedPostingListCodec impl;
    impl.decode(in, max_cardinality, postings, has_term_frequencies, buffer);
}

void SegmentedPostingListCodecBase::decode(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, bool has_term_frequencies, PaddedPODArray<char> & buffer) const
{
    SegmentedPostingListCodec impl;
    impl.decode(in, max_cardinality, row_ids, has_term_frequencies, buffer);
}

void SegmentedPostingListCodecBase::decodeWithTermFrequencies(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & tfs, PaddedPODArray<char> & buffer) const
{
    SegmentedPostingListCodec impl;
    impl.decodeWithTermFrequencies(in, max_cardinality, row_ids, tfs, buffer);
}

std::unique_ptr<IPostingListEncoder> SegmentedPostingListCodecBase::createEncoder(size_t segment_size) const
{
    return std::make_unique<SegmentedPostingListEncoder>(getType(), segment_size);
}

void PostingListEncoderNone::append(
    std::span<const UInt32> row_ids,
    std::span<const UInt32> tf_minus_one,
    const PostingListBuildContext & context)
{
    chassert(!row_ids.empty());

    if (context.enable_scoring || !tf_minus_one.empty())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Text index scoring is not supported with the 'none' posting list codec");

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

std::unique_ptr<IPostingListEncoder> PostingListCodecNone::createEncoder(size_t segment_size) const
{
    return std::make_unique<PostingListEncoderNone>(segment_size);
}

/// Upper bound of the portable serialization of a Roaring bitmap with at most `max_cardinality` values.
/// Every container takes at most 2 bytes per value: arrays hold up to 4096 values of 2 bytes, bitsets take 8192 bytes
/// for more values, and `runOptimize` turns them into runs only when the runs are smaller (see `finishSegment`).
/// The header takes 8 bytes plus at most 9 bytes per container, and every container holds at least one value.
static UInt64 getMaxPortableBitmapBytes(UInt64 max_cardinality)
{
    return 8 + 11 * max_cardinality;
}

void PostingListCodecNone::decode(ReadBuffer & in, UInt64 max_cardinality, PostingList & postings, bool has_term_frequencies, PaddedPODArray<char> & buffer) const
{
    if (has_term_frequencies)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Text index scoring is not supported with the 'none' posting list codec");

    size_t num_bytes = 0;
    readVarUInt(num_bytes, in);

    /// The size prefix comes from disk: bound it before growing any buffer to it.
    const UInt64 max_bytes = getMaxPortableBitmapBytes(max_cardinality);
    if (num_bytes > max_bytes)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted posting list segment: bitmap of {} bytes exceeds the upper bound of {} bytes for {} row ids",
            num_bytes, max_bytes, max_cardinality);
    }

    postings = PostingList::readSafe(readContiguousBytes(in, num_bytes, buffer), num_bytes);

    /// The bitmap is bounded by its size prefix; its row ids are bounded by the token metadata.
    if (postings.cardinality() > max_cardinality)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted posting list segment: cardinality {} exceeds the upper bound {} allowed by the token metadata",
            postings.cardinality(), max_cardinality);
    }
}

void PostingListCodecNone::decode(ReadBuffer & in, UInt64 max_cardinality, PaddedPODArray<UInt32> & row_ids, bool has_term_frequencies, PaddedPODArray<char> & buffer) const
{
    PostingList postings;
    decode(in, max_cardinality, postings, has_term_frequencies, buffer);

    size_t old_size = row_ids.size();
    row_ids.resize(old_size + postings.cardinality());
    postings.toUint32Array(row_ids.data() + old_size);
}

void PostingListCodecNone::decodeWithTermFrequencies(ReadBuffer &, UInt64, PaddedPODArray<UInt32> &, PaddedPODArray<UInt32> &, PaddedPODArray<char> &) const
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Text index scoring is not supported with the 'none' posting list codec");
}

}
