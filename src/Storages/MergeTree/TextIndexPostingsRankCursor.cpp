#include <Storages/MergeTree/TextIndexPostingsRankCursor.h>

#include <Storages/MergeTree/BitpackingBlockCodec.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <IO/ReadHelpers.h>

#include <algorithm>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace
{

UInt32 requireUInt32(UInt64 value, const char * what)
{
    if (value > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupt text index: {} = {} does not fit UInt32", what, value);
    return static_cast<UInt32>(value);
}

}

TextIndexPostingsRankCursor::TextIndexPostingsRankCursor(MergeTreeReaderStream & stream_, const TokenPostingsInfo & info_)
    : stream(&stream_), info(&info_)
{
    if (!(info->header & PostingsSerialization::Flags::HasBlockIndex))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index: rank cursor needs the per-segment block index (HasBlockIndex is not set)");

    total_segments = info->offsets.size();
    segment_ranks.assign(total_segments + 1, 0);

    if (total_segments != 0)
    {
        loadSegment(0);
        decodeBlock(0);
        is_valid = decoded_count != 0;
        if (is_valid)
            current_doc_id = decoded_doc_ids[0];
    }
}

UInt64 TextIndexPostingsRankCursor::readSegmentDocCount(size_t segment_idx)
{
    stream->seekToMark({info->offsets[segment_idx], 0});
    auto * data_buffer = stream->getDataBuffer();

    UInt64 codec_type = 0;
    UInt64 payload_bytes = 0;
    UInt64 doc_count = 0;
    readVarUInt(codec_type, *data_buffer);
    readVarUInt(payload_bytes, *data_buffer);
    readVarUInt(doc_count, *data_buffer);

    if (doc_count == 0 || doc_count > info->cardinality)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index: posting segment {} holds {} of the token's {} documents",
            segment_idx, doc_count, info->cardinality);

    return doc_count;
}

void TextIndexPostingsRankCursor::ensureSegmentRank(size_t segment_idx)
{
    while (ranks_known <= segment_idx)
    {
        const size_t previous = ranks_known - 1;
        segment_ranks[ranks_known] = segment_ranks[previous] + readSegmentDocCount(previous);
        ++ranks_known;
    }
}

void TextIndexPostingsRankCursor::loadSegment(size_t segment_idx)
{
    ensureSegmentRank(segment_idx);

    stream->seekToMark({info->offsets[segment_idx], 0});
    auto * data_buffer = stream->getDataBuffer();

    UInt64 codec_type = 0;
    UInt64 payload_bytes = 0;
    UInt64 doc_count = 0;
    UInt64 first_row_id = 0;
    readVarUInt(codec_type, *data_buffer);
    readVarUInt(payload_bytes, *data_buffer);
    readVarUInt(doc_count, *data_buffer);
    readVarUInt(first_row_id, *data_buffer);

    if (codec_type != static_cast<UInt64>(IPostingListCodec::Type::Bitpacking))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupt text index: unexpected posting codec {}", codec_type);

    segment.codec_type = static_cast<IPostingListCodec::Type>(codec_type);
    segment.doc_count = doc_count;
    segment.first_row_id = requireUInt32(first_row_id, "first_row_id");

    if (!block_codec || block_codec->type() != segment.codec_type)
        block_codec = createPostingListBlockCodec(segment.codec_type);

    const UInt64 max_blocks = (doc_count + BLOCK_SIZE - 1) / BLOCK_SIZE;
    /// Bound the payload before allocating, so corrupt metadata cannot force a huge read.
    if (payload_bytes > max_blocks * block_codec->maxBlockBytes())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index: posting payload of {} bytes exceeds the bound for {} documents", payload_bytes, doc_count);

    segment.payload.resize(payload_bytes);
    data_buffer->readStrict(reinterpret_cast<char *>(segment.payload.data()), payload_bytes);

    UInt64 num_blocks = 0;
    readVarUInt(num_blocks, *data_buffer);
    if (num_blocks != max_blocks)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index: {} blocks recorded for {} documents", num_blocks, doc_count);

    segment.block_last_row_ids.resize(num_blocks);
    segment.block_offsets.resize(num_blocks);
    for (size_t i = 0; i < num_blocks; ++i)
    {
        UInt64 v = 0;
        readVarUInt(v, *data_buffer);
        segment.block_last_row_ids[i] = requireUInt32(v, "block_last_row_id");
        if (i > 0 && segment.block_last_row_ids[i] <= segment.block_last_row_ids[i - 1])
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupt text index: block row ids do not increase at block {}", i);
    }
    for (size_t i = 0; i < num_blocks; ++i)
    {
        UInt64 v = 0;
        readVarUInt(v, *data_buffer);
        if (v >= payload_bytes || (i > 0 && v <= segment.block_offsets[i - 1]))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupt text index: bad block offset {} at block {}", v, i);
        segment.block_offsets[i] = v;
    }

    current_segment_idx = segment_idx;
    segment_first_rank = segment_ranks[segment_idx];
    has_segment = true;
    /// Block indices restart per segment, so drop the decoded block instead of matching it by index.
    current_block = std::numeric_limits<size_t>::max();
    decoded_count = 0;
}

void TextIndexPostingsRankCursor::decodeBlock(size_t block_idx)
{
    const size_t block_count = segment.blockCount();

    /// A block's delta base is the previous block's last row id, so any block decodes on its own.
    const UInt32 base = (block_idx == 0) ? segment.first_row_id : segment.block_last_row_ids[block_idx - 1];

    const size_t count = (block_idx + 1 == block_count)
        ? segment.doc_count - (block_count - 1) * BLOCK_SIZE
        : BLOCK_SIZE;

    const size_t payload_offset = static_cast<size_t>(segment.block_offsets[block_idx]);
    const size_t next_offset = (block_idx + 1 < block_count)
        ? static_cast<size_t>(segment.block_offsets[block_idx + 1])
        : segment.payload.size();
    if (payload_offset >= segment.payload.size() || next_offset <= payload_offset)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupt text index: bad posting block span at block {}", block_idx);

    std::span<const std::byte> block_data(
        reinterpret_cast<const std::byte *>(segment.payload.data() + payload_offset), next_offset - payload_offset);

    decoded_doc_ids.resize(count);
    const size_t expected = block_data.size();
    const size_t consumed = block_codec->decodeBlock(block_data, count, std::span<UInt32>(decoded_doc_ids.data(), count));
    if (consumed != expected)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index: posting block {} consumed {} of {} bytes", block_idx, consumed, expected);

    std::inclusive_scan(decoded_doc_ids.begin(), decoded_doc_ids.end(), decoded_doc_ids.begin(), std::plus<UInt32>{}, base);

    current_block = block_idx;
    decoded_count = count;
    index_in_block = 0;
    block_first_rank = static_cast<UInt64>(block_idx) * BLOCK_SIZE;
}

void TextIndexPostingsRankCursor::next()
{
    if (!is_valid)
        return;

    ++index_in_block;
    if (index_in_block < decoded_count)
    {
        current_doc_id = decoded_doc_ids[index_in_block];
        return;
    }

    if (current_block + 1 < segment.blockCount())
    {
        decodeBlock(current_block + 1);
        current_doc_id = decoded_doc_ids[0];
        return;
    }

    if (current_segment_idx + 1 < total_segments)
    {
        loadSegment(current_segment_idx + 1);
        decodeBlock(0);
        current_doc_id = decoded_doc_ids[0];
        return;
    }

    is_valid = false;
}

bool TextIndexPostingsRankCursor::seekWithinLoadedSegment(UInt32 target)
{
    const auto & last_ids = segment.block_last_row_ids;
    const auto it = std::lower_bound(last_ids.begin(), last_ids.end(), target);
    if (it == last_ids.end())
        return false;

    const size_t block_idx = static_cast<size_t>(it - last_ids.begin());
    if (block_idx != current_block || decoded_count == 0)
        decodeBlock(block_idx);

    const auto doc_it = std::lower_bound(decoded_doc_ids.begin(), decoded_doc_ids.begin() + decoded_count, target);
    if (doc_it == decoded_doc_ids.begin() + decoded_count)
        return false;

    index_in_block = static_cast<size_t>(doc_it - decoded_doc_ids.begin());
    current_doc_id = *doc_it;
    return true;
}

void TextIndexPostingsRankCursor::advance(UInt32 target)
{
    if (!is_valid || current_doc_id >= target)
        return;

    /// info->ranges bounds each segment's row ids, so the covering segment is found without reading payloads.
    for (size_t idx = current_segment_idx; idx < total_segments; ++idx)
    {
        if (info->ranges[idx].end < target)
            continue;

        if (idx != current_segment_idx || !has_segment)
            loadSegment(idx);

        if (seekWithinLoadedSegment(target))
            return;
    }

    is_valid = false;
}

}
