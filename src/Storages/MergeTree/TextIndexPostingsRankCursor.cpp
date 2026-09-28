#include <Storages/MergeTree/TextIndexPostingsRankCursor.h>

#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

TextIndexPostingsRankCursor::TextIndexPostingsRankCursor(MergeTreeReaderStream & stream_, const TokenPostingsInfo & info_)
    : cursor(std::make_shared<PostingListCursor>(stream_, info_))
    , stream(&stream_)
    , info(&info_)
    , segment_ranks(info_.offsets.size() + 1, 0)
{
    if (!(info->header & PostingsSerialization::Flags::HasBlockIndex))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index: rank cursor needs the per-segment block index (HasBlockIndex is not set)");

    /// A compressed cursor prepares no segment until it is first positioned.
    cursor->advance(0);
}

TextIndexPostingsRankCursor::TextIndexPostingsRankCursor(FlatPostingsPtr docs)
    : cursor(std::make_shared<PostingListCursor>(std::move(docs)))
{
}

UInt64 TextIndexPostingsRankCursor::rank()
{
    const auto position = cursor->position();
    if (!info)
        return position.index_in_segment;

    while (ranks_known <= position.segment)
        setSegmentRank(ranks_known - 1, readSegmentDocCount(ranks_known - 1));

    /// The loaded segment's count is the next segment's rank, so a sequential walk reads no header.
    if (ranks_known == position.segment + 1)
        setSegmentRank(position.segment, position.segment_doc_count);

    return segment_ranks[position.segment] + position.index_in_segment;
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

    const auto & range = info->ranges[segment_idx];
    const UInt64 range_span = range.begin <= range.end ? static_cast<UInt64>(range.end) - range.begin + 1 : 0;
    if (doc_count == 0 || doc_count > range_span)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index: posting segment {} holds {} documents in a row range of {}", segment_idx, doc_count, range_span);

    return doc_count;
}

void TextIndexPostingsRankCursor::setSegmentRank(size_t segment_idx, UInt64 doc_count)
{
    /// The segments partition the token's documents, so the ranks never pass the cardinality and the last segment ends on it.
    const UInt64 rank_end = segment_ranks[segment_idx] + doc_count;
    const bool is_last = segment_idx + 1 == info->offsets.size();
    if (rank_end > info->cardinality || (is_last && rank_end != info->cardinality))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index: posting segment {} ends at rank {} of the token's {} documents", segment_idx, rank_end, info->cardinality);

    segment_ranks[segment_idx + 1] = rank_end;
    ranks_known = segment_idx + 2;
}

}
