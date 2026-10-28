#include <Storages/MergeTree/TextIndexPostingsRankCursor.h>

#include <Storages/MergeTree/PostingListBlockCodec.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

TextIndexPostingsRankCursor::TextIndexPostingsRankCursor(PostingListCursorPtr cursor_, const TokenPostingsInfo & info_)
    : cursor(std::move(cursor_))
    , info(&info_)
{
    if (!(info->header & PostingsSerialization::Flags::HasBlockIndex))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Rank cursor over a posting list without the per-segment block index");

    /// A compressed cursor prepares no segment until it is first positioned.
    cursor->advance(0);
    if (!cursor->valid())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index: posting list of {} documents in {} segments yields no documents",
            info->cardinality, info->offsets.size());

    /// Writers seal segments at a fixed size, so a segment's first rank follows from its index.
    const UInt64 num_segments = info->offsets.size();
    const UInt64 cardinality = info->cardinality;
    segment_size = cursor->position().segment_doc_count;

    bool fits = segment_size == cardinality;
    if (num_segments > 1)
    {
        /// Full segments are whole blocks; the last one holds the remaining 1..segment_size documents.
        const UInt64 docs_in_full_segments = (num_segments - 1) * segment_size;
        fits = segment_size % IPostingListBlockCodec::BLOCK_SIZE == 0
            && docs_in_full_segments < cardinality
            && cardinality - docs_in_full_segments <= segment_size;
    }

    if (!fits)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index: {} posting segments of {} documents cannot hold the token's {} documents",
            num_segments, segment_size, cardinality);

    num_documents = cardinality;
}

TextIndexPostingsRankCursor::TextIndexPostingsRankCursor(PostingListCursorPtr cursor_)
    : cursor(std::move(cursor_))
    , num_documents(cursor->cardinality())
{
}

UInt64 TextIndexPostingsRankCursor::rank()
{
    const auto position = cursor->position();
    if (!info)
        return position.index_in_segment;

    if (position.segment != checked_segment)
    {
        const bool is_last = position.segment + 1 == info->offsets.size();
        const UInt64 expected = is_last ? info->cardinality - position.segment * segment_size : segment_size;
        if (position.segment_doc_count != expected)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupt text index: posting segment {} holds {} documents instead of {}",
                position.segment, position.segment_doc_count, expected);
        checked_segment = position.segment;
    }

    return position.segment * segment_size + position.index_in_segment;
}

}
