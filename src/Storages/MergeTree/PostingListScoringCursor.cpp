#include <Storages/MergeTree/PostingListScoringCursor.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event TextScoreRowsScored;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

PostingListScoringCursor::PostingListScoringCursor(
    MergeTreeReaderStream & stream_,
    const TokenPostingsInfo & info_,
    const TextIndexDocLengthsReader * doc_lengths_,
    TextIndexPostingsCache * postings_cache_,
    const String & index_id_for_cache_)
    : PostingListCursor(stream_, info_, postings_cache_, index_id_for_cache_)
    , doc_lengths(doc_lengths_)
{
    chassert(doc_lengths);
}

PostingListScoringCursor::PostingListScoringCursor(std::shared_ptr<const ScoringPostings> scoring_postings_, const TextIndexDocLengthsReader * doc_lengths_)
    : PostingListCursor(PaddedPODArrayPtr(scoring_postings_, &scoring_postings_->row_ids))
    , doc_lengths(doc_lengths_)
    , embedded_scoring_postings(std::move(scoring_postings_))
{
    chassert(doc_lengths);
    chassert(embedded_scoring_postings->term_frequencies.size() == decoded_count);
}

UInt32 PostingListScoringCursor::termFrequency() const
{
    chassert(index < decoded_count);
    return is_embedded ? embedded_scoring_postings->term_frequencies[index] : decoded_tfs[index];
}

UInt8 PostingListScoringCursor::documentLengthByte() const
{
    chassert(doc_lengths && value() < doc_lengths->numDocs());
    return doc_lengths->getByte(value());
}

void PostingListScoringCursor::seekBlock(uint32_t doc_id)
{
    if (is_embedded)
        return;

    chassert(current_segment);

    /// Binary-search `block_last_row_ids` for the first block whose last row id is >= `doc_id`.
    const auto & block_last_row_ids = current_segment->block_last_row_ids;
    const auto * it = std::lower_bound(block_last_row_ids.begin(), block_last_row_ids.end(), doc_id);
    chassert(it != block_last_row_ids.end());
    current_block = static_cast<size_t>(it - block_last_row_ids.begin());
}

void PostingListScoringCursor::decodeBlock(size_t block_idx)
{
    const size_t consumed_bytes = decodeBlockPostings(block_idx);

    const auto & segment = *current_segment;
    const size_t payload_offset = static_cast<size_t>(segment.block_offsets[block_idx]);
    const size_t tf_offset = payload_offset + consumed_bytes;

    const size_t next_offset = (block_idx + 1 < segment.block_count)
        ? static_cast<size_t>(segment.block_offsets[block_idx + 1])
        : segment.payload_buffer.size();

    if (tf_offset >= next_offset)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in scoring posting list cursor: term-frequency sub-payload offset {} is "
            "outside block bounds [{}, {}) for block {}",
            tf_offset, payload_offset, next_offset, block_idx);
    }

    std::span<const std::byte> compressed_tf_data(
        reinterpret_cast<const std::byte *>(segment.payload_buffer.data() + tf_offset),
        next_offset - tf_offset);

    std::span<uint32_t> decoded_tfs_span(decoded_tfs, decoded_count);
    block_codec->decodeBlock(compressed_tf_data, decoded_count, decoded_tfs_span);

    /// Block stores `(tf - 1)`, restore the original term frequencies.
    for (size_t i = 0; i < decoded_count; ++i)
        decoded_tfs[i] += 1u;
}

UInt8 PostingListScoringCursor::minDocumentLengthByte(size_t block_idx) const
{
    return is_embedded ? 0 : current_segment->block_min_dl_byte[block_idx];
}

UInt8 PostingListScoringCursor::maxTermFrequencyMinusOne(size_t block_idx) const
{
    return is_embedded
        ? embedded_scoring_postings->max_tf_minus_one
        : current_segment->block_max_tf_minus_one[block_idx];
}

UInt8 PostingListScoringCursor::segmentMinDocumentLengthByte() const
{
    return is_embedded ? 0 : current_segment->min_dl_byte;
}

UInt8 PostingListScoringCursor::segmentMaxTermFrequencyMinusOne() const
{
    return is_embedded
        ? embedded_scoring_postings->max_tf_minus_one
        : current_segment->max_tf_minus_one;
}

Float32 PostingListScoringCursor::blockMaxScore(const BM25Weight & w) const
{
    /// The embedded cursor has no `current_segment`; its single block carries the precomputed UB pair.
    chassert(is_embedded || (current_block < current_segment->block_count && !current_segment->block_max_tf_minus_one.empty()));
    const size_t block_index = currentBlockIndex();

    return maxTermFrequencyMinusOne(block_index) == 255
        ? w.weight
        : w.contribution(maxTermFrequencyMinusOne(block_index) + 1, minDocumentLengthByte(block_index));
}

Float32 PostingListScoringCursor::segmentMaxScore(const BM25Weight & w) const
{
    return segmentMaxTermFrequencyMinusOne() == 255
        ? w.weight
        : w.contribution(segmentMaxTermFrequencyMinusOne() + 1, segmentMinDocumentLengthByte());
}

void scoreCursorsUnion(
    Float32 * data,
    UInt8 * matches,
    std::vector<ScoreCursor> & cursors,
    size_t row_offset,
    size_t num_rows)
{
    const size_t window_end = row_offset + num_rows;
    size_t rows_scored = 0;

    for (auto & entry : cursors)
    {
        auto & cursor = *entry.cursor;
        cursor.advance(static_cast<UInt32>(row_offset));

        while (cursor.valid() && cursor.value() < window_end)
        {
            const size_t row = cursor.value() - row_offset;
            data[row] += entry.weight->contribution(cursor.termFrequency(), cursor.documentLengthByte());
            matches[row] = 1;
            ++rows_scored;
            cursor.next();
        }
    }

    ProfileEvents::increment(ProfileEvents::TextScoreRowsScored, rows_scored);
}

void scoreCursorsIntersection(
    Float32 * data,
    UInt8 * matches,
    std::vector<ScoreCursor> & cursors,
    size_t row_offset,
    size_t num_rows)
{
    const size_t window_end = row_offset + num_rows;
    size_t rows_scored = 0;

    size_t target = row_offset;
    while (target < window_end)
    {
        /// Leapfrog: raise `agreed` until every cursor sits exactly on it (an intersection row) or some cursor is exhausted.
        bool aligned = false;
        size_t agreed = target;

        while (!aligned)
        {
            aligned = true;

            for (auto & entry : cursors)
            {
                auto & cursor = *entry.cursor;
                cursor.advance(static_cast<UInt32>(agreed));

                if (!cursor.valid())
                {
                    ProfileEvents::increment(ProfileEvents::TextScoreRowsScored, rows_scored);
                    return;
                }

                if (cursor.value() > agreed)
                {
                    agreed = cursor.value();
                    aligned = false;
                }
            }
        }

        if (agreed >= window_end)
            break;

        Float32 score = 0;
        for (const auto & entry : cursors)
            score += entry.weight->contribution(entry.cursor->termFrequency(), entry.cursor->documentLengthByte());

        data[agreed - row_offset] = score;
        matches[agreed - row_offset] = 1;
        ++rows_scored;
        target = agreed + 1;
    }

    ProfileEvents::increment(ProfileEvents::TextScoreRowsScored, rows_scored);
}

}
