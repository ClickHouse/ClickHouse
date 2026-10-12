#pragma once

#include <Storages/MergeTree/PostingListCursor.h>
#include <Storages/MergeTree/TextIndexDocLengthsReader.h>
#include <Storages/MergeTree/BM25Kernel.h>
#include <memory>
#include <vector>

namespace DB
{

struct ScoringPostings;

/// Scoring extension of `PostingListCursor`: also decodes per-block term frequencies and exposes
/// the per-block / per-segment block-max upper-bound (UB) inputs for BM25 pruning (WAND / MaxScore).
class PostingListScoringCursor : public PostingListCursor
{
public:
    /// Streaming cursor for compressed multi-block tokens.
    PostingListScoringCursor(
        MergeTreeReaderStream & stream_,
        const TokenPostingsInfo & info_,
        const TextIndexDocLengthsReader * doc_lengths_,
        TextIndexPostingsCache * postings_cache_ = nullptr,
        const String & index_id_for_cache_ = {});

    /// Embedded cursor over already-decoded flat postings.
    PostingListScoringCursor(std::shared_ptr<const ScoringPostings> scoring_postings_, const TextIndexDocLengthsReader * doc_lengths_);

    /// Exact term frequency of the current row.
    UInt32 termFrequency() const;

    /// `SmallFloat` doc-length byte of the current row.
    UInt8 documentLengthByte() const;

    /// Index of the current packed block (always 0 for the embedded cursor).
    size_t currentBlockIndex() const { return is_embedded ? 0 : current_block; }

    /// Point `current_block` at the block containing `doc_id` without decoding its body.
    void seekBlock(uint32_t doc_id);

    /// Per-block block-max UB inputs of the current segment.
    UInt8 minDocumentLengthByte(size_t block_idx) const;
    UInt8 maxTermFrequencyMinusOne(size_t block_idx) const;

    /// Per-segment block-max UB inputs of the current segment.
    UInt8 segmentMinDocumentLengthByte() const;
    UInt8 segmentMaxTermFrequencyMinusOne() const;

    /// Block-max score upper bound of the current block under weight `w`.
    Float32 blockMaxScore(const BM25Weight & w) const;

    /// Block-max score upper bound over the whole current segment under weight `w`.
    Float32 segmentMaxScore(const BM25Weight & w) const;

protected:
    /// Decodes postings from the packed block into `decoded_values` and term frequencies into `decoded_tfs`.
    void decodeBlock(size_t block_idx) override;

private:
    /// Per-granule `SmallFloat` doc-length cursor, queried by the granule-local row id.
    const TextIndexDocLengthsReader * doc_lengths = nullptr;

    /// Term frequencies of the current packed block, parallel to `decoded_values`.
    alignas(16) UInt32 decoded_tfs[IPostingListBlockCodec::BLOCK_SIZE]{};

    /// Embedded-only: the flat postings (sorted row ids with their term frequencies) the cursor iterates.
    std::shared_ptr<const ScoringPostings> embedded_scoring_postings;
};

/// A scoring cursor of one scoring token with its BM25 weight and cardinality.
struct ScoreCursor
{
    std::shared_ptr<PostingListScoringCursor> cursor;
    const BM25Weight * weight = nullptr;
    UInt32 cardinality = 0;
};

/// Union scorer: per-token union walk over `cursors`, adds each token's BM25 contribution
/// at its hit rows of the window [row_offset, row_offset + num_rows) into `data` and marks
/// the hit rows with 1 in `matches` (the match column of the predicate).
void scoreCursorsUnion(
    Float32 * data,
    UInt8 * matches,
    std::vector<ScoreCursor> & cursors,
    size_t row_offset,
    size_t num_rows);

/// Intersection scorer: joint leapfrog over all `cursors`, sums every token's BM25 contribution
/// at each intersection row of the window [row_offset, row_offset + num_rows) into `data` and marks
/// the intersection rows with 1 in `matches` (the match column of the predicate).
void scoreCursorsIntersection(
    Float32 * data,
    UInt8 * matches,
    std::vector<ScoreCursor> & cursors,
    size_t row_offset,
    size_t num_rows);

}
