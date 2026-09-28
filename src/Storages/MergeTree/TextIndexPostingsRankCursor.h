#pragma once

#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>

#include <vector>

namespace DB
{

class MergeTreeReaderStream;

/// Walks a token's posting list, reporting each document's rank (its ordinal in the list) without materialising the list.
class TextIndexPostingsRankCursor
{
public:
    TextIndexPostingsRankCursor(MergeTreeReaderStream & stream_, const TokenPostingsInfo & info_);
    /// Over a flat posting list (embedded or raw), where a document's rank is its index.
    explicit TextIndexPostingsRankCursor(FlatPostingsPtr docs);

    bool valid() const { return cursor->valid(); }
    UInt32 docId() const { return cursor->value(); }
    UInt64 rank();

    void next() { cursor->next(); }
    /// Positions the cursor on the first document >= target, or invalidates it.
    void advance(UInt32 target) { cursor->advance(target); }

private:
    /// Reads the header of a segment `advance` skipped, to learn its document count.
    UInt64 readSegmentDocCount(size_t segment_idx);
    void setSegmentRank(size_t segment_idx, UInt64 doc_count);

    PostingListCursorPtr cursor;
    MergeTreeReaderStream * stream = nullptr;
    const TokenPostingsInfo * info = nullptr;

    /// segment_ranks[i] is the rank of segment i's first document; entries below `ranks_known` are filled.
    std::vector<UInt64> segment_ranks;
    size_t ranks_known = 1;
};

}
