#pragma once

#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>

#include <limits>

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
    PostingListCursorPtr cursor;
    const TokenPostingsInfo * info = nullptr;
    /// Documents in every segment but the last.
    UInt64 segment_size = 0;
    size_t checked_segment = std::numeric_limits<size_t>::max();
};

}
