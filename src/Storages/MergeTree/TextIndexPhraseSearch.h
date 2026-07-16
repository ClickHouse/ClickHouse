#pragma once

#include <Storages/MergeTree/TextIndexPositionCodec.h>
#include <Storages/MergeTree/TextIndexPositionData.h>

#include <vector>

namespace DB
{

/// View over [offset, offset + length) of a PositionList; accessor indexes are view-relative.
struct PositionListView
{
    const PositionList * list = nullptr;
    size_t offset = 0;
    size_t length = 0;

    PositionListView() = default;
    explicit PositionListView(const PositionList & list_) : list(&list_), offset(0), length(list_.size()) {}
    PositionListView(const PositionList & list_, size_t offset_, size_t length_) : list(&list_), offset(offset_), length(length_) {}

    size_t size() const { return length; }
    bool empty() const { return length == 0; }

    UInt64 key(size_t i) const { return list->key(offset + i); }
    UInt32 doc(size_t i) const { return list->doc[offset + i]; }
    UInt32 group(size_t i) const { return list->group[offset + i]; }
    UInt32 bitmap(size_t i) const { return list->bitmap[offset + i]; }
};

/// Lazily decoding source of one token's position segments: segments the search skips are never read.
class IPositionSegmentSource
{
public:
    virtual ~IPositionSegmentSource() = default;

    /// The token's segment directory: doc ranges are disjoint and increasing.
    virtual const std::vector<TextIndexPositionCodec::SegmentMeta> & segments() const = 0;

    /// Decodes segment `idx`; the returned list stays valid until the next readSegment call.
    virtual const PositionList & readSegment(size_t idx) = 0;
};

/// Scalar phrase search using Roaringish two-phase intersection.
///
/// Given position lists for consecutive phrase terms, finds all documents
/// where the terms appear in order with the specified positional gaps.
///
/// The algorithm intersects sorted arrays of RoaringishEntry values:
///   Phase 1 (within-group): for matching (doc_id, group) keys,
///     shift LHS bitmap left by the phrase offset and AND with RHS bitmap.
///   Phase 2 (boundary-crossing): when the shift overflows past the bitmap width,
///     check the wrapped bits against (doc_id, group+1) in the RHS.
///
/// Returns a sorted vector of unique doc_ids that match the phrase.
struct TextIndexPhraseSearch
{
    /// Intersect two views with a positional shift: for a phrase "A B", shift=1 means B at (A position + 1).
    static PositionList intersect(PositionListView lhs, PositionListView rhs, UInt32 shift);
    static PositionList intersect(const PositionList & lhs, const PositionList & rhs, UInt32 shift);

    /// Phrase search over fully materialized position lists (one per term, in phrase order); returns sorted unique matching doc_ids.
    static PaddedPODArray<UInt32> phraseSearch(const std::vector<PositionList> & position_lists);

    /// Streaming phraseSearch: leapfrogs on segment doc ranges (skipped segments are never read) and runs the same kernel per doc-aligned window.
    static PaddedPODArray<UInt32> phraseSearchStreaming(const std::vector<IPositionSegmentSource *> & sources, size_t * segments_skipped = nullptr);

    /// Extract unique sorted doc_ids from a position list.
    static PaddedPODArray<UInt32> extractDocIds(const PositionList & pl);

    /// Appending extractDocIds; windows cover disjoint doc ranges, so no cross-call deduplication.
    static void appendDocIds(const PositionList & pl, PaddedPODArray<UInt32> & doc_ids);
};

}
