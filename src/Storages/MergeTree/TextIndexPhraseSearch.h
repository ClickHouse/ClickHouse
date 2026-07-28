#pragma once

#include <Storages/MergeTree/TextIndexPositionData.h>

#include <vector>

namespace DB
{

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
    /// Intersect two position lists (struct-of-arrays) with a given positional shift.
    /// For a phrase "A B", shift=1: term B must be at position (term A position + 1).
    /// Returns a PositionList of entries where the phrase constraint is satisfied.
    static PositionList intersect(const PositionList & lhs, const PositionList & rhs, UInt32 shift);

    /// Multi-term phrase search.
    /// position_lists[0] = positions for first term, [1] = second term, etc.
    /// Returns sorted unique doc_ids where the full phrase matches.
    static PaddedPODArray<UInt32> phraseSearch(const std::vector<PositionList> & position_lists);

    /// Extract unique sorted doc_ids from a position list.
    static PaddedPODArray<UInt32> extractDocIds(const PositionList & pl);
};

}
