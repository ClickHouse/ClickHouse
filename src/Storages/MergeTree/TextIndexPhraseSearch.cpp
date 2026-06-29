#include <Storages/MergeTree/TextIndexPhraseSearch.h>

#include <Common/Exception.h>

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>

namespace DB
{

PositionList TextIndexPhraseSearch::intersect(const PositionList & lhs, const PositionList & rhs, UInt32 shift)
{
    PositionList result;
    if (lhs.empty() || rhs.empty() || shift == 0)
        return result;

    chassert(shift < RoaringishEntry::BITMAP_BITS);

    /// Collect matches as (key, bitmap); the intersection is small relative to the inputs.
    /// key = (doc_id << 32) | group, computed inline from the SoA lanes.
    std::vector<std::pair<UInt64, UInt32>> matches;
    matches.reserve(std::min(lhs.size(), rhs.size()));

    const size_t ln = lhs.size();
    const size_t rn = rhs.size();
    size_t lhs_idx = 0;
    size_t rhs_idx = 0;

    while (lhs_idx < ln && rhs_idx < rn)
    {
        UInt64 lhs_key = lhs.key(lhs_idx);
        UInt64 rhs_key = rhs.key(rhs_idx);

        if (lhs_key == rhs_key)
        {
            /// Phase 1: same (doc_id, group). Shift LHS bitmap left and AND with RHS.
            UInt32 lhs_shifted = lhs.bitmap[lhs_idx] << shift;
            UInt32 matched_positions_bitmap = lhs_shifted & rhs.bitmap[rhs_idx];
            if (matched_positions_bitmap)
                matches.emplace_back(rhs_key, matched_positions_bitmap);

            /// Phase 2: boundary crossing. High bits of LHS bitmap overflow into group+1.
            /// Skip when group is at max to avoid crossing into a different document.
            UInt32 overflow_positions_bitmap = lhs.bitmap[lhs_idx] >> (RoaringishEntry::BITMAP_BITS - shift);
            if (overflow_positions_bitmap && lhs.group[lhs_idx] < std::numeric_limits<UInt32>::max())
            {
                UInt64 boundary_key = lhs_key + 1; /// Same doc_id, group+1.

                size_t i = rhs_idx + 1;
                while (i < rn && rhs.key(i) < boundary_key)
                    ++i;

                if (i < rn && rhs.key(i) == boundary_key)
                {
                    UInt32 boundary_match = overflow_positions_bitmap & rhs.bitmap[i];
                    if (boundary_match)
                        matches.emplace_back(boundary_key, boundary_match);
                }
            }

            ++lhs_idx;
            ++rhs_idx;
        }
        else if (lhs_key < rhs_key)
        {
            /// Phase 2 for non-matching LHS: check if LHS overflows into RHS's group.
            UInt32 overflow_positions_bitmap = lhs.bitmap[lhs_idx] >> (RoaringishEntry::BITMAP_BITS - shift);
            if (overflow_positions_bitmap && lhs.group[lhs_idx] < std::numeric_limits<UInt32>::max())
            {
                UInt64 boundary_key = lhs_key + 1;
                if (boundary_key == rhs_key)
                {
                    UInt32 boundary_match = overflow_positions_bitmap & rhs.bitmap[rhs_idx];
                    if (boundary_match)
                        matches.emplace_back(boundary_key, boundary_match);
                }
            }
            ++lhs_idx;
        }
        else
        {
            ++rhs_idx;
        }
    }

    if (matches.empty())
        return result;

    chassert(std::is_sorted(matches.begin(), matches.end(),
                            [](const auto & a, const auto & b) { return a.first < b.first; }));

    result.reserve(matches.size());
    for (size_t i = 0; i < matches.size();)
    {
        const UInt64 k = matches[i].first;
        UInt32 bm = matches[i].second;
        size_t j = i + 1;
        while (j < matches.size() && matches[j].first == k)
        {
            bm |= matches[j].second;
            ++j;
        }
        result.pushBack(static_cast<UInt32>(k >> 32), static_cast<UInt32>(k & 0xFFFFFFFFu), bm);
        i = j;
    }

    return result;
}

PaddedPODArray<UInt32> TextIndexPhraseSearch::phraseSearch(const std::vector<PositionList> & position_lists)
{
    if (position_lists.empty())
        return {};

    if (position_lists.size() == 1)
        return extractDocIds(position_lists[0]);

    /// Chain pairwise intersections with shift=1 (consecutive tokens):
    ///   ((list[0] ∩ list[1]) ∩ list[2]) ...
    PositionList current = intersect(position_lists[0], position_lists[1], 1);

    for (size_t k = 2; k < position_lists.size(); ++k)
    {
        if (current.empty())
            return {};
        current = intersect(current, position_lists[k], 1);
    }

    if (current.empty())
        return {};

    return extractDocIds(current);
}

PaddedPODArray<UInt32> TextIndexPhraseSearch::extractDocIds(const PositionList & pl)
{
    PaddedPODArray<UInt32> doc_ids;
    doc_ids.reserve(pl.size());

    UInt32 prev_doc = std::numeric_limits<UInt32>::max();
    for (size_t i = 0; i < pl.size(); ++i)
    {
        if (pl.doc[i] != prev_doc)
        {
            doc_ids.push_back(pl.doc[i]);
            prev_doc = pl.doc[i];
        }
    }

    return doc_ids;
}

}
