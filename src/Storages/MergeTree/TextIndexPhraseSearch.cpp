#include <Storages/MergeTree/TextIndexPhraseSearch.h>

#include <Common/Exception.h>

namespace DB
{

std::vector<RoaringishEntry> TextIndexPhraseSearch::intersect(
    const std::vector<RoaringishEntry> & lhs,
    const std::vector<RoaringishEntry> & rhs,
    UInt32 shift)
{
    if (lhs.empty() || rhs.empty() || shift == 0)
        return {};

    chassert(shift < RoaringishEntry::BITMAP_BITS);

    std::vector<RoaringishEntry> result;
    result.reserve(std::min(lhs.size(), rhs.size()));

    size_t lhs_idx = 0;
    size_t rhs_idx = 0;

    while (lhs_idx < lhs.size() && rhs_idx < rhs.size())
    {
        UInt64 lhs_key = lhs[lhs_idx].key();
        UInt64 rhs_key = rhs[rhs_idx].key();

        if (lhs_key == rhs_key)
        {
            /// Phase 1: same (doc_id, group).
            /// Shift LHS bitmap left and AND with RHS bitmap.
            UInt32 lhs_shifted = lhs[lhs_idx].bitmap << shift;
            UInt32 matched_positions_bitmap = lhs_shifted & rhs[rhs_idx].bitmap;

            if (matched_positions_bitmap)
            {
                RoaringishEntry entry = rhs[rhs_idx];
                entry.bitmap = matched_positions_bitmap;
                result.push_back(entry);
            }

            /// Phase 2: boundary crossing.
            /// High bits of LHS bitmap overflow into the next group.
            /// Skip when group is at max to avoid crossing into a different document.
            UInt32 overflow_positions_bitmap = lhs[lhs_idx].bitmap >> (RoaringishEntry::BITMAP_BITS - shift);
            if (overflow_positions_bitmap && lhs[lhs_idx].group < std::numeric_limits<UInt32>::max())
            {
                UInt64 boundary_key = lhs_key + 1; /// Same doc_id, group+1.

                /// Scan forward in RHS for the boundary key.
                size_t i = rhs_idx + 1;
                while (i < rhs.size() && rhs[i].key() < boundary_key)
                    ++i;

                if (i < rhs.size() && rhs[i].key() == boundary_key)
                {
                    UInt32 boundary_match = overflow_positions_bitmap & rhs[i].bitmap;
                    if (boundary_match)
                    {
                        RoaringishEntry entry = rhs[i];
                        entry.bitmap = boundary_match;
                        result.push_back(entry);
                    }
                }
            }

            ++lhs_idx;
            ++rhs_idx;
        }
        else if (lhs_key < rhs_key)
        {
            /// Phase 2 for non-matching LHS: check if LHS overflows into RHS's group.
            UInt32 overflow_positions_bitmap = lhs[lhs_idx].bitmap >> (RoaringishEntry::BITMAP_BITS - shift);
            if (overflow_positions_bitmap && lhs[lhs_idx].group < std::numeric_limits<UInt32>::max())
            {
                UInt64 boundary_key = lhs_key + 1;

                if (boundary_key == rhs_key)
                {
                    UInt32 boundary_match = overflow_positions_bitmap & rhs[rhs_idx].bitmap;
                    if (boundary_match)
                    {
                        RoaringishEntry entry = rhs[rhs_idx];
                        entry.bitmap = boundary_match;
                        result.push_back(entry);
                    }
                }
            }
            ++lhs_idx;
        }
        else
        {
            ++rhs_idx;
        }
    }

    /// Handle remaining LHS entries for boundary phase.
    while (lhs_idx < lhs.size() && rhs_idx < rhs.size())
    {
        UInt32 overflow_positions_bitmap = lhs[lhs_idx].bitmap >> (RoaringishEntry::BITMAP_BITS - shift);
        if (overflow_positions_bitmap && lhs[lhs_idx].group < std::numeric_limits<UInt32>::max())
        {
            UInt64 boundary_key = lhs[lhs_idx].key() + 1;

            while (rhs_idx < rhs.size() && rhs[rhs_idx].key() < boundary_key)
                ++rhs_idx;

            if (rhs_idx < rhs.size() && rhs[rhs_idx].key() == boundary_key)
            {
                UInt32 boundary_match = overflow_positions_bitmap & rhs[rhs_idx].bitmap;
                if (boundary_match)
                {
                    RoaringishEntry entry = rhs[rhs_idx];
                    entry.bitmap = boundary_match;
                    result.push_back(entry);
                }
            }
        }
        ++lhs_idx;
    }

    return result;
}

std::vector<UInt32> TextIndexPhraseSearch::phraseSearch(const std::vector<std::vector<RoaringishEntry>> & position_lists)
{
    if (position_lists.empty())
        return {};

    if (position_lists.size() == 1)
        return extractDocIds(position_lists[0]);

    /// Chain pairwise intersections: ((list[0] ∩₁ list[1]) ∩₂ list[2]) ...
    /// Each intersection uses shift=1 (consecutive tokens).
    std::vector<RoaringishEntry> current = position_lists[0];

    for (size_t k = 1; k < position_lists.size(); ++k)
    {
        current = intersect(current, position_lists[k], 1);
        if (current.empty())
            return {};
    }

    return extractDocIds(current);
}

std::vector<UInt32> TextIndexPhraseSearch::extractDocIds(const std::vector<RoaringishEntry> & entries)
{
    std::vector<UInt32> doc_ids;
    doc_ids.reserve(entries.size());

    UInt32 prev_doc = std::numeric_limits<UInt32>::max();
    for (const auto & entry : entries)
    {
        if (entry.doc_id != prev_doc)
        {
            doc_ids.push_back(entry.doc_id);
            prev_doc = entry.doc_id;
        }
    }

    return doc_ids;
}

}
