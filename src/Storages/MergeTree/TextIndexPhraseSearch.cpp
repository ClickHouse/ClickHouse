#include <Storages/MergeTree/TextIndexPhraseSearch.h>

namespace DB
{

std::vector<RoaringishEntry> TextIndexPhraseSearch::intersect(
    const std::vector<RoaringishEntry> & lhs,
    const std::vector<RoaringishEntry> & rhs,
    UInt32 shift)
{
    if (lhs.empty() || rhs.empty() || shift == 0)
        return {};

    /// We need shift < BITMAP_BITS (64). For shift >= 64, the entire bitmap
    /// shifts into the next group, which this algorithm handles naturally
    /// via the boundary phase only — but UB on 64-bit shift must be avoided.
    /// In practice phrase shifts are small (1-20 words).
    chassert(shift < RoaringishEntry::BITMAP_BITS);

    /// Mask for the upper 64 bits (doc_id + group) used as the intersection key.
    /// Two entries match in phase 1 if they have the same key.
    /// In phase 2, LHS key's group+1 must equal RHS key's group.

    std::vector<RoaringishEntry> result;
    result.reserve(std::min(lhs.size(), rhs.size()));

    /// Phase 1: within-group matches.
    /// Phase 2: boundary-crossing matches (overflow into next group).
    /// Both phases are done in a single pass for efficiency.

    size_t i = 0;
    size_t j = 0;

    while (i < lhs.size() && j < rhs.size())
    {
        UInt64 l_key = lhs[i].key();
        UInt64 r_key = rhs[j].key();

        if (l_key == r_key)
        {
            /// Phase 1: same (doc_id, group).
            /// Shift LHS bitmap left and AND with RHS bitmap.
            UInt64 shifted = lhs[i].bitmap() << shift;
            UInt64 match_bm = shifted & rhs[j].bitmap();

            if (match_bm)
            {
                /// Store the result with the RHS key and the matched bitmap.
                UInt128 result_value = (static_cast<UInt128>(r_key) << 64) | static_cast<UInt128>(match_bm);
                result.push_back({result_value});
            }

            /// Phase 2: boundary crossing.
            /// When shifting, high bits of LHS bitmap may overflow into the next group.
            UInt64 overflow_bm = lhs[i].bitmap() >> (RoaringishEntry::BITMAP_BITS - shift);
            if (overflow_bm)
            {
                /// Look for RHS entry with group+1 (same doc_id).
                UInt32 lhs_doc = lhs[i].docId();
                UInt32 lhs_group = lhs[i].group();
                UInt64 boundary_key = (static_cast<UInt64>(lhs_doc) << 32) | static_cast<UInt64>(lhs_group + 1);

                /// Search forward in RHS for the boundary key.
                /// Since RHS is sorted, we can scan from current j.
                size_t jj = j + 1;
                while (jj < rhs.size() && rhs[jj].key() < boundary_key)
                    ++jj;

                if (jj < rhs.size() && rhs[jj].key() == boundary_key)
                {
                    UInt64 boundary_match = overflow_bm & rhs[jj].bitmap();
                    if (boundary_match)
                    {
                        UInt128 result_value = (static_cast<UInt128>(boundary_key) << 64) | static_cast<UInt128>(boundary_match);
                        result.push_back({result_value});
                    }
                }
            }

            ++i;
            ++j;
        }
        else if (l_key < r_key)
        {
            /// Phase 2 for non-matching LHS: check if LHS overflows into RHS's group.
            UInt64 overflow_bm = lhs[i].bitmap() >> (RoaringishEntry::BITMAP_BITS - shift);
            if (overflow_bm)
            {
                UInt32 lhs_doc = lhs[i].docId();
                UInt32 lhs_group = lhs[i].group();
                UInt64 boundary_key = (static_cast<UInt64>(lhs_doc) << 32) | static_cast<UInt64>(lhs_group + 1);

                if (boundary_key == r_key)
                {
                    UInt64 boundary_match = overflow_bm & rhs[j].bitmap();
                    if (boundary_match)
                    {
                        UInt128 result_value = (static_cast<UInt128>(boundary_key) << 64) | static_cast<UInt128>(boundary_match);
                        result.push_back({result_value});
                    }
                }
            }
            ++i;
        }
        else
        {
            ++j;
        }
    }

    /// Handle remaining LHS entries for boundary phase.
    while (i < lhs.size() && j < rhs.size())
    {
        UInt64 overflow_bm = lhs[i].bitmap() >> (RoaringishEntry::BITMAP_BITS - shift);
        if (overflow_bm)
        {
            UInt32 lhs_doc = lhs[i].docId();
            UInt32 lhs_group = lhs[i].group();
            UInt64 boundary_key = (static_cast<UInt64>(lhs_doc) << 32) | static_cast<UInt64>(lhs_group + 1);

            while (j < rhs.size() && rhs[j].key() < boundary_key)
                ++j;

            if (j < rhs.size() && rhs[j].key() == boundary_key)
            {
                UInt64 boundary_match = overflow_bm & rhs[j].bitmap();
                if (boundary_match)
                {
                    UInt128 result_value = (static_cast<UInt128>(boundary_key) << 64) | static_cast<UInt128>(boundary_match);
                    result.push_back({result_value});
                }
            }
        }
        ++i;
    }

    /// Phase 1 and Phase 2 of the same iteration can both emit at the same key
    /// (e.g. the within-group match at group `g+1` and the boundary match coming
    /// from group `g`), and the result is not guaranteed to be sorted because
    /// boundary results carry `group + 1`. Sort and merge same-bucket entries so
    /// that the next chained `intersect` call sees one entry per key — otherwise
    /// the pointer advancement in the next pass can skip a duplicate and produce
    /// false negatives for 3+ term phrases.
    if (result.size() > 1)
    {
        std::sort(result.begin(), result.end());
        size_t out_idx = 0;
        for (size_t idx = 1; idx < result.size(); ++idx)
        {
            if (result[out_idx].sameBucket(result[idx]))
                result[out_idx].mergeBitmap(result[idx]);
            else
                result[++out_idx] = result[idx];
        }
        result.resize(out_idx + 1);
    }

    return result;
}

std::vector<UInt32> TextIndexPhraseSearch::phraseSearch(
    const std::vector<std::vector<RoaringishEntry>> & position_lists)
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
        UInt32 doc = entry.docId();
        if (doc != prev_doc)
        {
            doc_ids.push_back(doc);
            prev_doc = doc;
        }
    }

    return doc_ids;
}

}
