#pragma once

#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <base/types.h>

#include <utility>
#include <vector>


namespace DB
{

/** A coverage index over `MergeTreePartInfo` ranges that tolerates pairs of parts whose block
  * ranges intersect without one fully containing the other.
  *
  * `ActiveDataPartSet` enforces a strict invariant: every two parts it stores are either disjoint
  * or in a containment relation. That is correct for sets that participate in merge selection.
  *
  * It is too strict for `StorageReplicatedMergeTree::checkPartsImpl`'s "set of empty unexpected
  * parts on disk" — the set of empty drop markers left over after concurrent
  * `DROP`/`INSERT`/merge traffic. Two such markers can have block ranges that intersect without
  * containment (for example, `all_0_5_2` (blocks 0..5) and `all_2_11_3` (blocks 2..11)) yet both
  * pass the "no other unexpected part contains me" filter. Calling `ActiveDataPartSet::add` on
  * the second one throws `LOGICAL_ERROR` and aborts the table-attach thread (STID `2352-49be`).
  *
  * `OverlappingPartCovering` is a thin wrapper that:
  *   - keeps a primary `ActiveDataPartSet` for the common non-overlapping case (fast lookup);
  *   - holds the parts that would otherwise have caused an intersection in a small auxiliary
  *     vector and consults it via linear scan only when the primary lookup misses.
  *
  * Both stored groups participate in `getContainingPart`, so an unexpected part that is covered
  * only by the auxiliary part is still correctly classified as covered — independent of the
  * order in which markers were added.
  */
class OverlappingPartCovering
{
public:
    explicit OverlappingPartCovering(MergeTreeDataFormatVersion format_version_)
        : set(format_version_)
    {
    }

    /** Add a part to the index. The part is placed into the underlying `ActiveDataPartSet` if
      * it neither intersects nor is contained by any existing entry; otherwise it goes into the
      * overlapping list (or is silently dropped if a covering entry already exists, since the
      * existing covering entry already implies coverage of the new one).
      *
      * `out_skipped_reason`, if non-null, receives the human-readable description from
      * `ActiveDataPartSet::tryAddPart` when a part lands in the overlapping list. Empty when the
      * part is added to the primary set or when the part is already covered.
      */
    void add(const MergeTreePartInfo & part_info, const String & part_name, String * out_skipped_reason = nullptr)
    {
        String reason;
        const auto outcome = set.tryAddPart(part_info, &reason);
        if (outcome == ActiveDataPartSet::AddPartOutcome::HasIntersectingPart)
        {
            overlapping.emplace_back(part_info, part_name);
            if (out_skipped_reason != nullptr)
                *out_skipped_reason = std::move(reason);
        }
        else if (out_skipped_reason != nullptr)
        {
            out_skipped_reason->clear();
        }
    }

    void add(const String & part_name, String * out_skipped_reason = nullptr)
    {
        add(MergeTreePartInfo::fromPartName(part_name, set.getFormatVersion()), part_name, out_skipped_reason);
    }

    /** Returns the name of any stored part whose `MergeTreePartInfo::contains` is true for
      * `part_info`, or an empty string if none. Considers both the primary set and the
      * overlapping list, so coverage detection is independent of insertion order.
      */
    String getContainingPart(const MergeTreePartInfo & part_info) const
    {
        if (auto candidate = set.getContainingPart(part_info); !candidate.empty())
            return candidate;

        for (const auto & [extra_info, extra_name] : overlapping)
            if (extra_info.contains(part_info))
                return extra_name;

        return {};
    }

    String getContainingPart(const String & part_name) const
    {
        return getContainingPart(MergeTreePartInfo::fromPartName(part_name, set.getFormatVersion()));
    }

    size_t size() const
    {
        return set.size() + overlapping.size();
    }

    bool empty() const
    {
        return size() == 0;
    }

    /** Names of all parts in this index, primary set first, overlapping list after. */
    Strings getParts() const
    {
        Strings result = set.getParts();
        result.reserve(result.size() + overlapping.size());
        for (const auto & [_, name] : overlapping)
            result.push_back(name);
        return result;
    }

    /** Names of just the parts that landed in the overlapping list (intersected an existing
      * entry on `add`). Useful for diagnostics — most workloads should produce an empty list.
      */
    Strings getOverlappingParts() const
    {
        Strings result;
        result.reserve(overlapping.size());
        for (const auto & [_, name] : overlapping)
            result.push_back(name);
        return result;
    }

private:
    ActiveDataPartSet set;
    std::vector<std::pair<MergeTreePartInfo, String>> overlapping;
};

}
