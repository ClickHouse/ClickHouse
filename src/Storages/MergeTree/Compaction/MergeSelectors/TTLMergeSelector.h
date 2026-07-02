#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectors/IMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/DisjointPartsRangesSet.h>
#include <Storages/TTLDescription.h>

namespace DB
{

using PartitionIdToTTLs = std::map<String, time_t>;

/** Merge selector, which is used to remove values with expired ttl.
  * It selects parts to merge by greedy algorithm:
  *  1. `findCenters` picks every part whose `canConsiderPart` returns true
  *     and whose `getTTLForPart` is already past `current_time` as a
  *     candidate merge CENTER.
  *  2. For each center (heap-ordered by earliest expired TTL),
  *     `findLeftRangeBorder` / `findRightRangeBorder` extend the range
  *     into neighbors that pass `canIncludeInRange`. The neighbor gate is
  *     by default the same as `canConsiderPart`, but subclasses may
  *     loosen it to let finished parts piggy-back on an already-justified
  *     merge - see `TTLPartDropMergeSelector::canIncludeInRange` and
  *     `TTLRowDeleteMergeSelector::canIncludeInRange`.
  */
class ITTLMergeSelector : public IMergeSelector
{
    class MergeRangesConstructor;
    friend class MergeRangesConstructor;

public:
    ITTLMergeSelector(const PartitionIdToTTLs * merge_due_times_, time_t current_time_, size_t max_parts_to_merge_at_once_ = 0);

    PartsRanges select(
        const PartsRanges & parts_ranges,
        const MergeConstraints & merge_constraints,
        const RangeFilter & range_filter) const override;

protected:
    /// Get TTL value for part, may depend on child type and some settings in constructor.
    virtual time_t getTTLForPart(const PartProperties & part) const = 0;

    /// Get TTL value used for CENTER selection. By default this is the same
    /// value used for neighbor inclusion, but selectors can tighten it to
    /// ignore TTL entries that are valid to merge as neighbors but must not
    /// justify a new merge by themselves.
    virtual time_t getTTLForPartForCenter(const PartProperties & part) const { return getTTLForPart(part); }

    /// Returns true if part is a valid CENTER of a merge range - i.e., the
    /// selector will consider this part on its own as a reason to schedule a
    /// merge. Stricter checks (e.g., `has_any_non_finished_rows_affecting_ttls`)
    /// belong here so that a part with no remaining TTL work cannot keep
    /// re-triggering merges on every scheduler tick (issue #105647).
    virtual bool canConsiderPart(const PartProperties & part) const = 0;

    /// Returns true if part may be INCLUDED as a neighbor in a merge range
    /// whose center was selected via `canConsiderPart`. Defaults to
    /// `canConsiderPart`; subclasses may relax this to let finished neighbors
    /// piggy-back on an already-justified merge. Loosening here is safe
    /// because the center has already passed `canConsiderPart` and the
    /// disjoint-set tracking in `MergeRangesConstructor` prevents the same
    /// range from being re-merged.
    virtual bool canIncludeInRange(const PartProperties & part) const { return canConsiderPart(part); }

private:
    struct CenterPosition
    {
        RangesIterator range;
        PartsIterator center;
        time_t ttl{};
    };

    bool needToPostponePartition(const std::string & partition_id) const;

    std::vector<CenterPosition> findCenters(const PartsRanges & parts_ranges) const;

    PartsIterator findLeftRangeBorder(
        const CenterPosition & center_position,
        size_t & usable_memory,
        size_t & usable_rows,
        size_t & usable_parts,
        DisjointPartsRangesSet & disjoint_set) const;

    PartsIterator findRightRangeBorder(
        const CenterPosition & center_position,
        size_t & usable_memory,
        size_t & usable_rows,
        size_t & usable_parts,
        DisjointPartsRangesSet & disjoint_set) const;

    const time_t current_time;
    const PartitionIdToTTLs * merge_due_times;
    const size_t max_parts_to_merge_at_once;
};

/// Select parts that must be fully deleted because of ttl for part.
class TTLPartDropMergeSelector : public ITTLMergeSelector
{
public:
    explicit TTLPartDropMergeSelector(time_t current_time_, size_t max_parts_to_drop_at_once_);

private:
    time_t getTTLForPart(const PartProperties & part) const override;

    /// Center gate: a part is eligible as a merge center only if it has
    /// `general_ttl_info` and at least one rows-affecting TTL is still
    /// unfinished. Without this, a part whose `part_max_ttl` is past
    /// would be re-picked as a `TTLDrop` center on every scheduler tick
    /// because there is no per-partition cooldown for `TTLDrop` (issue
    /// #105647). Finished parts can still join a merge - see
    /// `canIncludeInRange` below.
    bool canConsiderPart(const PartProperties & part) const override;

    /// Looser than `canConsiderPart`: lets a finished neighbor join a range
    /// whose center is unfinished, so a single TTL merge can sweep both.
    bool canIncludeInRange(const PartProperties & part) const override;
};

/// Select parts that has some expired ttls.
class TTLRowDeleteMergeSelector : public ITTLMergeSelector
{
public:
    explicit TTLRowDeleteMergeSelector(const PartitionIdToTTLs & merge_due_times_, time_t current_time_);

private:
    time_t getTTLForPart(const PartProperties & part) const override;
    time_t getTTLForPartForCenter(const PartProperties & part) const override;

    /// Center gate: same shape as `TTLPartDropMergeSelector::canConsiderPart`
    /// - the part must have at least one unfinished rows-affecting TTL.
    /// `TTLRowDeleteMergeSelector` additionally throws out parts on
    /// no-merge volumes. Finished parts are excluded as CENTERS by both
    /// selectors; they can still be folded in as NEIGHBORS via
    /// `canIncludeInRange`.
    bool canConsiderPart(const PartProperties & part) const override;

    /// Looser than `canConsiderPart`: lets a finished neighbor join a range
    /// whose center is unfinished, so a single TTL merge can sweep both.
    bool canIncludeInRange(const PartProperties & part) const override;
};

/// Select parts to merge using information about recompression TTL and compression codec of existing parts.
class TTLRecompressMergeSelector : public ITTLMergeSelector
{
public:
    explicit TTLRecompressMergeSelector(const PartitionIdToTTLs & merge_due_times_, time_t current_time_);

private:
    /// Return part min recompression TTL.
    time_t getTTLForPart(const PartProperties & part) const override;

    /// Checks that part's codec is not already equal to required codec
    /// according to recompression TTL. It doesn't make sense to assign such merge.
    bool canConsiderPart(const PartProperties & part) const override;
};

}
