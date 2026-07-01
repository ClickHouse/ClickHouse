#pragma once

#include <Core/Names.h>
#include <Interpreters/StorageID.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/IMergeSelector.h>

namespace DB
{

class ActiveDataPartSet;

class ManualMergeSelector : public IMergeSelector
{
public:
    explicit ManualMergeSelector(StorageID storage_id_);

    PartsRanges select(
        const PartsRanges & parts_ranges,
        const MergeConstraints & merge_constraints,
        const RangeFilter & range_filter) const override;

    static void push(const StorageID & id, const Names & parts_to_merge);
    /// Whether every scheduled source part of `id` is covered by a larger active part. This is a
    /// pure predicate: it does NOT drain the scheduled set. SYSTEM SYNC MERGES has extra part_log
    /// waits after coverage, so it may loop (or time out / be cancelled) past this point; draining
    /// here would let a retry see an empty set and return before part_log is written. The scheduled
    /// set is cleared explicitly via clearScheduledParts() only once the whole command succeeds.
    static bool isAllScheduledPartsCovered(const StorageID & id, const ActiveDataPartSet & active_set);
    /// Canonical names of all source parts currently scheduled for a manual merge on `id`.
    /// Used by SYSTEM SYNC MERGES to scope its part_log wait to exactly the scheduled merges.
    static NameSet getScheduledPartNames(const StorageID & id);
    /// Drop the scheduled source parts named in `part_names` from `id`. Called by SYSTEM SYNC MERGES
    /// after the command has fully succeeded (all parts covered and their part_log rows queued),
    /// scoped to the names captured at the start of the call so a concurrent SCHEDULE MERGE that
    /// added new parts meanwhile is left intact.
    static void clearScheduledParts(const StorageID & id, const NameSet & part_names);
    static void erase(const StorageID & id);

private:
    StorageID storage_id;
};

}
