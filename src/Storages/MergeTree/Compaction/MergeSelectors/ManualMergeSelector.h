#pragma once

#include <Core/Names.h>
#include <Interpreters/StorageID.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/IMergeSelector.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <vector>

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
    /// Snapshot of the source parts currently scheduled for a manual merge on `id`. SYSTEM SYNC
    /// MERGES captures this once at entry so every one of its wait clauses (coverage, merge list,
    /// fetch) and the final clearScheduledParts() are scoped to exactly the same set, and a
    /// concurrent SCHEDULE MERGE that adds more parts meanwhile does not extend the wait.
    static std::vector<MergeTreePartInfo> getScheduledPartInfos(const StorageID & id);
    /// Whether every part in `scheduled_part_infos` is covered by a strictly larger part in
    /// `active_set`. Pure predicate over a caller-supplied snapshot: it reads no registry state and
    /// does NOT drain anything. SYSTEM SYNC MERGES has extra part_log waits after coverage, so it may
    /// loop (or time out / be cancelled) past this point; draining here would let a retry see an
    /// empty set and return before part_log is written. The scheduled set is cleared explicitly via
    /// clearScheduledParts() only once the whole command succeeds.
    static bool isAllScheduledPartsCovered(const std::vector<MergeTreePartInfo> & scheduled_part_infos, const ActiveDataPartSet & active_set);
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
