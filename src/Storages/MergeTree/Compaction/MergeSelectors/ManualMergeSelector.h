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
    static bool isAllScheduledPartsCovered(const StorageID & id, const ActiveDataPartSet & active_set);
    static void erase(const StorageID & id);

private:
    StorageID storage_id;
};

}
