#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/Compaction/PartitionStatistics.h>
#include <Storages/MergeTree/MergeType.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;

struct MergeSelectorChoice
{
    PartsRange range;
    PartsRange range_patches;
    MergeType merge_type{};
    MergeCoverage merge_coverage = MergeCoverage::PartitionSegment;
};
using MergeSelectorChoices = std::vector<MergeSelectorChoice>;

class MergeSelectorApplier
{
public:
    const std::vector<MergeConstraint> merge_constraints;
    const bool merge_with_ttl_allowed = false;
    const bool aggressive = false;
    const IMergeSelector::RangeFilter range_filter = nullptr;
    const StorageID storage_id;

    /// A cap on the number of parts in one regular merge, applied on top of the `max_parts_to_merge_at_once` setting;
    /// 0 means no cap. The caller derives it from the resources of the server that will execute the merge,
    /// see `CompactionStatistics::getMaxPartsToMergeAtOnceForMemory`. Not applied to merges with TTL.
    const size_t max_parts_to_merge_at_once = 0;

    MergeSelectorApplier(
        std::vector<MergeConstraint> && merge_constraints_,
        bool merge_with_ttl_allowed_,
        bool aggressive_,
        IMergeSelector::RangeFilter range_filter_,
        StorageID storage_id_,
        size_t max_parts_to_merge_at_once_ = 0);

    MergeSelectorChoices chooseMergesFrom(
        const PartsRanges & ranges,
        const PartitionsStatistics & partitions_stats,
        const IMergePredicate & predicate,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeSettingsPtr & merge_tree_settings,
        const PartitionIdToTTLs & next_delete_times,
        const PartitionIdToTTLs & next_recompress_times,
        bool can_use_ttl_merges,
        time_t current_time) const;
};

}
