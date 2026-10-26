#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/Compaction/PartitionStatistics.h>
#include <Storages/MergeTree/MergeTreeData.h>
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

    /// If this merges down to a single part in a partition
    bool final = false;
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
    /// The requested merge runs through `MergeAlgorithm::Horizontal` whatever the range (`OPTIMIZE ... DEDUPLICATE`
    /// or `OPTIMIZE ... FINAL CLEANUP`, see `MergeTask::ExecuteAndFinalizeHorizontalPart::chooseMergeAlgorithm`), so
    /// the memory-derived cap does not let a range merge wider on the grounds that it would merge vertically.
    const bool merge_forced_horizontal = false;

    MergeSelectorApplier(
        std::vector<MergeConstraint> && merge_constraints_,
        bool merge_with_ttl_allowed_,
        bool aggressive_,
        IMergeSelector::RangeFilter range_filter_,
        StorageID storage_id_,
        bool merge_forced_horizontal_ = false);

    MergeSelectorChoices chooseMergesFrom(
        const PartsRanges & ranges,
        const PartitionsStatistics & partitions_stats,
        const IMergePredicate & predicate,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeSettingsPtr & merge_tree_settings,
        /// `nullptr` for the storages that are not `MergeTreeData` (the compaction of Iceberg tables): the
        /// memory-derived cap of the merge width, which predicts the merge of `MergeTreeData` parts, does not apply then.
        const MergeTreeData * data,
        const PartitionIdToTTLs & next_delete_times,
        const PartitionIdToTTLs & next_recompress_times,
        bool can_use_ttl_merges,
        time_t current_time) const;
};

}
