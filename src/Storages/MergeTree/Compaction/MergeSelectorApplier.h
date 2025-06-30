#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>

#include <Storages/MergeTree/Compaction/PartProperties.h>

#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>

#include <Storages/MergeTree/MergeType.h>

#include <Storages/StorageInMemoryMetadata.h>

#include <optional>

namespace DB
{

struct MergeTreeSettings;
using MergeTreeSettingsPtr = std::shared_ptr<const MergeTreeSettings>;

struct MergeSelectorChoice
{
    PartsRange range;
    PartsRange range_patches;
    MergeType merge_type;

    /// If this merges down to a single part in a partition
    bool final = false;
};

class MergeSelectorApplier
{
public:
    const size_t max_total_size_to_merge = 0;
    const bool merge_with_ttl_allowed = false;
    const bool aggressive = false;
    const IMergeSelector::RangeFilter range_filter = nullptr;

    std::optional<MergeSelectorChoice> chooseMergeFrom(
        const PartsRanges & ranges,
        const IMergePredicate & predicate,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeSettingsPtr & data_settings,
        const PartitionIdToTTLs & next_delete_times,
        const PartitionIdToTTLs & next_recompress_times,
        bool can_use_ttl_merges,
        time_t current_time) const;
};

}
