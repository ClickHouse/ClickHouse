#pragma once

#include <optional>

#include <Common/Logger.h>

#include <Storages/StorageInMemoryMetadata.h>

#include <Storages/MergeTree/MergeType.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>

namespace DB
{

struct MergeSelectorChoice
{
    PartsRange range;
    MergeType merge_type;
};

class MergeSelectorApplier
{
    std::optional<MergeSelectorChoice> tryChooseTTLMerge(
        const PartsRanges & ranges,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeSettingsPtr & data_settings,
        const PartitionIdToTTLs & next_delete_times,
        const PartitionIdToTTLs & next_recompress_times,
        time_t current_time) const;

    std::optional<MergeSelectorChoice> tryChooseRegularMerge(
        const PartsRanges & ranges,
        const MergeTreeSettingsPtr & data_settings) const;

    std::optional<MergeSelectorChoice> chooseMergeFromImpl(
        const PartsRanges & ranges,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeSettingsPtr & data_settings,
        const PartitionIdToTTLs & next_delete_times,
        const PartitionIdToTTLs & next_recompress_times,
        bool can_use_ttl_merges,
        time_t current_time) const;

public:
    std::optional<MergeSelectorChoice> chooseMergeFrom(
        const PartsRanges & ranges,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeSettingsPtr & data_settings,
        const PartitionIdToTTLs & next_delete_times,
        const PartitionIdToTTLs & next_recompress_times,
        bool can_use_ttl_merges,
        time_t current_time,
        LoggerPtr & log,
        PreformattedMessage & out_disable_reason) const;

private:
    size_t max_total_size_to_merge = 0;
    bool merge_with_ttl_allowed = false;
    bool aggressive = false;
};

}
