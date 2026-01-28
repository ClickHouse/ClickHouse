#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/IPartsCollector.h>
#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>

#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/MutateTask.h>

namespace DB
{

PartsRanges grabAllPossibleRanges(
    const PartsCollectorPtr & parts_collector,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::optional<PartitionIdsHint> & partitions_hint,
    LogSeriesLimiter & series_log);

MergeSelectorChoices chooseMergesFrom(
    const MergeSelectorApplier & selector,
    const IMergePredicate & predicate,
    const PartsRanges & ranges,
    const PartitionsStatistics & partitions_stats,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeSettingsPtr & data_settings,
    const PartitionIdToTTLs & next_delete_times,
    const PartitionIdToTTLs & next_recompress_times,
    bool can_use_ttl_merges,
    time_t current_time,
    const LoggerPtr & log);

std::expected<PartsRange, PreformattedMessage> grabAllPartsInsidePartition(
    const PartsCollectorPtr & parts_collector,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::string & partition_id);

}
