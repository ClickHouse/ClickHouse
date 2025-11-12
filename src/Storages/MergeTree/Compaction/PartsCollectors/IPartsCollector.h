#pragma once

#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <memory>
#include <optional>
#include <expected>

namespace DB
{

using PartitionIdsHint = std::unordered_set<String>;

class IPartsCollector
{
public:
    virtual ~IPartsCollector() = default;

    virtual PartsRanges grabAllPossibleRanges(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr & storage_policy,
        const time_t & current_time,
        const std::optional<PartitionIdsHint> & partitions_hint,
        LogSeriesLimiter & series_log,
        bool ignore_prefer_not_to_merge) const = 0;

    virtual std::expected<PartsRange, PreformattedMessage> grabAllPartsInsidePartition(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr & storage_policy,
        const time_t & current_time,
        const std::string & partition_id,
        bool ignore_prefer_not_to_merge) const = 0;
};

using PartsCollectorPtr = std::shared_ptr<const IPartsCollector>;

}
