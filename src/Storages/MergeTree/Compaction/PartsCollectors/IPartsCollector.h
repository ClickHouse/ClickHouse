#pragma once

#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <memory>
#include <optional>

namespace DB
{

using PartitionIdsHint = std::unordered_set<String>;

class IPartsCollector
{
public:
    virtual ~IPartsCollector() = default;

    virtual PartsRanges collectPartsToUse(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr & storage_policy,
        const time_t & current_time,
        const std::optional<PartitionIdsHint> & partitions_hint) const = 0;
};

using PartsCollectorPtr = std::shared_ptr<const IPartsCollector>;

}
