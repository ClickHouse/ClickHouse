#pragma once

#include <memory>
#include <optional>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>

namespace DB
{

using PartitionIdsHint = std::unordered_set<String>;

class IPartsCollector
{
public:
    virtual ~IPartsCollector() = default;

    virtual PartsRanges collectPartsToUse(const std::optional<PartitionIdsHint> & partitions_hint) const = 0;
};

using PartsCollectorPtr = std::shared_ptr<const IPartsCollector>;

}
