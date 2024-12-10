#pragma once

#include <memory>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>

namespace DB
{

class IPartsCollector
{
protected:
    using PartitionIdsHint = std::unordered_set<String>;

public:
    virtual ~IPartsCollector() = default;

    virtual PartsRanges collectPartsToUse(const MergeTreeTransactionPtr & tx, const PartitionIdsHint * partitions_hint) const = 0;
};

using PartsCollectorPtr = std::shared_ptr<const IPartsCollector>;

}
