#pragma once

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

    virtual PartsRanges collectPartsToUse(const MergeTreeTransactionPtr & txn, const PartitionIdsHint * partitions_hint) const = 0;
};

}
