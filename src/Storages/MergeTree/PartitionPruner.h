#pragma once

#include <unordered_map>

#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

/// Pruning partitions in verbatim way using KeyCondition
class PartitionPruner
{
private:
    std::unordered_map<String, bool> partition_filter_map;
    const KeyDescription & partition_key;
    KeyCondition partition_condition;
    bool useless;
    using DataPart = IMergeTreeDataPart;
    using DataPartPtr = std::shared_ptr<const DataPart>;

public:
    PartitionPruner(const KeyDescription & partition_key_, const SelectQueryInfo & query_info, const Context & context, bool strict)
        : partition_key(partition_key_)
        , partition_condition(
              query_info, context, partition_key.column_names, partition_key.expression, true /* single_point */, strict)
        , useless(strict ? partition_condition.anyUnknownOrAlwaysTrue() : partition_condition.alwaysUnknownOrTrue())
    {
    }

    bool canBePruned(const DataPartPtr & part);

    bool isUseless() const { return useless; }
};

}
