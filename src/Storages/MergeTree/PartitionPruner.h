#pragma once

#include <unordered_map>

#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

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
    PartitionPruner(const KeyDescription & partition_key_, const SelectQueryInfo & query_info, const Context & context)
        : partition_key(partition_key_)
        , partition_condition(
              query_info, context, partition_key.column_names, partition_key.expression, true /* single_point */, true /* strict */)
        , useless(partition_condition.alwaysUnknownOrTrue())
    {
    }

    bool canBePruned(DataPartPtr part)
    {
        if (part->isEmpty())
            return true;
        const auto & partition_id = part->info.partition_id;
        bool is_valid;
        if (auto it = partition_filter_map.find(partition_id); it != partition_filter_map.end())
            is_valid = it->second;
        else
        {
            const auto & partition_value = part->partition.value;
            std::vector<FieldRef> index_value(partition_value.begin(), partition_value.end());
            is_valid = partition_condition.mayBeTrueInRange(
                partition_value.size(), index_value.data(), index_value.data(), partition_key.data_types);
            partition_filter_map.emplace(partition_id, is_valid);
        }
        return !is_valid;
    }

    bool isUseless() const { return useless; }
};

}
