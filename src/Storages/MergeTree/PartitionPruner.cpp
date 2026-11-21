#include <Storages/MergeTree/PartitionPruner.h>
#include <Common/logger_useful.h>

namespace DB
{

PartitionPruner::PartitionPruner(
    const StorageMetadataPtr & metadata,
    const ActionsDAGWithInversionPushDown & filter_dag,
    ContextPtr context,
    bool strict,
    bool skip_analysis)
    : partition_key(MergeTreePartition::adjustPartitionKey(metadata, context))
    , partition_condition(
          filter_dag,
          context,
          partition_key.column_names,
          partition_key.expression,
          true /* single_point */,
          skip_analysis)
    , useless((strict && partition_condition.isRelaxed()) || partition_condition.alwaysUnknownOrTrue())
{
}

bool PartitionPruner::canBePruned(const IMergeTreeDataPart & part) const
{
    if (part.isEmpty())
        return true;

    const auto & partition_id = part.info.getPartitionId();
    bool is_valid = false;

    if (auto it = partition_filter_map.find(partition_id); it != partition_filter_map.end())
    {
        is_valid = it->second;
    }
    else
    {
        const auto & partition_value = part.partition.value;
        std::vector<FieldRef> index_value(partition_value.begin(), partition_value.end());
        for (auto & field : index_value)
        {
            // NULL_LAST
            if (field.isNull())
                field = POSITIVE_INFINITY;
        }

        is_valid = partition_condition.mayBeTrueInRange(
            partition_value.size(), index_value.data(), index_value.data(), partition_key.data_types);
        partition_filter_map.emplace(partition_id, is_valid);

        if (!is_valid)
        {
            auto partition_str = part.partition.serializeToString(part.getMetadataSnapshot());
            LOG_TRACE(getLogger("PartitionPruner"), "Partition {} gets pruned", partition_str);
        }
    }

    return !is_valid;
}

}
