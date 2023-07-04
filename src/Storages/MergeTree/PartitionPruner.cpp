#include <Storages/MergeTree/PartitionPruner.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace
{

KeyCondition buildKeyCondition(const KeyDescription & partition_key, const SelectQueryInfo & query_info, ContextPtr context, bool strict)
{
    if (context->getSettingsRef().allow_experimental_analyzer)
        return {query_info.filter_actions_dag, context, partition_key.column_names, partition_key.expression, {}, true /* single_point */, strict};

    return {query_info, context, partition_key.column_names, partition_key.expression, true /* single_point */, strict};
}

}

PartitionPruner::PartitionPruner(const StorageMetadataPtr & metadata, const SelectQueryInfo & query_info, ContextPtr context, bool strict)
    : partition_key(MergeTreePartition::adjustPartitionKey(metadata, context))
    , partition_condition(buildKeyCondition(partition_key, query_info, context, strict))
    , useless(strict ? partition_condition.anyUnknownOrAlwaysTrue() : partition_condition.alwaysUnknownOrTrue())
{
}

PartitionPruner::PartitionPruner(const StorageMetadataPtr & metadata, ActionsDAGPtr filter_actions_dag, ContextPtr context, bool strict)
    : partition_key(MergeTreePartition::adjustPartitionKey(metadata, context))
    , partition_condition(filter_actions_dag, context, partition_key.column_names, partition_key.expression, {}, true /* single_point */, strict)
    , useless(strict ? partition_condition.anyUnknownOrAlwaysTrue() : partition_condition.alwaysUnknownOrTrue())
{
}

bool PartitionPruner::canBePruned(const IMergeTreeDataPart & part)
{
    if (part.isEmpty())
        return true;

    const auto & partition_id = part.info.partition_id;
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
            WriteBufferFromOwnString buf;
            part.partition.serializeText(part.storage, buf, FormatSettings{});
            LOG_TRACE(&Poco::Logger::get("PartitionPruner"), "Partition {} gets pruned", buf.str());
        }
    }

    return !is_valid;
}

}
