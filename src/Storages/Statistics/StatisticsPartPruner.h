#pragma once

#include <Core/Names.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/Statistics/Statistics.h>

namespace DB
{

/// Part pruner based on column statistics, now only supports MinMax.
/// Similar to PartitionPruner but uses per-column statistics instead of partition keys.
/// When MinMax statistics are available for columns used in the filter condition,
/// this pruner can skip entire parts where the min/max range doesn't overlap with the query condition.
class StatisticsPartPruner
{
public:
    StatisticsPartPruner(const StorageMetadataPtr & metadata, const ActionsDAG::Node & filter_node, ContextPtr context);

    /// Check if the part can potentially match the filter condition based on statistics.
    /// Returns BoolMask indicating whether the condition can be true/false for this part.
    BoolMask checkPartCanMatch(const Estimates & estimates);

    /// Returns true if the pruner has no useful conditions, then all parts will match.
    bool isUseless() const { return useless; }

    /// Get the list of column names used in the filter condition that have statistics.
    Names getUsedColumns() const { return {used_column_names.begin(), used_column_names.end()}; }

private:
    /// Get or create a KeyCondition for the given columns, using cache to avoid recreating for each part.
    KeyCondition * getKeyConditionForEstimates(const NamesAndTypesList & columns_and_types);

    /// Cache key_condition by column names to avoid recreating them for each part.
    std::unordered_map<Names, std::unique_ptr<KeyCondition>, NamesHash> key_condition_cache;

    const ActionsDAGWithInversionPushDown filter_dag;
    const ContextPtr context;
    std::map<String, DataTypePtr> stats_column_name_to_type_map;
    NameOrderedSet used_column_names;
    bool useless = true;
};

}
