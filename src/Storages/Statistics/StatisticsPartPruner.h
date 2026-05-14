#pragma once

#include <Core/Names.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/Statistics/Statistics.h>

namespace DB
{

/// Part pruner based on column statistics, currently supports MinMax and NullCount.
/// Similar to PartitionPruner but uses per-column statistics instead of partition keys.
/// When relevant statistics are available for columns used in the filter condition,
/// this pruner can skip entire parts where the statistics prove the condition cannot match.
class StatisticsPartPruner
{
public:
    StatisticsPartPruner(const StorageMetadataPtr & metadata, const ActionsDAG::Node & filter_node, ContextPtr context);

    /// Check if the part can potentially match the filter condition based on statistics.
    /// Returns BoolMask with the same semantics as KeyCondition::checkInHyperrectangle:
    ///   - can_be_true: whether any rows in the part might satisfy the condition
    ///   - can_be_false: whether any rows in the part might not satisfy the condition
    BoolMask checkPartCanMatch(const Estimates & estimates);

    /// Returns true if no columns with supported statistics are used in the filter, then all parts will match.
    bool isUseless() const { return useless; }

    /// Get the list of column names used in the filter condition that have statistics.
    Names getUsedColumns() const { return {used_column_names.begin(), used_column_names.end()}; }

private:
    /// Get or create a KeyCondition for the given columns, using cache to avoid recreating for each part.
    KeyCondition * getKeyConditionForEstimates(const NamesAndTypesList & columns_and_types);

    /// Cache key_condition by column names to avoid recreating them for each part.
    std::unordered_map<Names, std::unique_ptr<KeyCondition>, NamesHash> key_condition_cache;

    /// Names of `.null` subcolumns whose parent is a Nullable/LowCardinality(Nullable) column with
    /// `NullCount` statistics. Computed once in the constructor and passed to
    /// `ActionsDAGWithInversionPushDown` to trigger the `.null` → `!= 0` / `== 0` rewrite for
    /// those and only those names.
    NameSet null_subcolumns_to_normalize;

    const ActionsDAGWithInversionPushDown filter_dag;
    const ContextPtr context;
    std::map<String, DataTypePtr> stats_column_name_to_type_map;
    std::map<String, String> virtual_key_to_parent;
    NameOrderedSet used_column_names;
    bool useless = true;
};

}
