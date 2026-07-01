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
    /// Returns BoolMask with the same semantics as KeyCondition::checkInHyperrectangle:
    ///   - can_be_true: whether any rows in the part might satisfy the condition
    ///   - can_be_false: whether any rows in the part might not satisfy the condition
    BoolMask checkPartCanMatch(const Estimates & estimates);

    /// Returns true if no columns with MinMax statistics are used in the filter, then all parts will match.
    bool isUseless() const { return useless; }

    /// Columns referenced by the filter that carry MinMax/Basic statistics. Known right after
    /// construction, so callers can load exactly these statistics before the first
    /// `checkPartCanMatch` call (which is what populates `used_column_names`). Passing this set
    /// to `IMergeTreeDataPart::getEstimates` keeps statistics loading lazy: an empty argument
    /// would be treated as "load every column's statistics".
    Names getCandidateColumns() const
    {
        Names result;
        result.reserve(stats_column_name_to_type_map.size());
        for (const auto & [name, _] : stats_column_name_to_type_map)
            result.push_back(name);
        return result;
    }

    /// Get the list of column names actually used by the built key conditions. Only meaningful
    /// after `checkPartCanMatch` has run for at least one part; used for query-plan reporting.
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
