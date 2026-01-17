#pragma once

#include <Storages/Statistics/Statistics.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

class StatisticsPartPruner
{
public:
    static std::optional<StatisticsPartPruner> create(
        const StorageMetadataPtr & metadata,
        const ActionsDAG::Node * filter_node,
        ContextPtr context);

    BoolMask checkPartCanMatch(const Estimates & estimates) const;

    bool hasUsefulConditions() const { return !key_condition.alwaysUnknownOrTrue(); }

    const std::vector<String> & getUsedColumns() const { return used_column_names; }

private:
    StatisticsPartPruner(
        KeyCondition key_condition_,
        std::map<String, DataTypePtr> stats_column_name_to_type_map_,
        std::vector<String> used_column_names_);

    KeyCondition key_condition;
    std::map<String, DataTypePtr> stats_column_name_to_type_map;
    std::vector<String> used_column_names;
};

}
