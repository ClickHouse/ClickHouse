#pragma once

#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/MergeTree/BoolMask.h>

namespace DB
{

/// Part pruner based on Column Statistics, now only support MinMax
class StatisticsPartPruner
{
public:
    using RPNElement = ConditionSelectivityEstimator::RPNElement;

    static std::optional<StatisticsPartPruner> build(
        const StorageMetadataPtr & metadata,
        const ActionsDAG::Node * filter_node,
        ContextPtr context);

    BoolMask checkPartCanMatch(const Estimates & estimates) const;

    bool hasUsefulConditions() const { return has_minmax_conditions; }

    const std::vector<std::string> & getUsedColumns() const { return used_columns; }

private:
    explicit StatisticsPartPruner(std::vector<RPNElement> rpn_);

    void buildDescription();

    std::vector<RPNElement> rpn;
    bool has_minmax_conditions = false;
    std::vector<std::string> used_columns;

    static BoolMask checkRangeCondition(
        const String & column_name,
        const PlainRanges & condition_ranges,
        const Estimates & estimates);
};

}
