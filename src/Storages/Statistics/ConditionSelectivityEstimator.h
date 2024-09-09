#pragma once

#include <Storages/Statistics/Statistics.h>

namespace DB
{

class RPNBuilderTreeNode;

/// It estimates the selectivity of a condition.
class ConditionSelectivityEstimator
{
public:
    ConditionSelectivityEstimator() : stats(Statistics()) {}
    explicit ConditionSelectivityEstimator(const Statistics & stats_) : stats(stats_) {}

    /// TODO: Support the condition consists of CNF/DNF like (cond1 and cond2) or (cond3) ...
    /// Right now we only support simple condition like col = val / col < val
    Float64 estimateRowCount(const RPNBuilderTreeNode & node) const;

private:
    friend class ColumnStatistics;

    /// Used to estimate the selectivity of a condition when there is no statistics.
    static constexpr auto default_cond_range_factor = 0.5;
    static constexpr auto default_cond_equal_factor = 0.01;
    static constexpr auto default_unknown_cond_factor = 1;

    Statistics stats;
};

}
