#pragma once

#include <Storages/Statistics/Statistics.h>
#include <Core/Field.h>

namespace DB
{

class RPNBuilderTreeNode;

/// Estimates the selectivity of a condition.
class ConditionSelectivityEstimator
{
public:
    /// TODO: Support the condition consists of CNF/DNF like (cond1 and cond2) or (cond3) ...
    /// Right now we only support simple condition like col = val / col < val
    Float64 estimateRowCount(const RPNBuilderTreeNode & node) const;

    void addStatistics(String part_name, ColumnStatisticsPartPtr column_stat);
    void incrementRowCount(UInt64 rows);

private:
    friend class ColumnPartStatistics;

    struct ColumnSelectivityEstimator
    {
        /// We store the part_name and part_statistics.
        /// then simply get selectivity for every part_statistics and combine them.
        std::map<String, ColumnStatisticsPartPtr> part_statistics;

        void addStatistics(String part_name, ColumnStatisticsPartPtr stats);

        Float64 estimateLess(const Field & val, Float64 rows) const;
        Float64 estimateGreater(const Field & val, Float64 rows) const;
        Float64 estimateEqual(const Field & val, Float64 rows) const;
    };

    std::pair<String, Field> extractBinaryOp(const RPNBuilderTreeNode & node, const String & column_name) const;

    /// Magic constants for estimating the selectivity of a condition no statistics exists.
    static constexpr auto default_cond_range_factor = 0.5;
    static constexpr auto default_cond_equal_factor = 0.01;
    static constexpr auto default_unknown_cond_factor = 1;

    UInt64 total_rows = 0;
    std::map<String, ColumnSelectivityEstimator> column_estimators;
};

}
