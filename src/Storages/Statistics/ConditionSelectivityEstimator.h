#pragma once

#include <Storages/Statistics/Statistics.h>
#include <Core/Field.h>

namespace DB
{

class RPNBuilderTreeNode;

/// It estimates the selectivity of a condition.
class ConditionSelectivityEstimator
{
public:
    /// TODO: Support the condition consists of CNF/DNF like (cond1 and cond2) or (cond3) ...
    /// Right now we only support simple condition like col = val / col < val
    Float64 estimateRowCount(const RPNBuilderTreeNode & node) const;

    void merge(String part_name, ColumnStatisticsPtr column_stat);
    void addRows(UInt64 part_rows) { total_rows += part_rows; }

private:
    friend class ColumnStatistics;
    struct ColumnSelectivityEstimator
    {
        /// We store the part_name and part_statistics.
        /// then simply get selectivity for every part_statistics and combine them.
        std::map<String, ColumnStatisticsPtr> part_statistics;

        void merge(String part_name, ColumnStatisticsPtr stats);

        Float64 estimateLess(const Field & val, Float64 rows) const;

        Float64 estimateGreater(const Field & val, Float64 rows) const;

        Float64 estimateEqual(const Field & val, Float64 rows) const;
    };

    std::pair<String, Field> extractBinaryOp(const RPNBuilderTreeNode & node, const String & column_name) const;

    static constexpr auto default_good_cond_factor = 0.1;
    static constexpr auto default_normal_cond_factor = 0.5;
    static constexpr auto default_unknown_cond_factor = 1.0;
    /// Conditions like "x = N" are considered good if abs(N) > threshold.
    /// This is used to assume that condition is likely to have good selectivity.
    static constexpr auto threshold = 2;

    UInt64 total_rows = 0;
    std::map<String, ColumnSelectivityEstimator> column_estimators;
};

}
