#pragma once

#include <optional>
#include <Storages/Statistics/Statistics.h>
#include <Core/Field.h>
#include <Core/ColumnWithTypeAndName.h>

namespace DB
{

class RPNBuilderTreeNode;

/// Estimates the selectivity of a condition.
class ConditionSelectivityEstimator
{
public:
    /// TODO: Support the condition consists of CNF/DNF like (cond1 and cond2) or (cond3) ...
    /// Right now we only support simple conditions like col = val / col < val, or a conjunction of those like (cond1 and cond2 and ...)
    Float64 estimateRowCount(const RPNBuilderTreeNode & node, const std::unordered_map<std::string, ColumnWithTypeAndName> & unqualified_column_names = {}) const;

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

        Float64 estimateLess(const Field & val, Float64 rows, std::optional<Float64> left_bound, std::optional<Float64> right_bound, std::optional<Float64> & val_as_float_to_return) const;
        Float64 estimateGreater(const Field & val, Float64 rows, std::optional<Float64> left_bound, std::optional<Float64> right_bound, std::optional<Float64> & val_as_float_to_return) const;
        Float64 estimateEqual(const Field & val, Float64 rows, std::optional<Float64> left_bound, std::optional<Float64> right_bound, std::optional<Float64> & val_as_float_to_return) const;
    };

    bool extractOperators(const RPNBuilderTreeNode & node, const String & qualified_column_name, std::vector<std::pair<String, Field>> & result) const;

    /// Magic constants for estimating the selectivity of a condition no statistics exists.
    static constexpr auto default_cond_range_factor = 0.5;
    static constexpr auto default_cond_equal_factor = 0.01;
    static constexpr auto default_unknown_cond_factor = 1;

    UInt64 total_rows = 0;
    std::map<String, ColumnSelectivityEstimator> column_estimators;
};

}
