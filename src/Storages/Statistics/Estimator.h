#pragma once

#include <Storages/Statistics/Statistics.h>

namespace DB
{

class RPNBuilderTreeNode;

/// It estimates the selectivity of a condition.
class ConditionEstimator
{
private:
    friend class ColumnStatistics;
    static constexpr auto default_good_cond_factor = 0.1;
    static constexpr auto default_normal_cond_factor = 0.5;
    static constexpr auto default_unknown_cond_factor = 1.0;
    /// Conditions like "x = N" are considered good if abs(N) > threshold.
    /// This is used to assume that condition is likely to have good selectivity.
    static constexpr auto threshold = 2;

    UInt64 total_count = 0;

    /// An estimator for a column consists of several PartColumnEstimator.
    /// We simply get selectivity for every part estimator and combine the result.
    struct ColumnEstimator
    {
        std::map<std::string, ColumnStatisticsPtr> estimators;

        void merge(std::string part_name, ColumnStatisticsPtr stats);

        Float64 estimateLess(Float64 val, Float64 total) const;

        Float64 estimateGreater(Float64 val, Float64 total) const;

        Float64 estimateEqual(Float64 val, Float64 total) const;
    };

    std::set<String> part_names;
    std::map<String, ColumnEstimator> column_estimators;
    std::pair<std::string, Float64> extractBinaryOp(const RPNBuilderTreeNode & node, const std::string & column_name) const;

public:
    ConditionEstimator() = default;

    /// TODO: Support the condition consists of CNF/DNF like (cond1 and cond2) or (cond3) ...
    /// Right now we only support simple condition like col = val / col < val
    Float64 estimateRowCount(const RPNBuilderTreeNode & node) const;

    void merge(std::string part_name, UInt64 part_count, ColumnStatisticsPtr column_stat);
};

}
