#pragma once

#include <Storages/Statistics/TDigestStatistic.h>

namespace DB
{

class RPNBuilderTreeNode;

/// It estimates the selectivity of a condition.
class ConditionEstimator
{
private:

    static constexpr auto default_good_cond_factor = 0.1;
    static constexpr auto default_normal_cond_factor = 0.5;
    static constexpr auto default_unknown_cond_factor = 1.0;
    /// Conditions like "x = N" are considered good if abs(N) > threshold.
    /// This is used to assume that condition is likely to have good selectivity.
    static constexpr auto threshold = 2;

    UInt64 total_count = 0;

    /// Minimum estimator for values in a part. It can contains multiple types of statistics.
    /// But right now we only have tdigest;
    struct PartColumnEstimator
    {
        UInt64 part_count = 0;

        std::shared_ptr<TDigestStatistic> tdigest;

        void merge(StatisticPtr statistic)
        {
            UInt64 cur_part_count = statistic->count();
            if (part_count == 0)
                part_count = cur_part_count;

            if (typeid_cast<TDigestStatistic *>(statistic.get()))
            {
                tdigest = std::static_pointer_cast<TDigestStatistic>(statistic);
            }
        }

        Float64 estimateLess(Float64 val) const
        {
            if (tdigest != nullptr)
                return tdigest -> estimateLess(val);
            return part_count * default_normal_cond_factor;
        }

        Float64 estimateGreator(Float64 val) const
        {
            if (tdigest != nullptr)
                return part_count - tdigest -> estimateLess(val);
            return part_count * default_normal_cond_factor;
        }
    };

    /// An estimator for a column consists of several PartColumnEstimator.
    /// We simply get selectivity for every part estimator and combine the result.
    struct ColumnEstimator
    {
        std::map<std::string, PartColumnEstimator> estimators;

        void merge(std::string part_name, StatisticPtr statistic)
        {
            estimators[part_name].merge(statistic);
        }

        Float64 estimateLess(Float64 val) const
        {
            if (estimators.empty())
                return default_normal_cond_factor;
            Float64 result = 0;
            for (const auto & [key, estimator] : estimators)
                result += estimator.estimateLess(val);
            return result;
        }

        Float64 estimateGreater(Float64 val) const
        {
            if (estimators.empty())
                return default_normal_cond_factor;
            Float64 result = 0;
            for (const auto & [key, estimator] : estimators)
                result += estimator.estimateGreator(val);
            return result;
        }
    };

    std::map<String, ColumnEstimator> column_estimators;
    /// std::optional<std::string> extractSingleColumn(const RPNBuilderTreeNode & node) const;
    std::pair<std::string, Float64> extractBinaryOp(const RPNBuilderTreeNode & node, const std::string & column_name) const;

public:
    ConditionEstimator() = default;

    /// TODO: Support the condition consists of CNF/DNF like (cond1 and cond2) or (cond3) ...
    /// Right now we only support simple condition like col = val / col < val
    Float64 estimateSelectivity(const RPNBuilderTreeNode & node) const;

    void merge(std::string part_name, UInt64 part_count, StatisticPtr statistic)
    {
        total_count += part_count;
        if (statistic != nullptr)
            column_estimators[statistic->columnName()].merge(part_name, statistic);
    }
};


}
