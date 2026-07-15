#pragma once

#include <Storages/Statistics/Statistics.h>

#include <Core/Field.h>
#include <Core/PlainRanges.h>

namespace DB
{

class RPNBuilderTreeNode;

struct ColumnProfile
{
    /// TODO: Support min max
    /// Field min_value, max_value;
    Float64 num_distinct_values = 0;
};

struct RelationProfile
{
    Float64 rows = 0;
    std::unordered_map<String, ColumnProfile> column_profile = {};
};

/// Estimates the selectivity of a condition and cardinality of columns.
class ConditionSelectivityEstimator
{
    struct ColumnEstimator;
    using ColumnEstimators = std::unordered_map<String, ColumnEstimator>;
public:
    RelationProfile estimateRelationProfile(const RPNBuilderTreeNode & node) const;

    void addStatistics(ColumnStatisticsPtr column_stat);
    void incrementRowCount(UInt64 rows);

    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_IN_RANGE,
            FUNCTION_UNKNOWN,
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        Function function = FUNCTION_UNKNOWN;
        using ColumnRanges = std::unordered_map<String, PlainRanges>;
        /// column in range (a, b) ...
        ColumnRanges column_ranges;
        /// column not in range (a, b) ...
        /// we use 'not ranges' to estimate condition a != 1 and a != 2 better.
        ColumnRanges column_not_ranges;
        bool finalized = false;
        Float64 selectivity;

        bool tryToMergeClauses(RPNElement & lhs, RPNElement & rhs);
        void finalize(const ColumnEstimators & column_estimators_);
    };
    using AtomMap = std::unordered_map<std::string, void(*)(RPNElement & out, const String & column, const Field & value)>;
    static const AtomMap atom_map;
private:
    friend class ColumnStatistics;

    struct ColumnEstimator
    {
        ColumnStatisticsPtr stats;

        void addStatistics(ColumnStatisticsPtr other_stats);

        Float64 estimateRanges(const PlainRanges & ranges) const;
        Float64 estimateCardinality() const;
    };

    bool extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out) const;
    UInt64 estimateSelectivity(const RPNBuilderTreeNode & node) const;

    /// Magic constants for estimating the selectivity of a condition no statistics exists.
    static constexpr Float64 default_cond_range_factor = 0.5;
    static constexpr Float64 default_cond_equal_factor = 0.01;
    static constexpr Float64 default_unknown_cond_factor = 1;
    static constexpr Float64 default_cardinality_ratio = 0.1;

    UInt64 total_rows = 0;
    ColumnEstimators column_estimators;
};

using ConditionSelectivityEstimatorPtr = std::shared_ptr<ConditionSelectivityEstimator>;

}
