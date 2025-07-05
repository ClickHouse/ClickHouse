#pragma once

#include <Storages/Statistics/Statistics.h>

#include <Core/Field.h>
#include <Core/PlainRanges.h>

namespace DB
{

class RPNBuilderTreeNode;

/// Estimates the selectivity of a condition.
class ConditionSelectivityEstimator
{
    struct ColumnSelectivityEstimator;
    using ColumnEstimators = std::unordered_map<String, ColumnSelectivityEstimator>;
public:
    Float64 estimateRowCount(const RPNBuilderTreeNode & node) const;

    void addStatistics(String part_name, ColumnStatisticsPartPtr column_stat);
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
    friend class ColumnPartStatistics;

    struct ColumnSelectivityEstimator
    {
        /// We store the part_name and part_statistics.
        /// then simply get selectivity for every part_statistics and combine them.
        std::map<String, ColumnStatisticsPartPtr> part_statistics;

        void addStatistics(String part_name, ColumnStatisticsPartPtr stats);

        Float64 estimateRanges(const PlainRanges & ranges) const;
    };

    bool extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out) const;

    /// Magic constants for estimating the selectivity of a condition no statistics exists.
    static constexpr auto default_cond_range_factor = 0.5;
    static constexpr auto default_cond_equal_factor = 0.01;
    static constexpr auto default_unknown_cond_factor = 1;

    UInt64 total_rows = 0;
    ColumnEstimators column_estimators;
};

}
