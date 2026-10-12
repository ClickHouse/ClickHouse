#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Core/Joins.h>
#include <Common/Logger.h>

#include <functional>

namespace DB
{

/// The TRUE fraction of a filter expression, estimated from the input column NDVs and
/// equivalence classes. `used_default` is set when a default factor stood in for a predicate the
/// statistics could not estimate. Exposed for testing.
Float64 estimatePredicateSelectivity(const ActionsDAG::Node * node, const ExpressionStatistics & input_statistics, bool & used_default);
Float64 estimatePredicateSelectivity(const ActionsDAG::Node * node, const ExpressionStatistics & input_statistics);


class Memo;
class JoinStepLogical;
class ReadFromMergeTree;
class FilterStep;
class ExpressionStep;
class AggregatingStep;
class SortingStep;
class LimitStep;
class DistinctStep;
class UnionStep;

/// Derives statistics for groups in the Cascades optimizer.
/// Statistics are logical properties that describe the data (row counts, NDVs)
/// and should be available before cost estimation and rule application.
class StatisticsDerivation
{
public:
    StatisticsDerivation(Memo & memo_, const IOptimizerStatistics & statistics_lookup_)
        : memo(memo_)
        , statistics_lookup(statistics_lookup_)
    {}

    /// Derive statistics for a group based on one of its logical expressions, recursively
    /// deriving the input groups' statistics first. The first call derives every leaf group.
    void deriveStatistics(GroupId group_id);

private:
    void deriveGroupStatistics(GroupId group_id);
    /// Derives every leaf group, then gives the sources without an estimate or a bound the largest
    /// known leaf as their search value.
    void deriveLeafGroups();
    bool leaves_derived = false;

    ExpressionStatistics deriveJoinStatistics(const JoinStepLogical & join_step, const ExpressionStatistics & left_statistics, const ExpressionStatistics & right_statistics);
    ExpressionStatistics deriveReadStatistics(const ReadFromMergeTree & read_step);
    ExpressionStatistics deriveFilterStatistics(const FilterStep & filter_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveExpressionStatistics(const ExpressionStep & expression_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveAggregatingStatistics(const AggregatingStep & aggregating_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveSortingStatistics(const SortingStep & sorting_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveLimitStatistics(const LimitStep & limit_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveDistinctStatistics(const DistinctStep & distinct_step, const ExpressionStatistics & input_statistics);

    /// Estimate bytes per row of a read: table-level hint if present, otherwise the sum of the
    /// per-column widths already filled into `statistics`.
    Float64 estimateReadBytesPerRow(const ReadFromMergeTree & read_step, const ExpressionStatistics & statistics);

    ExpressionStatistics deriveUnionStatistics(
        const UnionStep & union_step, const std::function<const ExpressionStatistics &(size_t)> & input_statistics, size_t input_count);

    /// Fill per-column average value sizes of the read (storage-derived, hint overrides).
    void fillReadColumnWidths(ExpressionStatistics & statistics, const ReadFromMergeTree & read_step, const String & table_name);

    /// Search value for a source without any estimate or bound: the largest known leaf of the query.
    std::optional<Float64> largestKnownLeafRowCount() const;

    Memo & memo;
    const IOptimizerStatistics & statistics_lookup;
    LoggerPtr log = getLogger("StatisticsDerivation");
};

}
