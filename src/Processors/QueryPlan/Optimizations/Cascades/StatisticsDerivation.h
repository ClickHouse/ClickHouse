#pragma once

#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Core/Joins.h>
#include <Common/Logger.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// Clamp an inner-join-style row-count estimate to the semantics of the join kind and strictness.
/// `base` is the multiplicative inner estimate; `left`/`right` are the input row estimates. A
/// directional cost heuristic, not exact cardinality. Exposed for testing.
Float64 clampJoinRowCount(JoinKind kind, JoinStrictness strictness, Float64 base, Float64 left, Float64 right);

/// A valid upper bound on the join output row count, given the input max counts. Unlike the estimate
/// clamp, this must never fall below a possible output: a FULL join over disjoint sides can emit
/// `left + right` rows, and an ANTI join can emit the whole preserved side. `product` is
/// `left * right`. Exposed for testing.
Float64 clampJoinMaxRowCount(JoinKind kind, JoinStrictness strictness, Float64 product, Float64 left, Float64 right);

/// The TRUE fraction of a filter expression, estimated from the input column NDVs and
/// equivalence classes. Exposed for testing.
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
class RollupStep;
class CubeStep;
class UnionStep;

/// The count of the distinct values that the `uniqExact` states of an aggregation hold, and the bytes of
/// these values in the hash sets of the states. A function with a combinator (`uniqExactIf`) is not
/// counted: the estimate does not know which values its condition accepts.
std::pair<Float64, Float64> estimateDistinctValuesOfStates(const AggregatingStep & aggregating_step, const ExpressionStatistics & input_statistics);

/// Derives statistics for groups in the Cascades optimizer.
/// Statistics are logical properties that describe the data (row counts, NDVs)
/// and should be available before cost estimation and rule application.
class StatisticsDerivation
{
public:
    explicit StatisticsDerivation(Memo & memo_, const IOptimizerStatistics & statistics_lookup_)
        : memo(memo_)
        , statistics_lookup(statistics_lookup_)
    {}

    /// Derive statistics for a group based on one of its logical expressions, recursively
    /// deriving the input groups' statistics first.
    void deriveStatistics(GroupId group_id);

private:
    ExpressionStatistics deriveJoinStatistics(const JoinStepLogical & join_step, const ExpressionStatistics & left_statistics, const ExpressionStatistics & right_statistics);
    ExpressionStatistics deriveReadStatistics(const ReadFromMergeTree & read_step);
    ExpressionStatistics deriveFilterStatistics(const FilterStep & filter_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveExpressionStatistics(const ExpressionStep & expression_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveSortingStatistics(const SortingStep & sorting_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveLimitStatistics(const LimitStep & limit_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveDistinctStatistics(const DistinctStep & distinct_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveRollupStatistics(const RollupStep & rollup_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveCubeStatistics(const CubeStep & cube_step, const ExpressionStatistics & input_statistics);
    ExpressionStatistics deriveUnionStatistics(const UnionStep & union_step, const VectorWithMemoryTracking<const ExpressionStatistics *> & inputs_statistics);
    /// A final `DISTINCT` directly over a preliminary one on the same columns.
    static bool isOverPreliminaryDistinct(const DistinctStep & distinct_step, const Group & input_group);

    /// Fill per-column average value sizes of the read (storage-derived, hint overrides).
    void fillReadColumnWidths(ExpressionStatistics & statistics, const ReadFromMergeTree & read_step, const String & table_name);

    Memo & memo;
    const IOptimizerStatistics & statistics_lookup;
    LoggerPtr log = getLogger("StatisticsDerivation");
};

}
