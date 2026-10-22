#pragma once

#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// Apply a unary plan step to statistics already estimated for its input. Returns `std::nullopt`
/// when the step is not a supported unary statistics transformation.
std::optional<RelationStats> estimateUnaryStepStats(const IQueryPlanStep & step, RelationStats input_stats);

struct RelationStatsOptions
{
    /// Propagate the row estimate and column statistics of an already optimized `JoinStepLogical`
    /// to its parents. Off by default: join-algorithm selection (`rhs_size_estimation`), the
    /// small-probe decline and broadcast/shuffle decisions consume `estimated_rows` without checking
    /// how it was derived, and doing so regressed performance when join-key NDVs were unknown
    /// (#97114, reverted in #99957 and backported to 26.2/26.3; robust fix pending in #101398).
    /// Consumers that gate every decision on `ColumnStatsProvenance` may opt in: a sub-join output
    /// carries `Unsupported`, so they fail closed. Join reordering opts in when it collects the
    /// relations of a join graph (restored in #120110): a sub-join enters its parent graph as a single
    /// relation, and without its estimate the cost model reads the missing count as one row.
    bool propagate_join_estimates = false;

    /// The estimate is a hard row cap for a transported runtime filter's exact phase, so an unindexed
    /// plan-time filter yields no estimate instead of a statistics guess.
    bool for_runtime_filter_transport = false;
};

/// Estimate the number of rows and per-column statistics of the relation produced by the subtree
/// rooted at `node`, keyed by the subtree's output column names. `filter` is an optional predicate
/// over these columns to account for. Runtime filters prune nothing at plan time, so the row estimate
/// ignores a predicate that holds whenever its runtime filters pass: a runtime filter, an `and` of
/// them, or `__applyFilter(...) OR isNull(key)`. Such a predicate still drops rows at execution time,
/// so the provenance of the result records it as a row subset.
RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr, RelationStatsOptions options = {});

}
