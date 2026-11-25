#pragma once

#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

struct RelationStatsOptions
{
    /// The estimate is a hard row cap for a transported runtime filter's exact phase, so an unindexed
    /// plan-time filter yields no estimate instead of a statistics guess.
    bool for_runtime_filter_transport = false;
    /// The one of `QueryPlanOptimizationSettings`, by pointer to keep the options cheap to copy. If it is set, a read
    /// without an analysis of its own is never analyzed here: it is estimated from the analysis the single-node read it
    /// finds already has, or from that read's last `selectRangesToReadForEstimation`, and without either it has no
    /// estimate.
    const FindSingleNodeRead * find_single_node_read = nullptr;
};

/// Estimate the number of rows and per-column statistics of the relation produced by the subtree
/// rooted at `node`, keyed by the subtree's output column names. `filter` is an optional predicate
/// over these columns to account for. Runtime filters prune nothing at plan time, so the estimate
/// ignores a predicate that holds whenever its runtime filters pass: a runtime filter, an `and` of
/// them, or `__applyFilter(...) OR isNull(key)`.
RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter, const RelationStatsOptions & options);

inline RelationStats estimateReadRowsCount(QueryPlan::Node & node, const RelationStatsOptions & options)
{
    return estimateReadRowsCount(node, nullptr, options);
}

inline RelationStats estimateReadRowsCount(
    QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr, bool for_runtime_filter_transport = false)
{
    return estimateReadRowsCount(node, filter, RelationStatsOptions{.for_runtime_filter_transport = for_runtime_filter_transport});
}

}
