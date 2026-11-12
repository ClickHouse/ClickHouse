#pragma once

#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// Estimate the number of rows and per-column statistics of the relation produced by the subtree
/// rooted at `node`, keyed by the subtree's output column names. `filter` is an optional predicate
/// over these columns to account for. Runtime filters prune nothing at plan time, so the estimate
/// ignores a predicate that holds whenever its runtime filters pass: a runtime filter, an `and` of
/// them, or `__applyFilter(...) OR isNull(key)`.
/// With `for_runtime_filter_transport` the estimate is a hard row cap for a transported filter's
/// exact phase, so an unindexed plan-time filter yields no estimate instead of a statistics guess.
/// `find_single_node_read` is the one of `QueryPlanOptimizationSettings`. With it, a read without an analysis of its
/// own is never analyzed here: it is estimated from the analysis the single-node read it finds already has, or from
/// that read's last `selectRangesToReadForEstimation`, and without either it has no estimate.
RelationStats estimateReadRowsCount(
    QueryPlan::Node & node,
    const ActionsDAG::Node * filter = nullptr,
    bool for_runtime_filter_transport = false,
    const FindSingleNodeRead & find_single_node_read = {});

}
