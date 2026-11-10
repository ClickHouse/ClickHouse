#pragma once

#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <functional>

namespace DB::QueryPlanOptimizations
{

/// Estimate the number of rows and per-column statistics of the relation produced by the subtree
/// rooted at `node`, keyed by the subtree's output column names. `filter` is an optional predicate
/// over these columns to account for. Runtime filters prune nothing at plan time, so the estimate
/// ignores a predicate that holds whenever its runtime filters pass: a runtime filter, an `and` of
/// them, or `__applyFilter(...) OR isNull(key)`.
/// With `for_runtime_filter_transport` the estimate is a hard row cap for a transported filter's
/// exact phase, so an unindexed plan-time filter yields no estimate instead of a statistics guess.
RelationStats estimateReadRowsCount(
    QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr, bool for_runtime_filter_transport = false);

/// Whether an output column of the subtree rooted at a node holds the same value in every row: a constant, a deterministic
/// stateless function of constants, or a column passing one through unchanged. Not exhaustive.
/// Copies of the lookup share one cache, so the plan must not change while it is used.
using ConstantColumnLookup = std::function<bool(const QueryPlan::Node & node, const String & column_name)>;
ConstantColumnLookup makeConstantColumnLookup();

}
