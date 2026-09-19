#pragma once

#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// Estimate the number of rows and per-column statistics of the relation produced by the subtree
/// rooted at `node`, keyed by the subtree's output column names. `filter` is an optional predicate
/// over these columns to account for. A predicate made only of runtime filters is ignored: it
/// prunes nothing at plan time.
/// With `for_runtime_filter_transport` the estimate is a hard row cap for a transported filter's
/// exact phase, so an unindexed plan-time filter yields no estimate instead of a statistics guess.
RelationStats estimateReadRowsCount(
    QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr, bool for_runtime_filter_transport = false);

}
