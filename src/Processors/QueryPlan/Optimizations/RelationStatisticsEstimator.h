#pragma once

#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// Apply a unary plan step to statistics already estimated for its input. Returns `std::nullopt`
/// when the step is not a supported unary statistics transformation.
std::optional<RelationStats> estimateUnaryStepStats(const IQueryPlanStep & step, RelationStats input_stats);

/// Estimate the number of rows and per-column statistics of the relation produced by the subtree
/// rooted at `node`, keyed by the subtree's output column names. `filter` is an optional predicate
/// over these columns to account for.
RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr);

}
