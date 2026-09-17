#pragma once

#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// Estimate the number of rows and per-column statistics of the relation produced by the subtree
/// rooted at `node`, keyed by the subtree's output column names. `filter` is an optional predicate
/// over these columns to account for.
/// Pass `keep_index_analysis = false` when the plan under `node` is not optimized yet. Its
/// `ReadFromMergeTree` steps then have no pushed-down filter. If a step keeps an index analysis from
/// that state, the executed read prunes nothing.
RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr, bool keep_index_analysis = true);

}
