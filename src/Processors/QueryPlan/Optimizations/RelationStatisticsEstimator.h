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
    /// The estimate is a hard row cap for a transported runtime filter's exact phase, so an unindexed
    /// plan-time filter yields no estimate instead of a statistics guess.
    bool for_runtime_filter_transport = false;
};

/// Estimate the number of rows and per-column statistics of the relation produced by the subtree
/// rooted at `node`, keyed by the subtree's output column names.
RelationStats estimateReadRowsCount(QueryPlan::Node & node, RelationStatsOptions options = {});

}
