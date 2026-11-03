#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// Optimization that replaces aggregation over MergeTree parts with a split pipeline:
/// cached parts are read from PartAggregationCache, uncached parts are aggregated normally.
/// Results are merged together using the same mechanism as aggregate projections.
///
/// When cache is cold and writes are enabled, eagerly populates the cache by running
/// per-part aggregation before the main query pipeline.
void optimizeUsePartAggregationCache(
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes,
    bool is_explain);

}
