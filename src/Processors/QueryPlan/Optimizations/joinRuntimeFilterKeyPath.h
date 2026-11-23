#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

struct QueryPlanOptimizationSettings;

namespace QueryPlanOptimizations
{

/// Whether a runtime filter of the join at `join_node`, pushed down into the probe (left) input as `tryPushDownFilter` does,
/// is applied to an input estimated above `join_runtime_filter_min_probe_rows`: a join input on its way, or the step it
/// stays above. With `check_left_does_not_contain` (LEFT ANTI JOIN) the keys share one filter. Only the push-downs that are
/// certain are followed, so a `false` may miss one.
bool runtimeFilterReachesInputAbove(
    const QueryPlan::Node & join_node, bool check_left_does_not_contain, const QueryPlanOptimizationSettings & settings);

/// The runtime filters of a join created despite its small probe side, because `runtimeFilterReachesInputAbove` predicted
/// their push-down to a larger input.
struct RuntimeFiltersKeptForKeyPath
{
    /// The rewrites after `tryAddJoinRuntimeFilter` can move the join to another node, but keep its step.
    const IQueryPlanStep * join_step = nullptr;
    /// The rendezvous keys of the filters.
    VectorWithMemoryTracking<String> filter_keys;
};

/// Checks, after the filter push-down, that every join of `kept` in the plan at `root` applies one of its filters below a
/// join input, or directly above any input, estimated above `join_runtime_filter_min_probe_rows`, as
/// `runtimeFilterReachesInputAbove` predicted. Throws LOGICAL_ERROR if not. Filters absorbed by a step, such as
/// `ReadFromMerge`, are not found and not checked.
void checkRuntimeFiltersKeptForKeyPath(
    QueryPlan::Node & root,
    const VectorWithMemoryTracking<RuntimeFiltersKeptForKeyPath> & kept,
    const QueryPlanOptimizationSettings & settings);

}

}
