#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

struct QueryPlanOptimizationSettings;

namespace QueryPlanOptimizations
{

/// Whether a runtime filter of the join at `join_node`, pushed down into the probe (left) input as `tryPushDownFilter` does,
/// is applied to rows of an input estimated above `join_runtime_filter_min_probe_rows`: a join input on the way, or the
/// step above which the filter stays. With `check_left_does_not_contain` (LEFT ANTI JOIN) the keys share one filter. Only
/// the push-downs that are certain are followed, so a `false` may miss one.
bool runtimeFilterReachesInputAbove(
    const QueryPlan::Node & join_node, bool check_left_does_not_contain, const QueryPlanOptimizationSettings & settings);

/// The runtime filters of a join that `join_runtime_filter_min_probe_rows` would have declined for its small probe side,
/// created because `runtimeFilterReachesInputAbove` found them pushed down to a larger input.
struct RuntimeFiltersKeptForKeyPath
{
    QueryPlan::Node * join_node = nullptr;
    /// The rendezvous keys of the filters.
    VectorWithMemoryTracking<String> filter_keys;
};

/// Checks, once the filters are pushed down, that every join of `kept` applies one of its filters below a join input or to
/// an input estimated above `join_runtime_filter_min_probe_rows`, as `runtimeFilterReachesInputAbove` predicted, and throws
/// LOGICAL_ERROR if not. A filter pushed down into a step that keeps it, such as `ReadFromMerge`, is not found and not checked.
void checkRuntimeFiltersKeptForKeyPath(
    const VectorWithMemoryTracking<RuntimeFiltersKeptForKeyPath> & kept, const QueryPlanOptimizationSettings & settings);

}

}
