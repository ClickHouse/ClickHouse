#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

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

}

}
