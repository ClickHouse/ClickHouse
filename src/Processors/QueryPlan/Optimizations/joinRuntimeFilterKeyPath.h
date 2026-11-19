#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// Whether a runtime filter of the join at `join_node`, pushed down into the probe (left) input as `tryPushDownFilter` does,
/// reaches a join input estimated above `limit` rows. With `check_left_does_not_contain` (LEFT ANTI JOIN) the keys share
/// one filter. Only the push-downs that are certain are followed, so a `false` may miss one.
bool runtimeFilterReachesInputAbove(const QueryPlan::Node & join_node, bool check_left_does_not_contain, UInt64 limit);

}
