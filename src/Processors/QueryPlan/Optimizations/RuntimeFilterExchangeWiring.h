#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// Runs after the plan is cut into stages. Matches each `BuildRuntimeFilterStep` with the
/// `__applyFilter` sites of other stages by filter key and adds the exchanges that carry the filter:
/// the build tasks feed a merge tree of fan-in `RUNTIME_FILTER_MERGE_FAN_IN`, and its root sends the
/// union to the receiving tasks. `kind` is the kind of every exchange of the plan.
void wireRuntimeFilterExchangeTopology(
    DistributedQueryPlan & distributed_plan, size_t & next_exchange_id, ExchangeDescription::Kind kind);

}
