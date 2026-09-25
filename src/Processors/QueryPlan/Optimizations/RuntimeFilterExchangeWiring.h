#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// Runs after the plan is cut into stages. Matches each `BuildRuntimeFilterStep` to the `__applyFilter`
/// sites of other stages by rendezvous key and wires the filter exchanges. The S build tasks feed a
/// merge tree of fan-in `RUNTIME_FILTER_MERGE_FAN_IN`, even when S = 1, and its root broadcasts to the
/// D receiving tasks: `S + O(S / fan_in) + D` streams in total.
/// `default_kind` is the plan's data-exchange kind. Probe producers are siblings of the build stage,
/// so there is often no data edge whose kind could be copied.
void wireRuntimeFilterExchangeTopology(
    DistributedQueryPlan & distributed_plan, size_t & next_exchange_id, ExchangeDescription::Kind default_kind);

/// After deserialize, copy `filter_key` from a sibling `__applyFilter` whose const result_name
/// equals the step's structural id. The key is not serialized (it must not enter a plan-step hash).
void restoreRuntimeFilterRendezvousKeys(QueryPlan & plan);

}
