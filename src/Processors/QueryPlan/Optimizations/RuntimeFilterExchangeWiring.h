#pragma once

#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// Post-cut: match `BuildRuntimeFilterStep` to `__applyFilter` by rendezvous key, then wire
/// filter exchanges. S=1 -> one stream per destination task. S>1 -> fan-in
/// `RUNTIME_FILTER_MERGE_FAN_IN` tree, stream count `S + O(S / fan_in) + D` (all-to-all was `S * D`).
/// `default_kind` is the plan's data-exchange kind; probe producers are siblings of the build
/// stage, so there is often no data edge whose kind could be copied.
void wireRuntimeFilterExchangeTopology(
    DistributedQueryPlan & distributed_plan, size_t & next_exchange_id, ExchangeDescription::Kind default_kind);

/// After deserialize, copy `filter_key` from a sibling `__applyFilter` whose const result_name
/// equals the step's structural id. The key is not serialized (it must not enter a plan-step hash).
void restoreRuntimeFilterRendezvousKeys(QueryPlan & plan);

}
