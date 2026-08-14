#pragma once
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB::QueryPlanOptimizations
{

/// Assigns exchanges to every send/receive runtime filter pair of a distributed plan (matched by
/// the rendezvous key; see `wireDistributedRuntimeFilters`), once the stages and their task lists
/// exist. Called at the end of `makeDistributedPlan`; standalone so tests can drive it against
/// synthetic stage/task sets and assert the exact exchange stream counts.
///
/// A filter built by a single task is broadcast directly: one stream per destination task of every
/// receiving stage. A filter built by `S > 1` tasks goes through a bounded fan-in merge tree
/// instead of all-to-all delivery: every build task sends its partial once to its parent merge
/// task, intermediate merge stages (new stages of `MergeRuntimeFiltersStep` tasks, fan-in
/// `RUNTIME_FILTER_MERGE_FAN_IN`) merge complete child states, and the single root task broadcasts
/// the global union once to every destination task. The stream count is thus `S + O(S / fan_in)`
/// tree edges plus one stream per destination, i.e. linear in the task count, where all-to-all was
/// `S x D`.
///
/// `next_exchange_id` continues the caller's exchange counter; it names the new exchanges and
/// merge stages.
///
/// `default_kind` is the kind the caller assigns to its own data exchanges (i.e. the resolved
/// `distributed_plan_force_exchange_kind`). Filter exchanges start from it, so a forced or
/// auto-selected Persisted plan does not get Streaming filter exchanges: the probe producer stages
/// that receive a filter are siblings of the build stage, not its dependents, so there is often no
/// data edge between the two whose kind could be copied.
void wireRuntimeFilterExchangeTopology(
    DistributedQueryPlan & distributed_plan, size_t & next_exchange_id, ExchangeDescription::Kind default_kind);

}
