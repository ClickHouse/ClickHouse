#pragma once
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

struct QueryPlanSerializationSettings;

/// Helpers shared by the steps that carry a full (not merge-only) `Aggregator::Params` through
/// query-plan serialization: `Aggregating`, `Rollup` and `Cube`.
/// Step-specific knobs stay with the steps: `max_block_size` (a step field in `Aggregating`),
/// the stats-collecting settings (only `Aggregating` sends a stats key), and
/// `enable_packed_string_keys_in_aggregation` (needs per-step old-peer gating).

/// Writes the `Params` knobs into the plan settings the reader rebuilds them from.
void serializeAggregatorParamsToSettings(const Aggregator::Params & params, QueryPlanSerializationSettings & settings);

/// Rebuilds full `Params` from the deserialization settings.
Aggregator::Params deserializeAggregatorParams(
    Names keys,
    AggregateDescriptions aggregates,
    bool overflow_row,
    StatsCollectingParams stats_collecting_params,
    const IQueryPlanStep::Deserialization & ctx);

}
