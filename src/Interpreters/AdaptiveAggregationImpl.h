#pragma once

#include <Interpreters/AdaptiveAggregation.h>
#include <Interpreters/AdaptiveAggregationProducer.h>
#include <Interpreters/AdaptiveAggregationSession.h>
#include <Interpreters/AdaptiveAggregationStaging.h>
#include <Interpreters/Aggregator.h>

namespace DB
{

/// Tuning of the drain side of the adaptive aggregation. The decision rules own their
/// constants (see `LearningState`, `FrozenState`, `ThawSampler`, `StagedChunkBuilder`).
/// The drain reserves a bucket's table after sampling this fraction of its records.
constexpr size_t adaptive_reserve_sample_inverse = 8;
/// Headroom over the sampled insert rate when reserving.
constexpr double adaptive_reserve_headroom = 1.25;
/// Fixed lookahead of the drain's hash prefetch.
constexpr size_t adaptive_drain_prefetch_look_ahead = 16;

struct StagedChunkPreparation
{
    Columns materialized_columns;
    Aggregator::AggregateColumns aggregate_columns;
    Aggregator::NestedColumnsHolder nested_columns_holder;
    Aggregator::AggregateFunctionInstructions instructions;
};


}
