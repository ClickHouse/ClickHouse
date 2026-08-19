#pragma once

#include <Interpreters/AdaptiveAggregation.h>
#include <Interpreters/AdaptiveAggregationDrain.h>
#include <Interpreters/AdaptiveAggregationProducer.h>
#include <Interpreters/AdaptiveAggregationSession.h>
#include <Interpreters/AdaptiveAggregationStaging.h>
#include <Interpreters/Aggregator.h>

namespace DB
{

struct StagedChunkPreparation
{
    Columns materialized_columns;
    Aggregator::AggregateColumns aggregate_columns;
    Aggregator::NestedColumnsHolder nested_columns_holder;
    Aggregator::AggregateFunctionInstructions instructions;
};


}
