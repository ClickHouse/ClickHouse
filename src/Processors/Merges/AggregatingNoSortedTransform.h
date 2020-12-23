#pragma once

#include <Processors/Merges/Algorithms/AggregatingNoSortedAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>

namespace DB
{
class ColumnAggregateFunction;

/// Implementation of IMergingTransform via AggregatingSortedAlgorithm.
class AggregatingNoSortedTransform final : public IMergingTransform<AggregatingNoSortedAlgorithm>
{
public:
    AggregatingNoSortedTransform(const Block & header, size_t num_inputs) : IMergingTransform(num_inputs, header, header, true, header) {}

    String getName() const override { return "AggregatingNoSortedTransform"; }
};

}
