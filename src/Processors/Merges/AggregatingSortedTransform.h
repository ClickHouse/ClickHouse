#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/AggregatingSortedAlgorithm.h>

namespace DB
{

class ColumnAggregateFunction;

/// Implementation of IMergingTransform via AggregatingSortedAlgorithm.
class AggregatingSortedTransform final : public IMergingTransform<AggregatingSortedAlgorithm>
{
public:
    AggregatingSortedTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_, size_t max_block_size)
        : IMergingTransform(
            num_inputs, header, header, true,
            header,
            num_inputs,
            std::move(description_),
            max_block_size)
    {
    }

    String getName() const override { return "AggregatingSortedTransform"; }
};

}
