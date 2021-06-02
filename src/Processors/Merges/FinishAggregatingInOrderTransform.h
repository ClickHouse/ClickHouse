#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/FinishAggregatingInOrderAlgorithm.h>

namespace DB
{

class ColumnAggregateFunction;

/// Implementation of IMergingTransform via FinishAggregatingInOrderAlgorithm.
class FinishAggregatingInOrderTransform final : public IMergingTransform<FinishAggregatingInOrderAlgorithm>
{
public:
    FinishAggregatingInOrderTransform(
        const Block & header,
        size_t num_inputs,
        AggregatingTransformParamsPtr params,
        SortDescription description,
        size_t max_block_size)
        : IMergingTransform(
            num_inputs, header, header, true,
            header,
            num_inputs,
            params,
            std::move(description),
            max_block_size)
    {
    }

    String getName() const override { return "FinishAggregatingInOrderTransform"; }
};

}
