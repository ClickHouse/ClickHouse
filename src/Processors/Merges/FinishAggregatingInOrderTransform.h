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
        size_t max_block_size,
        size_t max_block_bytes)
        : IMergingTransform(
            num_inputs, header, {}, /*have_all_inputs_=*/ true, /*limit_hint_=*/ 0,
            header,
            num_inputs,
            params,
            std::move(description),
            max_block_size,
            max_block_bytes)
    {
    }

    String getName() const override { return "FinishAggregatingInOrderTransform"; }
};

}
