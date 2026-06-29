#pragma once

#include <Core/Block.h>
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
        SharedHeader header,
        size_t num_inputs,
        AggregatingTransformParamsPtr params,
        SortDescription description,
        size_t max_block_size_rows,
        size_t max_block_size_bytes)
        : IMergingTransform(
            num_inputs, header, std::make_shared<Block>(Block{}), /*have_all_inputs_=*/ true, /*limit_hint_=*/ 0, /*always_read_till_end_=*/ false,
            header,
            num_inputs,
            params,
            std::move(description),
            max_block_size_rows,
            max_block_size_bytes)
    {
    }

    String getName() const override { return "FinishAggregatingInOrderTransform"; }
};

}
