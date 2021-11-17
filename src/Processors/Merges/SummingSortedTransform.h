#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/SummingSortedAlgorithm.h>

namespace DB
{

/// Implementation of IMergingTransform via SummingSortedAlgorithm.
class SummingSortedTransform final : public IMergingTransform<SummingSortedAlgorithm>
{
public:

    SummingSortedTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_,
        /// List of columns to be summed. If empty, all numeric columns that are not in the description are taken.
        const Names & column_names_to_sum,
        const Names & partition_key_columns,
        size_t max_block_size)
        : IMergingTransform(
            num_inputs, header, header, /*have_all_inputs_=*/ true, /*limit_hint_=*/ 0,
            header,
            num_inputs,
            std::move(description_),
            column_names_to_sum,
            partition_key_columns,
            max_block_size)
    {
    }

    String getName() const override { return "SummingSortedTransform"; }
};

}
