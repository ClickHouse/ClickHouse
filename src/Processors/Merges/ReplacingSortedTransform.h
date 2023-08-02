#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>


namespace DB
{

/// Implementation of IMergingTransform via ReplacingSortedAlgorithm.
class ReplacingSortedTransform final : public IMergingTransform<ReplacingSortedAlgorithm>
{
public:
    ReplacingSortedTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_, const String & version_column,
        size_t max_block_size,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false)
        : IMergingTransform(
            num_inputs, header, header, /*have_all_inputs_=*/ true, /*limit_hint_=*/ 0,
            header,
            num_inputs,
            std::move(description_),
            version_column,
            max_block_size,
            out_row_sources_buf_,
            use_average_block_sizes)
    {
    }

    String getName() const override { return "ReplacingSorted"; }
};

}
