#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/CollapsingSortedAlgorithm.h>

namespace DB
{

/// Implementation of IMergingTransform via CollapsingSortedAlgorithm.
class CollapsingSortedTransform final : public IMergingTransform<CollapsingSortedAlgorithm>
{
public:
    CollapsingSortedTransform(
        const Block & header,
        size_t num_inputs,
        SortDescription description_,
        const String & sign_column,
        bool only_positive_sign,
        size_t max_block_size,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false)
        : IMergingTransform(
            num_inputs, header, header, /*have_all_inputs_=*/ true, /*limit_hint_=*/ 0,
            header,
            num_inputs,
            std::move(description_),
            sign_column,
            only_positive_sign,
            max_block_size,
            &Poco::Logger::get("CollapsingSortedTransform"),
            out_row_sources_buf_,
            use_average_block_sizes)
    {
    }

    String getName() const override { return "CollapsingSortedTransform"; }
};

}
