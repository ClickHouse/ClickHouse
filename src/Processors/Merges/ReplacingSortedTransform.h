#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/ReplacingSortedAlgorithm.h>


namespace DB
{

/** Merges several sorted ports into one.
  * For each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  *  keeps row with max `version` value.
  */
class ReplacingSortedTransform final : public IMergingTransform2<ReplacingSortedAlgorithm>
{
public:
    ReplacingSortedTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_, const String & version_column,
        size_t max_block_size,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false)
        : IMergingTransform2(
            num_inputs, header, header, true,
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
