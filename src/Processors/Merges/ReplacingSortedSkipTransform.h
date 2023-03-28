#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/ReplacingSortedSkipAlgorithm.h>


namespace DB
{

/// Implementation of IMergingTransform via ReplacingSortedAlgorithm.
class ReplacingSortedSkipTransform final : public IMergingTransform<ReplacingSortedSkipAlgorithm>
{
public:
    static Block transformHeader(const Block &input_header)
    {
        Block header = input_header.cloneEmpty();

        ColumnWithTypeAndName filter_column(std::make_shared<DataTypeUInt8>(), "internal_filter_final_flags");
        header.insert(std::move(filter_column));

        return header;
    }

    ReplacingSortedSkipTransform(
        const Block & input_header, const Block &output_header, size_t num_inputs,
        SortDescription description_, const String & version_column,
        size_t max_block_size
        )
        : IMergingTransform(
            num_inputs, input_header, output_header, /*have_all_inputs_=*/ true, /*limit_hint_=*/ 0, /*always_read_till_end_=*/ false,
            input_header,
            output_header,
            num_inputs,
            std::move(description_),
            version_column,
            max_block_size,
            false)
    {
    }

    String getName() const override { return "ReplacingSortedSkip"; }
};

}
