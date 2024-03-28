#pragma once

#include <Processors/Merges/Algorithms/StatelessAggregatingSortedAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>

namespace DB
{

/// Implementation of IMergingTransform via StatelessAggregatingSortedAlgorithm.
class StatelessAggregatingSortedTransform final : public IMergingTransform<StatelessAggregatingSortedAlgorithm>
{
public:
    StatelessAggregatingSortedTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_,
        /// List of columns to be aggregated. If empty, all numeric columns that are not in the description are taken.
        const Names & column_names_to_aggregate,
        const Strings & simple_aggregate_functions,
        const Names & partition_key_columns,
        size_t max_block_size_rows,
        size_t max_block_size_bytes
        )
        : IMergingTransform(
              num_inputs, header, header, /*have_all_inputs_=*/ true, /*limit_hint_=*/ 0, /*always_read_till_end_=*/ false,
              header,
              num_inputs,
              std::move(description_),
              column_names_to_aggregate,
              simple_aggregate_functions,
              partition_key_columns,
              max_block_size_rows,
              max_block_size_bytes)
    {
    }

    String getName() const override { return "StatelessAggregatingSortedTransform"; }
};

}
