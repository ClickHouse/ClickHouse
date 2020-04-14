#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/SummingSortedAlgorithm.h>

namespace DB
{

/** Merges several sorted ports into one.
  * For each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  *  collapses them into one row, summing all the numeric columns except the primary key.
  * If in all numeric columns, except for the primary key, the result is zero, it deletes the row.
  */
class SummingSortedTransform final : public IMergingTransform<SummingSortedAlgorithm>
{
public:

    SummingSortedTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_,
        /// List of columns to be summed. If empty, all numeric columns that are not in the description are taken.
        const Names & column_names_to_sum,
        size_t max_block_size)
        : IMergingTransform(
            num_inputs, header, header, true,
            header,
            num_inputs,
            std::move(description_),
            column_names_to_sum,
            max_block_size)
    {
    }

    String getName() const override { return "SummingSortedTransform"; }
};

}
