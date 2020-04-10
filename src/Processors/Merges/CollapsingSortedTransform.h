#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/CollapsingSortedAlgorithm.h>

namespace DB
{

/** Merges several sorted ports to one.
  * For each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  *  keeps no more than one row with the value of the column `sign_column = -1` ("negative row")
  *  and no more than a row with the value of the column `sign_column = 1` ("positive row").
  * That is, it collapses the records from the change log.
  *
  * If the number of positive and negative rows is the same, and the last row is positive, then the first negative and last positive rows are written.
  * If the number of positive and negative rows is the same, and the last line is negative, it writes nothing.
  * If the positive by 1 is greater than the negative rows, then only the last positive row is written.
  * If negative by 1 is greater than positive rows, then only the first negative row is written.
  * Otherwise, a logical error.
  */
class CollapsingSortedTransform final : public IMergingTransform2<CollapsingSortedAlgorithm>
{
public:
    CollapsingSortedTransform(
        const Block & header,
        size_t num_inputs,
        SortDescription description_,
        const String & sign_column,
        size_t max_block_size,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool use_average_block_sizes = false)
        : IMergingTransform2(
            num_inputs, header, header, true,
            header,
            num_inputs,
            std::move(description_),
            sign_column,
            max_block_size,
            out_row_sources_buf_,
            use_average_block_sizes,
            &Logger::get("CollapsingSortedTransform"))
    {
    }

    String getName() const override { return "CollapsingSortedTransform"; }
};

}
