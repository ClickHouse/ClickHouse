#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/AggregatingSortedAlgorithm.h>

namespace DB
{

class ColumnAggregateFunction;

/** Merges several sorted ports to one.
  * During this for each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  * merges them into one row. When merging, the data is pre-aggregated - merge of states of aggregate functions,
  * corresponding to a one value of the primary key. For columns that are not part of the primary key and which do not have the AggregateFunction type,
  * when merged, the first value is selected.
  */
class AggregatingSortedTransform : public IMergingTransform<AggregatingSortedAlgorithm>
{
public:
    AggregatingSortedTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_, size_t max_block_size)
        : IMergingTransform(
            num_inputs, header, header, true,
            header,
            num_inputs,
            std::move(description_),
            max_block_size)
    {
    }

    String getName() const override { return "AggregatingSortedTransform"; }
};

}
