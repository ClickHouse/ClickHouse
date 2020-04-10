#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/GraphiteRollupSortedAlgorithm.h>

namespace DB
{
/** Merges several sorted ports into one.
  *
  * For each group of consecutive identical values of the `path` column,
  *  and the same `time` values, rounded to some precision
  *  (where rounding accuracy depends on the template set for `path`
  *   and the amount of time elapsed from `time` to the specified time),
  * keeps one line,
  *  performing the rounding of time,
  *  merge `value` values using the specified aggregate functions,
  *  as well as keeping the maximum value of the `version` column.
  */
class GraphiteRollupSortedTransform : public IMergingTransform2<GraphiteRollupSortedAlgorithm>
{
public:
    GraphiteRollupSortedTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_, size_t max_block_size,
        Graphite::Params params_, time_t time_of_merge_)
        : IMergingTransform2(
            num_inputs, header, header, true,
            header,
            num_inputs,
            std::move(description_),
            max_block_size,
            std::move(params_),
            time_of_merge_)
    {
    }

    String getName() const override { return "GraphiteRollupSortedTransform"; }
};

}
