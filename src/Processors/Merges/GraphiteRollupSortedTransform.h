#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/GraphiteRollupSortedAlgorithm.h>

namespace DB
{

/// Implementation of IMergingTransform via GraphiteRollupSortedAlgorithm.
class GraphiteRollupSortedTransform final : public IMergingTransform<GraphiteRollupSortedAlgorithm>
{
public:
    GraphiteRollupSortedTransform(
        const Block & header, size_t num_inputs,
        SortDescription description_, size_t max_block_size,
        Graphite::Params params_, time_t time_of_merge_)
        : IMergingTransform(
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
