#include <Processors/Transforms/GroupingSetsTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>

namespace DB
{

GroupingSetsTransform::GroupingSetsTransform(Block header, AggregatingTransformParamsPtr params_)
    : IAccumulatingTransform(std::move(header), params_->getHeader())
    , params(std::move(params_))
    , keys(params->params.keys)
    , keys_vector(params->params.keys_vector)
    , keys_vector_idx(0)
{
}

void GroupingSetsTransform::consume(Chunk chunk)
{
    consumed_chunks.emplace_back(std::move(chunk));
}

Chunk GroupingSetsTransform::merge(Chunks && chunks, bool final)
{
    BlocksList grouping_sets_blocks;
    for (auto & chunk : chunks)
        grouping_sets_blocks.emplace_back(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));

    auto grouping_sets_block = params->aggregator.mergeBlocks(grouping_sets_blocks, final);
    auto num_rows = grouping_sets_block.rows();
    return Chunk(grouping_sets_block.getColumns(), num_rows);
}

Chunk GroupingSetsTransform::generate()
{
    if (!consumed_chunks.empty())
    {
        if (consumed_chunks.size() > 1)
            grouping_sets_chunk = merge(std::move(consumed_chunks), false);
        else
            grouping_sets_chunk = std::move(consumed_chunks.front());

        consumed_chunks.clear();

        auto num_rows = grouping_sets_chunk.getNumRows();

        current_columns = grouping_sets_chunk.getColumns();
        current_zero_columns.clear();

        for (auto key : keys)
            current_zero_columns.emplace(key, current_columns[key]->cloneEmpty()->cloneResized(num_rows));
    }

    Chunk gen_chunk;

    if (keys_vector_idx < keys_vector.size())
    {
        auto columns = current_columns;
        std::set<size_t> key_vector(keys_vector[keys_vector_idx].begin(), keys_vector[keys_vector_idx].end());

        for (auto key : keys)
        {
            if (!key_vector.contains(key))
                columns[key] = current_zero_columns[key];
        }

        Chunks chunks;
        chunks.emplace_back(std::move(columns), current_columns.front()->size());
        gen_chunk = merge(std::move(chunks), false);

        ++keys_vector_idx;
    }

    finalizeChunk(gen_chunk);
    return gen_chunk;
}

}
