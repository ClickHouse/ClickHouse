#include <Processors/Transforms/RollupTransform.h>

namespace DB
{

static Chunk finalizeChunk(Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (auto & column : columns)
        if (auto * agg_function = typeid_cast<const ColumnAggregateFunction *>(column.get()))
            column = agg_function->convertToValues();

    return Chunk(std::move(columns), num_rows);
}

RollupTransform::RollupTransform(Block header, AggregatingTransformParamsPtr params_)
    : IInflatingTransform(std::move(header)
    , params_->getHeader())
    , params(std::move(params_))
    , keys(params->params.keys)
{
}

void RollupTransform::consume(Chunk chunk)
{
    consumed_chunk = std::move(chunk);
    last_removed_key = consumed_chunk.getNumColumns();
}

bool RollupTransform::canGenerate()
{
    return consumed_chunk;
}

Chunk RollupTransform::generate()
{
    auto gen_chunk = std::move(consumed_chunk);

    if (last_removed_key)
    {
        --last_removed_key;

        auto num_rows = gen_chunk.getNumRows();
        auto columns = gen_chunk.getColumns();
        columns[last_removed_key] = columns[last_removed_key]->cloneEmpty()->cloneResized(num_rows);

        BlocksList rollup_blocks = { getInputPort().getHeader().cloneWithColumns(columns) };
        auto rollup_block = params->aggregator.mergeBlocks(rollup_blocks, false);

        num_rows = rollup_block.rows();
        consumed_chunk = Chunk(rollup_block.getColumns(), num_rows);
    }

    return finalizeChunk(std::move(gen_chunk));
}

}
