#include <Processors/Transforms/RollupTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>

namespace DB
{

RollupTransform::RollupTransform(Block header, AggregatingTransformParamsPtr params_)
    : IAccumulatingTransform(std::move(header), appendGroupingSetColumn(params_->getHeader()))
    , params(std::move(params_))
    , keys(params->params.keys)
    , aggregates_mask(getAggregatesMask(params->getHeader(), params->params.aggregates))
{
}

void RollupTransform::consume(Chunk chunk)
{
    consumed_chunks.emplace_back(std::move(chunk));
}

Chunk RollupTransform::merge(Chunks && chunks, bool final)
{
    BlocksList rollup_blocks;
    for (auto & chunk : chunks)
        rollup_blocks.emplace_back(getInputPort().getHeader().cloneWithColumns(chunk.detachColumns()));

    auto rollup_block = params->aggregator.mergeBlocks(rollup_blocks, final);
    auto num_rows = rollup_block.rows();
    return Chunk(rollup_block.getColumns(), num_rows);
}

MutableColumnPtr getColumnWithDefaults(Block const & header, size_t key, size_t n)
{
    auto const & col = header.getByPosition(key);
    auto result_column = col.column->cloneEmpty();
    col.type->insertManyDefaultsInto(*result_column, n);
    return result_column;
}

Chunk RollupTransform::generate()
{
    if (!consumed_chunks.empty())
    {
        if (consumed_chunks.size() > 1)
            rollup_chunk = merge(std::move(consumed_chunks), false);
        else
            rollup_chunk = std::move(consumed_chunks.front());

        consumed_chunks.clear();
        last_removed_key = keys.size();
    }

    auto gen_chunk = std::move(rollup_chunk);

    if (last_removed_key)
    {
        --last_removed_key;
        auto key = keys[last_removed_key];

        auto num_rows = gen_chunk.getNumRows();
        auto columns = gen_chunk.getColumns();
        columns[key] = getColumnWithDefaults(getInputPort().getHeader(), key, num_rows);

        Chunks chunks;
        chunks.emplace_back(std::move(columns), num_rows);
        rollup_chunk = merge(std::move(chunks), false);
    }

    finalizeChunk(gen_chunk, aggregates_mask);
    if (!gen_chunk.empty())
        gen_chunk.addColumn(0, ColumnUInt64::create(gen_chunk.getNumRows(), set_counter++));
    return gen_chunk;
}

}
