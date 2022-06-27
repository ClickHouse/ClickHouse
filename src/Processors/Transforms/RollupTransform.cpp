#include <Processors/Transforms/RollupTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Poco/Logger.h>
#include "Common/logger_useful.h"
#include "Columns/ColumnNullable.h"

namespace DB
{

RollupTransform::RollupTransform(Block header, AggregatingTransformParamsPtr params_)
    : IAccumulatingTransform(std::move(header), generateOutputHeader(params_->getHeader(), params_->params.keys))
    , params(std::move(params_))
    , keys(params->params.keys)
    , aggregates_mask(getAggregatesMask(params->getHeader(), params->params.aggregates))
{
    auto output_aggregator_params = params->params;
    intermediate_header = getOutputPort().getHeader();
    intermediate_header.erase(0);
    output_aggregator_params.src_header = intermediate_header;
    output_aggregator = std::make_unique<Aggregator>(output_aggregator_params);
}

void RollupTransform::consume(Chunk chunk)
{
    consumed_chunks.emplace_back(std::move(chunk));
}

Chunk RollupTransform::merge(Chunks && chunks, bool is_input, bool final)
{
    BlocksList rollup_blocks;
    auto header = is_input ? getInputPort().getHeader() : intermediate_header;
    for (auto & chunk : chunks)
        rollup_blocks.emplace_back(header.cloneWithColumns(chunk.detachColumns()));

    auto rollup_block = is_input ? params->aggregator.mergeBlocks(rollup_blocks, final) : output_aggregator->mergeBlocks(rollup_blocks, final);
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
            rollup_chunk = merge(std::move(consumed_chunks), true, false);
        else
            rollup_chunk = std::move(consumed_chunks.front());
        
        size_t rows = rollup_chunk.getNumRows();
        auto columns = rollup_chunk.getColumns();
        for (auto key : keys)
            columns[key] = makeNullable(columns[key]);
        rollup_chunk = Chunk{ columns, rows };
        LOG_DEBUG(&Poco::Logger::get("RollupTransform"), "Chunk source: {}", rollup_chunk.dumpStructure());

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
        columns[key] = getColumnWithDefaults(intermediate_header, key, num_rows);

        Chunks chunks;
        chunks.emplace_back(std::move(columns), num_rows);
        rollup_chunk = merge(std::move(chunks), false, false);
        LOG_DEBUG(&Poco::Logger::get("RollupTransform"), "Chunk generated: {}", rollup_chunk.dumpStructure());
    }

    finalizeChunk(gen_chunk, aggregates_mask);
    if (!gen_chunk.empty())
        gen_chunk.addColumn(0, ColumnUInt64::create(gen_chunk.getNumRows(), set_counter++));
    return gen_chunk;
}

}
