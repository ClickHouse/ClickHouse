#include <Processors/Transforms/RollupTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Poco/Logger.h>
#include "Common/logger_useful.h"
#include "Columns/ColumnNullable.h"

namespace DB
{

GroupByModifierTransform::GroupByModifierTransform(Block header, AggregatingTransformParamsPtr params_)
    : IAccumulatingTransform(std::move(header), generateOutputHeader(params_->getHeader(), params_->params.keys, params_->use_nulls))
    , params(std::move(params_))
    , keys(params->params.keys)
{
    intermediate_header = getOutputPort().getHeader();
    if (params->use_nulls)
    {
        auto output_aggregator_params = params->params;
        intermediate_header.erase(0);
        output_aggregator_params.src_header = intermediate_header;
        output_aggregator = std::make_unique<Aggregator>(output_aggregator_params);
    }
}

void GroupByModifierTransform::consume(Chunk chunk)
{
    consumed_chunks.emplace_back(std::move(chunk));
}

void GroupByModifierTransform::mergeConsumed()
{
    if (consumed_chunks.size() > 1)
        current_chunk = merge(std::move(consumed_chunks), true, false);
    else
        current_chunk = std::move(consumed_chunks.front());

    size_t rows = current_chunk.getNumRows();
    auto columns = current_chunk.getColumns();
    if (params->use_nulls)
    {
        for (auto key : keys)
            columns[key] = makeNullable(columns[key]);
    }
    current_chunk = Chunk{ columns, rows };

    consumed_chunks.clear();
}

Chunk GroupByModifierTransform::merge(Chunks && chunks, bool is_input, bool final)
{
    auto header = is_input ? getInputPort().getHeader() : intermediate_header;

    BlocksList blocks;
    for (auto & chunk : chunks)
        blocks.emplace_back(header.cloneWithColumns(chunk.detachColumns()));

    auto current_block = is_input ? params->aggregator.mergeBlocks(blocks, final) : output_aggregator->mergeBlocks(blocks, final);
    auto num_rows = current_block.rows();
    return Chunk(current_block.getColumns(), num_rows);
}

MutableColumnPtr GroupByModifierTransform::getColumnWithDefaults(size_t key, size_t n) const
{
    auto const & col = intermediate_header.getByPosition(key);
    auto result_column = col.column->cloneEmpty();
    col.type->insertManyDefaultsInto(*result_column, n);
    return result_column;
}

RollupTransform::RollupTransform(Block header, AggregatingTransformParamsPtr params_)
    : GroupByModifierTransform(std::move(header), params_)
    , aggregates_mask(getAggregatesMask(params->getHeader(), params->params.aggregates))
{}

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
        mergeConsumed();
        last_removed_key = keys.size();
    }

    auto gen_chunk = std::move(current_chunk);

    if (last_removed_key)
    {
        --last_removed_key;
        auto key = keys[last_removed_key];

        auto num_rows = gen_chunk.getNumRows();
        auto columns = gen_chunk.getColumns();
        columns[key] = getColumnWithDefaults(key, num_rows);

        Chunks chunks;
        chunks.emplace_back(std::move(columns), num_rows);
        current_chunk = merge(std::move(chunks), !params->use_nulls, false);
    }

    finalizeChunk(gen_chunk, aggregates_mask);
    if (!gen_chunk.empty())
        gen_chunk.addColumn(0, ColumnUInt64::create(gen_chunk.getNumRows(), set_counter++));
    return gen_chunk;
}

}
