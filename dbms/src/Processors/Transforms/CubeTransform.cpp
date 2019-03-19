#include <Processors/Transforms/CubeTransform.h>

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

CubeTransform::CubeTransform(Block header, AggregatingTransformParamsPtr params_)
    : IInflatingTransform(std::move(header), params_->getHeader())
    , params(std::move(params_))
    , keys(params->params.keys)
{
    if (keys.size() >= 8 * sizeof(mask))
        throw Exception("Too many keys are used for CubeTransform.", ErrorCodes::LOGICAL_ERROR);
}

void CubeTransform::consume(Chunk chunk)
{
    consumed_chunk = std::move(chunk);
    auto num_rows = current_chunk.getNumRows();
    mask = (UInt64(1) << keys.size()) - 1;

    current_columns = consumed_chunk.getColumns();
    current_zero_columns.clear();
    current_zero_columns.reserve(current_columns.size());

    for (auto & column : current_columns)
        current_zero_columns.emplace_back(column->cloneEmpty()->cloneResized(num_rows));
}

bool CubeTransform::canGenerate()
{
    return consumed_chunk;
}

Chunk CubeTransform::generate()
{
    auto gen_chunk = std::move(consumed_chunk);

    if (mask)
    {
        --mask;

        auto columns = current_columns;
        for (size_t i = 0; i < columns.size(); ++i)
            if (mask & (UInt64(1) << i))
                columns[i] = current_zero_columns[i];

        BlocksList cube_blocks = { getInputPort().getHeader().cloneWithColumns(columns) };
        auto cube_block = params->aggregator.mergeBlocks(cube_blocks, false);

        auto num_rows = cube_block.rows();
        consumed_chunk = Chunk(cube_block.getColumns(), num_rows);
    }

    return finalizeChunk(std::move(gen_chunk));
}

}
