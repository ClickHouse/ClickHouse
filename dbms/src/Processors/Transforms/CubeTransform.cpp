#include <Processors/Transforms/CubeTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>

namespace DB
{

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
    current_zero_columns.reserve(keys.size());

    for (auto key : keys)
        current_zero_columns.emplace_back(current_columns[key]->cloneEmpty()->cloneResized(num_rows));
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
        consumed_chunk = gen_chunk;

        auto columns = current_columns;
        for (size_t i = 0; i < keys.size(); ++i)
            if (mask & (UInt64(1) << i))
                columns[keys[i]] = current_zero_columns[i];

        BlocksList cube_blocks = { getInputPort().getHeader().cloneWithColumns(columns) };
        auto cube_block = params->aggregator.mergeBlocks(cube_blocks, false);

        auto num_rows = cube_block.rows();
        gen_chunk = Chunk(cube_block.getColumns(), num_rows);
    }

    finalizeChunk(gen_chunk);
    return gen_chunk;
}

}
