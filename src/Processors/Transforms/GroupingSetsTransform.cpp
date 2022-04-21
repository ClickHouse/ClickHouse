#include <cstddef>
#include <Processors/Transforms/GroupingSetsTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

GroupingSetsTransform::GroupingSetsTransform(
    Block input_header,
    Block output_header,
    AggregatingTransformParamsPtr params_,
    ColumnNumbersList const & missing_columns_,
    size_t set_id_
)
    : IAccumulatingTransform(std::move(input_header), std::move(output_header))
    , params(std::move(params_))
    , missing_columns(missing_columns_[set_id_])
    , set_id(set_id_)
    , output_size(getOutputPort().getHeader().columns())
{}

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
    Chunk result;
    if (!consumed_chunks.empty())
    {
        Chunk grouping_sets_chunk;
        if (consumed_chunks.size() > 1)
            grouping_sets_chunk = merge(std::move(consumed_chunks), false);
        else
            grouping_sets_chunk = std::move(consumed_chunks.front());

        consumed_chunks.clear();

        size_t rows = grouping_sets_chunk.getNumRows();

        auto columns = grouping_sets_chunk.detachColumns();
        Columns result_columns;
        auto const & output_header = getOutputPort().getHeader();
        size_t real_column_index = 0, missign_column_index = 0;
        for (size_t i = 0; i < output_header.columns() - 1; ++i)
        {
            if (missign_column_index < missing_columns.size() && missing_columns[missign_column_index] == i)
                result_columns.push_back(output_header.getByPosition(missing_columns[missign_column_index++]).column->cloneResized(rows));
            else
                result_columns.push_back(std::move(columns[real_column_index++]));
        }
        result_columns.push_back(ColumnUInt64::create(rows, set_id));

        result = Chunk(std::move(result_columns), rows);
    }

    return result;
}

}
