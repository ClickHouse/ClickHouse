#include <Processors/Transforms/PartialSortingTransform.h>
#include <Interpreters/sortBlock.h>
#include <Common/PODArray.h>

namespace DB
{

PartialSortingTransform::PartialSortingTransform(
    const Block & header_, SortDescription & description_, UInt64 limit_)
    : ISimpleTransform(header_, header_, false)
    , description(description_), limit(limit_)
{
}

static ColumnRawPtrs extractColumns(const Block & block, const SortDescription & description)
{
    size_t size = description.size();
    ColumnRawPtrs res;
    res.reserve(size);

    for (size_t i = 0; i < size; ++i)
    {
        const IColumn * column = !description[i].column_name.empty()
            ? block.getByName(description[i].column_name).column.get()
            : block.safeGetByPosition(description[i].column_number).column.get();
        res.emplace_back(column);
    }

    return res;
}

bool less(const ColumnRawPtrs & lhs, UInt64 lhs_row_num,
          const ColumnRawPtrs & rhs, UInt64 rhs_row_num, const SortDescription & description)
{
    size_t size = description.size();
    for (size_t i = 0; i < size; ++i)
    {
        int res = description[i].direction * lhs[i]->compareAt(lhs_row_num, rhs_row_num, *rhs[i], 1);
        if (res < 0)
            return true;
        else if (res > 0)
            return false;
    }
    return false;
}

void PartialSortingTransform::transform(Chunk & chunk)
{
    if (read_rows)
        read_rows->add(chunk.getNumRows());

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    chunk.clear();

    ColumnRawPtrs block_columns;
    UInt64 rows_num = block.rows();

    if (!threshold_block_columns.empty())
    {
        IColumn::Filter filter(rows_num, 0);
        block_columns = extractColumns(block, description);
        size_t filtered_count = 0;

        for (UInt64 i = 0; i < rows_num; ++i)
        {
            if (less(threshold_block_columns, limit - 1, block_columns, i, description))
            {
                ++filtered_count;
                filter[i] = 1;
            }
        }

        if (filtered_count)
        {
            for (auto & column : block.getColumns())
            {
                column = column->filter(filter, filtered_count);
            }
        }
    }

    sortBlock(block, description, limit);

    if (limit && limit < block.rows() &&
        (threshold_block_columns.empty() || less(block_columns, limit - 1, threshold_block_columns, limit - 1, description)))
    {
        threshold_block = block.cloneWithColumns(block.getColumns());
        threshold_block_columns = extractColumns(threshold_block, description);
    }

    chunk.setColumns(block.getColumns(), block.rows());
}

}
