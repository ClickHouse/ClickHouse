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

size_t getFilterMask(const ColumnRawPtrs & lhs, const ColumnRawPtrs & rhs, size_t rhs_row_num,
                     const SortDescription & description, size_t rows_num, IColumn::Filter & filter)
{
    filter.resize_fill(rows_num, 0);
    PaddedPODArray<UInt64> rows_to_compare(rows_num);
    PaddedPODArray<Int8> compare_results(rows_num, 0);
    size_t filtered_count = rows_num;

    for (size_t i = 0; i < rows_num; ++i)
        rows_to_compare[i] = i;

    size_t size = description.size();
    for (size_t i = 0; i < size; ++i)
    {
        lhs[i]->compareColumn(*rhs[i], rhs_row_num, rows_to_compare, compare_results, description[i].direction, 1);

        if (rows_to_compare.empty())
            break;
    }

    for (size_t i = 0; i != rows_num; ++i)
        filtered_count -= filter[i] = (compare_results[i] >= 0);

    return filtered_count;
}

void PartialSortingTransform::transform(Chunk & chunk)
{
    if (read_rows)
        read_rows->add(chunk.getNumRows());

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    chunk.clear();

    ColumnRawPtrs block_columns;
    UInt64 rows_num = block.rows();

    /** If we've saved columns from previously blocks we could filter all rows from current block
      * which are unnecessary for sortBlock(...) because they obviously won't be in the top LIMIT rows.
      */
    if (!threshold_block_columns.empty())
    {
        block_columns = extractColumns(block, description);
        IColumn::Filter filter;
        size_t filtered_count = getFilterMask(block_columns, threshold_block_columns, limit - 1, description, rows_num, filter);

        if (filtered_count == rows_num)
            return;

        if (filtered_count)
        {
            for (auto & column : block.getColumns())
            {
                column = column->filter(filter, rows_num - filtered_count);
            }
        }
    }

    sortBlock(block, description, limit);

    if (!threshold_block_columns.empty())
    {
        block_columns = extractColumns(block, description);
    }

    /** If this is the first processed block or (limit - 1)'th row of the current block
      * is less than current threshold row then we could update threshold.
      */
    if (limit && limit <= block.rows() &&
        (threshold_block_columns.empty() || less(block_columns, limit - 1, threshold_block_columns, limit - 1, description)))
    {
        threshold_block = block.cloneWithColumns(block.getColumns());
        threshold_block_columns = extractColumns(threshold_block, description);
    }

    chunk.setColumns(block.getColumns(), block.rows());
}

}
