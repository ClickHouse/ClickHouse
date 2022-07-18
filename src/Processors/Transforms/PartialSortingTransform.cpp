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
    // Sorting by no columns doesn't make sense.
    assert(!description.empty());
}

static ColumnRawPtrs extractColumns(const Block & block, const SortDescription & description)
{
    size_t size = description.size();
    ColumnRawPtrs res;
    res.reserve(size);

    for (size_t i = 0; i < size; ++i)
    {
        const IColumn * column = block.getByName(description[i].column_name).column.get();
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
        int res = description[i].direction * lhs[i]->compareAt(lhs_row_num, rhs_row_num, *rhs[i], description[i].nulls_direction);
        if (res < 0)
            return true;
        else if (res > 0)
            return false;
    }
    return false;
}

size_t getFilterMask(const ColumnRawPtrs & lhs, const ColumnRawPtrs & rhs, size_t rhs_row_num,
                     const SortDescription & description, size_t num_rows, IColumn::Filter & filter,
                     PaddedPODArray<UInt64> & rows_to_compare, PaddedPODArray<Int8> & compare_results)
{
    filter.resize(num_rows);
    compare_results.resize(num_rows);

    if (description.size() == 1)
    {
        /// Fast path for single column
        lhs[0]->compareColumn(*rhs[0], rhs_row_num, nullptr, compare_results,
                              description[0].direction, description[0].nulls_direction);
    }
    else
    {
        rows_to_compare.resize(num_rows);

        for (size_t i = 0; i < num_rows; ++i)
            rows_to_compare[i] = i;

        size_t size = description.size();
        for (size_t i = 0; i < size; ++i)
        {
            lhs[i]->compareColumn(*rhs[i], rhs_row_num, &rows_to_compare, compare_results,
                                  description[i].direction, description[i].nulls_direction);

            if (rows_to_compare.empty())
                break;
        }
    }

    size_t result_size_hint = 0;

    for (size_t i = 0; i < num_rows; ++i)
    {
        /// Leave only rows that are less then row from rhs.
        filter[i] = compare_results[i] < 0;

        if (filter[i])
            ++result_size_hint;
    }

    return result_size_hint;
}

void PartialSortingTransform::transform(Chunk & chunk)
{
    if (chunk.getNumRows())
    {
        // The following code works with Blocks and will lose the number of
        // rows when there are no columns. We shouldn't get such block, because
        // we have to sort by at least one column.
        assert(chunk.getNumColumns());
    }

    if (read_rows)
        read_rows->add(chunk.getNumRows());

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    /** If we've saved columns from previously blocks we could filter all rows from current block
      * which are unnecessary for sortBlock(...) because they obviously won't be in the top LIMIT rows.
      */
    if (!threshold_block_columns.empty())
    {
        UInt64 rows_num = block.rows();
        auto block_columns = extractColumns(block, description);

        size_t result_size_hint = getFilterMask(
                block_columns, threshold_block_columns, limit - 1,
                description, rows_num, filter, rows_to_compare, compare_results);

        /// Everything was filtered. Skip whole chunk.
        if (result_size_hint == 0)
            return;

        if (result_size_hint < rows_num)
        {
            for (auto & column : block)
                column.column = column.column->filter(filter, result_size_hint);
        }
    }

    sortBlock(block, description, limit);

    /// Check if we can use this block for optimization.
    if (min_limit_for_partial_sort_optimization <= limit && limit <= block.rows())
    {
        auto block_columns = extractColumns(block, description);

        if (threshold_block_columns.empty() ||
            less(block_columns, limit - 1, threshold_block_columns, limit - 1, description))
        {
            threshold_block = block;
            threshold_block_columns.swap(block_columns);
        }
    }

    chunk.setColumns(block.getColumns(), block.rows());
}

}
