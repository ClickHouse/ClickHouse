#include <Processors/Transforms/PartialSortingTransform.h>
#include <Interpreters/sortBlock.h>
#include <Core/SortCursor.h>
#include <Common/PODArray.h>

namespace DB
{

namespace
{

ColumnRawPtrs extractRawColumns(const Block & block, const SortDescriptionWithPositions & description)
{
    size_t size = description.size();
    ColumnRawPtrs result(size);

    for (size_t i = 0; i < size; ++i)
        result[i] = block.safeGetByPosition(description[i].column_number).column.get();

    return result;
}

size_t getFilterMask(const ColumnRawPtrs & raw_block_columns, const Columns & threshold_columns,
                     const SortDescription & description, size_t num_rows, IColumn::Filter & filter,
                     PaddedPODArray<UInt64> & rows_to_compare, PaddedPODArray<Int8> & compare_results)
{
    filter.resize(num_rows);
    compare_results.resize(num_rows);

    if (description.size() == 1)
    {
        /// Fast path for single column
        raw_block_columns[0]->compareColumn(*threshold_columns[0], 0, nullptr, compare_results,
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
            raw_block_columns[i]->compareColumn(*threshold_columns[i], 0, &rows_to_compare, compare_results,
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
        result_size_hint += filter[i];
    }

    return result_size_hint;
}

bool compareWithThreshold(const ColumnRawPtrs & raw_block_columns, size_t min_block_index, const Columns & threshold_columns, const SortDescription & sort_description)
{
    assert(raw_block_columns.size() == threshold_columns.size());
    assert(raw_block_columns.size() == sort_description.size());

    size_t raw_block_columns_size = raw_block_columns.size();
    for (size_t i = 0; i < raw_block_columns_size; ++i)
    {
        int res = sort_description[i].direction * raw_block_columns[i]->compareAt(min_block_index, 0, *threshold_columns[i], sort_description[i].nulls_direction);

        if (res < 0)
            return true;
        else if (res > 0)
            return false;
    }

    return false;
}

}

PartialSortingTransform::PartialSortingTransform(
    const Block & header_, const SortDescription & description_, UInt64 limit_)
    : ISimpleTransform(header_, header_, false)
    , description(description_)
    , limit(limit_)
{
    // Sorting by no columns doesn't make sense.
    assert(!description_.empty());

    for (const auto & column_sort_desc : description)
        description_with_positions.emplace_back(column_sort_desc, header_.getPositionByName(column_sort_desc.column_name));
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
    size_t block_rows_before_filter = block.rows();

    /** If we've saved columns from previously blocks we could filter all rows from current block
      * which are unnecessary for sortBlock(...) because they obviously won't be in the top LIMIT rows.
      */
    if (!sort_description_threshold_columns.empty())
    {
        UInt64 rows_num = block.rows();
        auto block_columns = extractRawColumns(block, description_with_positions);

        size_t result_size_hint = getFilterMask(
                block_columns, sort_description_threshold_columns,
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

    size_t block_rows_after_filter = block.rows();

    /// Check if we can use this block for optimization.
    if (min_limit_for_partial_sort_optimization <= limit && block_rows_after_filter > 0 && limit <= block_rows_before_filter)
    {
        /** If we filtered more than limit rows from block take block last row.
          * Otherwise take last limit row.
          *
          * If current threshold value is empty, update current threshold value.
          * If min block value is less than current threshold value, update current threshold value.
          */
        size_t min_row_to_compare = limit <= block_rows_after_filter ? (limit - 1) : (block_rows_after_filter - 1);
        auto raw_block_columns = extractRawColumns(block, description_with_positions);

        if (sort_description_threshold_columns.empty() ||
            compareWithThreshold(raw_block_columns, min_row_to_compare, sort_description_threshold_columns, description))
        {
            size_t raw_block_columns_size = raw_block_columns.size();
            Columns sort_description_threshold_columns_updated(raw_block_columns_size);

            for (size_t i = 0; i < raw_block_columns_size; ++i)
            {
                MutableColumnPtr sort_description_threshold_column_updated = raw_block_columns[i]->cloneEmpty();
                sort_description_threshold_column_updated->insertFrom(*raw_block_columns[i], min_row_to_compare);
                sort_description_threshold_columns_updated[i] = std::move(sort_description_threshold_column_updated);
            }

            sort_description_threshold_columns = std::move(sort_description_threshold_columns_updated);
        }
    }

    chunk.setColumns(block.getColumns(), block.rows());
}

}
