#include <Core/SortCursor.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Common/PODArray.h>
#include <Common/iota.h>

#include <boost/make_shared.hpp>

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

bool compareWithThreshold(
    const ColumnRawPtrs & raw_block_columns,
    size_t min_block_index,
    const Columns & threshold_columns,
    const SortDescription & sort_description)
{
    chassert(raw_block_columns.size() == threshold_columns.size());
    chassert(raw_block_columns.size() == sort_description.size());

    size_t raw_block_columns_size = raw_block_columns.size();
    for (size_t i = 0; i < raw_block_columns_size; ++i)
    {
        int res = sort_description[i].direction
            * raw_block_columns[i]->compareAt(min_block_index, 0, *threshold_columns[i], sort_description[i].nulls_direction);

        if (res < 0)
            return true;
        if (res > 0)
            return false;
    }

    return false;
}
}

GlobalThresholdColumns::GlobalThresholdColumns() { current.store(boost::make_shared<Node>()); }

void GlobalThresholdColumns::updateThreshold(
    const ColumnRawPtrs & new_columns, size_t row_to_compare, const PartialSortingTransform * sorting_transform)
{
    auto current_ptr = current.load();
    while (current_ptr->columns.empty()
           || compareWithThreshold(new_columns, row_to_compare, current_ptr->columns, sorting_transform->description))
    {
        size_t num_columns = new_columns.size();
        Columns threshold_columns(num_columns);

        for (size_t i = 0; i < num_columns; ++i)
        {
            MutableColumnPtr threshold_column = new_columns[i]->cloneEmpty();
            threshold_column->insertFrom(*new_columns[i], row_to_compare);
            threshold_columns[i] = threshold_column->convertToFullColumnIfSparse();
        }

        auto new_ptr = boost::make_shared<Node>(std::move(threshold_columns), sorting_transform);
        if (current.compare_exchange_weak(current_ptr, new_ptr))
            break;
    }
}

PartialSortingTransform::PartialSortingTransform(
    SharedHeader header_, const SortDescription & description_, UInt64 limit_)
    : ISimpleTransform(header_, header_, false)
    , description(description_)
    , limit(limit_)
{
    // Sorting by no columns doesn't make sense.
    chassert(!description_.empty());

    for (const auto & column_sort_desc : description)
        description_with_positions.emplace_back(column_sort_desc, header_->getPositionByName(column_sort_desc.column_name));
}

size_t PartialSortingTransform::getFilterMask(
    const ColumnRawPtrs & raw_block_columns,
    const Columns & threshold_columns,
    size_t num_rows,
    IColumn::Filter & filter_,
    PaddedPODArray<UInt64> * rows_to_compare_,
    PaddedPODArray<Int8> & compare_results_,
    bool include_equal_row) const
{
    filter_.resize(num_rows);
    compare_results_.resize(num_rows);

    if (description.size() == 1 || rows_to_compare_ == nullptr)
    {
        /// Fast path for single column
        raw_block_columns[0]->compareColumn(
            *threshold_columns[0], 0, nullptr, compare_results_, description[0].direction, description[0].nulls_direction);
    }
    else
    {
        rows_to_compare_->resize(num_rows);
        iota(rows_to_compare_->data(), num_rows, UInt64(0));

        size_t size = raw_block_columns.size();
        for (size_t i = 0; i < size; ++i)
        {
            raw_block_columns[i]->compareColumn(
                *threshold_columns[i], 0, rows_to_compare_, compare_results_, description[i].direction, description[i].nulls_direction);

            if (rows_to_compare_->empty())
                break;
        }
    }

    size_t result_size_hint = 0;

    if (include_equal_row)
    {
        for (size_t i = 0; i < num_rows; ++i)
        {
            /// Leave only rows that are less or equal to row from rhs.
            filter_[i] = compare_results_[i] <= 0;
            result_size_hint += filter_[i];
        }
    }
    else
    {
        for (size_t i = 0; i < num_rows; ++i)
        {
            /// Leave only rows that are less than row from rhs.
            filter_[i] = compare_results_[i] < 0;
            result_size_hint += filter_[i];
        }
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
        chassert(chunk.getNumColumns());
    }

    if (read_rows)
        read_rows->add(chunk.getNumRows());

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    if (global_threshold_columns_ptr && min_limit_for_partial_sort_optimization <= limit)
        sort_description_threshold_columns = global_threshold_columns_ptr->getCurrentThreshold()->columns;

    /** If we've saved columns from previously blocks we could filter all rows from current block
      * which are unnecessary for sortBlock(...) because they obviously won't be in the top LIMIT rows.
      */
    if (!sort_description_threshold_columns.empty())
    {
        UInt64 rows_num = block.rows();
        auto block_columns = extractRawColumns(block, description_with_positions);

        size_t result_size_hint
            = getFilterMask(block_columns, sort_description_threshold_columns, rows_num, filter, &rows_to_compare, compare_results, false);

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
    if (global_threshold_columns_ptr && limit > 0 && limit <= block.rows())
    {
        size_t min_row_to_compare = limit - 1;
        auto raw_block_columns = extractRawColumns(block, description_with_positions);
        global_threshold_columns_ptr->updateThreshold(raw_block_columns, min_row_to_compare, this);
    }
    else if (min_limit_for_partial_sort_optimization <= limit && limit <= block.rows())
    {
        /** If we filtered more than limit rows from block take block last row.
          * Otherwise take last limit row.
          *
          * If current threshold value is empty, update current threshold value.
          * If min block value is less than current threshold value, update current threshold value.
          */
        size_t min_row_to_compare = limit - 1;
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
                sort_description_threshold_columns_updated[i] = sort_description_threshold_column_updated->convertToFullColumnIfSparse();
            }

            sort_description_threshold_columns = std::move(sort_description_threshold_columns_updated);
        }
    }

    chunk.setColumns(block.getColumns(), block.rows());
}

}
