#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Core/SortCursor.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Common/PODArray.h>
#include <Common/iota.h>
#include <Common/logger_useful.h>

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
        iota(rows_to_compare.data(), num_rows, UInt64(0));

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
    chassert(raw_block_columns.size() == threshold_columns.size());
    chassert(raw_block_columns.size() == sort_description.size());

    size_t raw_block_columns_size = raw_block_columns.size();
    for (size_t i = 0; i < raw_block_columns_size; ++i)
    {
        int res = sort_description[i].direction * raw_block_columns[i]->compareAt(min_block_index, 0, *threshold_columns[i], sort_description[i].nulls_direction);

        if (res < 0)
            return true;
        if (res > 0)
            return false;
    }

    return false;
}

}

TopKThresholdCollector::TopKThresholdCollector(
    TopKThresholdTrackerPtr tracker_, const SortColumnDescription & description_, size_t limit_)
    : tracker(std::move(tracker_))
    , description(description_)
    , limit(limit_)
{
    chassert(tracker);
    chassert(limit > 0);
}

bool TopKThresholdCollector::isSupportedType(const IDataType & type)
{
    const IDataType * value_type = &type;
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(value_type))
        value_type = nullable_type->getNestedType().get();

    /// `__topKFilter` drops `nan` rows (https://github.com/ClickHouse/ClickHouse/issues/116705).
    if (isFloat(*value_type))
        return false;

    return value_type->isValueRepresentedByNumber() || isStringOrFixedString(*value_type);
}

int TopKThresholdCollector::compare(const IColumn & lhs, size_t lhs_row, const IColumn & rhs, size_t rhs_row) const
{
    if (description.collator)
        return description.direction
            * lhs.compareAtWithCollation(lhs_row, rhs_row, rhs, description.nulls_direction, *description.collator);
    return description.direction * lhs.compareAt(lhs_row, rhs_row, rhs, description.nulls_direction);
}

void TopKThresholdCollector::add(const IColumn & sorted, size_t rows)
{
    rows = std::min(rows, limit);
    if (rows == 0)
        return;

    MutableColumnPtr to_merge;
    {
        std::lock_guard lock(mutex);

        /// Only values before the published one can change it; `sorted` is sorted, so they are a prefix of it.
        if (boundary)
        {
            size_t lo = 0;
            size_t hi = rows;
            while (lo < hi)
            {
                size_t mid = lo + (hi - lo) / 2;
                if (compare(sorted, mid, *boundary, 0) < 0)
                    lo = mid + 1;
                else
                    hi = mid;
            }
            rows = lo;
            if (rows == 0)
                return;
        }

        if (!values)
            values = sorted.cloneEmpty();
        values->insertRangeFrom(sorted, 0, rows);

        if (merging || values->size() < limit + (boundary ? limit / 2 : 0))
            return;

        to_merge = std::move(values);
        values = to_merge->cloneEmpty();
        merging = true;
    }

    auto direction = description.direction == -1 ? IColumn::PermutationSortDirection::Descending
                                                 : IColumn::PermutationSortDirection::Ascending;
    while (to_merge)
    {
        IColumn::Permutation perm;
        if (description.collator)
            to_merge->getPermutationWithCollation(
                *description.collator, direction, IColumn::PermutationSortStability::Unstable, limit, description.nulls_direction, perm);
        else
            to_merge->getPermutation(direction, IColumn::PermutationSortStability::Unstable, limit, description.nulls_direction, perm);

        ColumnPtr best = to_merge->permute(perm, limit);

        Field value;
        best->get(limit - 1, value);
        tracker->testAndSet(value);

        std::lock_guard lock(mutex);
        size_t added_meanwhile = values->size();
        values->insertRangeFrom(*best, 0, limit);
        boundary = best->cut(limit - 1, 1);

        /// Other threads did not merge what they added meanwhile because `merging` was set.
        if (added_meanwhile && values->size() >= limit + limit / 2)
        {
            to_merge = std::move(values);
            values = to_merge->cloneEmpty();
        }
        else
        {
            to_merge = nullptr;
            merging = false;
        }
    }
}

PartialSortingTransform::PartialSortingTransform(
    SharedHeader header_, const SortDescription & description_, UInt64 limit_,
    TopKThresholdTrackerPtr threshold_tracker_, TopKThresholdCollectorPtr threshold_collector_)
    : ISimpleTransform(header_, header_, false)
    , description(description_)
    , limit(limit_)
    , threshold_tracker(threshold_tracker_)
    , threshold_collector(threshold_collector_)
{
    // Sorting by no columns doesn't make sense.
    chassert(!description_.empty());

    for (const auto & column_sort_desc : description)
        description_with_positions.emplace_back(column_sort_desc, header_->getPositionByName(column_sort_desc.column_name));
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

    /// Sorting reorders the rows, so source row numbers no longer describe them.
    chunk.getChunkInfos().extract<ChunkInfoRowNumbers>();

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    /// Materialize sort key columns that are ColumnReplicated to avoid index indirection during comparison and sorting.
    for (const auto & col_desc : description)
    {
        auto & column_entry = block.getByName(col_desc.column_name);
        column_entry.column = convertToFullColumnIfReplicationNotUseful(column_entry.column);
    }

    /** If we've saved columns from previously blocks we could filter all rows from current block
      * which are unnecessary for sortBlock(...) because they obviously won't be in the top LIMIT rows.
      */
    if (!sort_description_threshold_columns.empty() && !threshold_tracker)
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

    if (threshold_collector && block.rows())
        threshold_collector->add(
            *removeSpecialRepresentations(
                block.safeGetByPosition(description_with_positions[0].column_number).column->convertToFullColumnIfConst()),
            block.rows());

    /// Check if we can use this block for optimization.
    if ((min_limit_for_partial_sort_optimization <= limit || threshold_tracker) && limit <= block.rows())
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
                /// Without this, `assertTypeEquality` may fail when comparing a ColumnNullable against a ColumnReplicated (received from JOIN).
                sort_description_threshold_columns_updated[i]
                    = removeSpecialRepresentations(sort_description_threshold_column_updated->getPtr());
            }

            sort_description_threshold_columns = std::move(sort_description_threshold_columns_updated);
            if (threshold_tracker)
            {
                Field value;
                sort_description_threshold_columns[0]->get(0, value);
                threshold_tracker->testAndSet(value);
            }
        }
    }

    chunk.setColumns(block.getColumns(), block.rows());
}

}
