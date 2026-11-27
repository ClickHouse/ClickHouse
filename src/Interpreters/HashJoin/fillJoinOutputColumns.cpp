#include <Interpreters/HashJoin/fillJoinOutputColumns.h>

#include <algorithm>

namespace DB
{

namespace
{

void fillFromRowStorePtrs(
    MutableColumns & columns,
    const ColumnAccessIndexes & output_access_indexes,
    const RowStorePointers & row_store_ptrs,
    std::optional<size_t> row_store_batch_size,
    const NamesAndTypes & type_name)
{
    const size_t row_store_rows = row_store_ptrs.ptrs.size();

    /// Reserve once up front for all batches.
    for (size_t dst_idx = 0; dst_idx < output_access_indexes.size(); ++dst_idx)
        if (output_access_indexes[dst_idx].type == ColumnAccessIndex::Type::RowStore)
            columns[dst_idx]->reserve(columns[dst_idx]->size() + row_store_rows);

    const size_t batch_size = row_store_batch_size.value_or(row_store_rows);
    for (size_t batch_start = 0; batch_start < row_store_rows; batch_start += batch_size)
    {
        const size_t remaining_batch_size = std::min(batch_size, row_store_rows - batch_start);
        for (size_t dst_idx = 0; dst_idx < output_access_indexes.size(); ++dst_idx)
        {
            const auto & access_index = output_access_indexes[dst_idx];
            if (access_index.type != ColumnAccessIndex::Type::RowStore)
                continue;

            columns[dst_idx]->fillFromRowStorePtrs(type_name[dst_idx].type, row_store_ptrs, access_index.field_offset, access_index.field_size, batch_start, remaining_batch_size);
        }
    }
}

void fillFromBlocksAndRowNumbers(
    MutableColumns & columns,
    const ColumnAccessIndexes & output_access_indexes,
    const ColumnsWithRowNumbers & columns_with_row_numbers,
    const NamesAndTypes & type_name)
{
    for (size_t dst_idx = 0; dst_idx < output_access_indexes.size(); ++dst_idx)
    {
        const auto & access_index = output_access_indexes[dst_idx];
        if (access_index.type != ColumnAccessIndex::Type::Columns)
            continue;

        columns[dst_idx]->fillFromBlocksAndRowNumbers(type_name[dst_idx].type, access_index.index, columns_with_row_numbers);
    }
}

}

void fillJoinOutputColumns(
    MutableColumns & columns,
    const ColumnAccessIndexes & output_access_indexes,
    const RowStorePointers & row_store_ptrs,
    std::optional<size_t> batch_size,
    const ColumnsWithRowNumbers & columns_with_row_numbers,
    const NamesAndTypes & type_name)
{
    fillFromRowStorePtrs(columns, output_access_indexes, row_store_ptrs, batch_size, type_name);
    fillFromBlocksAndRowNumbers(columns, output_access_indexes, columns_with_row_numbers, type_name);
}

}
