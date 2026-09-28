#include <Interpreters/HashJoin/fillRowStoreOutputColumns.h>

#include <algorithm>

namespace DB
{

void fillRowStoreOutputColumns(
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

}
