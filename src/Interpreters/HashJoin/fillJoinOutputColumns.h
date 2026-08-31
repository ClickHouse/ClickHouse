#pragma once

#include <Columns/IColumn.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/RowRefs.h>

#include <optional>

namespace DB
{

/// Fills join output columns by dispatching on `ColumnAccessIndex::Type`:
/// RowStore columns are filled from `row_store_ptrs`, the rest from `columns_with_row_numbers`.
/// A `gathered` entry with a non-null `node` marks a column the caller has already filled.
void fillJoinOutputColumns(
    MutableColumns & columns,
    const ColumnAccessIndexes & output_access_indexes,
    const RowStorePointers & row_store_ptrs,
    std::optional<size_t> row_store_batch_size,
    const ColumnsWithRowNumbers & columns_with_row_numbers,
    const NamesAndTypes & type_name,
    const std::vector<DirectGatherColumn> * gathered = nullptr);

}
