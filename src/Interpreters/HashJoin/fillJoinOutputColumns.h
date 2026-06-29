#pragma once

#include <Columns/IColumn.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/RowRefs.h>

#include <optional>

namespace DB
{

/// Fills join output columns by dispatching on `ColumnAccessIndex::Type`:
/// RowStore columns are filled from `row_store_ptrs`, the rest from `columns_with_row_numbers`.
/// `with_defaults` decides whether the output contains default rows (represented by nullptr)
/// that have to be filled by the default value of type `type_name`.
template <bool with_defaults>
void fillJoinOutputColumns(
    MutableColumns & columns,
    const ColumnAccessIndexes & output_access_indexes,
    const PaddedPODArray<const char *> & row_store_ptrs,
    std::optional<size_t> row_store_batch_size,
    const ColumnsWithRowNumbers & columns_with_row_numbers,
    const NamesAndTypes & type_name = {});

}
