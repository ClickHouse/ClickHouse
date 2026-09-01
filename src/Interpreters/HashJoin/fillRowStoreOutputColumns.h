#pragma once

#include <Columns/IColumn.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/RowRefs.h>

#include <optional>

namespace DB
{

/// Fills the row-store output columns of a join from `row_store_ptrs`, sweeping all of them per
/// L2-sized batch so one pass over a batch of rows serves every field. Columnar output columns are
/// addressed by ref words instead and are filled by their own emit kernels.
void fillRowStoreOutputColumns(
    MutableColumns & columns,
    const ColumnAccessIndexes & output_access_indexes,
    const RowStorePointers & row_store_ptrs,
    std::optional<size_t> row_store_batch_size,
    const NamesAndTypes & type_name);

}
