#pragma once

#include <Columns/IColumn.h>
#include <Core/TypeId.h>
#include <Interpreters/RowRefs.h>

namespace DB
{

/// Fixed-width types whose `insertDefaultInto` writes bitwise zero, which is what the gather writes for
/// an unmatched row. `Date32` qualifies (0, not `getDefault`'s 1900-01-01); `Enum8`/`Enum16` would get
/// their first value instead, so this must be keyed on `getTypeId` - `getColumnType` maps them to `Int8`.
bool directGatherAdmits(TypeIndex type_id);

/// Appends `rows_to_add` values of one output column straight from the per-block raw data bases that
/// `StoredColumnsIndex::resolveEmitColumns` resolved, reading the encoded ref words as they are
/// instead of expanding them into `(StoredBlock *, row_number)` pairs first.
template <bool from_row_list>
void gatherColumnDirect(
    IColumn & dst,
    const DirectGatherColumn & src,
    const UInt64 * row_refs_begin,
    const UInt64 * row_refs_end,
    size_t rows_to_add);

/// How many output rows `[row_refs_begin, row_refs_end)` expands to, a zero word counting as one
/// default row. `insertRawUninitialized` needs the exact count, which the emit builders' reserve hint
/// only bounds from above.
template <bool from_row_list>
size_t countDirectGatherRows(const UInt64 * row_refs_begin, const UInt64 * row_refs_end)
{
    if constexpr (from_row_list)
    {
        size_t rows = 0;
        for (const UInt64 * word = row_refs_begin; word != row_refs_end; ++word)
            rows += *word ? refWordRows(*word) : 1;
        return rows;
    }
    else
        return static_cast<size_t>(row_refs_end - row_refs_begin);
}

}
