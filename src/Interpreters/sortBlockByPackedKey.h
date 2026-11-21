#pragma once

#include <Columns/IColumn.h>
#include <Core/SortDescription.h>


namespace DB
{

/// Column with description for sort
struct ColumnWithSortDescription
{
    ColumnPtr column = nullptr;
    SortColumnDescription description;

    /// It means, that this column is ColumnConst
    bool column_const = false;
};

using ColumnsWithSortDescriptions = std::vector<ColumnWithSortDescription>;

/// Sorts the rows by a packed key of the leading integer columns of the description that fit together into 64 bits.
/// Returns the number of leading entries of `columns` that the permutation is sorted by (zero if the packed sort is
/// not applicable, and then nothing is written); `equal_ranges` receive the ranges of rows equal on them, if needed.
size_t trySortByPackedKey(
    const ColumnsWithSortDescriptions & columns,
    size_t rows,
    bool stable,
    bool need_equal_ranges,
    IColumn::Permutation & permutation,
    EqualRanges & equal_ranges);

}
