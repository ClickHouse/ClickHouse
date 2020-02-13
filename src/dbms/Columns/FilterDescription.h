#pragma once

#include <Columns/IColumn.h>


namespace DB
{

/// Support methods for implementation of WHERE, PREWHERE and HAVING.


/// Analyze if the column for filter is constant thus filter is always false or always true.
struct ConstantFilterDescription
{
    bool always_false = false;
    bool always_true = false;

    ConstantFilterDescription() {}
    explicit ConstantFilterDescription(const IColumn & column);
};


/// Obtain a filter from non constant Column, that may have type: UInt8, Nullable(UInt8).
struct FilterDescription
{
    const IColumn::Filter * data = nullptr; /// Pointer to filter when it is not always true or always false.
    ColumnPtr data_holder;                  /// If new column was generated, it will be owned by holder.

    explicit FilterDescription(const IColumn & column);
};


struct ColumnWithTypeAndName;

/// Will throw an exception if column_elem is cannot be used as a filter column.
void checkColumnCanBeUsedAsFilter(const ColumnWithTypeAndName & column_elem);

}
