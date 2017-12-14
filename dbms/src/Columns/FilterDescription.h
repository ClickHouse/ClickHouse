#pragma once

#include <Columns/IColumn.h>


namespace DB
{

/// Obtain a filter from Column, that may have type: UInt8, Nullable(UInt8), Const(UInt8), Const(Nullable(UInt8)).
struct FilterDescription
{
    bool always_false = false;
    bool always_true = false;
    const IColumn::Filter * data = nullptr; /// Pointer to filter when it is not always true or always false.
    ColumnPtr data_holder;                  /// If new column was generated, it will be owned by holder.

    FilterDescription(const IColumn & column);
};

}
