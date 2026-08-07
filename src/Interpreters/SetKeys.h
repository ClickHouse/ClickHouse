#pragma once
#include <Columns/ColumnsNumber.h>

namespace DB
{

/// Prepared key columns for set which can be added to fill set elements.
/// Used only to upgrade set from tuple.
struct SetKeyColumns
{
    /// The constant columns to the right of IN are not supported directly. For this, they first materialize.
    ColumnRawPtrs key_columns;
    Columns materialized_columns;
    ColumnPtr null_map_holder;
    ColumnUInt8::MutablePtr filter;
};

}
