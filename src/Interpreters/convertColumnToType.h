#pragma once

#include <Columns/IColumn_fwd.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class IDataType;

/** Column-native counterpart of `convertFieldToType` (see `convertFieldToType.h` for the exact
  * semantics — `strict` / `convert_inexact_floats`, the "not representable -> Null" contract, etc.).
  *
  * Converts a single value — row 0 of the size-1 column `value` of type `from` — into type `to`, and
  * returns it as a size-1 column of type `to`. Returns a null `ColumnPtr{}` when the value is not
  * representable in `to` (the column twin of a Null `Field` returned by `convertFieldToType`).
  *
  * The purpose is to convert constants WITHOUT materializing a `Field`. The current implementation
  * still delegates the actual conversion to `convertFieldToType` for the cases it does not yet handle
  * column-natively; those cases behave exactly as before (just not yet `Field`-free). The behavior is
  * pinned by `gtest_convert_column_to_type` against `convertFieldToType`, so column-native fast paths
  * can be added incrementally without changing results.
  */
ColumnPtr convertColumnToTypeOrNull(
    const IColumn & value,
    const IDataType & from,
    const IDataType & to,
    const FormatSettings & format_settings = {},
    bool strict = false,
    bool convert_inexact_floats = false);

/// Same, but also returns `ColumnPtr{}` if conversion throws (twin of `tryConvertFieldToType`).
ColumnPtr tryConvertColumnToTypeOrNull(
    const IColumn & value,
    const IDataType & from,
    const IDataType & to,
    const FormatSettings & format_settings = {},
    bool strict = false,
    bool convert_inexact_floats = false);

/// Twin of `convertFieldToTypeOrThrow`: throws `ARGUMENT_OUT_OF_BOUND` if the value is not
/// representable in `to`.
ColumnPtr convertColumnToTypeOrThrow(
    const IColumn & value,
    const IDataType & from,
    const IDataType & to,
    const FormatSettings & format_settings = {},
    bool convert_inexact_floats = false);

}
