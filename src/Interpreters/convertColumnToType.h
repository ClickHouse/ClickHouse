#pragma once

#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>


namespace DB
{

/** Column-native counterpart of `convertFieldToType` (see `convertFieldToType.h` for the exact
  * semantics — `strict` / `convert_inexact_floats`, the "not representable -> Null" contract, etc.).
  *
  * Converts a single value — row 0 of the size-1 column `value` of type `from` — into type `to`, and
  * returns it as a size-1 column of type `to`. Returns a null `ColumnPtr{}` when the value is not
  * representable in `to` (the column twin of a Null `Field` returned by `convertFieldToType`). A
  * legitimate NULL result (a NULL input into a type that can hold NULL) is returned as a size-1
  * column holding NULL — NOT as `ColumnPtr{}`.
  *
  * The purpose is to convert constants WITHOUT materializing a `Field`. Cases that can be done
  * column-natively (plain numeric-to-numeric, and an identity conversion of a `Variant`-carrying type)
  * go through `IColumn`/CAST; the rest still delegate to `convertFieldToType` (same behavior, just not
  * yet `Field`-free). The behavior is pinned by `gtest_convert_column_to_type` against
  * `convertFieldToType`, so more column-native fast paths can be added without changing results.
  *
  * The equivalence holds for scalar `Bool` and for `Bool` nested under the structural carriers
  * `Array`/`Tuple`/`Map` (and under `Nullable`/`LowCardinality`), including tag-sensitive conversions
  * such as `Bool -> String`: `IColumn::get` does not round-trip the `Bool` `Field` tag (a `DataTypeBool`
  * column is a plain `ColumnUInt8`, so `get` yields `UInt64`), so the delegation path re-tags `Bool`
  * values before calling `convertFieldToType`. The differential test pins these cases.
  *
  * Known limitation of a NON-IDENTITY conversion: `Bool` nested under `Variant` (and therefore
  * `Dynamic`/`JSON`) is NOT faithful. `ColumnVariant::get` erases the active alternative to the nested
  * column's field (e.g. `UInt64` for a `Bool` alternative), and for an ambiguous variant such as
  * `Variant(Bool, UInt8)` the reconstructed `Field` no longer records which alternative was active, so
  * it cannot be recovered structurally the way the carriers above can; that would need a
  * `ColumnVariant`-aware path before the generic `get`. No current caller needs `Variant`-of-`Bool`
  * textual conversion. An identity conversion of a `Variant`-carrying type is the one deliberate
  * divergence from the legacy `Field` path: it returns the input column unchanged, so a `Variant`
  * discriminator survives here and not there, and inside such a type a `Bool` alternative's raw byte is
  * not clamped either, which is inherent to keeping the discriminator. Every other identity conversion
  * goes through the `Field` path, so it clamps.
  */
ColumnPtr convertColumnToTypeOrNull(
    const IColumn & value,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings = {},
    bool strict = false,
    bool convert_inexact_floats = false);

/// Same, but also returns `ColumnPtr{}` if conversion throws (twin of `tryConvertFieldToType`).
ColumnPtr tryConvertColumnToTypeOrNull(
    const IColumn & value,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings = {},
    bool strict = false,
    bool convert_inexact_floats = false);

/// Twin of `convertFieldToTypeOrThrow`: throws `TYPE_MISMATCH` for a NULL value that `to` cannot hold,
/// and `ARGUMENT_OUT_OF_BOUND` for a non-NULL value that is not representable in `to`.
ColumnPtr convertColumnToTypeOrThrow(
    const IColumn & value,
    const DataTypePtr & from,
    const DataTypePtr & to,
    const FormatSettings & format_settings = {},
    bool convert_inexact_floats = false);

}
