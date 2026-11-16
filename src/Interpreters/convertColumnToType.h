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
  * column-natively (currently: plain numeric-to-numeric in the default mode) go through
  * `IColumn`/CAST; the rest still delegate to `convertFieldToType` (same behavior, just not yet
  * `Field`-free). The behavior is pinned by `gtest_convert_column_to_type` against `convertFieldToType`,
  * so more column-native fast paths can be added without changing results.
  *
  * The equivalence holds for scalar `Bool` and for `Bool` nested under the structural carriers
  * `Array`/`Tuple`/`Map` (and under `Nullable`/`LowCardinality`), including tag-sensitive conversions
  * such as `Bool -> String`: `IColumn::get` does not round-trip the `Bool` `Field` tag (a `DataTypeBool`
  * column is a plain `ColumnUInt8`, so `get` yields `UInt64`), so the delegation path re-tags `Bool`
  * values before calling `convertFieldToType`. The differential test pins these cases.
  *
  * A `Variant`/`Dynamic` source is faithful too. `IColumn::get` returns the active alternative's value
  * without recording which alternative it came from, while the conversion is keyed on the source type, so
  * the alternative's type is read back from the column and used in place of the carrier's. An ambiguous
  * variant such as `Variant(Bool, UInt8)` is included, because the discriminator still distinguishes the
  * alternatives where the `Field` no longer can. This is the one thing the legacy `Field` path
  * (`convertFieldToType` on `(*column)[0]`) cannot do, the column being gone by then, so moving a caller
  * onto this helper corrects such conversions rather than preserving them.
  *
  * Known limitation: a conversion whose `from` is itself a composite over the carrier - e.g.
  * `Array(Dynamic)` to `Array(String)` - is NOT faithful, because `convertFieldToType` converts the
  * elements of a composite with no element type at hand, so a per-element alternative is never reached.
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
