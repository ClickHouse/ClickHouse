#pragma once

#include <Columns/IColumn.h>


namespace DB
{

/** Replaces negative zeros with positive zeros in a floating point column,
  * recursively for `Nullable`, `Array`, `Tuple`, `Map`, `Variant` and `Dynamic` columns.
  *
  * Negative zero is equal to positive zero by the rules of comparison, but has a different binary
  * representation, and hash tables compare floating point values bitwise - see `normalizeNegativeZero`.
  * Hash methods also reinterpret floating point columns as integer columns of the same width
  * (a `Float64` key is looked up in a `UInt64` hash table, and composite keys are packed or serialized
  * as raw bytes), so by the time a key is hashed, the information that it is a floating point value
  * is already lost. That is why the canonicalization is done on the columns.
  *
  * Returns `nullptr` if the column contains no negative zeros, which is by far the most common case,
  * so that the caller can use the original column and avoid making a copy.
  */
ColumnPtr canonicalizeNegativeZero(const IColumn & column);

/// The same, in place, for a list of key columns.
/// The canonicalized columns are appended to `holder`, which has to outlive the usage of `key_columns`.
void canonicalizeNegativeZeroInKeyColumns(ColumnRawPtrs & key_columns, Columns & holder);

/** A value of a column can be represented in memory as a flat sequence of floating point values of
  * the same width: a `Float64` value, an `Array(Float32)` row, a `LowCardinality(Float64)` value.
  * Such a value is used as a hash table key by the raw bytes that `IColumn::getDataAt` returns, and
  * these bytes cannot be canonicalized in place, so a canonicalized copy has to be made.
  *
  * Returns the width of one floating point value in such a representation, or 0 if a value of this
  * column contains no floating point values, which is by far the most common case.
  */
size_t rawFloatValueWidth(const IColumn & column);

/// Copies `value` to `res`, replacing the negative zeros in it with positive zeros.
/// `width` is what `rawFloatValueWidth` returned for the column that `value` came from.
void canonicalizeNegativeZeroInRawValue(std::string_view value, size_t width, char * res);

}
