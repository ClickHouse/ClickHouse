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

}
