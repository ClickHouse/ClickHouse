#pragma once

#include <Columns/IColumn.h>


namespace DB
{

/** Negative zero is equal to positive zero by the rules of comparison, but it has a different binary representation.
  * Hash tables compare floating point keys bitwise (so that `NaN` is equal to itself), and the hash methods
  * reinterpret floating point columns as integers or serialize keys as raw bytes, so `-0.` and `0.` end up
  * as different keys in `IN`, `GROUP BY`, `DISTINCT`, hash `JOIN`, and in the hash sharding of a block.
  *
  * These functions are applied to the key columns right before they are hashed. A column is quickly scanned
  * for the presence of negative zeros, and only if there are any, a copy is made with negative zeros replaced
  * by positive zeros. Floating point values are found at any depth of the column, by walking its subcolumns
  * (except for the values that `Dynamic` and `JSON` keep serialized in their shared storage).
  */

/// Returns `nullptr` if the column contains no negative zeros, which is the common case.
ColumnPtr replaceNegativeZeros(const IColumn & column);

/// Returns the original column if it contains no negative zeros.
ColumnPtr replaceNegativeZeros(const ColumnPtr & column);

/// Replaces the key columns that contain negative zeros. The replacement columns are appended to `holder`,
/// which has to outlive the usage of `key_columns`.
void replaceNegativeZeros(ColumnRawPtrs & key_columns, Columns & holder);

}
