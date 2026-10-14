#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/** Returns true if from_type can be safely cast to to_type.
  *
  * "Safely" means every value of from_type is representable in to_type, including NULL: a target that
  * cannot hold a NULL is not a safe destination for a source that can produce one.
  *
  * Examples:
  * From type UInt8 to type UInt16 returns true.
  * From type UInt16 to type UInt8 returns false.
  * From type String to type LowCardinality(String) returns true.
  * From type LowCardinality(String) to type String returns true.
  * From type String to type UInt8 returns false.
  */
bool canBeSafelyCast(const DataTypePtr & from_type, const DataTypePtr & to_type);

/** Returns true if converting a column from `from` to `to` keeps the order of its values AND maps distinct
  * values to distinct ones. A stream sorted by the `from` column is still sorted after the conversion, and
  * so are the runs of equal values that DISTINCT and LIMIT BY over a sorted stream rely on.
  *
  * Injectivity is required because the `Array` branch composes the check elementwise, and a collapsing
  * element conversion would reorder arrays. Unrecognised pairs are refused: a false "safe" gives wrong
  * results, a false "unsafe" costs a pushdown.
  *
  * Examples:
  * From type Int32 to type Int64 returns true.
  * From type UInt64 to type Int64 returns false (equal width, the sign bit flips the order).
  * From type String to type Int8 returns false ('10' sorts before '2' as a string and after it as a number).
  * From type Int64 to type Nullable(Int64) returns true.
  * From type LowCardinality(Int32) to type Int64 returns true.
  * From type Float32 to type Float64 returns true.
  * From type Decimal(9, 2) to type Decimal(18, 4) returns true; to type Decimal(18, 1) returns false (it rounds).
  * From type FixedString(3) to type String returns true.
  */
bool conversionPreservesOrder(const IDataType & from, const IDataType & to);

}
