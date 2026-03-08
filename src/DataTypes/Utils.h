#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/** Returns true if from_type can be safely cast to to_type.
  *
  * Examples:
  * From type UInt8 to type UInt16 returns true.
  * From type UInt16 to type UInt8 returns false.
  * From type String to type LowCardinality(String) returns true.
  * From type LowCardinality(String) to type String returns true.
  * From type String to type UInt8 returns false.
  */
bool canBeSafelyCast(const DataTypePtr & from_type, const DataTypePtr & to_type);

/** Returns true if two types can be compared with equals/notEquals.
  * Checks `isComparableForEquality` on each type, then validates that
  * the type pair is compatible (numeric, string, date, UUID, IP, enum,
  * tuple of equal size, or has a common supertype).
  */
bool areTypesComparableForEquality(const DataTypePtr & lhs, const DataTypePtr & rhs);

/** Returns true if two types can be compared with less/greater/lessOrEquals/greaterOrEquals.
  * Same type-pair rules as `areTypesComparableForEquality` but uses the
  * weaker `isComparable` guard instead of `isComparableForEquality`.
  */
bool areTypesComparableForOrdering(const DataTypePtr & lhs, const DataTypePtr & rhs);

}
