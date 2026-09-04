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

}
