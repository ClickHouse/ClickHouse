#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/** Returns true if from_type can be safely casted to to_type.
  *
  * Examples:
  * From type UInt8 to type UInt16 returns true.
  * From type UInt16 to type UInt8 returns false.
  * From type String to type LowCardinality(String) returns true.
  * From type LowCardinality(String) to type String returns true.
  * From type String to type UInt8 returns false.
  */
bool canBeSafelyCasted(const DataTypePtr & from_type, const DataTypePtr & to_type);

}
