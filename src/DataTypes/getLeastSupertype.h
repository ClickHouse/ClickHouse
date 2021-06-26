#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/** Get data type that covers all possible values of passed data types.
  * If there is no such data type, throws an exception or return nullptr.
  *
  * Examples: least common supertype for UInt8, Int8 - Int16.
  * Examples: there is no least common supertype for Array(UInt8), Int8.
  */
DataTypePtr getLeastSupertype(const DataTypes & types, bool throw_on_no_common_type = true);

}
