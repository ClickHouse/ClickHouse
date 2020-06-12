#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/** Get data type that covers all possible values of passed data types.
  * If there is no such data type, throws an exception.
  *
  * Examples: least common supertype for UInt8, Int8 - Int16.
  * Examples: there is no least common supertype for Array(UInt8), Int8.
  */
DataTypePtr getLeastSupertype(const DataTypes & types);

}
