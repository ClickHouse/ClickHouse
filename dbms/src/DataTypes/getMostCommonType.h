#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/** Get data type that covers intersection of all possible values of passed data types.
  * DataTypeNothing is the most common for all possible types. If there is no such data type, throws an exception.
  *
  * Examples: most common type for UInt16, UInt8 and Int8 - Unt16.
  * Examples: there is no common type for Array(UInt8), Int8.
  */
DataTypePtr getMostCommonType(const DataTypes & types, bool throw_if_result_is_nothing);

}
