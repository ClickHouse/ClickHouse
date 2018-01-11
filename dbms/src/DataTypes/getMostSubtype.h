#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/** Get data type that covers intersection of all possible values of passed data types.
  * DataTypeNothing is the most common subtype for all types.
  * Examples: most common subtype for UInt16, UInt8 and Int8 - Unt16.
  * Examples: most common subtype for Array(UInt8), Int8 is Nothing
  */
DataTypePtr getMostSubtype(const DataTypes & types, bool throw_if_result_is_nothing = false);

}
