#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/** Get data type that covers all possible values of passed data types.
  * If there is no such data type, throws an exception
  * or if 'allow_conversion_to_string' is true returns String as common type.
  *
  * Examples: least common supertype for UInt8, Int8 - Int16.
  * Examples: there is no least common supertype for Array(UInt8), Int8.
  */
DataTypePtr getLeastSupertype(const DataTypes & types, bool allow_conversion_to_string = false);

using TypeIndexSet = std::unordered_set<TypeIndex>;
DataTypePtr getLeastSupertype(const TypeIndexSet & types, bool allow_conversion_to_string = false);

/// Same as above but return nullptr instead of throwing exception.
DataTypePtr tryGetLeastSupertype(const DataTypes & types);

}
