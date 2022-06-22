#pragma once
#include <DataTypes/IDataType.h>

namespace DB
{

enum class LeastSupertypeOnError
{
    Throw,
    String,
    Null,
};

/** Get data type that covers all possible values of passed data types.
  * If there is no such data type, throws an exception.
  *
  * Examples: least common supertype for UInt8, Int8 - Int16.
  * Examples: there is no least common supertype for Array(UInt8), Int8.
  */
template <LeastSupertypeOnError on_error = LeastSupertypeOnError::Throw>
DataTypePtr getLeastSupertype(const DataTypes & types);

/// Same as above but return String type instead of throwing exception.
/// All types can be casted to String, because they can be serialized to String.
DataTypePtr getLeastSupertypeOrString(const DataTypes & types);

/// Same as above but return nullptr instead of throwing exception.
DataTypePtr tryGetLeastSupertype(const DataTypes & types);

using TypeIndexSet = std::unordered_set<TypeIndex>;

template <LeastSupertypeOnError on_error = LeastSupertypeOnError::Throw>
DataTypePtr getLeastSupertype(const TypeIndexSet & types);

DataTypePtr getLeastSupertypeOrString(const TypeIndexSet & types);

DataTypePtr tryGetLeastSupertype(const TypeIndexSet & types);

}
