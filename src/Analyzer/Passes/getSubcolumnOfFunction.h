#pragma once

#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>

#include <optional>

namespace DB
{

class Field;
class DataTypeMap;
class DataTypeQBit;
class DataTypeTuple;
class DataTypeVariant;

/// Type-level parts of the rewrites of functions to subcolumns, shared by `FunctionToSubcolumnsPass` and the
/// query plan optimization that reads subcolumns instead of whole columns.

/// The type of the `size` subcolumn of a `String` or `Nullable(String)` column of type `column_type`.
DataTypePtr getStringSizesSubcolumnType(const DataTypePtr & column_type);

/// The name of the element subcolumn the second argument of `tupleElement` / `variantElement` addresses.
std::optional<String> getElementSubcolumnName(const Field & value, const DataTypeTuple & data_type_tuple);
std::optional<String> getElementSubcolumnName(const Field & value, const DataTypeVariant & data_type_variant);
std::optional<String> getElementSubcolumnName(const Field & value, const DataTypeQBit & data_type_qbit);

bool tupleElementNameIsAmbiguousWhenFlattened(const DataTypeTuple & tuple, const String & element_name);

bool isSameSubstream(const ISerialization::Substream & lhs, const ISerialization::Substream & rhs);

/// The name of the subcolumn of a `Map` holding the values of the key `key` (`key_<serialized key>`),
/// or nothing if `key` cannot be a key of the map.
std::optional<String> tryGetMapKeySubcolumnName(const DataTypeMap & data_type_map, const Field & key);

}
