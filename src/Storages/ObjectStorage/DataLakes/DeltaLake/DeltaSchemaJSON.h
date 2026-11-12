#pragma once

#include <DataTypes/IDataType.h>
#include <Poco/JSON/Object.h>

namespace DB
{

/// Parsing of Delta Lake schema types in their JSON representation
/// (https://github.com/delta-io/delta/blob/master/PROTOCOL.md#schema-serialization-format).
/// Used by data lake catalogs (Unity) that return the table schema as Delta JSON.

/// Get the type of `field[type_key]`, which is either a primitive type name or a complex type object.
DataTypePtr getDeltaLakeFieldType(const Poco::JSON::Object::Ptr & field, const String & type_key, bool is_nullable);

/// Get the type for a Delta Lake primitive type name, e.g. `long` or `decimal(10,2)`.
DataTypePtr getDeltaLakeSimpleTypeByName(const String & type_name);

}
