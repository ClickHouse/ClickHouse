#pragma once

#include <base/types.h>
#include <Core/MultiEnum.h>
#include <DataTypes/IDataType.h>
#include <Poco/JSON/Object.h>
namespace DB
{
DataTypePtr convertYTSchema(const Poco::JSON::Object::Ptr & json);
DataTypePtr convertYTPrimitiveType(const String & data_type, bool type_v3);
DataTypePtr convertYTTypeV3(const Poco::JSON::Object::Ptr & json);

DataTypePtr convertYTItemType(const Poco::Dynamic::Var & item);

DataTypePtr convertYTDecimal(const Poco::JSON::Object::Ptr & json);
DataTypePtr convertYTOptional(const Poco::JSON::Object::Ptr & json);
DataTypePtr convertYTList(const Poco::JSON::Object::Ptr & json);
DataTypePtr convertYTStruct(const Poco::JSON::Object::Ptr & json);
DataTypePtr convertYTTuple(const Poco::JSON::Object::Ptr & json);
DataTypePtr convertYTVariant(const Poco::JSON::Object::Ptr & json);
DataTypePtr convertYTDict(const Poco::JSON::Object::Ptr & json);
DataTypePtr convertYTTagged(const Poco::JSON::Object::Ptr & json);
}
