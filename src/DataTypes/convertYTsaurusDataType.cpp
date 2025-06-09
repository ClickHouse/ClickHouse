#include "convertYTsaurusDataType.h"
#include <memory>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeVariant.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int INCORRECT_DATA;
}


/// https://ytsaurus.tech/docs/ru/user-guide/storage/data-types#schema_primitive
DataTypePtr convertYTPrimitiveType(const String & data_type, bool type_v3)
{
    DataTypePtr data_type_ptr;
    if (data_type == "int64")
    {
        data_type_ptr = std::make_shared<DataTypeInt64>();
    }
    else if (data_type == "int32")
    {
        data_type_ptr = std::make_shared<DataTypeInt32>();
    }
    else if (data_type == "int16")
    {
        data_type_ptr = std::make_shared<DataTypeInt16>();
    }
    else if (data_type == "int8")
    {
        data_type_ptr = std::make_shared<DataTypeInt8>();
    }
    else if (data_type == "uint64")
    {
        data_type_ptr = std::make_shared<DataTypeUInt64>();
    }
    else if (data_type == "uint32")
    {
        data_type_ptr = std::make_shared<DataTypeUInt32>();
    }
    else if (data_type == "uint16")
    {
        data_type_ptr = std::make_shared<DataTypeUInt16>();
    }
    else if (data_type == "uint8")
    {
        data_type_ptr = std::make_shared<DataTypeUInt8>();
    }
    else if (data_type == "float")
    {
        data_type_ptr = std::make_shared<DataTypeFloat32>();
    }
    else if (data_type == "double")
    {
        data_type_ptr = std::make_shared<DataTypeFloat64>();
    }
    else if ((data_type == "boolean" && !type_v3) || (data_type == "bool" && type_v3))
    {
        data_type_ptr = std::make_shared<DataTypeUInt8>();
    }
    else if (data_type == "string")
    {
        data_type_ptr = std::make_shared<DataTypeString>();
    }
    else if (data_type == "utf8")
    {
        data_type_ptr = std::make_shared<DataTypeString>();
    }
    else if ((data_type == "json" && !type_v3) || (data_type == "yson" && type_v3))
    {
        data_type_ptr = std::make_shared<DataTypeObject>(DB::DataTypeObject::SchemaFormat::JSON);
    }
    else if (data_type == "uuid")
    {
        data_type_ptr = std::make_shared<DataTypeUUID>();
    }
    else if (data_type == "date32")
    {
        data_type_ptr = std::make_shared<DataTypeDate>();
    }
    else if (data_type == "datetime64")
    {
        data_type_ptr = std::make_shared<DataTypeDateTime>(); // In seconds
    }
    else if (data_type == "timestamp64")
    {
        data_type_ptr = std::make_shared<DataTypeDateTime64>(6); // In microseconds
    }
    else if (data_type == "interval64")
    {
        data_type_ptr = std::make_shared<DataTypeInterval>(IntervalKind::Kind::Microsecond); // In YT all intervals are in microseconds
    }
    else if (data_type == "date")
    {
        data_type_ptr = std::make_shared<DataTypeDate>();
    }
    else if (data_type == "datetime")
    {
        data_type_ptr = std::make_shared<DataTypeDateTime>();
    }
    else if (data_type == "timestamp")
    {
        data_type_ptr = std::make_shared<DataTypeDateTime64>(6); // In microseconds
    }
    else if (data_type == "interval")
    {
        data_type_ptr = std::make_shared<DataTypeInterval>(IntervalKind::Kind::Microsecond); // In YT all intervals are in microseconds
    }
    else if (data_type == "any")
    {
        data_type_ptr = std::make_shared<DataTypeObject>(DB::DataTypeObject::SchemaFormat::JSON);
    }
    else if (data_type == "null")
    {
        data_type_ptr = std::make_shared<DataTypeNothing>();
    }
    else if (data_type == "void")
    {
        data_type_ptr = std::make_shared<DataTypeNothing>();
    }

    return data_type_ptr;
}

DataTypePtr convertYTItemType(const Poco::Dynamic::Var & item)
{
    if (item.isString())
    {
        return convertYTPrimitiveType(item.extract<String>(), false);
    }
    return convertYTTypeV3(item.extract<Poco::JSON::Object::Ptr>());
}

DataTypePtr convertYTDecimal(const Poco::JSON::Object::Ptr & json)
{
    DataTypePtr data_type_ptr;
    if (!json->has("precision") || !json->has("scale"))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse 'decimal' type from YT('precision' or 'scale' is not exist)");
    }
    auto precision_var = json->get("precision");
    auto scale_var = json->get("scale");
    if (!precision_var.isInteger() || !scale_var.isInteger())
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse 'decimal' type from YT('precision' or 'scale' is not integer)");
    }
    auto precision = precision_var.extract<size_t>();
    auto scale = scale_var.extract<size_t>();
    if (precision <=  DataTypeDecimalBase<Decimal32>::maxPrecision())
        data_type_ptr = std::make_shared<DataTypeDecimal<Decimal32>>(precision, scale);
    else if (precision <= DataTypeDecimalBase<Decimal64>::maxPrecision())
        data_type_ptr = std::make_shared<DataTypeDecimal<Decimal64>>(precision, scale);
    else if (precision <= DataTypeDecimalBase<Decimal128>::maxPrecision())
        data_type_ptr = std::make_shared<DataTypeDecimal<Decimal128>>(precision, scale);
    else if (precision <= DataTypeDecimalBase<Decimal256>::maxPrecision())
        data_type_ptr = std::make_shared<DataTypeDecimal<Decimal256>>(precision, scale);
    return data_type_ptr;
}

DataTypePtr convertYTOptional(const Poco::JSON::Object::Ptr & json)
{
    if (!json->has("item"))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse 'optional' type from YT('item' was not found)");
    }
    auto nested_type = convertYTItemType(json->get("item"));
    if (!nested_type)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse 'optional' type from YT(incorrect nested type)");
    }
    if (nested_type->isNullable() || !nested_type->canBeInsideNullable())
    {
        return nested_type;
    }
    return std::make_shared<DataTypeNullable>(nested_type);
}

DataTypePtr convertYTList(const Poco::JSON::Object::Ptr & json)
{
    return std::make_shared<DataTypeArray>(convertYTItemType(json->get("item")));
}

DataTypePtr convertYTTuple(const Poco::JSON::Object::Ptr & json)
{
    DataTypes types;
    auto tuple_types = json->getArray("elements");
    types.reserve(tuple_types->size());
    for (const auto & tuple_type : *tuple_types)
    {
        auto element_json = tuple_type.extract<Poco::JSON::Object::Ptr>();
        types.push_back(convertYTItemType(element_json->get("type")));
    }
    return std::make_shared<DataTypeTuple>(types);
}

DataTypePtr convertYTDict(const Poco::JSON::Object::Ptr &json)
{
    auto key = convertYTItemType(json->get("key"));
    auto value = convertYTItemType(json->get("value"));
    return std::make_shared<DataTypeMap>(key, value);
}

DataTypePtr convertYTVariant(const Poco::JSON::Object::Ptr &json)
{
    if (!json->has("elements"))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse 'variant' type from YTsaurus");
    }
    auto elements_json = json->getArray("elements");
    DataTypes types;
    types.reserve(elements_json->size());
    for (const auto & element : *elements_json)
    {
        auto element_json = element.extract<Poco::JSON::Object::Ptr>();
        types.push_back(convertYTItemType(element_json->get("type")));
    }
    return std::make_shared<DataTypeVariant>(types);
}

DataTypePtr convertYTStruct(const Poco::JSON::Object::Ptr &json)
{
    if (!json->has("members"))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse 'struct' type from YTsaurus");
    }
    auto members_json = json->getArray("members");
    DataTypes types;
    types.reserve(members_json->size());
    for (const auto & member : *members_json)
    {
        auto member_json = member.extract<Poco::JSON::Object::Ptr>();
        if (!member_json->get("name") || !member_json->has("type"))
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse 'struct' type from YTsaurus");
        }
        types.push_back(std::make_shared<DB::DataTypeTuple>(
            std::vector<DataTypePtr>{
                std::make_shared<DB::DataTypeString>(),
                convertYTItemType(member_json->get("type"))
            }
        ));
    }
    return std::make_shared<DB::DataTypeTuple>(types);
}

DataTypePtr convertYTTagged(const Poco::JSON::Object::Ptr & json)
{
    if (!json->has("tag") || !json->has("type"))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse 'tagged' type from YTsaurus");
    }
    return std::make_shared<DB::DataTypeTuple>(DB::DataTypes{std::make_shared<DB::DataTypeString>(), convertYTItemType(json->get("type"))});
}

DataTypePtr convertYTTypeV3(const Poco::JSON::Object::Ptr & json)
{
    if (!json->has("type_name"))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Coudn't parse the YT json schema('type_name' was not found)");
    }
    auto data_type_var = json->get("type_name");
    if (!data_type_var.isString())
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Coudn't parse the YT json schema('type_name' is not string)");
    }
    auto data_type = data_type_var.extract<std::string>();
    DataTypePtr data_type_ptr;
    if (data_type == "decimal")
    {
        data_type_ptr = convertYTDecimal(json);
    }
    else if (data_type == "optional")
    {
        data_type_ptr = convertYTOptional(json);
    }
    else if (data_type == "list")
    {
        data_type_ptr = convertYTList(json);
    }
    else if (data_type == "struct")
    {
        data_type_ptr = convertYTStruct(json);
    }
    else if (data_type == "tuple")
    {
        data_type_ptr = convertYTTuple(json);
    }
    else if (data_type == "variant")
    {
        data_type_ptr = convertYTVariant(json);
    }
    else if (data_type == "dict")
    {
        data_type_ptr = convertYTDict(json);
    }
    else if (data_type == "tagged")
    {
        data_type_ptr = convertYTTagged(json);
    }
    return data_type_ptr;
}

DataTypePtr convertYTSchema(const Poco::JSON::Object::Ptr & json)
{
    DataTypePtr data_type_ptr;
    if (json->has("type"))
    {
        data_type_ptr = convertYTPrimitiveType(json->getValue<String>("type"), false);
        if (data_type_ptr)
        {
            if (!json->has("required"))
            {
                throw Exception(ErrorCodes::INCORRECT_DATA, "Coudn't parse the YT json schema('required' was not found)");
            }
            bool required = json->getValue<bool>("required");
            if (required)
            {
                return data_type_ptr;
            }

            if (data_type_ptr->isNullable() || !data_type_ptr->canBeInsideNullable())
            {
                return data_type_ptr;
            }

            auto nullable_data_type_ptr = std::make_shared<DataTypeNullable>(std::move(data_type_ptr));
            return nullable_data_type_ptr;
        }
    }

    if (!json->has("type_v3"))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Coudn't parse the YT schema json('type_v3' was not found)");
    }
    auto value = json->get("type_v3");
    if (value.isString())
    {
        return convertYTPrimitiveType(value.extract<String>(), true);
    }
    try {
        return convertYTTypeV3(value.extract<Poco::JSON::Object::Ptr>());
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse type_v3 from YT metadata: {}", e.what());
    }
}
}
