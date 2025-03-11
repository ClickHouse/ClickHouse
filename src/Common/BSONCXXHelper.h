#pragma once

#include "config.h"

#if USE_MONGODB
#include <Common/Base64.h>
#include <DataTypes/FieldToDataType.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TYPE_MISMATCH;
extern const int NOT_IMPLEMENTED;
}

namespace BSONCXXHelper
{

using bsoncxx::builder::basic::array;
using bsoncxx::builder::basic::document;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

static bsoncxx::types::bson_value::value fieldAsBSONValue(const Field & field, const DataTypePtr & type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::String:
            return bsoncxx::types::b_string{field.safeGet<String>()};
        case TypeIndex::UInt8: {
            if (isBool(type))
                return bsoncxx::types::b_bool{field.safeGet<UInt8>() != 0};
            return bsoncxx::types::b_int32{static_cast<Int32>(field.safeGet<UInt8 &>())};
        }
        case TypeIndex::UInt16:
            return bsoncxx::types::b_int32{static_cast<Int32>(field.safeGet<UInt16 &>())};
        case TypeIndex::UInt32:
            return bsoncxx::types::b_int64{static_cast<Int64>(field.safeGet<UInt32 &>())};
        case TypeIndex::UInt64:
            return bsoncxx::types::b_double{static_cast<Float64>(field.safeGet<UInt64 &>())};
        case TypeIndex::Int8:
            return bsoncxx::types::b_int32{static_cast<Int32>(field.safeGet<Int8 &>())};
        case TypeIndex::Int16:
            return bsoncxx::types::b_int32{static_cast<Int32>(field.safeGet<Int16 &>())};
        case TypeIndex::Int32:
            return bsoncxx::types::b_int32{static_cast<Int32>(field.safeGet<Int32 &>())};
        case TypeIndex::Int64:
            return bsoncxx::types::b_int64{field.safeGet<Int64 &>()};
        case TypeIndex::Float32:
            return bsoncxx::types::b_double{field.safeGet<Float32 &>()};
        case TypeIndex::Float64:
            return bsoncxx::types::b_double{field.safeGet<Float64 &>()};
        case TypeIndex::Date:
            return bsoncxx::types::b_date{std::chrono::seconds{field.safeGet<UInt16 &>() * 86400}};
        case TypeIndex::Date32:
            return bsoncxx::types::b_date{std::chrono::seconds{field.safeGet<Int32 &>() * 86400}};
        case TypeIndex::DateTime:
            return bsoncxx::types::b_date{std::chrono::seconds{field.safeGet<UInt32 &>()}};
        case TypeIndex::UUID:
            return bsoncxx::types::b_string{static_cast<String>(formatUUID(field.safeGet<UUID &>()))};
        case TypeIndex::Tuple: {
            auto arr = array();
            for (const auto & elem : field.safeGet<Tuple &>())
                arr.append(fieldAsBSONValue(elem, applyVisitor(FieldToDataType(), elem)));
            return arr.view();
        }
        case TypeIndex::Array: {
            auto arr = array();
            for (const auto & elem : field.safeGet<Array &>())
                arr.append(fieldAsBSONValue(elem, applyVisitor(FieldToDataType(), elem)));
            return arr.view();
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Fields with type '{}' is not supported.", type->getPrettyName());
    }
}

template <typename T>
static JSONBuilder::ItemPtr BSONElementAsJSON(const T & value)
{
    switch (value.type())
    {
        case bsoncxx::type::k_string:
            return std::make_unique<JSONBuilder::JSONString>(std::string(value.get_string().value));
        case bsoncxx::type::k_symbol:
            return std::make_unique<JSONBuilder::JSONString>(std::string(value.get_string().value));
        case bsoncxx::type::k_oid:
            return std::make_unique<JSONBuilder::JSONString>(value.get_oid().value.to_string());
        case bsoncxx::type::k_binary:
            return std::make_unique<JSONBuilder::JSONString>(
                base64Encode(std::string(reinterpret_cast<const char *>(value.get_binary().bytes), value.get_binary().size)));
        case bsoncxx::type::k_bool:
            return std::make_unique<JSONBuilder::JSONBool>(value.get_bool());
        case bsoncxx::type::k_int32:
            return std::make_unique<JSONBuilder::JSONNumber<Int32>>(value.get_int32());
        case bsoncxx::type::k_int64:
            return std::make_unique<JSONBuilder::JSONNumber<Int64>>(value.get_int64());
        case bsoncxx::type::k_double:
            return std::make_unique<JSONBuilder::JSONNumber<Float64>>(value.get_double());
        case bsoncxx::type::k_date:
            return std::make_unique<JSONBuilder::JSONString>(DateLUT::instance().timeToString(value.get_date().to_int64() / 1000));
        case bsoncxx::type::k_timestamp:
            return std::make_unique<JSONBuilder::JSONString>(DateLUT::instance().timeToString(value.get_timestamp().timestamp));
        case bsoncxx::type::k_document:
        {
            auto doc = std::make_unique<JSONBuilder::JSONMap>();
            for (const auto & elem : value.get_document().value)
                doc->add(std::string(elem.key()), BSONElementAsJSON(elem));
            return doc;
        }
        case bsoncxx::type::k_array:
        {
            auto arr = std::make_unique<JSONBuilder::JSONArray>();
            for (const auto & elem : value.get_array().value)
                arr->add(BSONElementAsJSON(elem));
            return arr;
        }
        case bsoncxx::type::k_regex:
        {
            auto doc = std::make_unique<JSONBuilder::JSONMap>();
            doc->add(std::string(value.get_regex().regex), std::string(value.get_regex().options));
            return doc;
        }
        case bsoncxx::type::k_dbpointer:
        {
            auto doc = std::make_unique<JSONBuilder::JSONMap>();
            doc->add(value.get_dbpointer().value.to_string(), std::string(value.get_dbpointer().collection));
            return doc;
        }
        case bsoncxx::type::k_null:
            return std::make_unique<JSONBuilder::JSONNull>();

        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization BSON type '{}' is not supported", bsoncxx::to_string(value.type()));
    }
}

template <typename T>
static std::string BSONElementAsString(const T & value, const JSONBuilder::FormatSettings & json_format_settings)
{
    switch (value.type())
    {
        case bsoncxx::type::k_string:
            return std::string(value.get_string().value);
        case bsoncxx::type::k_oid:
            return value.get_oid().value.to_string();
        case bsoncxx::type::k_binary:
            return std::string(reinterpret_cast<const char *>(value.get_binary().bytes), value.get_binary().size);
        case bsoncxx::type::k_bool:
            return value.get_bool().value ? "true" : "false";
        case bsoncxx::type::k_int32:
            return std::to_string(static_cast<Int64>(value.get_int32().value));
        case bsoncxx::type::k_int64:
            return std::to_string(value.get_int64().value);
        case bsoncxx::type::k_double:
            return std::to_string(value.get_double().value);
        case bsoncxx::type::k_decimal128:
            return value.get_decimal128().value.to_string();
        case bsoncxx::type::k_date:
            return DateLUT::instance().timeToString(value.get_date().to_int64() / 1000);
        case bsoncxx::type::k_timestamp:
            return DateLUT::instance().timeToString(value.get_timestamp().timestamp);
        // MongoDB's documents and arrays may not have strict types or be nested, so the most optimal solution is store their JSON representations.
        // bsoncxx::to_json function will return something like "'number': {'$numberInt': '321'}", this why we have to use own implementation.
        case bsoncxx::type::k_document:
        case bsoncxx::type::k_array:
        case bsoncxx::type::k_regex:
        case bsoncxx::type::k_dbpointer:
        case bsoncxx::type::k_symbol:
        {
            WriteBufferFromOwnString buf;
            auto format_context = JSONBuilder::FormatContext{.out = buf};
            BSONElementAsJSON(value)->format(json_format_settings, format_context);
            return buf.str();
        }
        case bsoncxx::type::k_undefined:
            return "undefined";
        case bsoncxx::type::k_null:
            return "null";
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BSON type {} is unserializable.", bsoncxx::to_string(value.type()));
    }
}

template <typename T, typename T2>
static T BSONElementAsNumber(const T2 & value, const std::string & name)
{
    switch (value.type())
    {
        case bsoncxx::type::k_bool:
            return static_cast<T>(value.get_bool());
        case bsoncxx::type::k_int32:
            return static_cast<T>(value.get_int32());
        case bsoncxx::type::k_int64:
            return static_cast<T>(value.get_int64());
        case bsoncxx::type::k_double:
            return static_cast<T>(value.get_double());
        default:
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Type mismatch, {} cannot be converted to number for column {}.",
                bsoncxx::to_string(value.type()),
                name);
    }
}

static Array BSONArrayAsArray(
    size_t dimensions,
    const bsoncxx::types::b_array & array,
    const DataTypePtr & type,
    const Field & default_value,
    const std::string & name,
    const JSONBuilder::FormatSettings & json_format_settings)
{
    auto arr = Array();
    if (dimensions > 0)
    {
        --dimensions;
        for (auto const & elem : array.value)
        {
            if (elem.type() != bsoncxx::type::k_array)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Array {} have less dimensions then defined in the schema.", name);

            arr.emplace_back(BSONArrayAsArray(dimensions, elem.get_array(), type, default_value, name, json_format_settings));
        }
    }
    else
    {
        for (auto const & value : array.value)
        {
            if (value.type() == bsoncxx::type::k_null)
                arr.emplace_back(default_value);
            else
            {
                switch (type->getTypeId())
                {
                    case TypeIndex::Int8:
                        arr.emplace_back(BSONElementAsNumber<Int8, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::UInt8:
                        arr.emplace_back(BSONElementAsNumber<UInt8, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Int16:
                        arr.emplace_back(BSONElementAsNumber<Int16, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::UInt16:
                        arr.emplace_back(BSONElementAsNumber<UInt16, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Int32:
                        arr.emplace_back(BSONElementAsNumber<Int32, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::UInt32:
                        arr.emplace_back(BSONElementAsNumber<UInt32, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Int64:
                        arr.emplace_back(BSONElementAsNumber<Int64, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::UInt64:
                        arr.emplace_back(BSONElementAsNumber<UInt64, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Int128:
                        arr.emplace_back(BSONElementAsNumber<Int128, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::UInt128:
                        arr.emplace_back(BSONElementAsNumber<UInt128, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Int256:
                        arr.emplace_back(BSONElementAsNumber<Int256, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::UInt256:
                        arr.emplace_back(BSONElementAsNumber<UInt256, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Float32:
                        arr.emplace_back(BSONElementAsNumber<Float32, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Float64:
                        arr.emplace_back(BSONElementAsNumber<Float64, bsoncxx::array::element>(value, name));
                        break;
                    case TypeIndex::Date: {
                        if (value.type() != bsoncxx::type::k_date)
                            throw Exception(
                                ErrorCodes::TYPE_MISMATCH,
                                "Type mismatch, expected date, got {} for column {}.",
                                bsoncxx::to_string(value.type()),
                                name);

                        arr.emplace_back(DateLUT::instance().toDayNum(value.get_date().to_int64() / 1000).toUnderType());
                        break;
                    }
                    case TypeIndex::Date32: {
                        if (value.type() != bsoncxx::type::k_date)
                            throw Exception(
                                ErrorCodes::TYPE_MISMATCH,
                                "Type mismatch, expected date, got {} for column {}.",
                                bsoncxx::to_string(value.type()),
                                name);

                        arr.emplace_back(DateLUT::instance().toDayNum(value.get_date().to_int64() / 1000).toUnderType());
                        break;
                    }
                    case TypeIndex::DateTime: {
                        if (value.type() != bsoncxx::type::k_date)
                            throw Exception(
                                ErrorCodes::TYPE_MISMATCH,
                                "Type mismatch, expected date, got {} for column {}.",
                                bsoncxx::to_string(value.type()),
                                name);

                        arr.emplace_back(static_cast<UInt32>(value.get_date().to_int64() / 1000));
                        break;
                    }
                    case TypeIndex::DateTime64: {
                        if (value.type() != bsoncxx::type::k_date)
                            throw Exception(
                                ErrorCodes::TYPE_MISMATCH,
                                "Type mismatch, expected date, got {} for column {}.",
                                bsoncxx::to_string(value.type()),
                                name);

                        arr.emplace_back(static_cast<Decimal64>(value.get_date().to_int64()));
                        break;
                    }
                    case TypeIndex::UUID: {
                        if (value.type() != bsoncxx::type::k_string)
                            throw Exception(
                                ErrorCodes::TYPE_MISMATCH,
                                "Type mismatch, expected string (UUID), got {} for column {}.",
                                bsoncxx::to_string(value.type()),
                                name);

                        arr.emplace_back(parse<UUID>(value.get_string().value.data()));
                        break;
                    }
                    case TypeIndex::String:
                        arr.emplace_back(BSONElementAsString(value, json_format_settings));
                        break;
                    default:
                        throw Exception(
                            ErrorCodes::NOT_IMPLEMENTED,
                            "Array {} has unsupported nested type {}.",
                            name,
                            type->getName());
                }
            }
        }
    }
    return arr;
}

static bsoncxx::types::bson_value::value fieldAsOID(const Field & field)
{
    switch (field.getType())
    {
        case Field::Types::String:
            return bsoncxx::oid(field.safeGet<String &>());
        case Field::Types::Array: {
            auto arr = array();
            for (const auto & elem : field.safeGet<Array &>())
                arr.append(fieldAsOID(elem));
            return arr.view();
        }
        case Field::Types::Tuple: {
            auto tuple = array();
            for (const auto & elem : field.safeGet<Tuple &>())
                tuple.append(fieldAsOID(elem));
            return tuple.view();
        }
        default:
            throw Exception(ErrorCodes::TYPE_MISMATCH, "{} can't be converted to oid.", field.getType());
    }
}
}

}
#endif
