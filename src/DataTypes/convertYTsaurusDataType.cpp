#include <DataTypes/convertYTsaurusDataType.h>
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
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeVariant.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}


/// https://ytsaurus.tech/docs/en/user-guide/storage/data-types#schema_primitive
DataTypePtr convertYTPrimitiveType(const String & data_type, bool type_v3)
{
    DataTypePtr data_type_ptr;
    if (data_type == "int64")
    {
        data_type_ptr = DataTypeFactory::instance().get("Int64");
    }
    else if (data_type == "int32")
    {
        data_type_ptr = DataTypeFactory::instance().get("Int32");
    }
    else if (data_type == "int16")
    {
        data_type_ptr = DataTypeFactory::instance().get("Int16");
    }
    else if (data_type == "int8")
    {
        data_type_ptr = DataTypeFactory::instance().get("Int8");
    }
    else if (data_type == "uint64")
    {
        data_type_ptr = DataTypeFactory::instance().get("UInt64");
    }
    else if (data_type == "uint32")
    {
        data_type_ptr = DataTypeFactory::instance().get("UInt32");
    }
    else if (data_type == "uint16")
    {
        data_type_ptr = DataTypeFactory::instance().get("UInt16");
    }
    else if (data_type == "uint8")
    {
        data_type_ptr = DataTypeFactory::instance().get("UInt8");
    }
    else if (data_type == "float")
    {
        data_type_ptr = DataTypeFactory::instance().get("Float");
    }
    else if (data_type == "double")
    {
        data_type_ptr = DataTypeFactory::instance().get("Float64");
    }
    else if ((data_type == "boolean" && !type_v3) || (data_type == "bool" && type_v3))
    {
        data_type_ptr = DataTypeFactory::instance().get("Bool");
    }
    else if (data_type == "string")
    {
        data_type_ptr = DataTypeFactory::instance().get("String");
    }
    else if (data_type == "utf8")
    {
        data_type_ptr = DataTypeFactory::instance().get("String");
    }
    else if (data_type == "json")
    {
        data_type_ptr = DataTypeFactory::instance().get("JSON");
    }
    else if (data_type == "uuid")
    {
        data_type_ptr = DataTypeFactory::instance().get("UUID");
    }
    else if (data_type == "date32")
    {
        data_type_ptr = DataTypeFactory::instance().get("Date");
    }
    else if (data_type == "datetime64")
    {
        data_type_ptr = DataTypeFactory::instance().get("DateTime64(0)"); // In seconds
    }
    else if (data_type == "timestamp64")
    {
        data_type_ptr = DataTypeFactory::instance().get("DateTime64(6)"); // In microseconds
    }
    else if (data_type == "interval64")
    {
        data_type_ptr = DataTypeFactory::instance().get("Int64");
    }
    else if (data_type == "date")
    {
        data_type_ptr = DataTypeFactory::instance().get("Date");
    }
    else if (data_type == "datetime")
    {
        data_type_ptr = DataTypeFactory::instance().get("DateTime64(0)");
    }
    else if (data_type == "timestamp")
    {
        data_type_ptr = DataTypeFactory::instance().get("DateTime64(6)"); // In microseconds
    }
    else if (data_type == "interval")
    {
        data_type_ptr = DataTypeFactory::instance().get("Int64");
    }
    else if (data_type == "any" || data_type == "yson")
    {
        data_type_ptr = DataTypeFactory::instance().get("Dynamic");
    }
    else if (data_type == "null")
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "ClickHouse couldn't parse YT data type \"null\"");
    }
    else if (data_type == "void")
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "ClickHouse couldn't parse YT data type \"void\"");
    }

    return data_type_ptr;
}

bool isTypeComplex(const Poco::Dynamic::Var & item)
{
    return !item.isString();
}

DataTypePtr convertYTItemType(const Poco::Dynamic::Var & item)
{
    if (!isTypeComplex(item))
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
    /// ClickHouse/docs/engines/table-engines/integrations/ytsaurus.md *See Also*
    if (!nested_type->canBeInsideNullable())
    {
        switch (nested_type->getTypeId())
        {
            case TypeIndex::Dynamic:
                return nested_type;

            default:
                throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse 'optional' type from YT(incorrect nested type)");
        }
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
        const auto & element_json = tuple_type.extract<Poco::JSON::Object::Ptr>();
        types.push_back(convertYTItemType(element_json->get("type")));
    }
    return std::make_shared<DataTypeTuple>(types);
}

DataTypePtr convertYTDict(const Poco::JSON::Object::Ptr &json)
{
    auto key = convertYTItemType(json->get("key"));
    auto value = convertYTItemType(json->get("value"));
    DataTypes types = {key, value};
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
        const auto & element_json = element.extract<Poco::JSON::Object::Ptr>();
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
    Strings names;
    types.reserve(members_json->size());
    names.reserve(members_json->size());
    for (const auto & member : *members_json)
    {
        const auto & member_json = member.extract<Poco::JSON::Object::Ptr>();
        if (!member_json->get("name") || !member_json->has("type"))
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse 'struct' type from YTsaurus. Missing field `member`.");
        }
        types.push_back(convertYTItemType(member_json->get("type")));
        names.push_back(member_json->get("name"));
    }
    return std::make_shared<DB::DataTypeTuple>(types, names);
}

DataTypePtr convertYTTagged(const Poco::JSON::Object::Ptr & json)
{
    if (!json->has("tag") || !json->has("type"))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse 'tagged' type from YTsaurus");
    }
    return convertYTItemType(json->get("type"));
}

DataTypePtr convertYTTypeV3(const Poco::JSON::Object::Ptr & json)
{
    if (!json->has("type_name"))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse the YT json schema('type_name' was not found)");
    }
    auto data_type_var = json->get("type_name");
    if (!data_type_var.isString())
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse the YT json schema('type_name' is not string)");
    }
    const auto & data_type = data_type_var.extract<std::string>();
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
        if (data_type_ptr && data_type_ptr->getName() != "Dynamic")
        {
            if (!json->has("required"))
            {
                throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse the YT json schema('required' was not found)");
            }
            bool required = json->getValue<bool>("required");
            if (required)
            {
                return data_type_ptr;
            }

            if (data_type_ptr->isNullable() || !data_type_ptr->canBeInsideNullable())
            {
                switch (data_type_ptr->getTypeId())
                {
                    case TypeIndex::Array:
                    case TypeIndex::Nullable:
                    case TypeIndex::Tuple:
                    case TypeIndex::Dynamic:
                        return data_type_ptr;

                    default:
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse 'optional' type from YT(incorrect nested type)");
                }
            }

            auto nullable_data_type_ptr = std::make_shared<DataTypeNullable>(std::move(data_type_ptr));
            return nullable_data_type_ptr;
        }
    }

    if (!json->has("type_v3"))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse the YT schema json('type_v3' was not found)");
    }
    auto value = json->get("type_v3");
    try
    {
        if (!isTypeComplex(value))
        {
            chassert(value.isString());
            auto data_type = convertYTPrimitiveType(value.extract<String>(), true);
            if (!data_type)
            {
                throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect primitive type");
            }
            return data_type;
        }
        else
        {
            return convertYTTypeV3(value.extract<Poco::JSON::Object::Ptr>());
        }
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Couldn't parse type_v3 from YT metadata: {}", e.what());
    }
}
}
