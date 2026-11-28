#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaSchemaJSON.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Poco/JSON/Array.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

DataTypePtr getComplexTypeFromObject(const Poco::JSON::Object::Ptr & type)
{
    String type_name = type->getValue<String>("type");

    if (type_name == "struct")
    {
        DataTypes element_types;
        Names element_names;
        auto fields = type->get("fields").extract<Poco::JSON::Array::Ptr>();
        element_types.reserve(fields->size());
        element_names.reserve(fields->size());
        for (size_t i = 0; i != fields->size(); ++i)
        {
            auto field = fields->getObject(static_cast<Int32>(i));
            element_names.push_back(field->getValue<String>("name"));

            auto is_nullable = field->getValue<bool>("nullable");
            element_types.push_back(getDeltaLakeFieldType(field, "type", is_nullable));
        }

        return std::make_shared<DataTypeTuple>(element_types, element_names);
    }

    if (type_name == "array")
    {
        bool element_nullable = type->getValue<bool>("containsNull");
        auto element_type = getDeltaLakeFieldType(type, "elementType", element_nullable);
        return std::make_shared<DataTypeArray>(element_type);
    }

    if (type_name == "map")
    {
        auto key_type = getDeltaLakeFieldType(type, "keyType", /* is_nullable */false);
        bool value_nullable = type->getValue<bool>("valueContainsNull");
        auto value_type = getDeltaLakeFieldType(type, "valueType", value_nullable);
        return std::make_shared<DataTypeMap>(key_type, value_type);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported DeltaLake type: {}", type_name);
}

}

DataTypePtr getDeltaLakeFieldType(const Poco::JSON::Object::Ptr & field, const String & type_key, bool is_nullable)
{
    if (field->isObject(type_key))
        return getComplexTypeFromObject(field->getObject(type_key));

    auto type = field->get(type_key);
    if (type.isString())
    {
        const String & type_name = type.extract<String>();
        auto data_type = getDeltaLakeSimpleTypeByName(type_name);
        return is_nullable ? makeNullable(data_type) : data_type;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected 'type' field: {}", type.toString());
}

DataTypePtr getDeltaLakeSimpleTypeByName(const String & type_name)
{
    /// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types

    if (type_name == "string" || type_name == "binary")
        return std::make_shared<DataTypeString>();
    if (type_name == "long")
        return std::make_shared<DataTypeInt64>();
    if (type_name == "integer")
        return std::make_shared<DataTypeInt32>();
    if (type_name == "short")
        return std::make_shared<DataTypeInt16>();
    if (type_name == "byte")
        return std::make_shared<DataTypeInt8>();
    if (type_name == "float")
        return std::make_shared<DataTypeFloat32>();
    if (type_name == "double")
        return std::make_shared<DataTypeFloat64>();
    if (type_name == "boolean")
        return DataTypeFactory::instance().get("Bool");
    if (type_name == "date")
        return std::make_shared<DataTypeDate32>();
    if (type_name == "timestamp" || type_name == "timestamp_ntz")
        return std::make_shared<DataTypeDateTime64>(6);
    if (type_name.starts_with("decimal(") && type_name.ends_with(')'))
    {
        ReadBufferFromString buf(std::string_view(type_name.begin() + 8, type_name.end() - 1));
        size_t precision = 0;
        size_t scale = 0;
        readIntText(precision, buf);
        skipWhitespaceIfAny(buf);
        assertChar(',', buf);
        skipWhitespaceIfAny(buf);
        tryReadIntText(scale, buf);
        return createDecimal<DataTypeDecimal>(precision, scale);
    }
    /// varchar(n) and char(n) are valid Delta Lake types that map to string in Parquet.
    /// The length constraint is a SQL-level annotation only; we ignore it and use String.
    if ((type_name.starts_with("varchar(") || type_name.starts_with("char(")) && type_name.ends_with(')'))
        return std::make_shared<DataTypeString>();

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported DeltaLake type: {}", type_name);
}

}
