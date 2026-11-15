#include <Databases/DataLake/Common.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <cctype>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int DATALAKE_DATABASE_ERROR;
}

namespace DataLake
{

String trim(const String & str)
{
    size_t start = str.find_first_not_of(' ');
    size_t end = str.find_last_not_of(' ');
    return (start == String::npos || end == String::npos) ? "" : str.substr(start, end - start + 1);
}

std::vector<String> splitTypeArguments(const String & type_str)
{
    std::vector<String> args;
    int angle_depth = 0;
    int paren_depth = 0;
    size_t start = 0;

    for (size_t i = 0; i < type_str.size(); ++i)
    {
        char c = type_str[i];
        if (c == '<')
            angle_depth++;
        else if (c == '>')
            angle_depth--;
        else if (c == '(')
            paren_depth++;
        else if (c == ')')
            paren_depth--;
        else if (c == ',' && angle_depth == 0 && paren_depth == 0)
        {
            args.push_back(trim(type_str.substr(start, i - start)));
            start = i + 1;
        }
    }

    args.push_back(trim(type_str.substr(start)));
    return args;
}

DB::DataTypePtr getType(const String & type_name, bool nullable, const String & prefix)
{
    String name = trim(type_name);

    /// Iceberg names this type `list`, Hive names it `array`; Glue passes through whichever spelling
    /// the writer that registered the table used.
    if ((name.starts_with("array<") || name.starts_with("list<")) && name.ends_with(">"))
    {
        const size_t open = name.find('<');
        String inner = name.substr(open + 1, name.size() - open - 2);
        return std::make_shared<DB::DataTypeArray>(getType(inner, nullable));
    }

    if (name.starts_with("map<") && name.ends_with(">"))
    {
        String inner = name.substr(4, name.size() - 5);
        auto args = splitTypeArguments(inner);

        if (args.size() != 2)
            throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid data type {}", type_name);

        return std::make_shared<DB::DataTypeMap>(getType(args[0], false), getType(args[1], nullable));
    }

    if (name.starts_with("struct<") && name.ends_with(">"))
    {
        String inner = name.substr(7, name.size() - 8);
        auto args = splitTypeArguments(inner);

        std::vector<String> field_names;
        DB::DataTypes field_types;

        for (const auto & arg : args)
        {
            size_t colon = arg.find(':');
            if (colon == String::npos)
                throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Invalid data type {}", type_name);

            String field_name = trim(arg.substr(0, colon));
            String field_type = trim(arg.substr(colon + 1));
            String full_field_name = prefix.empty() ? field_name : prefix + "." + field_name;

            field_names.push_back(full_field_name);
            field_types.push_back(getType(field_type, nullable, full_field_name));
        }
        return std::make_shared<DB::DataTypeTuple>(field_types, field_names);
    }

    return nullable ? DB::makeNullable(DB::Iceberg::IcebergSchemaProcessor::getSimpleType(name))
                    : DB::Iceberg::IcebergSchemaProcessor::getSimpleType(name);
}

namespace
{

String icebergPrimitiveTypeToGlueType(const String & type_name_arg)
{
    using namespace DB::Iceberg;

    /// Glue/Hive spell parameterized types without inner whitespace: `decimal(9,2)`.
    String type_name;
    type_name.reserve(type_name_arg.size());
    for (char c : type_name_arg)
        if (!std::isspace(static_cast<unsigned char>(c)))
            type_name += c;

    if (type_name == f_boolean)
        return "boolean";
    if (type_name == f_int)
        return "int";
    if (type_name == f_long)
        return "bigint";
    if (type_name == f_float)
        return "float";
    if (type_name == f_double)
        return "double";
    if (type_name == f_date)
        return "date";
    /// Hive has no time-of-day type; Iceberg's Glue integration stores it as string.
    if (type_name == f_time)
        return "string";
    /// Glue uses `timestamp` for all variants; ClickHouse recovers the exact type from `metadata.json`.
    if (type_name == f_timestamp || type_name == f_timestamptz || type_name == f_timestamp_ns || type_name == f_timestamptz_ns)
        return "timestamp";
    if (type_name == f_string || type_name == f_uuid)
        return "string";
    if (type_name == f_binary || type_name.starts_with("fixed["))
        return "binary";
    if (type_name.starts_with("decimal("))
        return type_name;

    /// Pass unknown Iceberg types through; Glue accepts them even if some consumers cannot.
    return type_name;
}

}

String icebergTypeToGlueType(const Poco::Dynamic::Var & type)
{
    using namespace DB::Iceberg;

    if (type.isString())
        return icebergPrimitiveTypeToGlueType(type.extract<String>());

    Poco::JSON::Object::Ptr type_object;
    if (type.type() == typeid(Poco::JSON::Object::Ptr))
        type_object = type.extract<Poco::JSON::Object::Ptr>();
    else if (type.type() == typeid(Poco::JSON::Object))
        type_object = new Poco::JSON::Object(type.extract<Poco::JSON::Object>());
    else
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Unexpected representation of Iceberg type in schema: {}", type.toString());

    const auto type_name = type_object->getValue<String>(f_type);

    if (type_name == f_struct)
    {
        auto fields = type_object->getArray(f_fields);
        String result = "struct<";
        for (size_t i = 0; i < fields->size(); ++i)
        {
            auto field = fields->getObject(static_cast<unsigned>(i));
            if (i != 0)
                result += ",";
            result += field->getValue<String>(f_name);
            result += ":";
            result += icebergTypeToGlueType(field->get(f_type));
        }
        result += ">";
        return result;
    }

    if (type_name == f_list)
        return "array<" + icebergTypeToGlueType(type_object->get(f_element)) + ">";

    if (type_name == f_map)
        return "map<" + icebergTypeToGlueType(type_object->get(f_key)) + "," + icebergTypeToGlueType(type_object->get(f_value)) + ">";

    throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Unknown Iceberg complex type: {}", type_name);
}

std::pair<std::string, std::string> parseTableName(const std::string & name)
{
    auto pos = name.rfind('.');
    if (pos == std::string::npos)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Table cannot have empty namespace: {}", name);

    auto table_name = name.substr(pos + 1);
    auto namespace_name = name.substr(0, name.size() - table_name.size() - 1);
    return {namespace_name, table_name};
}

}
