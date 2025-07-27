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

#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>

namespace DB::ErrorCodes
{
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

    if (name.starts_with("array<") && name.ends_with(">"))
    {
        String inner = name.substr(6, name.size() - 7);
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
        std::vector<DB::DataTypePtr> field_types;

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

    return nullable ? DB::makeNullable(DB::IcebergSchemaProcessor::getSimpleType(name)) : DB::IcebergSchemaProcessor::getSimpleType(name);
}

}
