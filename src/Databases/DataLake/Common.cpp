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

#include <fmt/format.h>
#include <Poco/URI.h>

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

    return nullable ? DB::makeNullable(DB::Iceberg::IcebergSchemaProcessor::getSimpleType(name))
                    : DB::Iceberg::IcebergSchemaProcessor::getSimpleType(name);
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

String constructTableLocation(
    const String & location_scheme,
    const String & storage_endpoint,
    const String & namespace_name,
    const String & table_name)
{
    Poco::URI uri(storage_endpoint);
    auto path = uri.getPath();
    while (path.starts_with('/'))
        path.erase(0, 1);
    while (path.ends_with('/'))
        path.pop_back();

    if (location_scheme == "abfss")
    {
        /// Azure: `abfss://<container>@<host>/<path>`. `storage_endpoint` is
        /// `https://<host>/<container>/<extra>` or `abfss://<container>@<host>/<extra>`
        String container = uri.getUserInfo();
        String account_host = uri.getHost();
        String extra_path = path;

        if (container.empty())
        {
            auto first_slash = extra_path.find('/');
            if (first_slash == String::npos)
            {
                container = std::move(extra_path);
                extra_path.clear();
            }
            else
            {
                container = extra_path.substr(0, first_slash);
                extra_path = extra_path.substr(first_slash + 1);
            }
        }

        if (account_host.empty() || container.empty())
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "`storage_endpoint` ({}) for Azure must include both account host and container "
                "(expected https://<account>.dfs.core.windows.net/<container>[/<sub-path>] or "
                "abfss://<container>@<account>.dfs.core.windows.net[/<sub-path>])",
                storage_endpoint);

        if (extra_path.empty())
            return fmt::format("abfss://{}@{}/{}/{}", container, account_host, namespace_name, table_name);
        return fmt::format("abfss://{}@{}/{}/{}/{}", container, account_host, extra_path, namespace_name, table_name);
    }

    if (location_scheme == "s3")
    {
        if (path.empty())
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "`storage_endpoint` ({}) does not contain a bucket; "
                "CREATE TABLE in DataLakeCatalog requires `storage_endpoint` to include a non-empty bucket path.",
                storage_endpoint);
        return fmt::format("s3://{}/{}/{}", path, namespace_name, table_name);
    }

    /// HDFS / file / other schemes that may have `authority`.
    String authority = uri.getAuthority();
    if (authority.empty())
    {
        if (path.empty())
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "`storage_endpoint` ({}) does not contain a path",
                storage_endpoint);
        return fmt::format("{}:///{}/{}/{}", location_scheme, path, namespace_name, table_name);
    }

    if (path.empty())
        return fmt::format("{}://{}/{}/{}", location_scheme, authority, namespace_name, table_name);
    return fmt::format("{}://{}/{}/{}/{}", location_scheme, authority, path, namespace_name, table_name);
}

}
