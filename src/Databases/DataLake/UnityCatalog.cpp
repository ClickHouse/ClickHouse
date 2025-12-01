#include <Databases/DataLake/UnityCatalog.h>

#if USE_PARQUET

#include <DataTypes/DataTypeNullable.h>
#include <Poco/URI.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Common/checkStackSize.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Core/NamesAndTypes.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <fmt/ranges.h>

namespace DB::ErrorCodes
{
    extern const int DATALAKE_DATABASE_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace
{
    bool hasValueAndItsNotNone(const std::string value, const Poco::JSON::Object::Ptr & object)
    {
        return object->has(value) && !object->isNull(value) && !object->get(value).isEmpty();
    }
}
namespace DataLake
{

static const auto SCHEMAS_ENDPOINT = "schemas";
static const auto TABLES_ENDPOINT = "tables";
static const auto TEMPORARY_CREDENTIALS_ENDPOINT = "temporary-table-credentials";
static const std::unordered_set<std::string> READABLE_TABLES = {"TABLE_DELTA", "TABLE_DELTA_EXTERNAL"};
static const auto READABLE_DATA_SOURCE_FORMAT = "DELTA";

struct UnityCatalogFullSchemaName
{
    std::string catalog_name;
    std::string schema_name;
};

UnityCatalogFullSchemaName parseFullSchemaName(const std::string & full_name)
{
    auto first_dot = full_name.find('.');
    auto catalog_name = full_name.substr(0, first_dot);
    auto schema = full_name.substr(first_dot + 1);
    return UnityCatalogFullSchemaName{.catalog_name = catalog_name, .schema_name = schema};
}

std::pair<Poco::Dynamic::Var, std::string> UnityCatalog::getJSONRequest(const std::string & route, const Poco::URI::QueryParameters & params) const
{
    const auto & context = getContext();
    return makeHTTPRequestAndReadJSON(base_url / route, context, credentials, params, {auth_header});
}

std::pair<Poco::Dynamic::Var, std::string> UnityCatalog::postJSONRequest(const std::string & route, std::function<void(std::ostream &)> out_stream_callaback) const
{
    const auto & context = getContext();
    return makeHTTPRequestAndReadJSON(base_url / route, context, credentials, {}, {auth_header}, Poco::Net::HTTPRequest::HTTP_POST, out_stream_callaback);
}

bool UnityCatalog::empty() const
{

    auto all_schemas = getSchemas("");
    for (const auto & schema : all_schemas)
    {
        if (!getTablesForSchema(schema, 1).empty())
            return false;
    }

    return true;
}

DB::Names UnityCatalog::getTables() const
{
    DB::Names result;

    auto all_schemas = getSchemas("");
    for (const auto & schema : all_schemas)
    {
        auto schema_tables = getTablesForSchema(schema);
        result.insert(result.end(), schema_tables.begin(), schema_tables.end());
    }

    return result;
}

void UnityCatalog::getTableMetadata(
    const std::string & namespace_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    if (!tryGetTableMetadata(namespace_name, table_name, result))
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "No response from unity catalog");
}

void UnityCatalog::getCredentials(const std::string & table_id, TableMetadata & metadata) const
{
    LOG_DEBUG(log, "Getting credentials for table {}", table_id);
    auto storage_type = parseStorageTypeFromLocation(metadata.getLocation());
    switch (storage_type)
    {
        case StorageType::S3:
        {
            auto callback = [table_id] (std::ostream & os)
            {
                Poco::JSON::Object obj;
                obj.set("table_id", table_id);
                obj.set("operation", "READ");
                obj.stringify(os);
            };

            auto [json, _] = postJSONRequest(TEMPORARY_CREDENTIALS_ENDPOINT, callback);
            const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

            if (hasValueAndItsNotNone("aws_temp_credentials", object))
            {
                const Poco::JSON::Object::Ptr & creds_object = object->getObject("aws_temp_credentials");
                std::string access_key_id = creds_object->get("access_key_id").extract<String>();
                std::string secret_access_key = creds_object->get("secret_access_key").extract<String>();
                std::string session_token = creds_object->get("session_token").extract<String>();

                auto creds = std::make_shared<S3Credentials>(access_key_id, secret_access_key, session_token);
                metadata.setStorageCredentials(creds);
            }
            break;
        }
        default:
            break;
    }
}

bool UnityCatalog::tryGetTableMetadata(
    const std::string & schema_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    auto full_table_name = warehouse + "." + schema_name + "." + table_name;
    Poco::Dynamic::Var json;
    std::string json_str;
    try
    {
        std::tie(json, json_str) = getJSONRequest(std::filesystem::path{TABLES_ENDPOINT} / full_table_name);
        const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();
        if (hasValueAndItsNotNone("name", object) && object->get("name").extract<String>() == table_name)
        {
            std::string location;
            if (result.requiresLocation())
            {
                if (hasValueAndItsNotNone("storage_location", object))
                {
                    location = object->get("storage_location").extract<String>();
                    result.setLocation(location);
                }
                else
                {
                    result.setTableIsNotReadable(fmt::format("Cannot read table `{}` because it doesn't have storage location. " \
                        "It means that it's not a DeltaLake table, and it's unreadable with Unity catalog in ClickHouse", full_table_name));
                }

            }

            bool has_securable_kind = hasValueAndItsNotNone("securable_kind", object);
            bool has_data_source_format = hasValueAndItsNotNone("data_source_format", object);
            if (has_securable_kind && !READABLE_TABLES.contains(object->get("securable_kind").extract<String>()))
            {
                result.setTableIsNotReadable(fmt::format("Cannot read table `{}` because it has unsupported securable_kind: '{}'. " \
                    "It means that it's unreadable with Unity catalog in ClickHouse, readable tables are: [{}]",
                    full_table_name, object->get("securable_kind").extract<String>(), fmt::join(READABLE_TABLES, ", ")));
            }

            if (has_data_source_format && object->get("data_source_format").extract<String>() != READABLE_DATA_SOURCE_FORMAT)
            {
                result.setTableIsNotReadable(fmt::format("Cannot read table `{}` because it has unsupported data_source_format '{}'. " \
                    "It means that it's unreadable with Unity catalog in ClickHouse, readable tables must have data_source_format == '{}'",
                    full_table_name, object->get("securable_kind").extract<String>(), READABLE_DATA_SOURCE_FORMAT));
            }

            if (!has_data_source_format && !has_securable_kind)
            {
                result.setTableIsNotReadable(fmt::format("Cannot read table `{}` because it has no information about data_source_format or securable_kind. " \
                    "It means that it's unreadable with Unity catalog in ClickHouse", full_table_name));
            }

            LOG_DEBUG(log, "Processing table {} is default readable {}", table_name, result.isDefaultReadableTable());

            if (result.requiresSchema())
            {
                DB::NamesAndTypesList schema;
                auto columns_json = object->getArray("columns");

                for (size_t i = 0; i < columns_json->size(); ++i)
                {
                    const auto column_json = columns_json->get(static_cast<int>(i)).extract<Poco::JSON::Object::Ptr>();
                    std::string name = column_json->getValue<String>("name");
                    auto is_nullable = column_json->getValue<bool>("nullable");
                    auto type_json_str = column_json->get("type_json").extract<String>();
                    DB::DataTypePtr data_type;
                    /// NOTE: Weird case with OSS Unity catalog, when instead of JSON for simple we have just string with type name
                    if (type_json_str.starts_with("\"") && type_json_str.ends_with("\"") && !type_json_str.contains('{'))
                    {
                        type_json_str.pop_back();
                        String type_name = type_json_str.substr(1);
                        auto data_type_from_str = DB::DeltaLakeMetadata::getSimpleTypeByName(type_name);
                        data_type = is_nullable ? makeNullable(data_type_from_str) : data_type_from_str;
                    }
                    else
                    {
                        Poco::JSON::Parser parser;
                        auto parsed_json_type = parser.parse(type_json_str);
                        data_type = DB::DeltaLakeMetadata::getFieldType(parsed_json_type.extract<Poco::JSON::Object::Ptr>(), "type", is_nullable);
                    }
                    schema.push_back({name, data_type});
                }

                result.setSchema(schema);
            }
            else
            {
                LOG_DEBUG(log, "Doesn't require schema");
            }

            if (result.isDefaultReadableTable() && result.requiresCredentials())
                getCredentials(object->get("table_id"), result);

            return true;
        }
        return false;
    }
    catch (DB::Exception & e)
    {
        e.addMessage("while parsing JSON: " + json_str);
        throw;
    }
}

bool UnityCatalog::existsTable(const std::string & schema_name, const std::string & table_name) const
{
    String json_str;
    Poco::Dynamic::Var json;
    try
    {
        std::tie(json, json_str) = getJSONRequest(std::filesystem::path{TABLES_ENDPOINT} / (warehouse + "." + schema_name + "." + table_name));
        const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();
        if (hasValueAndItsNotNone("name", object) && object->get("name").extract<String>() == table_name)
            return true;
        return false;
    }
    catch (DB::Exception & e)
    {
        e.addMessage("while parsing JSON: " + json_str);
        throw;
    }
}

DB::Names UnityCatalog::getTablesForSchema(const std::string & schema, size_t limit) const
{
    Poco::URI::QueryParameters params;
    params.push_back({"catalog_name", warehouse});
    params.push_back({"schema_name", schema});
    params.push_back({"max_results", DB::toString(limit)});

    DB::Names tables;
    do
    {
        String json_str;
        Poco::Dynamic::Var json;

        try
        {
            std::tie(json, json_str) = getJSONRequest(TABLES_ENDPOINT, params);
            const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

            if (!hasValueAndItsNotNone("tables", object))
                return tables;

            auto tables_object = object->get("tables").extract<Poco::JSON::Array::Ptr>();
            if (!tables_object)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse result");

            for (size_t i = 0; i < tables_object->size(); ++i)
            {
                const auto current_table_json = tables_object->get(static_cast<int>(i)).extract<Poco::JSON::Object::Ptr>();
                const auto table_name = current_table_json->get("name").extract<String>();

                tables.push_back(schema + "." + table_name);
                if (limit && tables.size() >= limit)
                    break;
            }

            if (limit && tables.size() >= limit)
                break;

            if (hasValueAndItsNotNone("next_page_token", object))
            {
                auto continuation_token = object->get("next_page_token").extract<String>();

                if (continuation_token.empty())
                    break;

                if (params.size() == 4)
                    params.pop_back();

                params.push_back({"page_token", continuation_token});
            }
            else
            {
                break;
            }
        }
        catch (DB::Exception & e)
        {
            e.addMessage("while parsing JSON: " + json_str);
            throw;
        }
    }
    while (true);

    return tables;
}

DataLake::ICatalog::Namespaces UnityCatalog::getSchemas(const std::string & base_prefix, size_t limit) const
{
    Poco::URI::QueryParameters params;
    params.push_back({"catalog_name", warehouse});

    DataLake::ICatalog::Namespaces schemas;
    do
    {
        String json_str;
        Poco::Dynamic::Var json;

        try
        {
            std::tie(json, json_str) = getJSONRequest(SCHEMAS_ENDPOINT, params);
            const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

            auto schemas_object = object->get("schemas").extract<Poco::JSON::Array::Ptr>();
            if (!schemas_object)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse result");

            for (size_t i = 0; i < schemas_object->size(); ++i)
            {
                auto schema_info = schemas_object->get(static_cast<int>(i)).extract<Poco::JSON::Object::Ptr>();
                chassert(schema_info->get("catalog_name").extract<String>() == warehouse);
                UnityCatalogFullSchemaName schema_name = parseFullSchemaName(schema_info->get("full_name").extract<String>());

                if (schema_name.schema_name.starts_with(base_prefix))
                    schemas.push_back(schema_name.schema_name);

                if (limit && schemas.size() > limit)
                    break;
            }

            if (limit && schemas.size() > limit)
                break;

            if (hasValueAndItsNotNone("next_page_token", object))
            {
                auto continuation_token = object->get("next_page_token").extract<String>();

                if (continuation_token.empty())
                    break;

                if (params.size() == 2)
                    params.pop_back();

                params.push_back({"page_token", continuation_token});
            }
            else
            {
                break;
            }
        }
        catch (DB::Exception & e)
        {
            e.addMessage("while parsing JSON: " + json_str);
            throw;
        }

    } while (true);

    return schemas;
}

UnityCatalog::UnityCatalog(
    const std::string & catalog_,
    const std::string & base_url_,
    const std::string & catalog_credential_,
    DB::ContextPtr context_)
    : ICatalog(catalog_)
    , DB::WithContext(context_)
    , base_url(base_url_)
    , log(getLogger("UnityCatalog(" + catalog_ + ")"))
    , auth_header("Authorization", "Bearer " + catalog_credential_)
{
}

}

#endif
