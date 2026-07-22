#include <Databases/DataLake/UnityCatalog.h>
#include <Interpreters/StorageID.h>

#if USE_PARQUET

#include <sstream>
#include <DataTypes/DataTypeNullable.h>
#include <Poco/URI.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Common/checkStackSize.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Core/NamesAndTypes.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Databases/DataLake/StorageCredentials.h>
#include <fmt/ranges.h>

namespace DB::ErrorCodes
{
    extern const int DATALAKE_DATABASE_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
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

/// A Unity table is readable only if it is a DeltaLake table. `securable_kind`,
/// `data_source_format` and `storage_location` are in the bulk listing, so this matches `tryGetTableMetadata`.
static bool isReadableUnityTable(const Poco::JSON::Object::Ptr & table)
{
    if (!hasValueAndItsNotNone("storage_location", table))
        return false;

    const bool has_securable_kind = hasValueAndItsNotNone("securable_kind", table);
    const bool has_data_source_format = hasValueAndItsNotNone("data_source_format", table);

    if (has_securable_kind && !READABLE_TABLES.contains(table->get("securable_kind").extract<String>()))
        return false;

    if (has_data_source_format && table->get("data_source_format").extract<String>() != READABLE_DATA_SOURCE_FORMAT)
        return false;

    if (!has_securable_kind && !has_data_source_format)
        return false;

    return true;
}

struct UnityCatalogFullSchemaName
{
    std::string catalog_name;
    std::string schema_name;
};

static UnityCatalogFullSchemaName parseFullSchemaName(const std::string & full_name)
{
    auto first_dot = full_name.find('.');
    auto catalog_name = full_name.substr(0, first_dot);
    auto schema = full_name.substr(first_dot + 1);
    return UnityCatalogFullSchemaName{.catalog_name = catalog_name, .schema_name = schema};
}

/// Delta primitive type name (see `DeltaLakeMetadata::getSimpleTypeByName`) -> Unity `ColumnTypeName`.
static std::string deltaPrimitiveToUnityTypeName(const std::string & delta_type)
{
    if (delta_type == "boolean") return "BOOLEAN";
    if (delta_type == "byte")    return "BYTE";
    if (delta_type == "short")   return "SHORT";
    if (delta_type == "integer") return "INT";
    if (delta_type == "long")    return "LONG";
    if (delta_type == "float")   return "FLOAT";
    if (delta_type == "double")  return "DOUBLE";
    if (delta_type == "date")    return "DATE";
    if (delta_type == "timestamp" || delta_type == "timestamp_ntz") return "TIMESTAMP";
    if (delta_type == "string")  return "STRING";
    if (delta_type == "binary")  return "BINARY";
    if (delta_type.starts_with("decimal(")) return "DECIMAL";
    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Cannot map Delta type `{}` to a Unity column type", delta_type);
}

std::pair<Poco::Dynamic::Var, std::string> UnityCatalog::getJSONRequest(const std::string & route, const Poco::URI::QueryParameters & params) const
{
    const auto & context = getContext();
    return makeHTTPRequestAndReadJSON(base_url / route, context, credentials, params, {auth_header});
}

std::pair<Poco::Dynamic::Var, std::string> UnityCatalog::postJSONRequest(const std::string & route, std::function<void(std::ostream &)> out_stream_callaback) const
{
    const auto & context = getContext();
    /// Unity's server (Armeria) selects the JSON request converter based on `Content-Type`; without it the
    /// `@RequestObject` body is not deserialized and the server responds with HTTP 500.
    DB::HTTPHeaderEntries headers{auth_header, {"Content-Type", "application/json"}};
    return makeHTTPRequestAndReadJSON(base_url / route, context, credentials, {}, headers, Poco::Net::HTTPRequest::HTTP_POST, out_stream_callaback);
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

CatalogTables UnityCatalog::getTables() const
{
    CatalogTables result;

    auto all_schemas = getSchemas("");
    for (const auto & schema : all_schemas)
    {
        auto schema_tables = getTablesForSchema(schema);
        result.insert(result.end(), schema_tables.begin(), schema_tables.end());
    }

    return result;
}

DataLake::ICatalog::Namespaces UnityCatalog::getNamespaces() const
{
    /// Unity schemas are flat — they cannot contain nested namespaces.
    return getSchemas("");
}

CatalogTables UnityCatalog::listTablesInNamespaceDirect(const std::string & namespace_name) const
{
    return getTablesForSchema(namespace_name);
}

void UnityCatalog::getTableMetadata(
    const std::string & namespace_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    if (!tryGetTableMetadata(namespace_name, table_name, result))
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "No response from unity catalog");
}

Poco::JSON::Object::Ptr UnityCatalog::requestReadCredentials(const String & table_id) const
{
    Poco::JSON::Object request_body;
    request_body.set("table_id", table_id);
    request_body.set("operation", "READ");

    auto callback = [&request_body] (std::ostream & os) { request_body.stringify(os); };
    auto [json, _] = postJSONRequest(TEMPORARY_CREDENTIALS_ENDPOINT, callback);
    return json.extract<Poco::JSON::Object::Ptr>();
}

std::shared_ptr<IStorageCredentials> UnityCatalog::parseS3Credentials(const Poco::JSON::Object::Ptr & response) const
{
    if (!hasValueAndItsNotNone("aws_temp_credentials", response))
        return nullptr;

    const Poco::JSON::Object::Ptr & creds_object = response->getObject("aws_temp_credentials");
    return std::make_shared<S3Credentials>(
        creds_object->get("access_key_id").extract<String>(),
        creds_object->get("secret_access_key").extract<String>(),
        creds_object->get("session_token").extract<String>());
}

std::shared_ptr<IStorageCredentials> UnityCatalog::parseAzureCredentials(const Poco::JSON::Object::Ptr & response) const
{
    if (!hasValueAndItsNotNone("azure_user_delegation_sas", response))
        return nullptr;

    const Poco::JSON::Object::Ptr & creds_object = response->getObject("azure_user_delegation_sas");
    return std::make_shared<AzureCredentials>(
        creds_object->get("sas_token").extract<String>());
}

void UnityCatalog::getCredentials(const String & table_id, TableMetadata & metadata) const
{
    LOG_DEBUG(log, "Getting credentials for table {}", table_id);
    auto storage_type = parseStorageTypeFromLocation(metadata.getLocation());
    if (storage_type != StorageType::S3 && storage_type != StorageType::Azure)
        return;

    auto response = requestReadCredentials(table_id);

    std::shared_ptr<IStorageCredentials> creds;
    switch (storage_type)
    {
    case StorageType::S3:
        creds = parseS3Credentials(response);
        break;
    case StorageType::Azure:
        creds = parseAzureCredentials(response);
        break;
    default:
        break;
    }
    if (creds)
        metadata.setStorageCredentials(creds);
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
                try
                {
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
                    LOG_TEST(log, "Parsed schema: {}", schema.toString());
                }
                catch (...)
                {
                    /// Non-delta tables can have very weird datatypes in schemas like https://docs.databricks.com/aws/en/sql/language-manual/data-types/null-type
                    /// We still don't know how to read them so we can ignore absence of schema and return weird output for SHOW CREATE TABLE.
                    if (!result.isDefaultReadableTable())
                    {
                        LOG_DEBUG(
                            log, "Cannot read table `{}` because of schema parsing exception `{}`, but it is not delta table, so we ignore this error",
                            full_table_name, DB::getCurrentExceptionMessage(false));
                        return true;
                    }

                    throw;
                }

                result.setSchema(schema);
            }
            else
            {
                LOG_DEBUG(log, "Doesn't require schema");
            }

            if (hasValueAndItsNotNone("table_id", object))
                result.setTableUUID(object->get("table_id").extract<String>());

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

void UnityCatalog::createTable(
    const String & namespace_name,
    const String & table_name,
    const String & new_metadata_path,
    Poco::JSON::Object::Ptr metadata_content) const
{
    /// Build the Unity `ColumnInfo` array from the Delta schema fields. `type_json` matches what the
    /// read path (`tryGetTableMetadata`) parses back: a quoted type name for primitives, an object for nested.
    auto fields = metadata_content->getArray("fields");
    if (!fields)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Delta schema fields are missing for Unity createTable");

    Poco::JSON::Array::Ptr columns = new Poco::JSON::Array;
    for (size_t i = 0; i < fields->size(); ++i)
    {
        auto field = fields->getObject(static_cast<int>(i));
        const String name = field->getValue<String>("name");
        const bool nullable = field->getValue<bool>("nullable");
        auto type_var = field->get("type");

        Poco::JSON::Object::Ptr column = new Poco::JSON::Object;
        column->set("name", name);
        column->set("nullable", nullable);
        column->set("position", static_cast<int>(i));

        int precision = 0;
        int scale = 0;
        String type_name;
        String type_text;
        String type_json;

        if (type_var.isString())
        {
            const String & delta_type = type_var.extract<String>();
            type_text = delta_type;
            type_json = '"' + delta_type + '"';
            type_name = deltaPrimitiveToUnityTypeName(delta_type);
            if (type_name == "DECIMAL")
            {
                const auto lparen = delta_type.find('(');
                const auto comma = delta_type.find(',', lparen);
                const auto rparen = delta_type.find(')', comma);
                precision = std::stoi(delta_type.substr(lparen + 1, comma - lparen - 1));
                scale = std::stoi(delta_type.substr(comma + 1, rparen - comma - 1));
            }
        }
        else
        {
            const auto & descriptor = type_var.extract<Poco::JSON::Object::Ptr>();
            const String kind = descriptor->getValue<String>("type");
            if (kind == "array")       type_name = "ARRAY";
            else if (kind == "map")    type_name = "MAP";
            else if (kind == "struct") type_name = "STRUCT";
            else
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unexpected complex Delta type `{}`", kind);
            type_text = kind;

            /// Wrap so the read path's `getFieldType(parsed, "type")` sees the descriptor under `type`.
            Poco::JSON::Object::Ptr wrapper = new Poco::JSON::Object;
            wrapper->set("type", descriptor);
            std::ostringstream oss;  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            wrapper->stringify(oss);
            type_json = oss.str();
        }

        column->set("type_name", type_name);
        column->set("type_text", type_text);
        column->set("type_json", type_json);
        column->set("type_precision", precision);
        column->set("type_scale", scale);
        columns->add(column);
    }

    Poco::JSON::Object::Ptr body = new Poco::JSON::Object;
    body->set("name", table_name);
    body->set("catalog_name", warehouse);
    body->set("schema_name", namespace_name);
    body->set("table_type", "EXTERNAL");
    body->set("data_source_format", "DELTA");
    body->set("storage_location", new_metadata_path);
    body->set("columns", columns);
    body->set("properties", Poco::JSON::Object::Ptr(new Poco::JSON::Object));

    LOG_DEBUG(log, "Creating table {}.{}.{} at `{}` in Unity catalog", warehouse, namespace_name, table_name, new_metadata_path);

    try
    {
        auto response = postJSONRequest(
            TABLES_ENDPOINT,
            [&](std::ostream & os) { body->stringify(os); });
        LOG_TEST(log, "Unity createTable response: {}", response.second);
    }
    catch (const DB::Exception & ex)
    {
        throw DB::Exception(
            DB::ErrorCodes::DATALAKE_DATABASE_ERROR,
            "Failed to create table {}.{} in Unity catalog: {}",
            namespace_name, table_name, ex.message());
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
    catch (const DB::HTTPException & e)
    {
        /// Unity returns 404 for a table that does not exist; treat that as "does not exist" instead
        /// of an error (e.g. the existence check `InterpreterCreateQuery` runs before CREATE TABLE).
        if (e.getHTTPStatus() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND)
            return false;
        throw;
    }
    catch (DB::Exception & e)
    {
        e.addMessage("while parsing JSON: " + json_str);
        throw;
    }
}

CatalogTables UnityCatalog::getTablesForSchema(const std::string & schema, size_t limit) const
{
    Poco::URI::QueryParameters params;
    params.push_back({"catalog_name", warehouse});
    params.push_back({"schema_name", schema});
    params.push_back({"max_results", DB::toString(limit)});

    CatalogTables tables;
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

                tables.push_back(CatalogTable{
                    .name = schema + "." + table_name,
                    .is_readable = isReadableUnityTable(current_table_json),
                });
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

/// getCredentialsConfigurationCallback method is supported only for S3 storage
ICatalog::CredentialsRefreshCallback UnityCatalog::getCredentialsConfigurationCallback(const DB::StorageID & table_id)
{
    if (!table_id.hasUUID())
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Cannot build a Unity credentials refresh callback for `{}`: StorageID has no UUID",
            table_id.getNameForLogs());

    const String unity_table_id = toString(table_id.uuid);

    return [this, unity_table_id] () -> std::shared_ptr<IStorageCredentials>    {
        LOG_DEBUG(log, "Update credentials in the catalog");

        return parseS3Credentials(requestReadCredentials(unity_table_id));
    };
}


}

#endif
