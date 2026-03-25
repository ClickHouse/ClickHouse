#include <Databases/DataLake/UnifiedUnityCatalog.h>

#if USE_AVRO && USE_PARQUET

#include <Databases/DataLake/RestCatalog.h>
#include <DataTypes/DataTypeNullable.h>
#include <Poco/URI.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>
#include <Common/checkStackSize.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <IO/HTTPCommon.h>
#include <IO/ConnectionTimeouts.h>
#include <Core/NamesAndTypes.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Interpreters/Context.h>
#include <fmt/ranges.h>

namespace DB::ErrorCodes
{
    extern const int DATALAKE_DATABASE_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

bool hasValueAndItsNotNone(const std::string & value, const Poco::JSON::Object::Ptr & object)
{
    return object->has(value) && !object->isNull(value) && !object->get(value).isEmpty();
}

}

namespace DataLake
{

static const auto SCHEMAS_ENDPOINT = "schemas";
static const auto TABLES_ENDPOINT = "tables";
static const auto TEMPORARY_CREDENTIALS_ENDPOINT = "temporary-table-credentials";

static const std::unordered_set<std::string> READABLE_DELTA_TABLES = {"TABLE_DELTA", "TABLE_DELTA_EXTERNAL"};
static const std::unordered_set<std::string> READABLE_ICEBERG_TABLES = {"TABLE_ICEBERG", "TABLE_ICEBERG_EXTERNAL"};
static const std::unordered_set<std::string> UNIFORM_TABLES = {"TABLE_DELTA_ICEBERG_MANAGED", "TABLE_DELTA_ICEBERG_EXTERNAL"};

struct UnifiedUnityCatalogFullSchemaName
{
    std::string catalog_name;
    std::string schema_name;
};

static UnifiedUnityCatalogFullSchemaName parseFullSchemaName(const std::string & full_name)
{
    auto first_dot = full_name.find('.');
    auto catalog_name = full_name.substr(0, first_dot);
    auto schema = full_name.substr(first_dot + 1);
    return UnifiedUnityCatalogFullSchemaName{.catalog_name = catalog_name, .schema_name = schema};
}

UnifiedUnityCatalog::UnifiedUnityCatalog(
    const std::string & catalog_,
    const std::string & base_url_,
    const std::string & catalog_credential_,
    const std::string & auth_scope_,
    const std::string & oauth_server_uri_,
    bool oauth_server_use_request_body_,
    DB::ContextPtr context_)
    : ICatalog(catalog_)
    , DB::WithContext(context_)
    , base_url_str(base_url_)
    , base_url(base_url_)
    , log(getLogger("UnifiedUnityCatalog(" + catalog_ + ")"))
    , auth_scope(auth_scope_)
    , oauth_server_uri(oauth_server_uri_)
    , oauth_server_use_request_body(oauth_server_use_request_body_)
    , catalog_credential(catalog_credential_)
{
    auto colon_pos = catalog_credential_.find(':');
    if (colon_pos != std::string::npos)
    {
        use_oauth = true;
        client_id = catalog_credential_.substr(0, colon_pos);
        client_secret = catalog_credential_.substr(colon_pos + 1);

        if (auth_scope == "PRINCIPAL_ROLE:ALL")
            auth_scope = "all-apis";
    }
    else
    {
        use_oauth = false;
        bearer_token = catalog_credential_;
    }
}

UnifiedUnityCatalog::~UnifiedUnityCatalog() = default;

std::string UnifiedUnityCatalog::retrieveAccessToken() const
{
    DB::HTTPHeaderEntries headers;
    headers.emplace_back("Content-Type", "application/x-www-form-urlencoded");
    headers.emplace_back("Accepts", "application/json; charset=UTF-8");

    Poco::URI url;
    DB::ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback;

    std::string effective_oauth_uri = oauth_server_uri;
    if (effective_oauth_uri.empty())
    {
        Poco::URI base(base_url_str);
        effective_oauth_uri = base.getScheme() + "://" + base.getHost() + "/oidc/v1/token";
    }

    out_stream_callback = [&](std::ostream & os)
    {
        os << fmt::format(
            "grant_type=client_credentials&scope={}&client_id={}&client_secret={}",
            auth_scope, client_id, client_secret);
    };

    url = Poco::URI(effective_oauth_uri);

    const auto & context = getContext();
    auto wb = DB::BuilderRWBufferFromHTTP(url)
        .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
        .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
        .withSettings(context->getReadSettings())
        .withTimeouts(DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
        .withHostFilter(&context->getRemoteHostFilter())
        .withOutCallback(std::move(out_stream_callback))
        .withSkipNotFound(false)
        .withHeaders(headers)
        .create(credentials);

    std::string json_str;
    readJSONObjectPossiblyInvalid(json_str, *wb);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var res_json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & object = res_json.extract<Poco::JSON::Object::Ptr>();

    auto token = object->get("access_token").extract<String>();

    if (object->has("expires_in"))
    {
        Int64 expires_in = object->getValue<Int64>("expires_in");
        token_expires_at = std::chrono::system_clock::now() + std::chrono::seconds(expires_in * 9 / 10);
    }

    return token;
}

void UnifiedUnityCatalog::ensureBearerToken() const
{
    if (bearer_token.has_value())
    {
        if (!use_oauth)
            return;

        if (token_expires_at != std::chrono::system_clock::time_point{}
            && std::chrono::system_clock::now() < token_expires_at)
            return;

        LOG_DEBUG(log, "Bearer token expired, refreshing via OAuth");
        bearer_token.reset();
        iceberg_rest_catalog.reset();
    }

    if (!use_oauth)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "No bearer token and not using OAuth");

    bearer_token = retrieveAccessToken();
}

DB::HTTPHeaderEntries UnifiedUnityCatalog::getAuthHeaders() const
{
    ensureBearerToken();
    return {DB::HTTPHeaderEntry("Authorization", "Bearer " + bearer_token.value())};
}

std::pair<Poco::Dynamic::Var, std::string> UnifiedUnityCatalog::getJSONRequest(
    const std::string & route,
    const Poco::URI::QueryParameters & params) const
{
    const auto & context = getContext();
    auto headers = getAuthHeaders();
    return makeHTTPRequestAndReadJSON(base_url / route, context, credentials, params, headers);
}

std::pair<Poco::Dynamic::Var, std::string> UnifiedUnityCatalog::postJSONRequest(
    const std::string & route,
    std::function<void(std::ostream &)> out_stream_callback) const
{
    const auto & context = getContext();
    auto headers = getAuthHeaders();
    return makeHTTPRequestAndReadJSON(
        base_url / route, context, credentials, {}, headers,
        Poco::Net::HTTPRequest::HTTP_POST, std::move(out_stream_callback));
}

DataLakeTableFormat UnifiedUnityCatalog::detectTableFormat(const Poco::JSON::Object::Ptr & table_json) const
{
    bool has_securable_kind = hasValueAndItsNotNone("securable_kind", table_json);
    bool has_data_source_format = hasValueAndItsNotNone("data_source_format", table_json);

    if (has_securable_kind)
    {
        auto kind = table_json->get("securable_kind").extract<String>();

        if (UNIFORM_TABLES.contains(kind))
            return DataLakeTableFormat::ICEBERG;

        if (READABLE_DELTA_TABLES.contains(kind))
            return DataLakeTableFormat::DELTA;
        if (READABLE_ICEBERG_TABLES.contains(kind))
            return DataLakeTableFormat::ICEBERG;
    }

    if (has_data_source_format)
    {
        auto format = table_json->get("data_source_format").extract<String>();
        if (format == "DELTA")
            return DataLakeTableFormat::DELTA;
        if (format == "ICEBERG")
            return DataLakeTableFormat::ICEBERG;
    }

    if (has_securable_kind)
    {
        LOG_DEBUG(log, "Unrecognized securable_kind: '{}', data_source_format: '{}'",
            table_json->get("securable_kind").extract<String>(),
            has_data_source_format ? table_json->get("data_source_format").extract<String>() : "N/A");
    }

    return DataLakeTableFormat::UNKNOWN;
}

bool UnifiedUnityCatalog::empty() const
{
    auto all_schemas = getSchemas("");
    for (const auto & schema : all_schemas)
    {
        if (!getTablesForSchema(schema, 1).empty())
            return false;
    }
    return true;
}

DB::Names UnifiedUnityCatalog::getTables() const
{
    static constexpr auto CACHE_TTL = std::chrono::seconds(30);

    {
        std::lock_guard lock(table_cache_mutex);
        if (!cached_table_names.empty()
            && table_names_cached_at != std::chrono::system_clock::time_point{}
            && std::chrono::system_clock::now() < table_names_cached_at + CACHE_TTL)
        {
            return cached_table_names;
        }

        table_json_cache.clear();
    }

    DB::Names result;
    auto all_schemas = getSchemas("");
    for (const auto & schema : all_schemas)
    {
        auto schema_tables = getTablesForSchema(schema);
        result.insert(result.end(), schema_tables.begin(), schema_tables.end());
    }

    {
        std::lock_guard lock(table_cache_mutex);
        cached_table_names = result;
        table_names_cached_at = std::chrono::system_clock::now();
    }

    return result;
}

bool UnifiedUnityCatalog::existsTable(const std::string & schema_name, const std::string & table_name) const
{
    String json_str;
    Poco::Dynamic::Var json;
    try
    {
        std::tie(json, json_str) = getJSONRequest(
            std::filesystem::path{TABLES_ENDPOINT} / (warehouse + "." + schema_name + "." + table_name));
        const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();
        return hasValueAndItsNotNone("name", object)
            && object->get("name").extract<String>() == table_name;
    }
    catch (DB::Exception & e)
    {
        e.addMessage("while parsing JSON: " + json_str);
        throw;
    }
}

void UnifiedUnityCatalog::getTableMetadata(
    const std::string & namespace_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    if (!tryGetTableMetadata(namespace_name, table_name, result))
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "No response from Unity catalog");
}

bool UnifiedUnityCatalog::tryGetTableMetadata(
    const std::string & schema_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    auto full_table_name = warehouse + "." + schema_name + "." + table_name;
    auto cache_key = schema_name + "." + table_name;

    Poco::JSON::Object::Ptr object;
    std::string json_str;

    {
        std::lock_guard lock(table_cache_mutex);
        auto it = table_json_cache.find(cache_key);
        if (it != table_json_cache.end())
            object = it->second;
    }

    try
    {
        if (!object)
        {
            Poco::Dynamic::Var json;
            std::tie(json, json_str) = getJSONRequest(std::filesystem::path{TABLES_ENDPOINT} / full_table_name);
            object = json.extract<Poco::JSON::Object::Ptr>();
        }

        if (!hasValueAndItsNotNone("name", object) || object->get("name").extract<String>() != table_name)
            return false;

        auto table_format = detectTableFormat(object);
        result.setTableFormat(table_format);

        if (table_format == DataLakeTableFormat::ICEBERG)
        {
            auto iceberg_catalog = getIcebergRestCatalog();
            return iceberg_catalog->tryGetTableMetadata(schema_name, table_name, result);
        }

        return tryGetDeltaTableMetadata(full_table_name, object, result);
    }
    catch (DB::Exception & e)
    {
        e.addMessage("while parsing JSON: " + json_str);
        throw;
    }
}

bool UnifiedUnityCatalog::tryGetDeltaTableMetadata(
    const std::string & full_table_name,
    const Poco::JSON::Object::Ptr & object,
    TableMetadata & result) const
{
    if (result.requiresLocation())
    {
        if (hasValueAndItsNotNone("storage_location", object))
        {
            result.setLocation(object->get("storage_location").extract<String>());
        }
        else
        {
            result.setTableIsNotReadable(fmt::format(
                "Cannot read table `{}` because it doesn't have storage location. "
                "It means that it's not a DeltaLake table, and it's unreadable with Unity catalog in ClickHouse",
                full_table_name));
        }
    }

    bool has_securable_kind = hasValueAndItsNotNone("securable_kind", object);
    bool has_data_source_format = hasValueAndItsNotNone("data_source_format", object);

    if (has_securable_kind && !READABLE_DELTA_TABLES.contains(object->get("securable_kind").extract<String>()))
    {
        result.setTableIsNotReadable(fmt::format(
            "Cannot read table `{}` because it has unsupported securable_kind: '{}'. "
            "Readable Delta tables are: [{}]",
            full_table_name, object->get("securable_kind").extract<String>(),
            fmt::join(READABLE_DELTA_TABLES, ", ")));
    }

    if (has_data_source_format && object->get("data_source_format").extract<String>() != "DELTA")
    {
        result.setTableIsNotReadable(fmt::format(
            "Cannot read table `{}` as Delta because it has data_source_format '{}'",
            full_table_name, object->get("data_source_format").extract<String>()));
    }

    if (!has_data_source_format && !has_securable_kind)
    {
        result.setTableIsNotReadable(fmt::format(
            "Cannot read table `{}` because it has no information about data_source_format or securable_kind",
            full_table_name));
    }

    LOG_DEBUG(log, "Processing Delta table {} is default readable {}", full_table_name, result.isDefaultReadableTable());

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
                data_type = DB::DeltaLakeMetadata::getFieldType(
                    parsed_json_type.extract<Poco::JSON::Object::Ptr>(), "type", is_nullable);
            }
            schema.push_back({name, data_type});
        }
        result.setSchema(schema);
    }

    if (result.isDefaultReadableTable() && result.requiresCredentials())
        getDeltaCredentials(object->get("table_id"), result);

    return true;
}

void UnifiedUnityCatalog::getDeltaCredentials(const std::string & table_id, TableMetadata & metadata) const
{
    LOG_DEBUG(log, "Getting credentials for table {}", table_id);
    auto storage_type = parseStorageTypeFromLocation(metadata.getLocation());
    switch (storage_type)
    {
        case StorageType::S3:
        {
            auto callback = [table_id](std::ostream & os)
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

std::shared_ptr<RestCatalog> UnifiedUnityCatalog::getIcebergRestCatalog() const
{
    if (iceberg_rest_catalog)
        return iceberg_rest_catalog;

    std::string iceberg_rest_url = std::filesystem::path(base_url_str) / "iceberg-rest";

    ensureBearerToken();
    std::string rest_auth_header = "Authorization: Bearer " + bearer_token.value();

    iceberg_rest_catalog = std::make_shared<RestCatalog>(
        warehouse,
        iceberg_rest_url,
        /* catalog_credential= */ "",
        auth_scope,
        rest_auth_header,
        oauth_server_uri,
        oauth_server_use_request_body,
        DB::Context::getGlobalContextInstance());

    return iceberg_rest_catalog;
}

DB::Names UnifiedUnityCatalog::getTablesForSchema(const std::string & schema, size_t limit) const
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
                auto qualified_name = schema + "." + table_name;
                tables.push_back(qualified_name);

                {
                    std::lock_guard lock(table_cache_mutex);
                    table_json_cache[qualified_name] = current_table_json;
                }

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
    } while (true);

    return tables;
}

ICatalog::Namespaces UnifiedUnityCatalog::getSchemas(const std::string & base_prefix, size_t limit) const
{
    Poco::URI::QueryParameters params;
    params.push_back({"catalog_name", warehouse});

    ICatalog::Namespaces schemas;
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
                auto schema_name = parseFullSchemaName(schema_info->get("full_name").extract<String>());

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

}

#endif
