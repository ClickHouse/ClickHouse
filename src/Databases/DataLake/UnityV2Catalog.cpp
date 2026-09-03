#include <Databases/DataLake/UnityV2Catalog.h>

#if USE_AVRO && USE_PARQUET

#include <Databases/DataLake/RestCatalog.h>
#include <DataTypes/DataTypeNullable.h>
#include <Poco/URI.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>
#include <Common/Exception.h>
#include <Common/checkStackSize.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
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

/// UniForm ("DELTA_UNIFORM_ICEBERG") is a Delta table that also publishes Iceberg metadata;
/// read it as Delta because the Delta log is the source of truth.
static const std::unordered_set<std::string> READABLE_DELTA_FORMATS = {"DELTA", "DELTA_UNIFORM_ICEBERG"};

/// Other table types (views, materialized views, streaming tables, foreign tables, shallow clones)
/// do not support direct reads of their storage.
static const std::unordered_set<std::string> READABLE_TABLE_TYPES = {"MANAGED", "EXTERNAL"};

/// An absent `table_type` does not reject the table; the format checks handle that case.
static bool hasReadableTableType(const Poco::JSON::Object::Ptr & table_json)
{
    return !hasValueAndItsNotNone("table_type", table_json)
        || READABLE_TABLE_TYPES.contains(table_json->get("table_type").extract<String>());
}

/// Databricks reports managed Iceberg tables with `data_source_format` == `DELTA`,
/// so only `securable_kind` tells them apart from plain Delta tables.
static const std::unordered_set<std::string> ICEBERG_SECURABLE_KINDS = {
    "TABLE_DELTA_ICEBERG_MANAGED",
    "TABLE_DELTA_ICEBERG_EXTERNAL",
};

static bool hasIcebergSecurableKind(const Poco::JSON::Object::Ptr & table_json)
{
    return hasValueAndItsNotNone("securable_kind", table_json)
        && ICEBERG_SECURABLE_KINDS.contains(table_json->get("securable_kind").extract<String>());
}

/// Backwards compatibility with the pre-unified Unity catalog, which accepted a table by
/// `securable_kind` alone. Delta tables are documented to always carry `data_source_format`,
/// so this likely never fires, but it keeps the shipped acceptance rule for records without the format.
static bool hasCompatDeltaSecurableKind(const Poco::JSON::Object::Ptr & table_json)
{
    static const std::unordered_set<std::string> compat_delta_kinds = {"TABLE_DELTA", "TABLE_DELTA_EXTERNAL"};
    return hasValueAndItsNotNone("securable_kind", table_json)
        && compat_delta_kinds.contains(table_json->get("securable_kind").extract<String>());
}

struct UnityV2CatalogFullSchemaName
{
    std::string catalog_name;
    std::string schema_name;
};

static UnityV2CatalogFullSchemaName parseFullSchemaName(const std::string & full_name)
{
    auto first_dot = full_name.find('.');
    auto catalog_name = full_name.substr(0, first_dot);
    auto schema = full_name.substr(first_dot + 1);
    return UnityV2CatalogFullSchemaName{.catalog_name = catalog_name, .schema_name = schema};
}

UnityV2Catalog::UnityV2Catalog(
    const std::string & catalog_,
    const std::string & base_url_,
    const std::string & catalog_credential_,
    const std::string & auth_scope_,
    const std::string & oauth_server_uri_,
    DB::ContextPtr context_)
    : ICatalog(catalog_)
    , DB::WithContext(context_)
    , base_url_str(base_url_)
    , base_url(base_url_)
    , log(getLogger("UnityV2Catalog(" + catalog_ + ")"))
    , auth_scope(auth_scope_)
    , oauth_server_uri(oauth_server_uri_)
{
    auto colon_pos = catalog_credential_.find(':');
    if (colon_pos != std::string::npos)
    {
        use_oauth = true;
        client_id = catalog_credential_.substr(0, colon_pos);
        client_secret = catalog_credential_.substr(colon_pos + 1);
    }
    else
    {
        use_oauth = false;
        access_token = AccessToken{.token = catalog_credential_, .expires_at = std::nullopt};
    }
}

UnityV2Catalog::~UnityV2Catalog() = default;

AccessToken UnityV2Catalog::retrieveAccessToken() const
{
    DB::HTTPHeaderEntries headers;
    headers.emplace_back("Content-Type", "application/x-www-form-urlencoded");
    headers.emplace_back("Accept", "application/json");

    std::string effective_oauth_uri = oauth_server_uri;
    if (effective_oauth_uri.empty())
    {
        Poco::URI base(base_url_str);
        base.setPathEtc("/oidc/v1/token");
        effective_oauth_uri = base.toString();
    }

    /// The parameters always go into the request body, as RFC 6749 requires;
    /// `oauth_server_use_request_body = 0` is rejected on CREATE DATABASE.
    String encoded_auth_scope;
    String encoded_client_id;
    String encoded_client_secret;
    Poco::URI::encode(auth_scope, auth_scope, encoded_auth_scope);
    Poco::URI::encode(client_id, client_id, encoded_client_id);
    Poco::URI::encode(client_secret, client_secret, encoded_client_secret);

    String body = fmt::format(
        "grant_type=client_credentials&scope={}&client_id={}&client_secret={}",
        encoded_auth_scope, encoded_client_id, encoded_client_secret);
    DB::ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [body_ = std::move(body)](std::ostream & os)
    {
        os << body_;
    };

    auto [res_json, json_str] = makeHTTPRequestAndReadJSON(
        effective_oauth_uri, getContext(), /* bearer_token = */ "", {}, headers,
        Poco::Net::HTTPRequest::HTTP_POST, std::move(out_stream_callback));

    if (res_json.isEmpty())
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "Empty response from OAuth server {}", effective_oauth_uri);

    const Poco::JSON::Object::Ptr & object = res_json.extract<Poco::JSON::Object::Ptr>();

    AccessToken result;
    result.token = object->get("access_token").extract<String>();

    /// Expire the cached token at 90% of its lifetime, to renew it before the server rejects it.
    if (object->has("expires_in"))
    {
        Int64 expires_in = object->getValue<Int64>("expires_in");
        result.expires_at = std::chrono::system_clock::now() + std::chrono::seconds(expires_in * 9 / 10);
    }

    return result;
}

void UnityV2Catalog::ensureBearerToken(bool force_refresh) const
{
    if (access_token.has_value())
    {
        if (!use_oauth || (!force_refresh && !access_token->isExpired()))
            return;

        LOG_DEBUG(log, "Refreshing bearer token via OAuth ({})", force_refresh ? "rejected by the catalog" : "expired");
        access_token.reset();
        /// `iceberg_rest_catalog` keeps the bearer token inside its auth header, so it must be recreated with the new token.
        iceberg_rest_catalog.reset();
    }

    if (!use_oauth)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "No bearer token and not using OAuth");

    access_token = retrieveAccessToken();
}

std::string UnityV2Catalog::getBearerToken(bool force_refresh) const
{
    std::lock_guard lock(token_mutex);
    ensureBearerToken(force_refresh);
    return access_token->token;
}

template <typename Func>
auto UnityV2Catalog::requestWithRetry(Func && make_request) const
{
    return requestWithTokenRefresh(/* enable_refresh = */ use_oauth, std::forward<Func>(make_request));
}

std::pair<Poco::Dynamic::Var, std::string> UnityV2Catalog::getJSONRequest(
    const std::string & route,
    const Poco::URI::QueryParameters & params) const
{
    return requestWithRetry([&](bool force_refresh)
    {
        return makeHTTPRequestAndReadJSON(
            base_url / route, getContext(), getBearerToken(force_refresh), params);
    });
}

std::pair<Poco::Dynamic::Var, std::string> UnityV2Catalog::postJSONRequest(
    const std::string & route,
    std::function<void(std::ostream &)> out_stream_callback) const
{
    /// `out_stream_callback` is copied, not moved: the retry has to send the same body again.
    return requestWithRetry([&](bool force_refresh)
    {
        return makeHTTPRequestAndReadJSON(
            base_url / route, getContext(), getBearerToken(force_refresh), {}, {},
            Poco::Net::HTTPRequest::HTTP_POST, out_stream_callback);
    });
}

DataLakeTableFormat UnityV2Catalog::detectTableFormat(const Poco::JSON::Object::Ptr & table_json) const
{
    /// Checked before the format, because managed Iceberg tables report `data_source_format` == `DELTA`.
    if (hasIcebergSecurableKind(table_json))
        return DataLakeTableFormat::ICEBERG;

    if (!hasValueAndItsNotNone("data_source_format", table_json))
    {
        if (hasCompatDeltaSecurableKind(table_json))
            return DataLakeTableFormat::DELTA;

        LOG_DEBUG(log, "Table JSON has no data_source_format");
        return DataLakeTableFormat::UNKNOWN;
    }

    auto format = table_json->get("data_source_format").extract<String>();
    if (READABLE_DELTA_FORMATS.contains(format))
        return DataLakeTableFormat::DELTA;
    if (format == "ICEBERG")
        return DataLakeTableFormat::ICEBERG;

    LOG_DEBUG(log, "Unrecognized data_source_format: '{}'", format);
    return DataLakeTableFormat::UNKNOWN;
}

bool UnityV2Catalog::empty() const
{
    auto all_schemas = getSchemas("");
    for (const auto & schema : all_schemas)
    {
        if (!getTablesForSchema(schema, 1).empty())
            return false;
    }
    return true;
}

CatalogTables UnityV2Catalog::getTables() const
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

DataLake::ICatalog::Namespaces UnityV2Catalog::getNamespaces() const
{
    /// Unity schemas are flat — they cannot contain nested namespaces.
    return getSchemas("");
}

CatalogTables UnityV2Catalog::listTablesInNamespaceDirect(const std::string & namespace_name) const
{
    return getTablesForSchema(namespace_name);
}

bool UnityV2Catalog::existsTable(const std::string & schema_name, const std::string & table_name) const
{
    auto full_table_name = warehouse + "." + schema_name + "." + table_name;
    auto json = getJSONRequest(std::filesystem::path{TABLES_ENDPOINT} / full_table_name).first;

    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();
    return hasValueAndItsNotNone("name", object)
        && object->get("name").extract<String>() == table_name;
}

void UnityV2Catalog::getTableMetadata(
    const std::string & namespace_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    if (!tryGetTableMetadata(namespace_name, table_name, result))
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "No response from Unity catalog");
}

bool UnityV2Catalog::tryGetTableMetadata(
    const std::string & schema_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    auto full_table_name = warehouse + "." + schema_name + "." + table_name;

    auto json = getJSONRequest(std::filesystem::path{TABLES_ENDPOINT} / full_table_name).first;
    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

    if (!hasValueAndItsNotNone("name", object) || object->get("name").extract<String>() != table_name)
        return false;

    auto table_format = detectTableFormat(object);
    result.setTableFormat(table_format);

    if (!hasReadableTableType(object))
    {
        /// Fall through to the Delta arm: it fills the location and schema
        /// best-effort, so the table stays listed with its columns.
        result.setTableIsNotReadable(fmt::format(
            "Cannot read table `{}` because it has table_type '{}'. "
            "Readable table types are: [{}]",
            full_table_name, object->get("table_type").extract<String>(),
            fmt::join(READABLE_TABLE_TYPES, ", ")));
    }
    else if (table_format == DataLakeTableFormat::ICEBERG)
    {
        /// The Unity tables API describes the table but does not serve its Iceberg metadata;
        /// Databricks exposes that only through the Iceberg REST catalog endpoint.
        /// See https://docs.databricks.com/aws/en/external-access/iceberg
        return requestWithRetry([&](bool force_refresh)
        {
            return getIcebergRestCatalog(force_refresh)->tryGetTableMetadata(schema_name, table_name, result);
        });
    }

    return tryGetDeltaTableMetadata(full_table_name, object, result);
}

bool UnityV2Catalog::tryGetDeltaTableMetadata(
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

    if (hasValueAndItsNotNone("data_source_format", object))
    {
        if (!READABLE_DELTA_FORMATS.contains(object->get("data_source_format").extract<String>()))
        {
            result.setTableIsNotReadable(fmt::format(
                "Cannot read table `{}` as Delta because it has data_source_format '{}'",
                full_table_name, object->get("data_source_format").extract<String>()));
        }
    }
    else if (!hasCompatDeltaSecurableKind(object))
    {
        result.setTableIsNotReadable(fmt::format(
            "Cannot read table `{}` because it has no information about data_source_format",
            full_table_name));
    }

    LOG_DEBUG(log, "Processing Delta table {} is default readable {}", full_table_name, result.isDefaultReadableTable());

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
                const auto type_json_str = column_json->get("type_json").extract<String>();
                DB::DataTypePtr data_type;

                try
                {
                    /// OSS Unity catalog reports a simple type as a bare quoted name, such as `"integer"`,
                    /// where Databricks reports the JSON object every type gets there. Both forms occur.
                    if (type_json_str.starts_with("\"") && type_json_str.ends_with("\"") && !type_json_str.contains('{'))
                    {
                        String type_name = type_json_str.substr(1, type_json_str.size() - 2);
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
                }
                catch (DB::Exception & e)
                {
                    e.addMessage("while parsing the type of column `{}` of Delta table `{}`: {}",
                        name, full_table_name, type_json_str);
                    throw;
                }
                schema.push_back({name, data_type});
            }
            LOG_TEST(log, "Parsed schema: {}", schema.toString());
        }
        catch (...)
        {
            /// Tables that are not Delta can use types ClickHouse has no mapping for, such as the
            /// Databricks `NULL` type. They are unreadable anyway, so keep them listed without a
            /// schema instead of failing the whole catalog; `SHOW CREATE TABLE` then reports no columns.
            if (result.isDefaultReadableTable())
                throw;

            LOG_DEBUG(
                log,
                "Cannot read table `{}` because of schema parsing exception `{}`, "
                "but it is not a Delta table, so we ignore this error",
                full_table_name, DB::getCurrentExceptionMessage(false));
            return true;
        }

        result.setSchema(schema);
    }

    /// The UUID is what `getCredentialsConfigurationCallback` uses to refresh expired credentials.
    if (hasValueAndItsNotNone("table_id", object))
        result.setTableUUID(object->get("table_id").extract<String>());

    if (result.isDefaultReadableTable() && result.requiresCredentials())
    {
        const auto storage_type = parseStorageTypeFromLocation(result.getLocation());
        if (auto credentials = getDeltaCredentials(object->get("table_id"), storage_type))
            result.setStorageCredentials(credentials);
    }

    return true;
}

std::shared_ptr<IStorageCredentials> UnityV2Catalog::getDeltaCredentials(
    const std::string & table_id, StorageType storage_type) const
{
    LOG_DEBUG(log, "Getting credentials for table {}", table_id);
    if (storage_type != StorageType::S3 && storage_type != StorageType::Azure)
        return nullptr;

    Poco::JSON::Object request_body;
    request_body.set("table_id", table_id);
    request_body.set("operation", "READ");

    auto callback = [&request_body](std::ostream & os) { request_body.stringify(os); };

    auto [json, _] = postJSONRequest(TEMPORARY_CREDENTIALS_ENDPOINT, callback);
    const Poco::JSON::Object::Ptr & response = json.extract<Poco::JSON::Object::Ptr>();

    switch (storage_type)
    {
        case StorageType::S3:
            return parseS3Credentials(response);
        case StorageType::Azure:
            return parseAzureCredentials(response);
        default:
            return nullptr;
    }
}

std::shared_ptr<IStorageCredentials> UnityV2Catalog::parseS3Credentials(const Poco::JSON::Object::Ptr & response) const
{
    if (!hasValueAndItsNotNone("aws_temp_credentials", response))
        return nullptr;

    const Poco::JSON::Object::Ptr & creds_object = response->getObject("aws_temp_credentials");
    return std::make_shared<S3Credentials>(
        creds_object->get("access_key_id").extract<String>(),
        creds_object->get("secret_access_key").extract<String>(),
        creds_object->get("session_token").extract<String>());
}

std::shared_ptr<IStorageCredentials> UnityV2Catalog::parseAzureCredentials(const Poco::JSON::Object::Ptr & response) const
{
    if (!hasValueAndItsNotNone("azure_user_delegation_sas", response))
        return nullptr;

    const Poco::JSON::Object::Ptr & creds_object = response->getObject("azure_user_delegation_sas");
    return std::make_shared<AzureCredentials>(creds_object->get("sas_token").extract<String>());
}

/// Only S3 refreshes credentials: `StorageAzureConfiguration::createObjectStorage` discards the callback.
ICatalog::CredentialsRefreshCallback UnityV2Catalog::getCredentialsConfigurationCallback(
    const DB::StorageID & table_id, const TableMetadata & table_metadata)
{
    /// Iceberg credentials are vended by the Iceberg REST catalog.
    if (table_metadata.getTableFormat() == DataLakeTableFormat::ICEBERG)
    {
        return [this, table_id, table_metadata]() -> std::shared_ptr<IStorageCredentials>
        {
            /// Resolved per call, because refreshing the token replaces `iceberg_rest_catalog`.
            return requestWithRetry([&](bool force_refresh) -> std::shared_ptr<IStorageCredentials>
            {
                auto rest_catalog = getIcebergRestCatalog(force_refresh);
                auto refresh = rest_catalog->getCredentialsConfigurationCallback(table_id, table_metadata);
                return refresh ? (*refresh)() : nullptr;
            });
        };
    }

    /// Delta tables refresh using table_uuid and the TEMPORARY_TABLE_CREDENTIALS endpoint.
    const auto table_uuid = table_metadata.getTableUUID();
    if (!table_uuid)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Cannot build a Unity credentials refresh callback for `{}`: the catalog returned no table_id",
            table_id.getNameForLogs());

    return [this, unity_table_id = *table_uuid]() -> std::shared_ptr<IStorageCredentials>
    {
        LOG_DEBUG(log, "Update credentials in the catalog");

        return getDeltaCredentials(unity_table_id, StorageType::S3);
    };
}

std::shared_ptr<RestCatalog> UnityV2Catalog::getIcebergRestCatalog(bool force_refresh) const
{
    std::lock_guard lock(token_mutex);
    if (iceberg_rest_catalog && !force_refresh)
        return iceberg_rest_catalog;

    std::string iceberg_rest_url = std::filesystem::path(base_url_str) / "iceberg-rest";

    /// On `force_refresh` this resets `iceberg_rest_catalog`, so the catalog below embeds the new token.
    ensureBearerToken(force_refresh);
    std::string rest_auth_header = "Authorization: Bearer " + access_token->token;

    /// The RestCatalog authenticates via the ready-made auth header, which puts it in header mode.
    /// It never mints a token of its own, so every other auth parameter is left empty.
    iceberg_rest_catalog = std::make_shared<RestCatalog>(
        warehouse,
        iceberg_rest_url,
        /* catalog_credential= */ "",
        /* auth_scope= */ "",
        rest_auth_header,
        /* oauth_server_uri= */ "",
        /* oauth_server_use_request_body= */ false,
        getContext());

    return iceberg_rest_catalog;
}

CatalogTables UnityV2Catalog::getTablesForSchema(const std::string & schema, size_t limit) const
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

            /// A page may be empty (the "tables" field is omitted) while more pages exist,
            /// so fall through to the next_page_token check.
            if (hasValueAndItsNotNone("tables", object))
            {
                auto tables_object = object->get("tables").extract<Poco::JSON::Array::Ptr>();
                if (!tables_object)
                    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse result");

                for (size_t i = 0; i < tables_object->size(); ++i)
                {
                    const auto current_table_json = tables_object->get(static_cast<int>(i)).extract<Poco::JSON::Object::Ptr>();
                    const auto table_name = current_table_json->get("name").extract<String>();
                    auto qualified_name = schema + "." + table_name;

                    const auto table_format = detectTableFormat(current_table_json);
                    /// Delta needs `storage_location` from this response, while an Iceberg table
                    /// gets its location from the Iceberg REST catalog instead.
                    const bool has_location = table_format == DataLakeTableFormat::ICEBERG
                        || hasValueAndItsNotNone("storage_location", current_table_json);

                    tables.push_back(CatalogTable{
                        .name = qualified_name,
                        .is_readable = table_format != DataLakeTableFormat::UNKNOWN
                            && has_location
                            && hasReadableTableType(current_table_json),
                    });

                    if (limit && tables.size() >= limit)
                        break;
                }
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

ICatalog::Namespaces UnityV2Catalog::getSchemas(const std::string & base_prefix, size_t limit) const
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

            /// A page may be empty (the "schemas" field is omitted) while more pages exist,
            /// so fall through to the next_page_token check.
            if (hasValueAndItsNotNone("schemas", object))
            {
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

                    if (limit && schemas.size() >= limit)
                        break;
                }
            }

            if (limit && schemas.size() >= limit)
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
