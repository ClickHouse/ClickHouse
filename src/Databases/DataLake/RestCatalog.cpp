#include "config.h"

#if USE_AVRO
#include <Databases/DataLake/RestCatalog.h>
#include <Databases/DataLake/StorageCredentials.h>

#include <base/find_symbols.h>
#include <Core/Settings.h>
#include <Common/escapeForFileName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/Base64.h>
#include <Common/checkStackSize.h>

#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Server/HTTP/HTMLForm.h>
#include <Formats/FormatFactory.h>

#include <Poco/URI.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>


namespace DB::ErrorCodes
{
    extern const int DATALAKE_DATABASE_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace DataLake
{

static constexpr auto CONFIG_ENDPOINT = "config";
static constexpr auto NAMESPACES_ENDPOINT = "namespaces";

namespace
{

std::pair<std::string, std::string> parseCatalogCredential(const std::string & catalog_credential)
{
    /// Parse a string of format "<client_id>:<client_secret>"
    /// into separare strings client_id and client_secret.

    std::string client_id;
    std::string client_secret;
    if (!catalog_credential.empty())
    {
        auto pos = catalog_credential.find(':');
        if (pos == std::string::npos)
        {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS, "Unexpected format of catalog credential: "
                "expected client_id and client_secret separated by `:`");
        }
        client_id = catalog_credential.substr(0, pos);
        client_secret = catalog_credential.substr(pos + 1);
    }
    return std::pair(client_id, client_secret);
}

DB::HTTPHeaderEntry parseAuthHeader(const std::string & auth_header)
{
    /// Parse a string of format "Authorization: <auth_scheme> <auth_token>"
    /// into a key-value header "Authorization", "<auth_scheme> <auth_token>"

    auto pos = auth_header.find(':');
    if (pos == std::string::npos)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unexpected format of auth header");

    return DB::HTTPHeaderEntry(auth_header.substr(0, pos), auth_header.substr(pos + 1));
}

std::string correctAPIURI(const std::string & uri)
{
    if (uri.ends_with("v1"))
        return uri;
    return std::filesystem::path(uri) / "v1";
}

}

std::string RestCatalog::Config::toString() const
{
    DB::WriteBufferFromOwnString wb;

    if (!prefix.empty())
        wb << "prefix: " << prefix.string() << ", ";

    if (!default_base_location.empty())
        wb << "default_base_location: " << default_base_location << ", ";

    return wb.str();
}

RestCatalog::RestCatalog(
    const std::string & warehouse_,
    const std::string & base_url_,
    const std::string & catalog_credential_,
    const std::string & auth_scope_,
    const std::string & auth_header_,
    const std::string & oauth_server_uri_,
    DB::ContextPtr context_)
    : ICatalog(warehouse_)
    , DB::WithContext(context_)
    , base_url(correctAPIURI(base_url_))
    , log(getLogger("RestCatalog(" + warehouse_ + ")"))
    , auth_scope(auth_scope_)
    , oauth_server_uri(oauth_server_uri_)
{
    if (!catalog_credential_.empty())
    {
        std::tie(client_id, client_secret) = parseCatalogCredential(catalog_credential_);
        update_token_if_expired = true;
    }
    else if (!auth_header_.empty())
        auth_header = parseAuthHeader(auth_header_);

    config = loadConfig();
}

RestCatalog::Config RestCatalog::loadConfig()
{
    Poco::URI::QueryParameters params = {{"warehouse", warehouse}};
    auto buf = createReadBuffer(CONFIG_ENDPOINT, params);

    std::string json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

    LOG_TEST(log, "Received catalog configuration settings: {}", json_str);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

    Config result;

    auto defaults_object = object->get("defaults").extract<Poco::JSON::Object::Ptr>();
    parseCatalogConfigurationSettings(defaults_object, result);

    auto overrides_object = object->get("overrides").extract<Poco::JSON::Object::Ptr>();
    parseCatalogConfigurationSettings(overrides_object, result);

    LOG_TEST(log, "Parsed catalog configuration settings: {}", result.toString());
    return result;
}

void RestCatalog::parseCatalogConfigurationSettings(const Poco::JSON::Object::Ptr & object, Config & result)
{
    if (!object)
        return;

    if (object->has("prefix"))
        result.prefix = object->get("prefix").extract<String>();

    if (object->has("default-base-location"))
        result.default_base_location = object->get("default-base-location").extract<String>();
}

DB::HTTPHeaderEntries RestCatalog::getAuthHeaders(bool update_token) const
{
    /// Option 1: user specified auth header manually.
    /// Header has format: 'Authorization: <scheme> <token>'.
    if (auth_header.has_value())
    {
        return DB::HTTPHeaderEntries{auth_header.value()};
    }

    /// Option 2: user provided grant_type, client_id and client_secret.
    /// We would make OAuthClientCredentialsRequest
    /// https://github.com/apache/iceberg/blob/3badfe0c1fcf0c0adfc7aa4a10f0b50365c48cf9/open-api/rest-catalog-open-api.yaml#L3498C5-L3498C34
    if (!client_id.empty())
    {
        if (!access_token.has_value() || update_token)
        {
            access_token = retrieveAccessToken();
        }

        DB::HTTPHeaderEntries headers;
        headers.emplace_back("Authorization", "Bearer " + access_token.value());
        return headers;
    }

    return {};
}

std::string RestCatalog::retrieveAccessToken() const
{
    static constexpr auto oauth_tokens_endpoint = "oauth/tokens";

    /// TODO:
    /// 1. support oauth2-server-uri
    /// https://github.com/apache/iceberg/blob/918f81f3c3f498f46afcea17c1ac9cdc6913cb5c/open-api/rest-catalog-open-api.yaml#L183C82-L183C99

    DB::HTTPHeaderEntries headers;
    headers.emplace_back("Content-Type", "application/x-www-form-urlencoded");
    headers.emplace_back("Accepts", "application/json; charset=UTF-8");

    Poco::URI url;
    DB::ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback;
    if (oauth_server_uri.empty())
    {
        url = Poco::URI(base_url / oauth_tokens_endpoint);

        Poco::URI::QueryParameters params = {
            {"grant_type", "client_credentials"},
            {"scope", auth_scope},
            {"client_id", client_id},
            {"client_secret", client_secret},
        };
        url.setQueryParameters(params);
    }
    else
    {
        url = Poco::URI(oauth_server_uri);
        out_stream_callback = [&](std::ostream & os)
        {
            os << fmt::format(
                "grant_type=client_credentials&scope={}&client_id={}&client_secret={}",
                auth_scope, client_id, client_secret);
        };
    }

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

    return object->get("access_token").extract<String>();
}

std::optional<StorageType> RestCatalog::getStorageType() const
{
    if (config.default_base_location.empty())
        return std::nullopt;
    return parseStorageTypeFromLocation(config.default_base_location);
}

DB::ReadWriteBufferFromHTTPPtr RestCatalog::createReadBuffer(
    const std::string & endpoint,
    const Poco::URI::QueryParameters & params,
    const DB::HTTPHeaderEntries & headers) const
{
    const auto & context = getContext();

    Poco::URI url(base_url / endpoint);
    if (!params.empty())
        url.setQueryParameters(params);

    auto create_buffer = [&](bool update_token)
    {
        auto result_headers = getAuthHeaders(update_token);
        std::move(headers.begin(), headers.end(), std::back_inserter(result_headers));

        return DB::BuilderRWBufferFromHTTP(url)
            .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
            .withSettings(getContext()->getReadSettings())
            .withTimeouts(DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
            .withHostFilter(&getContext()->getRemoteHostFilter())
            .withHeaders(result_headers)
            .withDelayInit(false)
            .withSkipNotFound(false)
            .create(credentials);
    };

    LOG_TEST(log, "Requesting: {}", url.toString());

    try
    {
        return create_buffer(false);
    }
    catch (const DB::HTTPException & e)
    {
        const auto status = e.getHTTPStatus();
        if (update_token_if_expired &&
            (status == Poco::Net::HTTPResponse::HTTPStatus::HTTP_UNAUTHORIZED
             || status == Poco::Net::HTTPResponse::HTTPStatus::HTTP_FORBIDDEN))
        {
            return create_buffer(true);
        }
        throw;
    }
}

bool RestCatalog::empty() const
{
    /// TODO: add a test with empty namespaces and zero namespaces.
    bool found_table = false;
    auto stop_condition = [&](const std::string & namespace_name) -> bool
    {
        const auto tables = getTables(namespace_name, /* limit */1);
        found_table = !tables.empty();
        return found_table;
    };

    Namespaces namespaces;
    getNamespacesRecursive("", namespaces, stop_condition, /* execute_func */{});

    return found_table;
}

DB::Names RestCatalog::getTables() const
{
    auto & pool = getContext()->getIcebergCatalogThreadpool();
    DB::ThreadPoolCallbackRunnerLocal<void> runner(pool, "RestCatalog");

    DB::Names tables;
    std::mutex mutex;

    auto execute_for_each_namespace = [&](const std::string & current_namespace)
    {
        runner(
        [=, &tables, &mutex, this]
        {
            auto tables_in_namespace = getTables(current_namespace);
            std::lock_guard lock(mutex);
            std::move(tables_in_namespace.begin(), tables_in_namespace.end(), std::back_inserter(tables));
        });
    };

    Namespaces namespaces;
    getNamespacesRecursive(
        /* base_namespace */"", /// Empty base namespace means starting from root.
        namespaces,
        /* stop_condition */{},
        /* execute_func */execute_for_each_namespace);

    runner.waitForAllToFinishAndRethrowFirstError();
    return tables;
}

void RestCatalog::getNamespacesRecursive(
    const std::string & base_namespace,
    Namespaces & result,
    StopCondition stop_condition,
    ExecuteFunc func) const
{
    checkStackSize();

    auto namespaces = getNamespaces(base_namespace);
    result.reserve(result.size() + namespaces.size());
    result.insert(result.end(), namespaces.begin(), namespaces.end());

    for (const auto & current_namespace : namespaces)
    {
        chassert(current_namespace.starts_with(base_namespace));

        if (stop_condition && stop_condition(current_namespace))
            break;

        if (func)
            func(current_namespace);

        getNamespacesRecursive(current_namespace, result, stop_condition, func);
    }
}

Poco::URI::QueryParameters RestCatalog::createParentNamespaceParams(const std::string & base_namespace) const
{
    std::vector<std::string> parts;
    splitInto<'.'>(parts, base_namespace);
    std::string parent_param;
    for (const auto & part : parts)
    {
        /// 0x1F is a unit separator
        /// https://github.com/apache/iceberg/blob/70d87f1750627b14b3b25a0216a97db86a786992/open-api/rest-catalog-open-api.yaml#L264
        if (!parent_param.empty())
            parent_param += static_cast<char>(0x1F);
        parent_param += part;
    }
    return {{"parent", parent_param}};
}

RestCatalog::Namespaces RestCatalog::getNamespaces(const std::string & base_namespace) const
{
    Poco::URI::QueryParameters params;
    if (!base_namespace.empty())
        params = createParentNamespaceParams(base_namespace);

    try
    {
        auto buf = createReadBuffer(config.prefix / NAMESPACES_ENDPOINT, params);
        auto namespaces = parseNamespaces(*buf, base_namespace);
        LOG_TEST(log, "Loaded {} namespaces in base namespace {}", namespaces.size(), base_namespace);
        return namespaces;
    }
    catch (const DB::HTTPException & e)
    {
        std::string message = fmt::format(
            "Received error while fetching list of namespaces from iceberg catalog `{}`. ",
            warehouse);

        if (e.code() == Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND)
            message += "Namespace provided in the `parent` query parameter is not found. ";

        message += fmt::format(
            "Code: {}, status: {}, message: {}",
            e.code(), e.getHTTPStatus(), e.displayText());

        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "{}", message);
    }
}

RestCatalog::Namespaces RestCatalog::parseNamespaces(DB::ReadBuffer & buf, const std::string & base_namespace) const
{
    if (buf.eof())
        return {};

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, buf);

    LOG_TEST(log, "Received response: {}", json_str);

    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var json = parser.parse(json_str);
        const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

        auto namespaces_object = object->get("namespaces").extract<Poco::JSON::Array::Ptr>();
        if (!namespaces_object)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse result");

        Namespaces namespaces;
        for (size_t i = 0; i < namespaces_object->size(); ++i)
        {
            auto current_namespace_array = namespaces_object->get(static_cast<int>(i)).extract<Poco::JSON::Array::Ptr>();
            if (current_namespace_array->size() == 0)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Expected namespace array to be non-empty");

            const int idx = static_cast<int>(current_namespace_array->size()) - 1;
            const auto current_namespace = current_namespace_array->get(idx).extract<String>();
            const auto full_namespace = base_namespace.empty()
                ? current_namespace
                : base_namespace + "." + current_namespace;

            namespaces.push_back(full_namespace);
        }

        return namespaces;
    }
    catch (DB::Exception & e)
    {
        e.addMessage("while parsing JSON: " + json_str);
        throw;
    }
}

DB::Names RestCatalog::getTables(const std::string & base_namespace, size_t limit) const
{
    const std::string endpoint = std::filesystem::path(NAMESPACES_ENDPOINT) / base_namespace / "tables";

    auto buf = createReadBuffer(config.prefix / endpoint);
    return parseTables(*buf, base_namespace, limit);
}

DB::Names RestCatalog::parseTables(DB::ReadBuffer & buf, const std::string & base_namespace, size_t limit) const
{
    if (buf.eof())
        return {};

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, buf);

    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var json = parser.parse(json_str);
        const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

        auto identifiers_object = object->get("identifiers").extract<Poco::JSON::Array::Ptr>();
        if (!identifiers_object)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse result");

        DB::Names tables;
        for (size_t i = 0; i < identifiers_object->size(); ++i)
        {
            const auto current_table_json = identifiers_object->get(static_cast<int>(i)).extract<Poco::JSON::Object::Ptr>();
            const auto table_name = current_table_json->get("name").extract<String>();

            tables.push_back(base_namespace + "." + table_name);
            if (limit && tables.size() >= limit)
                break;
        }

        return tables;
    }
    catch (DB::Exception & e)
    {
        e.addMessage("while parsing JSON: " + json_str);
        throw;
    }
}

bool RestCatalog::existsTable(const std::string & namespace_name, const std::string & table_name) const
{
    TableMetadata table_metadata;
    return tryGetTableMetadata(namespace_name, table_name, table_metadata);
}

bool RestCatalog::tryGetTableMetadata(
    const std::string & namespace_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    try
    {
        return getTableMetadataImpl(namespace_name, table_name, result);
    }
    catch (...)
    {
        DB::tryLogCurrentException(log);
        return false;
    }
}

void RestCatalog::getTableMetadata(
    const std::string & namespace_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    if (!getTableMetadataImpl(namespace_name, table_name, result))
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "No response from iceberg catalog");
}

bool RestCatalog::getTableMetadataImpl(
    const std::string & namespace_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    LOG_TEST(log, "Checking table {} in namespace {}", table_name, namespace_name);

    DB::HTTPHeaderEntries headers;
    if (result.requiresCredentials())
    {
        /// Header `X-Iceberg-Access-Delegation` tells catalog to include storage credentials in LoadTableResponse.
        /// Value can be one of the two:
        /// 1. `vended-credentials`
        /// 2. `remote-signing`
        /// Currently we support only the first.
        /// https://github.com/apache/iceberg/blob/3badfe0c1fcf0c0adfc7aa4a10f0b50365c48cf9/open-api/rest-catalog-open-api.yaml#L1832
        headers.emplace_back("X-Iceberg-Access-Delegation", "vended-credentials");
    }

    const std::string endpoint = std::filesystem::path(NAMESPACES_ENDPOINT) / namespace_name / "tables" / table_name;
    auto buf = createReadBuffer(config.prefix / endpoint, /* params */{}, headers);

    if (buf->eof())
    {
        LOG_TEST(log, "Table doesn't exist (endpoint: {})", endpoint);
        return false;
    }

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

#ifdef DEBUG_OR_SANITIZER_BUILD
    /// This log message might contain credentials,
    /// so log it only for debugging.
    LOG_TEST(log, "Received metadata for table {}: {}", table_name, json_str);
#endif

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

    auto metadata_object = object->get("metadata").extract<Poco::JSON::Object::Ptr>();
    if (!metadata_object)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse result");

    std::string location;
    if (result.requiresLocation())
    {
        if (metadata_object->has("location"))
        {
            location = metadata_object->get("location").extract<String>();
            result.setLocation(location);
            LOG_TEST(log, "Location for table {}: {}", table_name, location);
        }
        else
        {
            result.setTableIsNotReadable(fmt::format("Cannot read table {}, because no 'location' in response", table_name));
        }
    }

    if (result.requiresSchema())
    {
        // int format_version = metadata_object->getValue<int>("format-version");
        auto schema_processor = DB::IcebergSchemaProcessor();
        auto id = DB::IcebergMetadata::parseTableSchema(metadata_object, schema_processor, log);
        auto schema = schema_processor.getClickhouseTableSchemaById(id);
        result.setSchema(*schema);
    }

    if (result.isDefaultReadableTable() && result.requiresCredentials() && object->has("config"))
    {
        auto config_object = object->get("config").extract<Poco::JSON::Object::Ptr>();
        if (!config_object)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse config result");

        auto storage_type = parseStorageTypeFromLocation(location);
        switch (storage_type)
        {
            case StorageType::S3:
            {
                static constexpr auto access_key_id_str = "s3.access-key-id";
                static constexpr auto secret_access_key_str = "s3.secret-access-key";
                static constexpr auto session_token_str = "s3.session-token";
                static constexpr auto storage_endpoint_str = "s3.endpoint";

                std::string access_key_id;
                std::string secret_access_key;
                std::string session_token;
                std::string storage_endpoint;
                if (config_object->has(access_key_id_str))
                    access_key_id = config_object->get(access_key_id_str).extract<String>();
                if (config_object->has(secret_access_key_str))
                    secret_access_key = config_object->get(secret_access_key_str).extract<String>();
                if (config_object->has(session_token_str))
                    session_token = config_object->get(session_token_str).extract<String>();
                if (config_object->has(storage_endpoint_str))
                    storage_endpoint = config_object->get(storage_endpoint_str).extract<String>();

                result.setStorageCredentials(
                    std::make_shared<S3Credentials>(access_key_id, secret_access_key, session_token));

                result.setEndpoint(storage_endpoint);
                break;
            }
            default:
                break;
        }
    }

    if (result.requiresDataLakeSpecificProperties())
    {
        if (object->has("metadata-location") && !object->get("metadata-location").isEmpty())
        {
            auto metadata_location = object->get("metadata-location").extract<String>();
            result.setDataLakeSpecificProperties(DataLakeSpecificProperties{ .iceberg_metadata_file_location = metadata_location });
        }
    }

    return true;
}

}

#endif
