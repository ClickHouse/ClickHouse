#include <Databases/Iceberg/RestCatalog.h>

#include <base/find_symbols.h>
#include <Common/escapeForFileName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/Base64.h>

#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>

#include <Storages/ObjectStorage/DataLakes/IcebergMetadata.h>
#include <Server/HTTP/HTMLForm.h>
#include <Formats/FormatFactory.h>

#include <Poco/URI.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>

namespace DB::ErrorCodes
{
    extern const int ICEBERG_CATALOG_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace CurrentMetrics
{
    extern const Metric MergeTreeDataSelectExecutorThreads;
    extern const Metric MergeTreeDataSelectExecutorThreadsActive;
    extern const Metric MergeTreeDataSelectExecutorThreadsScheduled;
}

namespace Iceberg
{

static constexpr auto config_endpoint = "config";
static constexpr auto namespaces_endpoint = "namespaces";

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
    const DB::HTTPHeaderEntries & headers_,
    DB::ContextPtr context_)
    : ICatalog(warehouse_)
    , DB::WithContext(context_)
    , log(getLogger("RestCatalog(" + warehouse_ + ")"))
    , base_url(base_url_)
    , headers(headers_)
{
    if (!catalog_credential_.empty())
    {
        auto pos = catalog_credential_.find(':');
        if (pos == std::string::npos)
        {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS, "Unexpected format of catalog credential: "
                "expected client_id and client_secret separated by `:`");
        }
        client_id = catalog_credential_.substr(0, pos);
        client_secret = catalog_credential_.substr(pos + 1);

        /// TODO: remove before merge.
        LOG_TEST(log, "Client id: {}, client secret: {}", client_id, client_secret);
    }
    config = loadConfig();
}

RestCatalog::Config RestCatalog::loadConfig()
{
    if (!client_id.empty())
    {
        static constexpr auto oauth_tokens_endpoint = "oauth/tokens";

        const auto & context = getContext();
        const auto timeouts = DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings());

        Poco::JSON::Object json;
        json.set("grant_type", "client_credentials");
        json.set("scope", "PRINCIPAL_ROLE:ALL");
        json.set("client_id", client_id);
        json.set("client_secret", client_secret);

        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(json, oss);
        std::string json_str = oss.str();

        auto current_headers = headers;
        current_headers.emplace_back("Content-Type", "application/x-www-form-urlencoded");
        current_headers.emplace_back("Accepts", "application/json; charset=UTF-8");

        Poco::URI url(base_url / oauth_tokens_endpoint);
        Poco::URI::QueryParameters params = {
            {"grant_type", "client_credentials"},
            {"scope", "PRINCIPAL_ROLE:ALL"},
            {"client_id", client_id},
            {"client_secret", client_secret},
        };
        url.setQueryParameters(params);

        LOG_TEST(log, "Writing {}: {}", url.toString(), json_str);

        auto wb = DB::BuilderRWBufferFromHTTP(url)
            .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
            .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
            .withSettings(getContext()->getReadSettings())
            .withTimeouts(timeouts)
            .withHostFilter(&getContext()->getRemoteHostFilter())
            .withSkipNotFound(false)
            .withHeaders(current_headers)
            .create(credentials);

        json_str.clear();
        readJSONObjectPossiblyInvalid(json_str, *wb);

        LOG_TEST(log, "Received token result: {}", json_str);

        Poco::JSON::Parser parser;
        Poco::Dynamic::Var res_json = parser.parse(json_str);
        const Poco::JSON::Object::Ptr & object = res_json.extract<Poco::JSON::Object::Ptr>();

        auto access_token = object->get("access_token").extract<String>();
        headers.emplace_back("Authorization", "Bearer " + access_token);
    }

    Poco::URI::QueryParameters params = {{"warehouse", warehouse}};
    auto buf = createReadBuffer(config_endpoint, params);

    std::string json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

    LOG_TEST(log, "Received config result: {}", json_str);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

    Config result;

    auto defaults_object = object->get("defaults").extract<Poco::JSON::Object::Ptr>();
    parseConfig(defaults_object, result);

    auto overrides_object = object->get("overrides").extract<Poco::JSON::Object::Ptr>();
    parseConfig(overrides_object, result);

    LOG_TEST(log, "Parsed config: {}", result.toString());
    return result;
}

void RestCatalog::parseConfig(const Poco::JSON::Object::Ptr & object, Config & result)
{
    if (!object)
        return;

    if (object->has("prefix"))
        result.prefix = object->get("prefix").extract<String>();

    if (object->has("default-base-location"))
        result.default_base_location = object->get("default-base-location").extract<String>();
}

std::optional<StorageType> RestCatalog::getStorageType() const
{
    if (config.default_base_location.empty())
        return std::nullopt;
    return ICatalog::getStorageType(config.default_base_location);
}

DB::ReadWriteBufferFromHTTPPtr RestCatalog::createReadBuffer(
    const std::string & endpoint,
    const Poco::URI::QueryParameters & params) const
{
    const auto & context = getContext();
    const auto timeouts = DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings());

    Poco::URI url(base_url / endpoint);
    if (!params.empty())
        url.setQueryParameters(params);

    LOG_TEST(log, "Requesting: {}", url.toString());

    return DB::BuilderRWBufferFromHTTP(url)
        .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
        .withSettings(getContext()->getReadSettings())
        .withTimeouts(timeouts)
        .withHostFilter(&getContext()->getRemoteHostFilter())
        .withSkipNotFound(false)
        .withHeaders(headers)
        .create(credentials);
}

bool RestCatalog::empty() const
{
    try
    {
        bool found_table = false;
        auto stop_condition = [&](const std::string & namespace_name) -> bool
        {
            const auto tables = getTables(namespace_name, /* limit */1);
            found_table = !tables.empty();
            return found_table;
        };

        Namespaces namespaces;
        getNamespacesRecursive("", namespaces, stop_condition, {});

        return found_table;
    }
    catch (...)
    {
        DB::tryLogCurrentException(log);
        return true;
    }
}

RestCatalog::Tables RestCatalog::getTables() const
{
    size_t num_threads = 10;
    ThreadPool pool(
        CurrentMetrics::MergeTreeDataSelectExecutorThreads,
        CurrentMetrics::MergeTreeDataSelectExecutorThreadsActive,
        CurrentMetrics::MergeTreeDataSelectExecutorThreadsScheduled,
        num_threads);

    DB::ThreadPoolCallbackRunnerLocal<void> runner(pool, "RestCatalog");

    Tables tables;
    std::mutex mutex;

    auto func = [&](const std::string & current_namespace)
    {
        runner(
        [&]{
            auto tables_in_namespace = getTables(current_namespace);
            {
                std::lock_guard lock(mutex);
                std::move(tables_in_namespace.begin(), tables_in_namespace.end(), std::back_inserter(tables));
            }
        });
    };

    Namespaces namespaces;
    getNamespacesRecursive("", namespaces, {}, func);

    runner.waitForAllToFinishAndRethrowFirstError();
    return tables;
}

void RestCatalog::getNamespacesRecursive(
    const std::string & base_namespace,
    Namespaces & result,
    StopCondition stop_condition,
    ExecuteFunc func) const
{
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
        auto buf = createReadBuffer(config.prefix / namespaces_endpoint, params);
        auto namespaces = parseNamespaces(*buf, base_namespace);
        LOG_TEST(log, "Loaded {} namespaces", namespaces.size());
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

        throw DB::Exception(DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "{}", message);
    }
}

RestCatalog::Namespaces RestCatalog::parseNamespaces(DB::ReadBuffer & buf, const std::string & base_namespace) const
{
    if (buf.eof())
        return {};

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, buf);

    LOG_TEST(log, "Received response: {}", json_str);

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

RestCatalog::Tables RestCatalog::getTables(const std::string & base_namespace, size_t limit) const
{
    const auto endpoint = std::string(namespaces_endpoint) + "/" + base_namespace + "/tables";
    auto buf = createReadBuffer(config.prefix / endpoint);
    return parseTables(*buf, base_namespace, limit);
}

RestCatalog::Tables RestCatalog::parseTables(DB::ReadBuffer & buf, const std::string & base_namespace, size_t limit) const
{
    if (buf.eof())
        return {};

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, buf);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

    auto identifiers_object = object->get("identifiers").extract<Poco::JSON::Array::Ptr>();
    if (!identifiers_object)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse result");

    Tables tables;
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
        throw DB::Exception(DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "No response from iceberg catalog");
}

bool RestCatalog::getTableMetadataImpl(
    const std::string & namespace_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    LOG_TEST(log, "Checking table {} in namespace {}", table_name, namespace_name);

    const auto endpoint = std::string(namespaces_endpoint) + "/" + namespace_name + "/tables/" + table_name;
    auto buf = createReadBuffer(config.prefix / endpoint);

    if (buf->eof())
    {
        LOG_TEST(log, "Table doesn't exist (endpoint: {})", endpoint);
        return false;
    }

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

    LOG_TEST(log, "Received metadata for table {}: {}", table_name, json_str);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

    auto metadata_object = object->get("metadata").extract<Poco::JSON::Object::Ptr>();
    if (!metadata_object)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse result");

    if (result.requiresLocation())
    {
        const auto location = metadata_object->get("location").extract<String>();
        result.setLocation(location);
        LOG_TEST(log, "Location for table {}: {}", table_name, location);
    }

    if (result.requiresSchema())
    {
        int format_version = metadata_object->getValue<int>("format-version");
        result.setSchema(DB::IcebergMetadata::parseTableSchema(metadata_object, format_version, true).first);
    }
    return true;
}

}
