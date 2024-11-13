#include <Databases/Iceberg/RestCatalog.h>

#include <base/find_symbols.h>
#include <Common/escapeForFileName.h>

#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>

#include <Storages/ObjectStorage/DataLakes/IcebergMetadata.h>
#include <Formats/FormatFactory.h>

#include <Poco/URI.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB::ErrorCodes
{
    extern const int ICEBERG_CATALOG_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace Iceberg
{

static constexpr auto namespaces_endpoint = "namespaces";

RestCatalog::RestCatalog(
    const std::string & catalog_name_,
    const std::string & base_url_,
    DB::ContextPtr context_)
    : ICatalog(catalog_name_)
    , DB::WithContext(context_)
    , base_url(base_url_)
    , log(getLogger("RestCatalog(" + catalog_name_ + ")"))
{
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
        getNamespacesRecursive("", namespaces, stop_condition);

        return found_table;
    }
    catch (...)
    {
        DB::tryLogCurrentException(log);
        return true;
    }
}

DB::ReadWriteBufferFromHTTPPtr RestCatalog::createReadBuffer(const std::string & endpoint, const Poco::URI::QueryParameters & params) const
{
    const auto & context = getContext();
    const auto timeouts = DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings());

    Poco::URI url(base_url / endpoint);
    if (!params.empty())
        url.setQueryParameters(params);

    return DB::BuilderRWBufferFromHTTP(url)
        .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
        .withSettings(getContext()->getReadSettings())
        .withTimeouts(timeouts)
        .withHostFilter(&getContext()->getRemoteHostFilter())
        .withSkipNotFound(true)
        .create(credentials);
}

RestCatalog::Tables RestCatalog::getTables() const
{
    Namespaces namespaces;
    getNamespacesRecursive("", namespaces, {});

    Tables tables;
    for (const auto & current_namespace : namespaces)
    {
        auto tables_in_namespace = getTables(current_namespace);
        std::move(tables_in_namespace.begin(), tables_in_namespace.end(), std::back_inserter(tables));
    }
    return tables;
}

void RestCatalog::getNamespacesRecursive(const std::string & base_namespace, Namespaces & result, StopCondition stop_condition) const
{
    auto namespaces = getNamespaces(base_namespace);
    result.reserve(result.size() + namespaces.size());
    result.insert(result.end(), namespaces.begin(), namespaces.end());

    for (const auto & current_namespace : namespaces)
    {
        chassert(current_namespace.starts_with(base_namespace));

        if (stop_condition && stop_condition(current_namespace))
            break;

        getNamespacesRecursive(current_namespace, result, stop_condition);
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
        auto buf = createReadBuffer(namespaces_endpoint, params);
        auto namespaces = parseNamespaces(*buf, base_namespace);
        LOG_TEST(log, "Loaded {} namespaces", namespaces.size());
        return namespaces;
    }
    catch (const DB::HTTPException & e)
    {
        std::string message = fmt::format(
            "Received error while fetching list of namespaces from iceberg catalog `{}`. ",
            catalog_name);

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
    auto buf = createReadBuffer(endpoint);
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
    auto buf = createReadBuffer(endpoint);

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

    if (result.with_location)
    {
        result.location = metadata_object->get("location").extract<String>();

        LOG_TEST(log, "Location for table {}: {}", table_name, result.location);
    }

    if (result.with_schema)
    {
        int format_version = metadata_object->getValue<int>("format-version");
        result.schema = DB::IcebergMetadata::parseTableSchema(metadata_object, format_version, true).first;
    }
    return true;
}

}
