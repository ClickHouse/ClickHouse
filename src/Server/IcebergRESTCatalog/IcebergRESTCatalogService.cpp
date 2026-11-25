#include <Server/IcebergRESTCatalog/IcebergRESTCatalogService.h>

#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCommitTable.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCreateTable.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogStorage.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>

#include <Common/ZooKeeper/KeeperException.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/String.h>

#include <boost/algorithm/string/split.hpp>
#include <fmt/ranges.h>

#include <algorithm>
#include <set>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_ddl;
    extern const SettingsUInt64 output_format_compression_level;
    extern const SettingsUInt64 readonly;
}

namespace ServerSetting
{
    extern const ServerSettingsBool enable_iceberg_rest_catalog;
}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int METADATA_MISMATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int QUERY_IS_PROHIBITED;
    extern const int READONLY;
    extern const int ICEBERG_CATALOG_CONCURRENT_UPDATE;
    extern const int KEEPER_EXCEPTION;
    extern const int NO_ZOOKEEPER;
    extern const int REQUIRED_PASSWORD;
    extern const int SUPPORT_IS_DISABLED;
    extern const int LOGICAL_ERROR;
}

using namespace IcebergRESTCatalog;

namespace
{

/// The Iceberg table property that names the codec of metadata files.
constexpr auto METADATA_COMPRESSION_CODEC_PROPERTY = "write.metadata.compression-codec";

CompressionMethod getMetadataCompressionMethod(const Poco::JSON::Object & metadata)
{
    const auto properties = getObject(metadata, Iceberg::f_properties);
    const auto codec = properties->optValue<String>(METADATA_COMPRESSION_CODEC_PROPERTY, "none");
    const auto lower = Poco::toLower(codec);
    if (lower == "none")
        return CompressionMethod::None;
    if (lower == "gzip")
        return CompressionMethod::Gzip;
    /// The spec defines only none and gzip. Other codecs would leave files that other engines cannot open.
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' must be 'none' or 'gzip', got '{}'", METADATA_COMPRESSION_CODEC_PROPERTY, codec);
}

/// Tables of the namespace go under `location`. `handleRequest` maps `BAD_ARGUMENTS` to a 400 response.
void validateNamespaceLocation(const IcebergRESTCatalogWarehouse & warehouse, const std::map<String, String> & properties)
{
    if (const auto it = properties.find("location"); it != properties.end())
        warehouse.validateLocation(it->second);
}

/// Codec suffix inside a metadata file name: `v1-<uuid>.gz.metadata.json`. The spec spells gzip as `gz`.
String getMetadataCompressionSuffix(CompressionMethod compression_method)
{
    return compression_method == CompressionMethod::Gzip ? ".gz" : "";
}

/// Same naming as the ClickHouse Iceberg writer: `<location>/metadata/v<version>-<uuid>.metadata.json`, starting at 1.
String firstMetadataLocation(const String & location, const String & uuid, CompressionMethod compression_method)
{
    return fmt::format("{}/metadata/v1-{}{}.metadata.json", location, uuid, getMetadataCompressionSuffix(compression_method));
}

/// `<namespace location or warehouse base>/<namespace path>/<name>-<uuid>`.
String tableLocation(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergNamespaceName & ns,
    const std::map<String, String> & namespace_properties,
    const String & name,
    const String & uuid)
{
    /// The uuid suffix keeps a dropped and recreated table away from the old files.
    if (const auto it = namespace_properties.find("location"); it != namespace_properties.end())
        return fmt::format("{}/{}-{}", stripTrailingSlashes(it->second), name, uuid);
    return fmt::format("{}/{}/{}-{}", warehouse.base_location, fmt::join(ns, "/"), name, uuid);
}

IcebergNamespaceName splitNamespace(const String & value)
{
    IcebergNamespaceName result;
    boost::split(result, value, [](char c) { return c == NAMESPACE_LEVEL_SEPARATOR; });
    return result;
}

String joinNamespace(const IcebergNamespaceName & name)
{
    return fmt::format("{}", fmt::join(name, "."));
}

IcebergNamespaceName parseNamespaceArray(const Poco::JSON::Object & json)
{
    const auto namespace_array = json.getArray("namespace");
    if (!namespace_array)
        throw Poco::Exception("'namespace' must be an array");

    IcebergNamespaceName result;
    for (const auto & level : *namespace_array)
        result.push_back(level.extract<String>());
    return result;
}

struct TableIdentifier
{
    IcebergNamespaceName ns;
    String name;
};

/// Parses a `TableIdentifier`: `namespace` is an array of levels, `name` is a string.
TableIdentifier parseTableIdentifier(const Poco::JSON::Object::Ptr & json)
{
    if (!json)
        throw Poco::Exception("table identifier must be an object");
    if (!json->has("name") || !json->get("name").isString())
        throw Poco::Exception("'name' must be a string");
    return {parseNamespaceArray(*json), json->getValue<String>("name")};
}

Poco::JSON::Array stringsToJSONArray(const Strings & strings)
{
    Poco::JSON::Array result;
    for (const auto & value : strings)
        result.add(value);
    return result;
}

std::optional<String> getQueryParameter(const Poco::URI & uri, const String & name)
{
    for (const auto & [key, value] : uri.getQueryParameters())
    {
        if (key == name)
            return value;
    }
    return std::nullopt;
}

/// Boolean query parameters arrive as `true`, `True` or `TRUE` depending on the client, so compare case-insensitively.
/// Throws for a value that is not a boolean.
bool getBooleanQueryParameter(const Poco::URI & uri, const String & name, bool default_value)
{
    const auto value = getQueryParameter(uri, name);
    if (!value)
        return default_value;
    if (Poco::icompare(*value, "true") == 0)
        return true;
    if (Poco::icompare(*value, "false") == 0)
        return false;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Query parameter '{}' must be 'true' or 'false', got '{}'", name, *value);
}

Poco::JSON::Object tableIdentifierToJSON(const IcebergNamespaceName & ns, const String & table)
{
    Poco::JSON::Object result;
    result.set("namespace", stringsToJSONArray(ns));
    result.set("name", table);
    return result;
}

IcebergRESTResponse makeJSON(const Poco::JSON::Object & json, Poco::Net::HTTPResponse::HTTPStatus status)
{
    return {.status = status, .body = toJSONString(json)};
}

IcebergRESTResponse makeNoContent()
{
    return {.status = Poco::Net::HTTPResponse::HTTP_NO_CONTENT, .body = {}};
}

IcebergRESTResponse makeBadRequestError(const String & message)
{
    return IcebergRESTCatalogService::makeIcebergError(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "BadRequestException", message);
}

IcebergRESTResponse makeConflictError(const String & type, const String & message)
{
    return IcebergRESTCatalogService::makeIcebergError(Poco::Net::HTTPResponse::HTTP_CONFLICT, type, message);
}

IcebergRESTResponse makeNoSuchNamespaceError(const IcebergNamespaceName & ns)
{
    return IcebergRESTCatalogService::makeIcebergError(
        Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
        "NoSuchNamespaceException",
        fmt::format("Namespace does not exist: {}", joinNamespace(ns)));
}

IcebergRESTResponse makeNoSuchTableError(const IcebergNamespaceName & ns, const String & table)
{
    return IcebergRESTCatalogService::makeIcebergError(
        Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
        "NoSuchTableException",
        fmt::format("Table does not exist: {}.{}", joinNamespace(ns), table));
}

IcebergRESTResponse makeNoSuchWarehouseError(const String & name)
{
    return IcebergRESTCatalogService::makeIcebergError(
        Poco::Net::HTTPResponse::HTTP_NOT_FOUND, "NoSuchWarehouseException", fmt::format("Unknown warehouse: {}", name));
}

/// A staged table has no metadata file yet, so `metadata_location` is empty.
IcebergRESTResponse makeLoadTableResult(
    const std::optional<String> & metadata_location, const Poco::JSON::Object::Ptr & metadata)
{
    Poco::JSON::Object result;
    if (metadata_location)
        result.set("metadata-location", *metadata_location);
    result.set("metadata", metadata);
    /// TODO: Add credential vending yet. For now, clients use their own storage credentials.
    result.set("config", Poco::JSON::Object());
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse makeCommitTableResult(
    const String & metadata_location, const Poco::JSON::Object::Ptr & metadata)
{
    Poco::JSON::Object result;
    result.set("metadata-location", metadata_location);
    result.set("metadata", metadata);
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse makeCommitRequestError(int code, const String & message)
{
    if (code == ErrorCodes::NOT_IMPLEMENTED)
        return IcebergRESTCatalogService::makeIcebergError(
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "UnsupportedOperationException", message);
    return makeBadRequestError(fmt::format("Malformed commit request: {}", message));
}

IcebergRESTResponse makeUpdateTableError(
    IcebergRESTCatalogWarehouse::UpdateTableResult result, const IcebergNamespaceName & ns, const String & table)
{
    using UpdateTableResult = IcebergRESTCatalogWarehouse::UpdateTableResult;
    switch (result)
    {
        case UpdateTableResult::Updated:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {}.{} was updated, there is no error to send", joinNamespace(ns), table);
        case UpdateTableResult::VersionMismatch:
            return makeConflictError(
                "CommitFailedException",
                fmt::format("Table {}.{} was updated concurrently, retry the commit", joinNamespace(ns), table));
        case UpdateTableResult::UuidNodeMissing:
            /// A concurrent drop. The client reloads and gets 404 if the name is gone.
            return makeConflictError(
                "CommitFailedException",
                fmt::format("Table {}.{} was dropped or re-created concurrently, reload the table", joinNamespace(ns), table));
    }
    UNREACHABLE();
}

struct TableChange
{
    IcebergNamespaceName ns;
    String table;
    Poco::JSON::Array::Ptr requirements;
    Poco::JSON::Array::Ptr updates;
};

TableChange parseTableChange(const Poco::JSON::Object & change)
{
    TableChange parsed;
    const auto identifier = getObject(change, "identifier");
    for (const auto & level : *getArray(*identifier, "namespace"))
        parsed.ns.push_back(level.extract<String>());
    parsed.table = getString(*identifier, "name");
    validateTableName(parsed.table);
    parsed.requirements = getArray(change, "requirements");
    parsed.updates = getArray(change, "updates");

    /// A staged table has no Keeper pointer yet, so the multi would need a create instead of a set.
    for (const auto & requirement : *parsed.requirements)
        if (requirement.type() == typeid(Poco::JSON::Object::Ptr)
            && requirement.extract<Poco::JSON::Object::Ptr>()->optValue<String>("type", "") == "assert-create")
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Staged creates are not supported in transactions");

    return parsed;
}

/// Parses the `table-changes` array of a `CommitTransactionRequest`. Each table may appear once.
std::vector<TableChange> parseTableChanges(const String & body)
{
    const auto json = Poco::JSON::Parser().parse(body).extract<Poco::JSON::Object::Ptr>();
    std::vector<TableChange> changes;
    std::set<std::pair<IcebergNamespaceName, String>> seen_tables;
    for (const auto & element : *getArray(*json, "table-changes"))
    {
        if (element.type() != typeid(Poco::JSON::Object::Ptr))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Every element of 'table-changes' must be an object");
        auto change = parseTableChange(*element.extract<Poco::JSON::Object::Ptr>());

        /// Each table gets one version-checked operation in the multi.
        if (!seen_tables.emplace(change.ns, change.table).second)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Table {}.{} appears more than once in the transaction", joinNamespace(change.ns), change.table);

        changes.push_back(std::move(change));
    }
    return changes;
}

void checkNotReadonly(const Context * user_context, const String & action)
{
    /// An internal request has no user. ClickHouse has already checked the query that caused it.
    if (!user_context)
        return;
    if (user_context->getSettingsRef()[Setting::readonly])
        throw Exception(ErrorCodes::READONLY, "Cannot {} in readonly mode", action);
}

void checkDDLAllowed(const Context * user_context, const String & action)
{
    /// An internal request has no user. ClickHouse has already checked the query that caused it.
    if (!user_context)
        return;
    checkNotReadonly(user_context, action);
    if (!user_context->getSettingsRef()[Setting::allow_ddl])
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Cannot {}. DDL queries are prohibited for the user", action);
}

}

IcebergRESTCatalogService::IcebergRESTCatalogService(ContextPtr global_context_, IcebergRESTCatalogWarehouseRegistryPtr warehouses_)
    : WithContext(global_context_)
    , log(getLogger("IcebergRESTCatalogService"))
    , warehouses(std::move(warehouses_))
{
}

IcebergNamespaceName IcebergRESTCatalogService::RoutedRequest::namespaceFromURL() const
{
    return splitNamespace(match.path_params.at("namespace"));
}

const String & IcebergRESTCatalogService::RoutedRequest::tableFromURL() const
{
    return match.path_params.at("table");
}

IcebergRESTResponse IcebergRESTCatalogService::handleExternalRequest(const IcebergRESTRequest & request, const Context & user_context) const
{
    return handleRequest(request, &user_context);
}

IcebergRESTResponse IcebergRESTCatalogService::handleInternalRequest(const IcebergRESTRequest & request) const
{
    return handleRequest(request, /* user_context */ nullptr);
}

IcebergRESTResponse IcebergRESTCatalogService::handleRequest(const IcebergRESTRequest & request, const Context * user_context) const
{
    try
    {
        return handleRequestImpl(request, user_context);
    }
    catch (...)
    {
        /// Ok. `convertCurrentExceptionToIcebergError` logs the exception.
        return convertCurrentExceptionToIcebergError();
    }
}

IcebergRESTResponse IcebergRESTCatalogService::handleRequestImpl(
    const IcebergRESTRequest & request, const Context * user_context) const
{
    Poco::URI uri;
    std::vector<std::string> segments;
    try
    {
        /// Split before decoding, so that an encoded `/` (`%2F`) stays inside its segment.
        uri = Poco::URI(request.uri, /* enable_url_encoding */ false);
        uri.getPathSegments(segments);
        for (auto & segment : segments)
        {
            String decoded;
            Poco::URI::decode(segment, decoded);
            segment = std::move(decoded);
        }
    }
    catch (const Poco::Exception &)
    {
        return makeBadRequestError("Malformed request URI");
    }

    auto match = matchIcebergRESTRoute(request.method, segments);
    if (!match)
    {
        return makeIcebergError(
            Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
            "NotFoundException",
            fmt::format("No route for {} {}", request.method, uri.getPath()));
    }

    /// The spec reserves 406 UnsupportedOperationResponse for operations the server does not support.
    if (!match->route->implemented)
    {
        return makeIcebergError(
            Poco::Net::HTTPResponse::HTTP_NOT_ACCEPTABLE,
            "UnsupportedOperationException",
            fmt::format("Not implemented: {}", toString(match->route->operation)));
    }

    /// `GET /v1/config` has no prefix. It gets the warehouse from the query string instead.
    if (match->route->operation == IcebergRESTOperation::GetConfig)
        return handleGetConfig(uri);

    /// Management routes create, drop, and read warehouses in the registry.
    switch (match->route->operation)
    {
        case IcebergRESTOperation::CreateWarehouse:
            return handleCreateWarehouse(request.body, user_context);
        case IcebergRESTOperation::ListWarehouses:
            return handleListWarehouses();
        case IcebergRESTOperation::GetWarehouse:
            return handleGetWarehouse(*match);
        case IcebergRESTOperation::ReplaceWarehouseCredential:
            return handleReplaceWarehouseCredential(*match, request.body, user_context);
        case IcebergRESTOperation::DeleteWarehouse:
            return handleDeleteWarehouse(*match, user_context);
        default:
            break;
    }

    const auto & prefix = match->path_params.at("prefix");
    const auto warehouse = warehouses->tryGet(prefix);
    if (!warehouse)
    {
        return makeIcebergError(
            Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
            "NotFoundException",
            fmt::format("Unknown prefix: {}", prefix));
    }

    const RoutedRequest routed{*warehouse, *match, uri, request.body, user_context};
    switch (match->route->operation)
    {
        case IcebergRESTOperation::ListNamespaces:
            return handleListNamespaces(routed);
        case IcebergRESTOperation::CreateNamespace:
            return handleCreateNamespace(routed);
        case IcebergRESTOperation::LoadNamespace:
            return handleLoadNamespace(routed);
        case IcebergRESTOperation::NamespaceExists:
            return handleNamespaceExists(routed);
        case IcebergRESTOperation::UpdateNamespaceProperties:
            return handleUpdateNamespaceProperties(routed);
        case IcebergRESTOperation::ListTables:
            return handleListTables(routed);
        case IcebergRESTOperation::CreateTable:
            return handleCreateTable(routed);
        case IcebergRESTOperation::LoadTable:
            return handleLoadTable(routed);
        case IcebergRESTOperation::TableExists:
            return handleTableExists(routed);
        case IcebergRESTOperation::DropTable:
            return handleDropTable(routed);
        case IcebergRESTOperation::UpdateTable:
            return handleUpdateTable(routed);
        case IcebergRESTOperation::RenameTable:
            return handleRenameTable(routed);
        case IcebergRESTOperation::ReportMetrics:
            return handleReportMetrics(routed);
        case IcebergRESTOperation::CommitTransaction:
            return handleCommitTransaction(routed);
        default:
            return makeIcebergError(
                Poco::Net::HTTPResponse::HTTP_NOT_ACCEPTABLE,
                "UnsupportedOperationException",
                fmt::format("Not implemented: {}", toString(match->route->operation)));
    }
}

IcebergRESTResponse IcebergRESTCatalogService::makeIcebergError(Poco::Net::HTTPResponse::HTTPStatus status, const String & type, const String & message)
{
    LOG_DEBUG(getLogger("IcebergRESTCatalogService"), "Request rejected with {} {}: {}", static_cast<int>(status), type, message);

    /// ErrorModel from the Iceberg REST specification.
    Poco::JSON::Object error;
    error.set("message", message);
    error.set("type", type);
    error.set("code", static_cast<int>(status));

    Poco::JSON::Object result;
    result.set("error", error);
    return makeJSON(result, status);
}

IcebergRESTResponse IcebergRESTCatalogService::convertCurrentExceptionToIcebergError() const
{
    auto status = Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
    String type = "InternalServerError";
    String message = "Internal server error";

    const int code = getCurrentExceptionCode();
    if (code == ErrorCodes::BAD_ARGUMENTS)
    {
        status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        type = "BadRequestException";
        message = getCurrentExceptionMessage(false);
    }
    /// `AccessControl` reports a wrong password for the `default` user as `REQUIRED_PASSWORD`.
    else if (code == ErrorCodes::AUTHENTICATION_FAILED || code == ErrorCodes::REQUIRED_PASSWORD)
    {
        status = Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED;
        type = "NotAuthorizedException";
        message = getCurrentExceptionMessage(false);
    }
    else if (code == ErrorCodes::ACCESS_DENIED || code == ErrorCodes::READONLY || code == ErrorCodes::QUERY_IS_PROHIBITED)
    {
        status = Poco::Net::HTTPResponse::HTTP_FORBIDDEN;
        type = "ForbiddenException";
        message = getCurrentExceptionMessage(false);
    }
    /// A valid request that this build cannot serve, for example a partition spec on a build without Avro.
    else if (code == ErrorCodes::SUPPORT_IS_DISABLED)
    {
        status = Poco::Net::HTTPResponse::HTTP_NOT_IMPLEMENTED;
        type = "UnsupportedOperationException";
        message = getCurrentExceptionMessage(false);
    }
    /// The store handles expected Keeper errors. Anything else means the store is unavailable.
    else if (code == ErrorCodes::KEEPER_EXCEPTION || code == ErrorCodes::NO_ZOOKEEPER)
    {
        status = Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
        type = "ServiceUnavailableException";
        message = "Catalog storage is unavailable";
    }
    /// The catalog is healthy, the client should retry the request.
    else if (code == ErrorCodes::ICEBERG_CATALOG_CONCURRENT_UPDATE)
    {
        status = Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
        type = "ServiceUnavailableException";
        message = getCurrentExceptionMessage(false);
    }

    tryLogCurrentException(log, "Failed to process Iceberg REST catalog request");
    return makeIcebergError(status, type, message);
}

IcebergRESTResponse IcebergRESTCatalogService::handleGetConfig(const Poco::URI & uri) const
{
    auto requested_warehouse = getQueryParameter(uri, "warehouse");
    if (!requested_warehouse)
        return makeBadRequestError("This server requires the warehouse query parameter");

    const auto warehouse = warehouses->tryGet(*requested_warehouse);
    if (!warehouse)
        return makeNoSuchWarehouseError(*requested_warehouse);

    Poco::JSON::Object defaults;
    Poco::JSON::Object overrides;
    Poco::JSON::Array endpoints;

    overrides.set("prefix", warehouse->name);
    for (const auto & route : getIcebergRESTRoutes())
    {
        if (!route.implemented)
            continue;
        endpoints.add(fmt::format("{} /{}", route.method, fmt::join(route.pattern, "/")));
    }

    Poco::JSON::Object result;
    result.set("defaults", defaults);
    result.set("overrides", overrides);
    result.set("endpoints", endpoints);
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleListNamespaces(const RoutedRequest & request) const
{
    IcebergNamespaceName parent;
    if (auto parent_param = getQueryParameter(request.uri, "parent"); parent_param && !parent_param->empty())
    {
        parent = splitNamespace(*parent_param);
        if (!request.warehouse.namespaceExists(parent))
            return makeNoSuchNamespaceError(parent);
    }

    Poco::JSON::Array namespaces;
    for (const auto & name : request.warehouse.listNamespaces(parent))
        namespaces.add(stringsToJSONArray(name));

    Poco::JSON::Object result;
    result.set("namespaces", namespaces);
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleCreateNamespace(const RoutedRequest & request) const
{
    checkDDLAllowed(request.user_context, "create namespace");

    IcebergNamespaceName name;
    std::map<String, String> properties;
    try
    {
        Poco::JSON::Parser parser;
        const auto json = parser.parse(request.body).extract<Poco::JSON::Object::Ptr>();

        name = parseNamespaceArray(*json);

        if (json->has("properties"))
        {
            const auto properties_object = json->getObject("properties");
            if (!properties_object)
                throw Poco::Exception("'properties' must be an object");
            for (const auto & [key, value] : *properties_object)
                properties[key] = value.extract<String>();
        }
    }
    catch (const Poco::Exception & e)
    {
        return makeBadRequestError(fmt::format("Malformed create namespace request: {}", e.displayText()));
    }

    validateNamespaceLocation(request.warehouse, properties);

    switch (request.warehouse.createNamespace(name, properties))
    {
        case IcebergRESTCatalogWarehouse::CreateNamespaceResult::Created:
            break;
        case IcebergRESTCatalogWarehouse::CreateNamespaceResult::NamespaceExists:
            return makeConflictError("AlreadyExistsException", fmt::format("Namespace already exists: {}", joinNamespace(name)));
        case IcebergRESTCatalogWarehouse::CreateNamespaceResult::ParentMissing:
            return makeIcebergError(
                Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
                "NotFoundException",
                fmt::format("Parent of namespace {} does not exist", joinNamespace(name)));
    }
    LOG_INFO(log, "Created namespace {}", joinNamespace(name));

    Poco::JSON::Object properties_json;
    for (const auto & [key, value] : properties)
        properties_json.set(key, value);

    Poco::JSON::Object result;
    result.set("namespace", stringsToJSONArray(name));
    result.set("properties", properties_json);
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleLoadNamespace(const RoutedRequest & request) const
{
    const auto ns = request.namespaceFromURL();
    /// The properties double as the existence check.
    const auto properties = request.warehouse.getNamespaceProperties(ns);
    if (!properties)
        return makeNoSuchNamespaceError(ns);

    Poco::JSON::Object properties_json;
    for (const auto & [key, value] : *properties)
        properties_json.set(key, value);

    Poco::JSON::Object result;
    result.set("namespace", stringsToJSONArray(ns));
    result.set("properties", properties_json);
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleNamespaceExists(const RoutedRequest & request) const
{
    const auto ns = request.namespaceFromURL();
    if (!request.warehouse.namespaceExists(ns))
        return makeNoSuchNamespaceError(ns);

    return makeNoContent();
}

IcebergRESTResponse IcebergRESTCatalogService::handleUpdateNamespaceProperties(const RoutedRequest & request) const
{
    checkDDLAllowed(request.user_context, "update namespace properties");

    const auto ns = request.namespaceFromURL();

    std::map<String, String> updates;
    std::set<String> removals;
    try
    {
        Poco::JSON::Parser parser;
        const auto json = parser.parse(request.body).extract<Poco::JSON::Object::Ptr>();

        if (json->has("updates"))
        {
            const auto updates_object = json->getObject("updates");
            if (!updates_object)
                throw Poco::Exception("'updates' must be an object");
            for (const auto & [key, value] : *updates_object)
                updates[key] = value.extract<String>();
        }

        if (json->has("removals"))
        {
            const auto removals_array = json->getArray("removals");
            if (!removals_array)
                throw Poco::Exception("'removals' must be an array");
            for (const auto & key : *removals_array)
                removals.insert(key.extract<String>());
        }
    }
    catch (const Poco::Exception & e)
    {
        return makeBadRequestError(fmt::format("Malformed update namespace properties request: {}", e.displayText()));
    }

    validateNamespaceLocation(request.warehouse, updates);

    const auto conflict = std::ranges::find_if(removals, [&](const auto & key) { return updates.contains(key); });
    if (conflict != removals.end())
    {
        return makeIcebergError(
            Poco::Net::HTTPResponse::HTTP_UNPROCESSABLE_ENTITY,
            "UnprocessableEntityException",
            fmt::format("Property key '{}' is in both 'updates' and 'removals'", *conflict));
    }

    const auto result = request.warehouse.updateNamespaceProperties(ns, updates, removals);
    if (!result)
        return makeNoSuchNamespaceError(ns);

    Poco::JSON::Object result_json;
    result_json.set("updated", stringsToJSONArray(result->updated));
    result_json.set("removed", stringsToJSONArray(result->removed));
    result_json.set("missing", stringsToJSONArray(result->missing));
    return makeJSON(result_json, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleListTables(const RoutedRequest & request) const
{
    const auto ns = request.namespaceFromURL();
    if (!request.warehouse.namespaceExists(ns))
        return makeNoSuchNamespaceError(ns);

    /// TODO: `pageToken` and `pageSize` are ignored: the whole list is returned at once.
    Poco::JSON::Array identifiers;
    if (const auto tables = request.warehouse.listTables(ns))
    {
        for (const auto & table : *tables)
            identifiers.add(tableIdentifierToJSON(ns, table));
    }

    Poco::JSON::Object result;
    result.set("identifiers", identifiers);
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleCreateTable(const RoutedRequest & request) const
{
    checkDDLAllowed(request.user_context, "create table");

    /// The properties double as the existence check.
    const auto ns = request.namespaceFromURL();
    const auto namespace_properties = request.warehouse.getNamespaceProperties(ns);
    if (!namespace_properties)
        return makeNoSuchNamespaceError(ns);

    const auto uuid = toString(UUIDHelpers::generateV4());

    String name;
    bool stage_create = false;
    IcebergTablePointer pointer{.uuid = uuid, .metadata_location = {}};
    CompressionMethod compression_method = CompressionMethod::None;
    String location;
    Poco::JSON::Object::Ptr metadata;
    try
    {
        const auto json = Poco::JSON::Parser().parse(request.body).extract<Poco::JSON::Object::Ptr>();

        if (!json->has("name") || !json->get("name").isString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'name' must be a string");
        name = json->getValue<String>("name");
        /// The store checks the name too, but only after the metadata file is written. Reject it before that.
        validateTableName(name);

        stage_create = json->optValue<bool>("stage-create", false);

        /// The catalog owns every table path, so two tables can never share a directory.
        /// TODO: Allow the ClickHouse client to send no location, so that the catalog can assign it.
        /// Currently the ClickHouse client always sends `location`. This needs a refactor of the client:
        /// create through the catalog first, then load the table from the answer.
        /// TODO: External tables need a per-table no-purge flag and an atomic location guard in Keeper.
        if (json->has("location") && !json->isNull("location"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'location' is not supported, the catalog chooses the table location");

        location = tableLocation(request.warehouse, ns, *namespace_properties, name, uuid);

        std::map<String, String> properties;
        if (const auto properties_object = getOptionalObject(*json, "properties"))
        {
            for (const auto & [key, value] : *properties_object)
                properties[key] = value.convert<String>();
        }

        metadata = buildInitialTableMetadata(
            uuid,
            location,
            getOptionalObject(*json, "schema"),
            getOptionalObject(*json, "partition-spec"),
            getOptionalObject(*json, "write-order"),
            std::move(properties));
        compression_method = getMetadataCompressionMethod(*metadata);

        pointer.metadata_location = firstMetadataLocation(location, uuid, compression_method);
    }
    /// `Exception` derives from `Poco::Exception`, so it goes first.
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::BAD_ARGUMENTS)
            throw;
        return makeBadRequestError(fmt::format("Malformed create table request: {}", e.message()));
    }
    catch (const Poco::Exception & e)
    {
        return makeBadRequestError(fmt::format("Malformed create table request: {}", e.displayText()));
    }

    if (stage_create)
    {
        /// A best-effort check. The create commit registers the table and fails if it exists by then.
        if (request.warehouse.tableExists(ns, name))
            return makeConflictError("TableAlreadyExistsException", fmt::format("Table already exists: {}.{}", joinNamespace(ns), name));
        LOG_INFO(log, "Staged table {}.{} at {}", joinNamespace(ns), name, location);
        return makeLoadTableResult(std::nullopt, metadata);
    }

    using CreateTableResult = IcebergRESTCatalogWarehouse::CreateTableResult;
    const auto created = writeAndRegisterTable(request.warehouse, ns, name, pointer, metadata, compression_method);
    if (created == CreateTableResult::TableExists)
    {
        return makeConflictError("TableAlreadyExistsException", fmt::format("Table already exists: {}.{}", joinNamespace(ns), name));
    }
    if (created == CreateTableResult::NamespaceMissing)
        return makeNoSuchNamespaceError(ns);

    LOG_INFO(log, "Created table {}.{} at {}", joinNamespace(ns), name, pointer.metadata_location);
    /// Answer from memory. The table is registered, so a failed read-back must not turn into a 500.
    return makeLoadTableResult(pointer.metadata_location, metadata);
}

IcebergRESTResponse IcebergRESTCatalogService::handleLoadTable(const RoutedRequest & request) const
{
    const auto ns = request.namespaceFromURL();
    if (!request.warehouse.namespaceExists(ns))
        return makeNoSuchNamespaceError(ns);

    const auto & table = request.tableFromURL();
    const auto pointer = request.warehouse.getTable(ns, table);
    if (!pointer)
        return makeNoSuchTableError(ns, table);

    return makeLoadTableResult(pointer->metadata_location, readTableMetadata(request.warehouse, *pointer));
}

IcebergRESTResponse IcebergRESTCatalogService::handleTableExists(const RoutedRequest & request) const
{
    const auto ns = request.namespaceFromURL();
    if (!request.warehouse.namespaceExists(ns))
        return makeNoSuchNamespaceError(ns);

    const auto & table = request.tableFromURL();
    if (!request.warehouse.tableExists(ns, table))
        return makeNoSuchTableError(ns, table);

    return makeNoContent();
}

IcebergRESTResponse IcebergRESTCatalogService::handleDropTable(const RoutedRequest & request) const
{
    checkDDLAllowed(request.user_context, "drop table");

    const auto ns = request.namespaceFromURL();
    if (!request.warehouse.namespaceExists(ns))
        return makeNoSuchNamespaceError(ns);

    const auto & table = request.tableFromURL();

    /// TODO: purge deletes every file the table owns: data files, manifests, manifest lists and metadata files.
    if (getBooleanQueryParameter(request.uri, "purgeRequested", /*default_value=*/ false))
        return makeBadRequestError("purgeRequested is not supported yet");

    if (!request.warehouse.dropTable(ns, table))
        return makeNoSuchTableError(ns, table);

    LOG_INFO(log, "Dropped table {}.{}", joinNamespace(ns), table);
    return makeNoContent();
}

IcebergRESTResponse IcebergRESTCatalogService::handleUpdateTable(const RoutedRequest & request) const
{
    checkNotReadonly(request.user_context, "commit to table");

    const auto ns = request.namespaceFromURL();
    if (!request.warehouse.namespaceExists(ns))
        return makeNoSuchNamespaceError(ns);
    const auto & table = request.tableFromURL();

    const auto pointer = request.warehouse.getTable(ns, table);

    Poco::JSON::Array::Ptr requirements;
    Poco::JSON::Array::Ptr updates;
    bool create_commit = false;
    try
    {
        const auto json = Poco::JSON::Parser().parse(request.body).extract<Poco::JSON::Object::Ptr>();
        requirements = getArray(*json, "requirements");
        updates = getArray(*json, "updates");
        if (!pointer)
            create_commit = isCreateCommit(*requirements);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::BAD_ARGUMENTS)
            throw;
        return makeCommitRequestError(e.code(), e.message());
    }
    catch (const Poco::Exception & e)
    {
        return makeCommitRequestError(ErrorCodes::BAD_ARGUMENTS, e.displayText());
    }

    if (!pointer)
    {
        /// A staged table has no pointer yet. Its create commit registers it.
        if (!create_commit)
            return makeNoSuchTableError(ns, table);
        return handleStagedCreateCommit(request, ns, table, *updates);
    }

    const auto prepared = prepareTableCommit(request.warehouse, ns, table, *pointer, *requirements, *updates);
    if (!prepared)
        return prepared.error();

    /// Do not write a new metadata version for an empty commit.
    if (prepared->new_location.empty())
        return makeCommitTableResult(pointer->metadata_location, prepared->metadata);

    if (auto error = commitPreparedTables(request.warehouse, {*prepared}))
        return *error;
    return makeCommitTableResult(prepared->new_location, prepared->metadata);
}

/// Stage-create takes two requests.
/// 1. `POST .../tables` with `stage-create: true`. The catalog assigns the uuid and the location and returns the metadata.
///    It writes nothing.
/// 2. `POST .../tables/{name}` with an `assert-create` requirement. The client sends the metadata back as updates.
///    The catalog builds the metadata again, writes the first metadata file and registers the table. This function is step 2.
IcebergRESTResponse IcebergRESTCatalogService::handleStagedCreateCommit(
    const RoutedRequest & request, const IcebergNamespaceName & ns, const String & table, const Poco::JSON::Array & updates) const
{
    checkDDLAllowed(request.user_context, "create table");

    const auto namespace_properties = request.warehouse.getNamespaceProperties(ns);
    if (!namespace_properties)
        return makeNoSuchNamespaceError(ns);

    Poco::JSON::Object::Ptr metadata;
    CompressionMethod compression_method = CompressionMethod::None;
    IcebergTablePointer pointer;
    try
    {
        validateTableName(table);
        metadata = buildMetadataFromStagedCreateCommit(updates);
        const auto location = getString(*metadata, Iceberg::f_location);
        compression_method = getMetadataCompressionMethod(*metadata);
        pointer.uuid = getString(*metadata, Iceberg::f_table_uuid);

        /// The client sends the location, so it must be the one the catalog assigned at stage time.
        const auto expected_location = tableLocation(request.warehouse, ns, *namespace_properties, table, pointer.uuid);
        if (location != expected_location)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'location' {} must be {}, the location assigned by the catalog", location, expected_location);

        /// The table uuid is the same on each retry. A fresh file name lets a retry succeed after a failed attempt left a file behind.
        pointer.metadata_location = firstMetadataLocation(location, toString(UUIDHelpers::generateV4()), compression_method);
    }
    catch (const Exception & e)
    {
        /// Other errors are not caused by the client. `handleRequest` reports them as internal errors.
        if (e.code() != ErrorCodes::NOT_IMPLEMENTED && e.code() != ErrorCodes::BAD_ARGUMENTS)
            throw;
        return makeCommitRequestError(e.code(), e.message());
    }
    catch (const Poco::Exception & e)
    {
        return makeCommitRequestError(ErrorCodes::BAD_ARGUMENTS, e.displayText());
    }

    using CreateTableResult = IcebergRESTCatalogWarehouse::CreateTableResult;
    std::optional<CreateTableResult> result;
    try
    {
        result = writeAndRegisterTable(request.warehouse, ns, table, pointer, metadata, compression_method);
    }
    catch (const zkutil::KeeperException & e)
    {
        /// The create may have been applied, so the file stays. The client reloads the table to find out.
        LOG_WARNING(log, "Create of {}.{} at {} has an unknown outcome: {}", joinNamespace(ns), table, pointer.metadata_location, e.message());
        return makeIcebergError(
            Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, "CommitStateUnknownException", "Commit state is unknown");
    }

    switch (*result)
    {
        case CreateTableResult::Created:
            LOG_INFO(log, "Created table {}.{} at {}", joinNamespace(ns), table, pointer.metadata_location);
            return makeCommitTableResult(pointer.metadata_location, metadata);
        case CreateTableResult::TableExists:
            return makeConflictError("CommitFailedException", fmt::format("Table already exists: {}.{}", joinNamespace(ns), table));
        case CreateTableResult::NamespaceMissing:
            return makeNoSuchNamespaceError(ns);
    }
    UNREACHABLE();
}

/// Commits all table changes of one request together. Either every table moves or none does.
IcebergRESTResponse IcebergRESTCatalogService::handleCommitTransaction(const RoutedRequest & request) const
{
    checkNotReadonly(request.user_context, "commit transaction");

    std::vector<TableChange> changes;
    try
    {
        changes = parseTableChanges(request.body);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::NOT_IMPLEMENTED && e.code() != ErrorCodes::BAD_ARGUMENTS)
            throw;
        return makeCommitRequestError(e.code(), e.message());
    }
    catch (const Poco::Exception & e)
    {
        return makeCommitRequestError(ErrorCodes::BAD_ARGUMENTS, e.displayText());
    }

    /// Check every table before writing anything, so a rejected table needs no cleanup.
    std::vector<PreparedTableCommit> prepared;
    prepared.reserve(changes.size());
    for (const auto & change : changes)
    {
        const auto pointer = request.warehouse.getTable(change.ns, change.table);
        if (!pointer)
            return makeNoSuchTableError(change.ns, change.table);
        auto table_commit = prepareTableCommit(request.warehouse, change.ns, change.table, *pointer, *change.requirements, *change.updates);
        if (!table_commit)
            return table_commit.error();
        prepared.push_back(std::move(*table_commit));
    }

    if (auto error = commitPreparedTables(request.warehouse, prepared))
        return *error;
    return makeNoContent();
}

IcebergRESTResponse IcebergRESTCatalogService::handleRenameTable(const RoutedRequest & request) const
{
    checkDDLAllowed(request.user_context, "rename table");

    TableIdentifier source;
    TableIdentifier dest;
    try
    {
        const auto json = Poco::JSON::Parser().parse(request.body).extract<Poco::JSON::Object::Ptr>();
        source = parseTableIdentifier(json->getObject("source"));
        dest = parseTableIdentifier(json->getObject("destination"));
    }
    catch (const Poco::Exception & e)
    {
        return makeBadRequestError(fmt::format("Malformed rename table request: {}", e.displayText()));
    }

    if (source.ns == dest.ns && source.name == dest.name)
        return makeBadRequestError("Source and destination are the same table");

    switch (request.warehouse.renameTable(source.ns, source.name, dest.ns, dest.name))
    {
        case IcebergRESTCatalogWarehouse::RenameTableResult::Renamed:
            LOG_INFO(log, "Renamed table {}.{} to {}.{}", joinNamespace(source.ns), source.name, joinNamespace(dest.ns), dest.name);
            return makeNoContent();
        case IcebergRESTCatalogWarehouse::RenameTableResult::TableMissing:
            return makeNoSuchTableError(source.ns, source.name);
        case IcebergRESTCatalogWarehouse::RenameTableResult::NamespaceMissing:
            return makeNoSuchNamespaceError(dest.ns);
        case IcebergRESTCatalogWarehouse::RenameTableResult::TargetExists:
            return makeConflictError(
                "AlreadyExistsException",
                fmt::format("Table already exists: {}.{}", joinNamespace(dest.ns), dest.name));
    }
    UNREACHABLE();
}

/// Clients send scan and commit reports after each operation. The server has nothing to do with them yet.
IcebergRESTResponse IcebergRESTCatalogService::handleReportMetrics(const RoutedRequest & request) const
{
    const auto ns = request.namespaceFromURL();
    if (!request.warehouse.namespaceExists(ns))
        return makeNoSuchNamespaceError(ns);

    const auto & table = request.tableFromURL();
    if (!request.warehouse.tableExists(ns, table))
        return makeNoSuchTableError(ns, table);

    return makeNoContent();
}

Poco::JSON::Object::Ptr IcebergRESTCatalogService::readTableMetadata(
    const IcebergRESTCatalogWarehouse & warehouse, const IcebergTablePointer & pointer) const
{
    const auto key = warehouse.objectKey(pointer.metadata_location);
    const auto content = readObjectToString(*warehouse.object_storage, key, getContext()->getReadSettings(), getMetadataFileCompressionMethod(key));
    return parseJSONObject(content, fmt::format("Metadata file {}", pointer.metadata_location));
}

IcebergRESTCatalogWarehouse::CreateTableResult IcebergRESTCatalogService::writeAndRegisterTable(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergNamespaceName & ns,
    const String & table,
    const IcebergTablePointer & pointer,
    const Poco::JSON::Object::Ptr & metadata,
    CompressionMethod compression_method) const
{
    /// Object storage first, then the Keeper pointer. A crash in between leaves an orphan file, not a dangling pointer.
    const auto object_key = warehouse.objectKey(pointer.metadata_location);
    writeNewObject(
        *warehouse.object_storage,
        object_key,
        toJSONString(*metadata, 4),
        getContext()->getWriteSettings(),
        compression_method,
        static_cast<int>(getContext()->getSettingsRef()[Setting::output_format_compression_level]));

    /// A Keeper exception (timeout, session loss) leaves the file. The node may exist, so deleting could leave a dangling pointer.
    using CreateTableResult = IcebergRESTCatalogWarehouse::CreateTableResult;
    const auto created = warehouse.createTable(ns, table, pointer);
    if (created != CreateTableResult::Created)
    {
        /// TODO: Make this removal best effort, as in `commitPreparedTables`. A failed removal must not replace the response.
        LOG_INFO(log, "Removing metadata file {} because table {}.{} was not registered", object_key, joinNamespace(ns), table);
        warehouse.object_storage->removeObjectIfExists(StoredObject(object_key));
    }
    return created;
}

std::expected<IcebergRESTCatalogService::PreparedTableCommit, IcebergRESTResponse> IcebergRESTCatalogService::prepareTableCommit(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergNamespaceName & ns,
    const String & table,
    const IcebergTablePointer & pointer,
    const Poco::JSON::Array & requirements,
    const Poco::JSON::Array & updates) const
{
    PreparedTableCommit prepared;
    prepared.ns = ns;
    prepared.table = table;
    prepared.current = pointer;
    prepared.metadata = readTableMetadata(warehouse, pointer);

    try
    {
        if (const auto result = checkTableRequirements(*prepared.metadata, requirements); !result)
            return std::unexpected(makeConflictError("CommitFailedException", result.error()));

        if (updates.size() == 0)
            return prepared;

        applyTableUpdatesToJSON(*prepared.metadata, updates, pointer.metadata_location);
        /// A `set-properties` update may change the codec. The new file follows the updated properties.
        prepared.compression_method = getMetadataCompressionMethod(*prepared.metadata);
    }
    catch (const Exception & e)
    {
        /// The request was based on metadata that another commit has since replaced. The client retries after reloading.
        if (e.code() == ErrorCodes::METADATA_MISMATCH)
            return std::unexpected(makeConflictError("CommitFailedException", e.message()));
        /// Other errors are not caused by the client. `handleRequest` reports them as internal errors.
        if (e.code() != ErrorCodes::NOT_IMPLEMENTED && e.code() != ErrorCodes::BAD_ARGUMENTS)
            throw;
        return std::unexpected(makeCommitRequestError(e.code(), e.message()));
    }
    catch (const Poco::Exception & e)
    {
        return std::unexpected(makeCommitRequestError(ErrorCodes::BAD_ARGUMENTS, e.displayText()));
    }

    prepared.new_location = nextMetadataLocation(
        *prepared.metadata, pointer.metadata_location, getMetadataCompressionSuffix(prepared.compression_method));
    return prepared;
}

std::optional<IcebergRESTResponse> IcebergRESTCatalogService::commitPreparedTables(
    const IcebergRESTCatalogWarehouse & warehouse, const std::vector<PreparedTableCommit> & prepared) const
{
    /// Best effort. A failed removal leaves an orphan file. It must not replace the result of the commit.
    const auto remove_written_files = [&](const std::vector<String> & keys)
    {
        if (keys.empty())
            return;
        LOG_INFO(log, "Removing metadata files because the commit was not applied: {}", fmt::join(keys, ", "));
        for (const auto & key : keys)
        {
            try
            {
                warehouse.object_storage->removeObjectIfExists(StoredObject(key));
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("Cannot remove metadata file {}", key));
            }
        }
    };

    /// Object storage first, then the Keeper pointers. A crash in between leaves orphan files, not dangling pointers.
    /// A table with an empty `new_location` is a read-only participant. The multi only checks that its version is unchanged.
    std::vector<String> written_keys;
    std::vector<IcebergRESTCatalogWarehouse::TableUpdate> keeper_updates;
    try
    {
        for (const auto & table_commit : prepared)
        {
            if (!table_commit.new_location.empty())
            {
                const auto object_key = warehouse.objectKey(table_commit.new_location);
                writeNewObject(
                    *warehouse.object_storage,
                    object_key,
                    toJSONString(*table_commit.metadata, 4),
                    getContext()->getWriteSettings(),
                    table_commit.compression_method,
                    static_cast<int>(getContext()->getSettingsRef()[Setting::output_format_compression_level]));
                written_keys.push_back(object_key);
            }
            keeper_updates.push_back(
                {.ns = table_commit.ns,
                 .table = table_commit.table,
                 .expected = table_commit.current,
                 .new_pointer = {.uuid = table_commit.current.uuid, .metadata_location = table_commit.new_location}});
        }
    }
    catch (...)
    {
        remove_written_files(written_keys);
        throw;
    }

    using UpdateTableResult = IcebergRESTCatalogWarehouse::UpdateTableResult;
    IcebergRESTCatalogWarehouse::UpdateTablesResult result;
    try
    {
        result = warehouse.updateTables(keeper_updates);
    }
    catch (const zkutil::KeeperException & e)
    {
        /// The multi may have been applied, so the new files must stay. The client reloads the tables to find out.
        Strings table_names;
        for (const auto & update : keeper_updates)
            table_names.push_back(joinNamespace(update.ns) + "." + update.table);
        LOG_WARNING(log, "Commit of {} has an unknown outcome: {}", fmt::join(table_names, ", "), e.message());
        return makeIcebergError(
            Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, "CommitStateUnknownException", "Commit state is unknown");
    }

    if (result.result != UpdateTableResult::Updated)
    {
        remove_written_files(written_keys);
        const auto & failed = keeper_updates[result.failed_index];
        return makeUpdateTableError(result.result, failed.ns, failed.table);
    }

    for (const auto & update : keeper_updates)
        if (!update.new_pointer.metadata_location.empty())
            LOG_INFO(
                log,
                "Committed table {}.{}: {} -> {}",
                joinNamespace(update.ns),
                update.table,
                update.expected.metadata_location,
                update.new_pointer.metadata_location);
    return std::nullopt;
}

IcebergRESTResponse IcebergRESTCatalogService::handleCreateWarehouse(const String & body, const Context * user_context) const
{
    checkDDLAllowed(user_context, "create warehouse");

    const auto json = parseRequestBody(body);
    const auto name = getString(*json, "name");
    IcebergRESTCatalogWarehouseDefinition::validateName(name);
    const auto definition = IcebergRESTCatalogWarehouseDefinition::fromJSON(*json);

    if (!warehouses->create(name, definition))
        return makeConflictError("AlreadyExistsException", fmt::format("Warehouse already exists: {}", name));

    return makeJSON(*definition.toJSON(name), Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleListWarehouses() const
{
    Poco::JSON::Array list;
    for (const auto & [name, definition] : warehouses->list())
        list.add(definition.toJSON(name));

    Poco::JSON::Object result;
    result.set("warehouses", list);
    return makeJSON(result, Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleGetWarehouse(const IcebergRESTRouteMatch & match) const
{
    const auto & name = match.path_params.at("warehouse");

    const auto definition = warehouses->tryGetDefinition(name);
    if (!definition)
        return makeNoSuchWarehouseError(name);

    return makeJSON(*definition->toJSON(name), Poco::Net::HTTPResponse::HTTP_OK);
}

IcebergRESTResponse IcebergRESTCatalogService::handleReplaceWarehouseCredential(
    const IcebergRESTRouteMatch & match, const String & body, const Context * user_context) const
{
    checkDDLAllowed(user_context, "replace warehouse credential");

    const auto & name = match.path_params.at("warehouse");

    const auto credential = IcebergRESTCatalogWarehouseCredential::fromJSON(*getObject(*parseRequestBody(body), "credential"));

    using Result = IcebergRESTCatalogWarehouseRegistry::ReplaceCredentialResult;
    switch (warehouses->replaceCredential(name, credential))
    {
        case Result::Replaced:
            return makeNoContent();
        case Result::NotFound:
            return makeNoSuchWarehouseError(name);
        case Result::Conflict:
            return makeConflictError("CommitFailedException", fmt::format("Warehouse {} was changed concurrently, retry", name));
    }
    UNREACHABLE();
}

IcebergRESTResponse IcebergRESTCatalogService::handleDeleteWarehouse(const IcebergRESTRouteMatch & match, const Context * user_context) const
{
    checkDDLAllowed(user_context, "drop warehouse");

    const auto & name = match.path_params.at("warehouse");

    using Result = IcebergRESTCatalogWarehouseRegistry::DropResult;
    switch (warehouses->drop(name))
    {
        case Result::Dropped:
            return makeNoContent();
        case Result::NotFound:
            return makeNoSuchWarehouseError(name);
        case Result::NotEmpty:
            return makeConflictError("WarehouseNotEmptyException", fmt::format("Warehouse {} is not empty. Drop its namespaces first", name));
    }
    UNREACHABLE();
}

IcebergRESTCatalogServicePtr createIcebergRESTCatalogService(ContextPtr global_context)
{
    if (!global_context->getServerSettings()[ServerSetting::enable_iceberg_rest_catalog])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "The Iceberg REST catalog of this server is experimental. "
            "To allow its usage, enable the server setting `enable_iceberg_rest_catalog`");

    auto warehouses = std::make_shared<IcebergRESTCatalogWarehouseRegistry>(global_context);
    return std::make_shared<const IcebergRESTCatalogService>(global_context, std::move(warehouses));
}

}
