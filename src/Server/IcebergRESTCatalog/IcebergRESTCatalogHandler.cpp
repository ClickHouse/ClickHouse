#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandler.h>

#include <Access/Credentials.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/CompressionMethod.h>
#include <IO/HTTPCommon.h>
#include <IO/LimitReadBuffer.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Server/HTTP/authenticateUserByHTTP.h>
#include <Server/HTTP/sendExceptionToHTTPClient.h>
#include <Server/HTTPHandler.h>
#include <Server/IServer.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCommitTable.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCreateTable.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogStorage.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>

#include <Common/ZooKeeper/KeeperException.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/String.h>

#include <base/unit.h>
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

constexpr size_t MAX_REQUEST_BODY_SIZE = 1_MiB;
constexpr size_t MAX_TABLE_NAME_LENGTH = 256;

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

/// Tables of the namespace go under `location`. The handler maps `BAD_ARGUMENTS` to a 400 response.
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

IcebergNamespaceName splitNamespace(const String & value)
{
    IcebergNamespaceName result;
    boost::split(result, value, [](char c) { return c == NAMESPACE_LEVEL_SEPARATOR; });
    return result;
}

/// The handler maps `BAD_ARGUMENTS` to a 400 response.
void validateTableName(const String & name)
{
    if (name.empty() || name.size() > MAX_TABLE_NAME_LENGTH)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table name must be non-empty and at most {} bytes", MAX_TABLE_NAME_LENGTH);
}

String joinNamespace(const IcebergNamespaceName & name)
{
    return fmt::format("{}", fmt::join(name, "."));
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

}

IcebergRESTCatalogHandler::IcebergRESTCatalogHandler(IServer & server_, IcebergRESTCatalogWarehousesPtr warehouses_)
    : log(getLogger("IcebergRESTCatalogHandler"))
    , server(server_)
    , warehouses(std::move(warehouses_))
{
}

ContextMutablePtr IcebergRESTCatalogHandler::authenticateUser(HTTPServerRequest & request, HTTPServerResponse & response, Session & session) const
{
    /// Only the URI is parsed here, so the body stays available for the route handlers.
    HTMLForm params(server.context()->getSettingsRef(), request);
    /// No fixed user: every request must carry its own credentials.
    const HTTPHandlerConnectionConfig connection_config;
    /// Each request gets a fresh handler, so partial multi-step credentials do not survive a 401 anyway.
    std::unique_ptr<Credentials> request_credentials;
    if (!authenticateUserByHTTP(request, params, response, session, request_credentials, connection_config, server.context(), log))
        return nullptr;

    auto context = session.makeQueryContext();
    context->setCurrentQueryId("");
    return context;
}

std::optional<String> IcebergRESTCatalogHandler::readRequestBody(HTTPServerRequest & request, HTTPServerResponse & response, size_t max_size)
{
    String body;
    /// Read one byte past the limit, otherwise an oversized body is silently truncated.
    LimitReadBuffer limited_stream(*request.getStream(), LimitReadBuffer::Settings{.read_no_more = max_size + 1});
    readStringUntilEOF(body, limited_stream);

    if (body.size() > max_size)
    {
        /// The rest of the body stays unread, so the connection cannot be reused for the next request.
        response.setKeepAlive(false);
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_REQUEST_ENTITY_TOO_LARGE,
            "RequestEntityTooLargeException",
            fmt::format("Request body must not exceed {} bytes", max_size));
        return {};
    }

    return body;
}

void IcebergRESTCatalogHandler::sendJSON(
    HTTPServerResponse & response, const Poco::JSON::Object & json, Poco::Net::HTTPResponse::HTTPStatus status)
{
    setResponseDefaultHeaders(response);
    response.setStatus(status);
    response.setContentType("application/json");
    *response.send() << toJSONString(json);
}

void IcebergRESTCatalogHandler::sendNoContent(HTTPServerResponse & response)
{
    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NO_CONTENT);
    response.send();
}

void IcebergRESTCatalogHandler::sendError(
    HTTPServerResponse & response, Poco::Net::HTTPResponse::HTTPStatus status, const String & type, const String & message)
{
    LOG_DEBUG(getLogger("IcebergRESTCatalogHandler"), "Request rejected with {} {}: {}", static_cast<int>(status), type, message);

    /// ErrorModel from the Iceberg REST specification.
    Poco::JSON::Object error;
    error.set("message", message);
    error.set("type", type);
    error.set("code", static_cast<int>(status));

    Poco::JSON::Object result;
    result.set("error", error);
    sendJSON(response, result, status);
}

void IcebergRESTCatalogHandler::sendNoSuchNamespace(HTTPServerResponse & response, const IcebergNamespaceName & ns)
{
    sendError(
        response,
        Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
        "NoSuchNamespaceException",
        fmt::format("Namespace does not exist: {}", joinNamespace(ns)));
}

void IcebergRESTCatalogHandler::sendNoSuchTable(HTTPServerResponse & response, const IcebergNamespaceName & ns, const String & table)
{
    sendError(
        response,
        Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
        "NoSuchTableException",
        fmt::format("Table does not exist: {}.{}", joinNamespace(ns), table));
}

void IcebergRESTCatalogHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    try
    {
        Session session(server.context(), ClientInfo::Interface::ICEBERG_REST_CATALOG, request.isSecure());
        auto context = authenticateUser(request, response, session);
        if (!context)
            return; /// 401 with `WWW-Authenticate` is already sent.

        Poco::URI uri;
        std::vector<std::string> segments;
        try
        {
            /// Split before decoding, so that an encoded `/` (`%2F`) stays inside its segment.
            uri = Poco::URI(request.getURI(), /* enable_url_encoding */ false);
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
            sendError(response, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "BadRequestException", "Malformed request URI");
            return;
        }

        auto match = matchIcebergRESTRoute(request.getMethod(), segments);
        if (!match)
        {
            sendError(
                response,
                Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
                "NotFoundException",
                fmt::format("No route for {} {}", request.getMethod(), uri.getPath()));
            return;
        }

        /// The spec reserves 406 UnsupportedOperationResponse for operations the server does not support.
        if (!match->route->implemented)
        {
            sendError(
                response,
                Poco::Net::HTTPResponse::HTTP_NOT_ACCEPTABLE,
                "UnsupportedOperationException",
                fmt::format("Not implemented: {}", toString(match->route->operation)));
            return;
        }

        /// `GET /v1/config` has no prefix. It gets the warehouse from the query string instead.
        if (match->route->operation == IcebergRESTOperation::GetConfig)
        {
            handleGetConfig(uri, response);
            return;
        }

        const auto & prefix = match->path_params.at("prefix");
        const auto warehouse = warehouses->tryGet(prefix);
        if (!warehouse)
        {
            sendError(
                response,
                Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
                "NotFoundException",
                fmt::format("Unknown prefix: {}", prefix));
            return;
        }

        switch (match->route->operation)
        {
            case IcebergRESTOperation::ListNamespaces:
                handleListNamespaces(*warehouse, uri, response);
                return;
            case IcebergRESTOperation::CreateNamespace:
                handleCreateNamespace(*warehouse, request, response, *context);
                return;
            case IcebergRESTOperation::LoadNamespace:
                handleLoadNamespace(*warehouse, *match, response);
                return;
            case IcebergRESTOperation::NamespaceExists:
                handleNamespaceExists(*warehouse, *match, response);
                return;
            case IcebergRESTOperation::UpdateNamespaceProperties:
                handleUpdateNamespaceProperties(*warehouse, *match, request, response, *context);
                return;
            case IcebergRESTOperation::ListTables:
                handleListTables(*warehouse, *match, response);
                return;
            case IcebergRESTOperation::CreateTable:
                handleCreateTable(*warehouse, *match, request, response, *context);
                return;
            case IcebergRESTOperation::LoadTable:
                handleLoadTable(*warehouse, *match, response);
                return;
            case IcebergRESTOperation::TableExists:
                handleTableExists(*warehouse, *match, response);
                return;
            case IcebergRESTOperation::DropTable:
                handleDropTable(*warehouse, *match, uri, response, *context);
                return;
            case IcebergRESTOperation::UpdateTable:
                handleUpdateTable(*warehouse, *match, request, response, *context);
                return;
            case IcebergRESTOperation::CommitTransaction:
                handleCommitTransaction(*warehouse, request, response, *context);
                return;
            default:
                sendError(
                    response,
                    Poco::Net::HTTPResponse::HTTP_NOT_ACCEPTABLE,
                    "UnsupportedOperationException",
                    fmt::format("Not implemented: {}", toString(match->route->operation)));
                return;
        }
    }
    catch (...)
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
        try
        {
            if (!response.sent())
            {
                /// Consume an unread POST body, so the connection can be reused for the next request.
                drainRequestIfNeeded(request, response);
                sendError(response, status, type, message);
            }
        }
        catch (...)
        {
            LOG_ERROR(log, "Cannot send exception to client");
        }
    }
}

void IcebergRESTCatalogHandler::handleGetConfig(const Poco::URI & uri, HTTPServerResponse & response) const
{
    auto requested_warehouse = getQueryParameter(uri, "warehouse");
    if (!requested_warehouse)
    {
        sendError(
            response, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "BadRequestException", "This server requires the warehouse query parameter");
        return;
    }

    const auto warehouse = warehouses->tryGet(*requested_warehouse);
    if (!warehouse)
    {
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
            "NoSuchWarehouseException",
            fmt::format("Unknown warehouse: {}", *requested_warehouse));
        return;
    }

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
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

void IcebergRESTCatalogHandler::handleListNamespaces(const IcebergRESTCatalogWarehouse & warehouse, const Poco::URI & uri, HTTPServerResponse & response) const
{
    IcebergNamespaceName parent;
    if (auto parent_param = getQueryParameter(uri, "parent"); parent_param && !parent_param->empty())
    {
        parent = splitNamespace(*parent_param);
        if (!warehouse.namespaceExists(parent))
        {
            sendNoSuchNamespace(response, parent);
            return;
        }
    }

    Poco::JSON::Array namespaces;
    for (const auto & name : warehouse.listNamespaces(parent))
        namespaces.add(stringsToJSONArray(name));

    Poco::JSON::Object result;
    result.set("namespaces", namespaces);
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

void IcebergRESTCatalogHandler::handleLoadNamespace(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const
{
    const auto ns = splitNamespace(match.path_params.at("namespace"));
    /// The properties double as the existence check.
    const auto properties = warehouse.getNamespaceProperties(ns);
    if (!properties)
    {
        sendNoSuchNamespace(response, ns);
        return;
    }

    Poco::JSON::Object properties_json;
    for (const auto & [key, value] : *properties)
        properties_json.set(key, value);

    Poco::JSON::Object result;
    result.set("namespace", stringsToJSONArray(ns));
    result.set("properties", properties_json);
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

void IcebergRESTCatalogHandler::handleNamespaceExists(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const
{
    if (!getNamespaceOrSendNotFound(warehouse, match, response))
        return;

    sendNoContent(response);
}

void IcebergRESTCatalogHandler::checkNotReadonly(const Context & context, const String & action)
{
    if (context.getSettingsRef()[Setting::readonly])
        throw Exception(ErrorCodes::READONLY, "Cannot {} in readonly mode", action);
}

void IcebergRESTCatalogHandler::checkDDLAllowed(const Context & context, const String & action)
{
    checkNotReadonly(context, action);
    if (!context.getSettingsRef()[Setting::allow_ddl])
        throw Exception(ErrorCodes::QUERY_IS_PROHIBITED, "Cannot {}. DDL queries are prohibited for the user", action);
}

void IcebergRESTCatalogHandler::handleCreateNamespace(const IcebergRESTCatalogWarehouse & warehouse, HTTPServerRequest & request, HTTPServerResponse & response, const Context & context) const
{
    checkDDLAllowed(context, "create namespace");

    const auto body = readRequestBody(request, response, MAX_REQUEST_BODY_SIZE);
    if (!body)
        return;

    IcebergNamespaceName name;
    std::map<String, String> properties;
    try
    {
        Poco::JSON::Parser parser;
        const auto json = parser.parse(*body).extract<Poco::JSON::Object::Ptr>();

        const auto namespace_array = json->getArray("namespace");
        if (!namespace_array)
            throw Poco::Exception("'namespace' must be an array");

        for (const auto & level : *namespace_array)
            name.push_back(level.extract<String>());

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
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST,
            "BadRequestException",
            fmt::format("Malformed create namespace request: {}", e.displayText()));
        return;
    }

    validateNamespaceLocation(warehouse, properties);

    if (!warehouse.createNamespace(name, properties))
    {
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_CONFLICT,
            "AlreadyExistsException",
            fmt::format("Namespace already exists: {}", joinNamespace(name)));
        return;
    }
    LOG_INFO(log, "Created namespace {}", joinNamespace(name));

    Poco::JSON::Object properties_json;
    for (const auto & [key, value] : properties)
        properties_json.set(key, value);

    Poco::JSON::Object result;
    result.set("namespace", stringsToJSONArray(name));
    result.set("properties", properties_json);
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

void IcebergRESTCatalogHandler::handleUpdateNamespaceProperties(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergRESTRouteMatch & match,
    HTTPServerRequest & request,
    HTTPServerResponse & response,
    const Context & context) const
{
    checkDDLAllowed(context, "update namespace properties");

    const auto ns = splitNamespace(match.path_params.at("namespace"));

    const auto body = readRequestBody(request, response, MAX_REQUEST_BODY_SIZE);
    if (!body)
        return;

    std::map<String, String> updates;
    std::set<String> removals;
    try
    {
        Poco::JSON::Parser parser;
        const auto json = parser.parse(*body).extract<Poco::JSON::Object::Ptr>();

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
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST,
            "BadRequestException",
            fmt::format("Malformed update namespace properties request: {}", e.displayText()));
        return;
    }

    validateNamespaceLocation(warehouse, updates);

    const auto conflict = std::ranges::find_if(removals, [&](const auto & key) { return updates.contains(key); });
    if (conflict != removals.end())
    {
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_UNPROCESSABLE_ENTITY,
            "UnprocessableEntityException",
            fmt::format("Property key '{}' is in both 'updates' and 'removals'", *conflict));
        return;
    }

    const auto result = warehouse.updateNamespaceProperties(ns, updates, removals);
    if (!result)
    {
        sendNoSuchNamespace(response, ns);
        return;
    }

    Poco::JSON::Object result_json;
    result_json.set("updated", stringsToJSONArray(result->updated));
    result_json.set("removed", stringsToJSONArray(result->removed));
    result_json.set("missing", stringsToJSONArray(result->missing));
    sendJSON(response, result_json, Poco::Net::HTTPResponse::HTTP_OK);
}

std::optional<IcebergNamespaceName> IcebergRESTCatalogHandler::getNamespaceOrSendNotFound(
    const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response)
{
    auto ns = splitNamespace(match.path_params.at("namespace"));
    if (!warehouse.namespaceExists(ns))
    {
        sendNoSuchNamespace(response, ns);
        return std::nullopt;
    }
    return ns;
}

Poco::JSON::Object::Ptr IcebergRESTCatalogHandler::readTableMetadata(
    const IcebergRESTCatalogWarehouse & warehouse, const IcebergTablePointer & pointer) const
{
    const auto key = warehouse.objectKey(pointer.metadata_location);
    const auto content = readObjectToString(*warehouse.object_storage, key, server.context()->getReadSettings(), getMetadataFileCompressionMethod(key));
    return parseJSONObject(content, fmt::format("Metadata file {}", pointer.metadata_location));
}

void IcebergRESTCatalogHandler::sendLoadTableResult(
    const String & metadata_location, const Poco::JSON::Object::Ptr & metadata, HTTPServerResponse & response)
{
    Poco::JSON::Object result;
    result.set("metadata-location", metadata_location);
    result.set("metadata", metadata);
    /// TODO: Add credential vending yet. For now, clients use their own storage credentials.
    result.set("config", Poco::JSON::Object());
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

void IcebergRESTCatalogHandler::handleListTables(
    const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const
{
    const auto ns = getNamespaceOrSendNotFound(warehouse, match, response);
    if (!ns)
        return;

    /// TODO: `pageToken` and `pageSize` are ignored: the whole list is returned at once.
    Poco::JSON::Array identifiers;
    if (const auto tables = warehouse.listTables(*ns))
    {
        for (const auto & table : *tables)
            identifiers.add(tableIdentifierToJSON(*ns, table));
    }

    Poco::JSON::Object result;
    result.set("identifiers", identifiers);
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

void IcebergRESTCatalogHandler::handleTableExists(
    const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const
{
    const auto ns = getNamespaceOrSendNotFound(warehouse, match, response);
    if (!ns)
        return;

    const auto & table = match.path_params.at("table");
    validateTableName(table);
    if (!warehouse.tableExists(*ns, table))
    {
        sendNoSuchTable(response, *ns, table);
        return;
    }

    sendNoContent(response);
}

void IcebergRESTCatalogHandler::handleLoadTable(
    const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const
{
    const auto ns = getNamespaceOrSendNotFound(warehouse, match, response);
    if (!ns)
        return;

    const auto & table = match.path_params.at("table");
    validateTableName(table);
    const auto pointer = warehouse.getTable(*ns, table);
    if (!pointer)
    {
        sendNoSuchTable(response, *ns, table);
        return;
    }

    sendLoadTableResult(pointer->metadata_location, readTableMetadata(warehouse, *pointer), response);
}

void IcebergRESTCatalogHandler::handleCreateTable(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergRESTRouteMatch & match,
    HTTPServerRequest & request,
    HTTPServerResponse & response,
    const Context & context) const
{
    checkDDLAllowed(context, "create table");

    /// Read the body before any early return, so a keep-alive connection has no unread bytes left.
    const auto body = readRequestBody(request, response, MAX_REQUEST_BODY_SIZE);
    if (!body)
        return;

    /// The properties double as the existence check.
    const auto ns = splitNamespace(match.path_params.at("namespace"));
    const auto namespace_properties = warehouse.getNamespaceProperties(ns);
    if (!namespace_properties)
    {
        sendNoSuchNamespace(response, ns);
        return;
    }

    const auto uuid = toString(UUIDHelpers::generateV4());

    String name;
    IcebergTablePointer pointer{.uuid = uuid, .metadata_location = {}};
    CompressionMethod compression_method = CompressionMethod::None;
    String location;
    String object_key;
    Poco::JSON::Object::Ptr metadata;
    try
    {
        const auto json = Poco::JSON::Parser().parse(*body).extract<Poco::JSON::Object::Ptr>();

        if (!json->has("name") || !json->get("name").isString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'name' must be a string");
        name = json->getValue<String>("name");
        validateTableName(name);

        /// TODO: Stage-create means "write the metadata, do not register it". Needed for CTAS.
        if (json->optValue<bool>("stage-create", false))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "stage-create is not supported");

        /// The catalog owns every table path, so two tables can never share a directory.
        /// TODO: Allow the ClickHouse client to send no location, so that the catalog can assign it.
        /// Currently the ClickHouse client always sends `location`. This needs a refactor of the client:
        /// create through the catalog first, then load the table from the answer.
        /// TODO: External tables need a per-table no-purge flag and an atomic location guard in Keeper.
        if (json->has("location") && !json->isNull("location"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'location' is not supported, the catalog chooses the table location");

        if (const auto it = namespace_properties->find("location"); it != namespace_properties->end())
        {
            /// The uuid suffix keeps a dropped and recreated table away from the old files.
            location = fmt::format("{}/{}-{}", stripTrailingSlashes(it->second), name, uuid);
        }
        else
        {
            location = fmt::format("{}/{}/{}-{}", warehouse.base_location, fmt::join(ns, "/"), name, uuid);
        }

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

        /// Same naming as the ClickHouse Iceberg writer: `<location>/metadata/v<version>-<uuid>.metadata.json`, starting at 1.
        pointer.metadata_location
            = fmt::format("{}/metadata/v1-{}{}.metadata.json", location, uuid, getMetadataCompressionSuffix(compression_method));
        object_key = warehouse.objectKey(pointer.metadata_location);
    }
    /// `Exception` derives from `Poco::Exception`, so it goes first.
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::BAD_ARGUMENTS)
            throw;
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST,
            "BadRequestException",
            fmt::format("Malformed create table request: {}", e.message()));
        return;
    }
    catch (const Poco::Exception & e)
    {
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST,
            "BadRequestException",
            fmt::format("Malformed create table request: {}", e.displayText()));
        return;
    }

    /// Object storage first, then the Keeper pointer. A crash in between leaves an orphan file, not a dangling pointer.
    writeNewObject(
        *warehouse.object_storage,
        object_key,
        toJSONString(*metadata, 4),
        server.context()->getWriteSettings(),
        compression_method,
        static_cast<int>(server.context()->getSettingsRef()[Setting::output_format_compression_level]));

    /// A Keeper exception (timeout, session loss) leaves the file. The node may exist, so deleting could leave a dangling pointer.
    using CreateTableResult = IcebergRESTCatalogWarehouse::CreateTableResult;
    const auto created = warehouse.createTable(ns, name, pointer);
    if (created != CreateTableResult::Created)
    {
        LOG_INFO(log, "Removing metadata file {} because table {}.{} was not registered", object_key, joinNamespace(ns), name);
        warehouse.object_storage->removeObjectIfExists(StoredObject(object_key));
    }

    if (created == CreateTableResult::TableExists)
    {
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_CONFLICT,
            "TableAlreadyExistsException",
            fmt::format("Table already exists: {}.{}", joinNamespace(ns), name));
        return;
    }
    if (created == CreateTableResult::NamespaceMissing)
    {
        sendNoSuchNamespace(response, ns);
        return;
    }

    LOG_INFO(log, "Created table {}.{} at {}", joinNamespace(ns), name, pointer.metadata_location);
    /// Answer from memory. The table is registered, so a failed read-back must not turn into a 500.
    sendLoadTableResult(pointer.metadata_location, metadata, response);
}

void IcebergRESTCatalogHandler::handleUpdateTable(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergRESTRouteMatch & match,
    HTTPServerRequest & request,
    HTTPServerResponse & response,
    const Context & context) const
{
    checkNotReadonly(context, "commit to table");

    const auto ns = getNamespaceOrSendNotFound(warehouse, match, response);
    if (!ns)
        return;
    const auto & table = match.path_params.at("table");

    const auto body = readRequestBody(request, response, MAX_REQUEST_BODY_SIZE);
    if (!body)
        return;

    const auto pointer = warehouse.getTable(*ns, table);
    if (!pointer)
    {
        sendNoSuchTable(response, *ns, table);
        return;
    }

    Poco::JSON::Array::Ptr requirements;
    Poco::JSON::Array::Ptr updates;
    try
    {
        const auto json = Poco::JSON::Parser().parse(*body).extract<Poco::JSON::Object::Ptr>();
        requirements = getArray(*json, "requirements");
        updates = getArray(*json, "updates");
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::BAD_ARGUMENTS)
            throw;
        sendCommitRequestError(response, e.code(), e.message());
        return;
    }
    catch (const Poco::Exception & e)
    {
        sendCommitRequestError(response, ErrorCodes::BAD_ARGUMENTS, e.displayText());
        return;
    }

    const auto prepared = prepareTableCommit(warehouse, *ns, table, *pointer, *requirements, *updates, response);
    if (!prepared)
        return;

    /// Do not write a new metadata version for an empty commit.
    if (prepared->new_location.empty())
    {
        sendCommitTableResult(pointer->metadata_location, prepared->metadata, response);
        return;
    }

    if (commitPreparedTables(warehouse, {*prepared}, response))
        sendCommitTableResult(prepared->new_location, prepared->metadata, response);
}

std::optional<IcebergRESTCatalogHandler::PreparedTableCommit> IcebergRESTCatalogHandler::prepareTableCommit(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergNamespaceName & ns,
    const String & table,
    const IcebergTablePointer & pointer,
    const Poco::JSON::Array & requirements,
    const Poco::JSON::Array & updates,
    HTTPServerResponse & response) const
{
    PreparedTableCommit prepared;
    prepared.ns = ns;
    prepared.table = table;
    prepared.current = pointer;
    prepared.metadata = readTableMetadata(warehouse, pointer);

    try
    {
        if (const auto result = checkTableRequirements(*prepared.metadata, requirements); !result)
        {
            sendError(response, Poco::Net::HTTPResponse::HTTP_CONFLICT, "CommitFailedException", result.error());
            return std::nullopt;
        }

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
        {
            sendError(response, Poco::Net::HTTPResponse::HTTP_CONFLICT, "CommitFailedException", e.message());
            return std::nullopt;
        }
        /// Other errors are not caused by the client. The HTTP layer reports them as internal errors.
        if (e.code() != ErrorCodes::NOT_IMPLEMENTED && e.code() != ErrorCodes::BAD_ARGUMENTS)
            throw;
        sendCommitRequestError(response, e.code(), e.message());
        return std::nullopt;
    }
    catch (const Poco::Exception & e)
    {
        sendCommitRequestError(response, ErrorCodes::BAD_ARGUMENTS, e.displayText());
        return std::nullopt;
    }

    prepared.new_location = nextMetadataLocation(
        *prepared.metadata, pointer.metadata_location, getMetadataCompressionSuffix(prepared.compression_method));
    return prepared;
}

bool IcebergRESTCatalogHandler::commitPreparedTables(
    const IcebergRESTCatalogWarehouse & warehouse, const std::vector<PreparedTableCommit> & prepared, HTTPServerResponse & response) const
{
    const auto remove_written_files = [&](const std::vector<String> & keys)
    {
        for (const auto & key : keys)
        {
            LOG_INFO(log, "Removing metadata file {} because the commit was not applied", key);
            warehouse.object_storage->removeObjectIfExists(StoredObject(key));
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
                    server.context()->getWriteSettings(),
                    table_commit.compression_method,
                    static_cast<int>(server.context()->getSettingsRef()[Setting::output_format_compression_level]));
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
        sendError(response, Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, "CommitStateUnknownException", "Commit state is unknown");
        return false;
    }

    if (result.result != UpdateTableResult::Updated)
    {
        remove_written_files(written_keys);
        const auto & failed = keeper_updates[result.failed_index];
        sendUpdateTableError(response, result.result, failed.ns, failed.table);
        return false;
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
    return true;
}

void IcebergRESTCatalogHandler::sendUpdateTableError(
    HTTPServerResponse & response, IcebergRESTCatalogWarehouse::UpdateTableResult result, const IcebergNamespaceName & ns, const String & table)
{
    using UpdateTableResult = IcebergRESTCatalogWarehouse::UpdateTableResult;
    switch (result)
    {
        case UpdateTableResult::Updated:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {}.{} was updated, there is no error to send", joinNamespace(ns), table);
        case UpdateTableResult::VersionMismatch:
            sendError(
                response,
                Poco::Net::HTTPResponse::HTTP_CONFLICT,
                "CommitFailedException",
                fmt::format("Table {}.{} was updated concurrently, retry the commit", joinNamespace(ns), table));
            return;
        case UpdateTableResult::UuidMismatch:
            sendError(
                response,
                Poco::Net::HTTPResponse::HTTP_CONFLICT,
                "CommitFailedException",
                fmt::format("Table {}.{} was dropped and re-created, reload the table", joinNamespace(ns), table));
            return;
        case UpdateTableResult::TableMissing:
            sendNoSuchTable(response, ns, table);
            return;
    }
}

namespace
{

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

}

/// Commits all table changes of one request together. Either every table moves or none does.
void IcebergRESTCatalogHandler::handleCommitTransaction(
    const IcebergRESTCatalogWarehouse & warehouse, HTTPServerRequest & request, HTTPServerResponse & response, const Context & context) const
{
    checkNotReadonly(context, "commit transaction");

    const auto body = readRequestBody(request, response, MAX_REQUEST_BODY_SIZE);
    if (!body)
        return;

    std::vector<TableChange> changes;
    try
    {
        changes = parseTableChanges(*body);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::NOT_IMPLEMENTED && e.code() != ErrorCodes::BAD_ARGUMENTS)
            throw;
        sendCommitRequestError(response, e.code(), e.message());
        return;
    }
    catch (const Poco::Exception & e)
    {
        sendCommitRequestError(response, ErrorCodes::BAD_ARGUMENTS, e.displayText());
        return;
    }

    /// Check every table before writing anything, so a rejected table needs no cleanup.
    std::vector<PreparedTableCommit> prepared;
    prepared.reserve(changes.size());
    for (const auto & change : changes)
    {
        const auto pointer = warehouse.getTable(change.ns, change.table);
        if (!pointer)
        {
            sendNoSuchTable(response, change.ns, change.table);
            return;
        }
        auto table_commit = prepareTableCommit(warehouse, change.ns, change.table, *pointer, *change.requirements, *change.updates, response);
        if (!table_commit)
            return;
        prepared.push_back(std::move(*table_commit));
    }

    if (commitPreparedTables(warehouse, prepared, response))
        sendNoContent(response);
}

void IcebergRESTCatalogHandler::sendCommitTableResult(
    const String & metadata_location, const Poco::JSON::Object::Ptr & metadata, HTTPServerResponse & response)
{
    Poco::JSON::Object result;
    result.set("metadata-location", metadata_location);
    result.set("metadata", metadata);
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

void IcebergRESTCatalogHandler::sendCommitRequestError(HTTPServerResponse & response, int code, const String & message)
{
    if (code == ErrorCodes::NOT_IMPLEMENTED)
        sendError(response, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "UnsupportedOperationException", message);
    else
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_BAD_REQUEST,
            "BadRequestException",
            fmt::format("Malformed commit request: {}", message));
}

void IcebergRESTCatalogHandler::handleDropTable(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergRESTRouteMatch & match,
    const Poco::URI & uri,
    HTTPServerResponse & response,
    const Context & context) const
{
    checkDDLAllowed(context, "drop table");

    const auto ns = getNamespaceOrSendNotFound(warehouse, match, response);
    if (!ns)
        return;

    const auto & table = match.path_params.at("table");
    validateTableName(table);

    /// TODO: purge deletes every file the table owns: data files, manifests, manifest lists and metadata files.
    if (getBooleanQueryParameter(uri, "purgeRequested", /*default_value=*/ false))
    {
        sendError(response, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "BadRequestException", "purgeRequested is not supported yet");
        return;
    }

    if (!warehouse.dropTable(*ns, table))
    {
        sendNoSuchTable(response, *ns, table);
        return;
    }

    LOG_INFO(log, "Dropped table {}.{}", joinNamespace(*ns), table);
    sendNoContent(response);
}

}
