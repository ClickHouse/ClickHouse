#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandler.h>

#include <Access/Credentials.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
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
#include <Server/HTTPHandler.h>
#include <Server/IServer.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogStorage.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogTableMetadata.h>

#include <Common/scope_guard_safe.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>

#include <base/unit.h>
#include <boost/algorithm/string/split.hpp>
#include <fmt/ranges.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_ddl;
    extern const SettingsUInt64 readonly;
}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int KEEPER_EXCEPTION;
    extern const int NO_ZOOKEEPER;
    extern const int QUERY_IS_PROHIBITED;
    extern const int READONLY;
    extern const int REQUIRED_PASSWORD;
}

namespace
{

constexpr size_t MAX_REQUEST_BODY_SIZE = 1_MiB;
/// Keeper limits path depth and node data size. Reject oversized requests here with 400 instead of a Keeper error.
constexpr size_t MAX_NAMESPACE_LEVELS = 16;
constexpr size_t MAX_NAMESPACE_LEVEL_LENGTH = 256;
constexpr size_t MAX_NAMESPACE_PROPERTIES_SIZE = 64_KiB;
constexpr size_t MAX_TABLE_NAME_LENGTH = 256;
/// Metadata files with many snapshots reach tens of megabytes.
constexpr size_t MAX_METADATA_FILE_SIZE = 64_MiB;
constexpr char NAMESPACE_LEVEL_SEPARATOR = '\x1F';

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

Poco::JSON::Array namespaceToJSON(const IcebergNamespaceName & name)
{
    Poco::JSON::Array result;
    for (const auto & level : name)
        result.add(level);
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

Poco::JSON::Object tableIdentifierToJSON(const IcebergNamespaceName & ns, const String & table)
{
    Poco::JSON::Object result;
    result.set("namespace", namespaceToJSON(ns));
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
            uri = Poco::URI(request.getURI());
            uri.getPathSegments(segments);
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
        const auto warehouse = warehouses->find(prefix);
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
            case IcebergRESTOperation::NamespaceExists:
                handleNamespaceExists(*warehouse, *match, response);
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
        /// `AccessControl` reports a wrong password for the `default` user as `REQUIRED_PASSWORD`.
        if (code == ErrorCodes::AUTHENTICATION_FAILED || code == ErrorCodes::REQUIRED_PASSWORD)
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
        /// The store handles expected Keeper errors. Anything else means the store is unavailable.
        else if (code == ErrorCodes::KEEPER_EXCEPTION || code == ErrorCodes::NO_ZOOKEEPER)
        {
            status = Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
            type = "ServiceUnavailableException";
            message = "Catalog storage is unavailable";
        }

        tryLogCurrentException(log, "Failed to process Iceberg REST catalog request");
        try
        {
            if (!response.sent())
                sendError(response, status, type, message);
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

    const auto warehouse = warehouses->find(*requested_warehouse);
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
        if (!warehouse.store->namespaceExists(parent))
        {
            sendNoSuchNamespace(response, parent);
            return;
        }
    }

    Poco::JSON::Array namespaces;
    for (const auto & name : warehouse.store->listNamespaces(parent))
        namespaces.add(namespaceToJSON(name));

    Poco::JSON::Object result;
    result.set("namespaces", namespaces);
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

void IcebergRESTCatalogHandler::handleNamespaceExists(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const
{
    if (!getNamespaceOrSendNotFound(warehouse, match, response))
        return;

    sendNoContent(response);
}

void IcebergRESTCatalogHandler::checkDDLAllowed(const Context & context, const String & action)
{
    const auto & settings = context.getSettingsRef();
    if (settings[Setting::readonly])
        throw Exception(ErrorCodes::READONLY, "Cannot {} in readonly mode", action);
    if (!settings[Setting::allow_ddl])
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
        if (!namespace_array || namespace_array->size() == 0)
            throw Poco::Exception("'namespace' must be a non-empty array");
        if (namespace_array->size() > MAX_NAMESPACE_LEVELS)
            throw Poco::Exception(fmt::format("'namespace' must have at most {} levels", MAX_NAMESPACE_LEVELS));

        for (const auto & level : *namespace_array)
        {
            auto level_string = level.extract<String>();
            if (level_string.empty())
                throw Poco::Exception("namespace levels must be non-empty strings");
            if (level_string.size() > MAX_NAMESPACE_LEVEL_LENGTH)
                throw Poco::Exception(fmt::format("namespace levels must be at most {} bytes", MAX_NAMESPACE_LEVEL_LENGTH));
            name.push_back(std::move(level_string));
        }

        if (json->has("properties"))
        {
            const auto properties_object = json->getObject("properties");
            if (!properties_object)
                throw Poco::Exception("'properties' must be an object");
            size_t properties_size = 0;
            for (const auto & [key, value] : *properties_object)
            {
                auto value_string = value.extract<String>();
                properties_size += key.size() + value_string.size();
                if (properties_size > MAX_NAMESPACE_PROPERTIES_SIZE)
                    throw Poco::Exception(fmt::format("'properties' must be at most {} bytes in total", MAX_NAMESPACE_PROPERTIES_SIZE));
                properties[key] = std::move(value_string);
            }
        }

        /// Tables of the namespace go under `location`. The server has credentials for one bucket only, so refuse others now.
        if (const auto it = properties.find("location"); it != properties.end() && !warehouse.ownsLocation(it->second))
            throw Poco::Exception("the 'location' property must be inside the warehouse bucket");
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

    if (!warehouse.store->createNamespace(name, properties))
    {
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_CONFLICT,
            "AlreadyExistsException",
            fmt::format("Namespace already exists: {}", joinNamespace(name)));
        return;
    }

    Poco::JSON::Object properties_json;
    for (const auto & [key, value] : properties)
        properties_json.set(key, value);

    Poco::JSON::Object result;
    result.set("namespace", namespaceToJSON(name));
    result.set("properties", properties_json);
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

std::optional<IcebergNamespaceName> IcebergRESTCatalogHandler::getNamespaceOrSendNotFound(
    const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response)
{
    auto ns = splitNamespace(match.path_params.at("namespace"));
    if (!warehouse.store->namespaceExists(ns))
    {
        sendNoSuchNamespace(response, ns);
        return std::nullopt;
    }
    return ns;
}

void IcebergRESTCatalogHandler::sendLoadTableResult(
    const IcebergRESTCatalogWarehouse & warehouse, const IcebergTablePointer & pointer, HTTPServerResponse & response) const
{
    const auto key = warehouse.objectKey(pointer.metadata_location);
    const auto content = readObjectToString(*warehouse.object_storage, key, server.context()->getReadSettings(), MAX_METADATA_FILE_SIZE);

    /// Parse instead of echoing the file, so a corrupt file is a 500 with a clear log message.
    const auto metadata = parseJSONObject(content, fmt::format("Metadata file {}", pointer.metadata_location));

    Poco::JSON::Object result;
    result.set("metadata-location", pointer.metadata_location);
    result.set("metadata", metadata);
    /// No credential vending yet. Clients use their own storage credentials.
    result.set("config", Poco::JSON::Object());
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

void IcebergRESTCatalogHandler::handleListTables(
    const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const
{
    const auto ns = getNamespaceOrSendNotFound(warehouse, match, response);
    if (!ns)
        return;

    /// `pageToken` and `pageSize` are ignored: the whole list is returned at once.
    Poco::JSON::Array identifiers;
    if (const auto tables = warehouse.store->listTables(*ns))
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
    if (!warehouse.store->tableExists(*ns, table))
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
    const auto pointer = warehouse.store->getTable(*ns, table);
    if (!pointer)
    {
        sendNoSuchTable(response, *ns, table);
        return;
    }

    sendLoadTableResult(warehouse, *pointer, response);
}

void IcebergRESTCatalogHandler::handleCreateTable(
    const IcebergRESTCatalogWarehouse & warehouse,
    const IcebergRESTRouteMatch & match,
    HTTPServerRequest & request,
    HTTPServerResponse & response,
    const Context & context) const
{
    checkDDLAllowed(context, "create table");

    /// The properties double as the existence check.
    const auto ns = splitNamespace(match.path_params.at("namespace"));
    const auto namespace_properties = warehouse.store->getNamespaceProperties(ns);
    if (!namespace_properties)
    {
        sendNoSuchNamespace(response, ns);
        return;
    }

    const auto body = readRequestBody(request, response, MAX_REQUEST_BODY_SIZE);
    if (!body)
        return;

    const auto uuid = toString(UUIDHelpers::generateV4());

    String name;
    IcebergTablePointer pointer{.uuid = uuid, .metadata_location = {}};
    String object_key;
    Poco::JSON::Object::Ptr metadata;
    try
    {
        const auto json = Poco::JSON::Parser().parse(*body).extract<Poco::JSON::Object::Ptr>();

        if (!json->has("name") || !json->get("name").isString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'name' must be a string");
        name = json->getValue<String>("name");
        if (name.empty() || name.size() > MAX_TABLE_NAME_LENGTH)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'name' must be non-empty and at most {} bytes", MAX_TABLE_NAME_LENGTH);

        /// Stage-create means "write the metadata, do not register it". The commit PR adds it.
        if (json->optValue<bool>("stage-create", false))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "stage-create is not supported");

        String location;
        if (json->has("location") && !json->isNull("location"))
        {
            if (!json->get("location").isString())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'location' must be a string");
            location = stripTrailingSlashes(json->getValue<String>("location"));
        }
        else if (const auto it = namespace_properties->find("location"); it != namespace_properties->end())
        {
            /// Same convention as the Java and Python catalogs.
            location = fmt::format("{}/{}-{}", stripTrailingSlashes(it->second), name, uuid);
        }
        else
        {
            /// The uuid suffix keeps a dropped and recreated table away from the old files.
            location = fmt::format("{}/{}/{}-{}", warehouse.base_location, fmt::join(ns, "/"), name, uuid);
        }

        /// The server has credentials for one bucket only. The bucket root itself is not a valid table location.
        if (!warehouse.ownsLocation(location))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'location' {} must be inside the warehouse bucket", location);

        std::map<String, String> properties;
        if (json->has("properties") && !json->isNull("properties"))
        {
            const auto properties_object = json->getObject("properties");
            if (!properties_object)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'properties' must be an object");
            for (const auto & [key, value] : *properties_object)
                properties[key] = value.convert<String>();
        }

        metadata = buildInitialTableMetadata(
            uuid, location, json->getObject("schema"), json->getObject("partition-spec"), json->getObject("write-order"), std::move(properties));

        /// Same naming as the ClickHouse Iceberg writer: `<location>/metadata/v<version>-<uuid>.metadata.json`, starting at 1.
        pointer.metadata_location = fmt::format("{}/metadata/v1-{}.metadata.json", location, uuid);
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
    writeNewObject(*warehouse.object_storage, object_key, toJSONString(*metadata, 4), server.context()->getWriteSettings());

    /// The file is only kept if the Keeper pointer is registered.
    using CreateTableResult = KeeperIcebergRESTCatalogStore::CreateTableResult;
    std::optional<CreateTableResult> created;
    SCOPE_EXIT_SAFE({
        if (created != CreateTableResult::Created)
            warehouse.object_storage->removeObjectIfExists(StoredObject(object_key));
    });
    created = warehouse.store->createTable(ns, name, pointer);

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
    sendLoadTableResult(warehouse, pointer, response);
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

    /// TODO: purge must delete every file the table owns: data files, manifests, manifest lists and metadata files.
    /// The client asked for the files to go away, so refusing is better than ignoring the flag.
    if (const auto purge = getQueryParameter(uri, "purgeRequested"); purge && *purge == "true")
    {
        sendError(response, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "BadRequestException", "purgeRequested is not supported yet");
        return;
    }

    if (!warehouse.store->dropTable(*ns, table))
    {
        sendNoSuchTable(response, *ns, table);
        return;
    }

    LOG_INFO(log, "Dropped table {}.{}", joinNamespace(*ns), table);
    sendNoContent(response);
}

}
