#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandler.h>

#include <Access/Credentials.h>
#include <Core/Settings.h>
#include <IO/HTTPCommon.h>
#include <IO/LimitReadBuffer.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Server/HTTP/authenticateUserByHTTP.h>
#include <Server/HTTPHandler.h>
#include <Server/IServer.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <base/unit.h>
#include <boost/algorithm/string/split.hpp>
#include <fmt/ranges.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int AUTHENTICATION_FAILED;
    extern const int REQUIRED_PASSWORD;
}

namespace
{

constexpr size_t MAX_NAMESPACE_CREATE_BODY_SIZE = 1_MiB;
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

}

IcebergRESTCatalogHandler::IcebergRESTCatalogHandler(IServer & server_, String warehouse_, IcebergRESTCatalogStorePtr store_)
    : log(getLogger("IcebergRESTCatalogHandler"))
    , server(server_)
    , warehouse(std::move(warehouse_))
    , store(std::move(store_))
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
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);

    setResponseDefaultHeaders(response);
    response.setStatus(status);
    response.setContentType("application/json");
    *response.send() << oss.str();
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

        if (auto prefix = match->path_params.find("prefix"); prefix != match->path_params.end())
        {
            if (prefix->second != warehouse)
            {
                sendError(
                    response,
                    Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
                    "NotFoundException",
                    fmt::format("Unknown prefix: {}", prefix->second));
                return;
            }
        }

        switch (match->route->operation)
        {
            case IcebergRESTOperation::GetConfig:
                handleGetConfig(uri, response);
                return;
            case IcebergRESTOperation::ListNamespaces:
                handleListNamespaces(uri, response);
                return;
            case IcebergRESTOperation::CreateNamespace:
                handleCreateNamespace(request, response);
                return;
            case IcebergRESTOperation::NamespaceExists:
                handleNamespaceExists(*match, response);
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
        else if (code == ErrorCodes::ACCESS_DENIED)
        {
            status = Poco::Net::HTTPResponse::HTTP_FORBIDDEN;
            type = "ForbiddenException";
            message = getCurrentExceptionMessage(false);
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

    if (*requested_warehouse != warehouse)
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


    overrides.set("prefix", warehouse);
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

void IcebergRESTCatalogHandler::handleListNamespaces(const Poco::URI & uri, HTTPServerResponse & response) const
{
    IcebergNamespaceName parent;
    if (auto parent_param = getQueryParameter(uri, "parent"); parent_param && !parent_param->empty())
    {
        parent = splitNamespace(*parent_param);
        if (!store->namespaceExists(parent))
        {
            sendError(
                response,
                Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
                "NoSuchNamespaceException",
                fmt::format("Namespace does not exist: {}", joinNamespace(parent)));
            return;
        }
    }

    Poco::JSON::Array namespaces;
    for (const auto & name : store->listNamespaces(parent))
        namespaces.add(namespaceToJSON(name));

    Poco::JSON::Object result;
    result.set("namespaces", namespaces);
    sendJSON(response, result, Poco::Net::HTTPResponse::HTTP_OK);
}

void IcebergRESTCatalogHandler::handleNamespaceExists(const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const
{
    const auto name = splitNamespace(match.path_params.at("namespace"));
    if (!store->namespaceExists(name))
    {
        sendError(
            response,
            Poco::Net::HTTPResponse::HTTP_NOT_FOUND,
            "NoSuchNamespaceException",
            fmt::format("Namespace does not exist: {}", joinNamespace(name)));
        return;
    }

    setResponseDefaultHeaders(response);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NO_CONTENT);
    response.send();
}

void IcebergRESTCatalogHandler::handleCreateNamespace(HTTPServerRequest & request, HTTPServerResponse & response) const
{
    const auto body = readRequestBody(request, response, MAX_NAMESPACE_CREATE_BODY_SIZE);
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

        for (const auto & level : *namespace_array)
        {
            if (level.extract<String>().empty())
                throw Poco::Exception("namespace levels must be non-empty strings");
            name.push_back(level.extract<String>());
        }

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

    if (!store->createNamespace(name, properties))
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

}
