#pragma once

#include <Interpreters/Context_fwd.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/IcebergRESTCatalog/IIcebergRESTCatalogStore.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogRouter.h>
#include <Common/logger_useful.h>

#include <Poco/JSON/Object.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>

#include <optional>

namespace DB
{

class IServer;
class Session;

/// Serves the Iceberg REST catalog v1 API (RFC: issue #114697).
/// Every request is authenticated with the regular HTTP credentials (Basic, `X-ClickHouse-User`, certificates).
class IcebergRESTCatalogHandler : public HTTPRequestHandler
{
public:
    IcebergRESTCatalogHandler(IServer & server_, String warehouse_, IcebergRESTCatalogStorePtr store_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    /// Returns nullptr when the response has already been sent (401 with `WWW-Authenticate`).
    ContextMutablePtr authenticateUser(HTTPServerRequest & request, HTTPServerResponse & response, Session & session) const;

    void handleGetConfig(const Poco::URI & uri, HTTPServerResponse & response) const;
    void handleListNamespaces(const Poco::URI & uri, HTTPServerResponse & response) const;
    void handleCreateNamespace(HTTPServerRequest & request, HTTPServerResponse & response) const;
    void handleNamespaceExists(const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const;

    /// Reads the whole request body. Returns nullopt after answering 413 if the body exceeds max_size.
    static std::optional<String> readRequestBody(HTTPServerRequest & request, HTTPServerResponse & response, size_t max_size);

    static void sendJSON(HTTPServerResponse & response, const Poco::JSON::Object & json, Poco::Net::HTTPResponse::HTTPStatus status);
    static void sendError(
        HTTPServerResponse & response, Poco::Net::HTTPResponse::HTTPStatus status, const String & type, const String & message);

    LoggerPtr log;
    IServer & server;
    const String warehouse;
    IcebergRESTCatalogStorePtr store;
};

}
