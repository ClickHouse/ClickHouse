#pragma once

#include <Interpreters/Context_fwd.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogService.h>
#include <Common/logger_useful.h>

#include <optional>

namespace DB
{

class IServer;
class Session;

/// Serves the Iceberg REST catalog v1 API (RFC: issue #114697) over HTTP.
/// Every request is authenticated with the regular HTTP credentials (Basic, `X-ClickHouse-User`, certificates).
/// The catalog logic is in `IcebergRESTCatalogService`.
class IcebergRESTCatalogHandler : public HTTPRequestHandler
{
public:
    IcebergRESTCatalogHandler(IServer & server_, IcebergRESTCatalogServicePtr service_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    /// Returns nullptr when the response has already been sent (401 with `WWW-Authenticate`).
    ContextMutablePtr authenticateUser(HTTPServerRequest & request, HTTPServerResponse & response, Session & session) const;

    /// Reads the whole request body. Returns nullopt after answering 413 if the body exceeds max_size.
    static std::optional<String> readRequestBody(HTTPServerRequest & request, HTTPServerResponse & response, size_t max_size);

    static void sendResponse(HTTPServerResponse & response, const IcebergRESTResponse & result);

    LoggerPtr log;
    IServer & server;
    IcebergRESTCatalogServicePtr service;
};

}
