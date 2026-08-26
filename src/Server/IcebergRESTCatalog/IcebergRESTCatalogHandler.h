#pragma once

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

/// Serves the Iceberg REST catalog v1 API (RFC: issue #114697).
class IcebergRESTCatalogHandler : public HTTPRequestHandler
{
public:
    IcebergRESTCatalogHandler(String warehouse_, IcebergRESTCatalogStorePtr store_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
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
    const String warehouse;
    IcebergRESTCatalogStorePtr store;
};

}
