#pragma once

#include <Interpreters/Context_fwd.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogRouter.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>
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
    IcebergRESTCatalogHandler(IServer & server_, IcebergRESTCatalogWarehousesPtr warehouses_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    /// Returns nullptr when the response has already been sent (401 with `WWW-Authenticate`).
    ContextMutablePtr authenticateUser(HTTPServerRequest & request, HTTPServerResponse & response, Session & session) const;

    void handleGetConfig(const Poco::URI & uri, HTTPServerResponse & response) const;
    void handleListNamespaces(const IcebergRESTCatalogWarehouse & warehouse, const Poco::URI & uri, HTTPServerResponse & response) const;
    void handleCreateNamespace(const IcebergRESTCatalogWarehouse & warehouse, HTTPServerRequest & request, HTTPServerResponse & response, const Context & context) const;
    void handleNamespaceExists(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const;

    void handleListTables(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const;
    void handleCreateTable(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerRequest & request, HTTPServerResponse & response, const Context & context) const;
    void handleLoadTable(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const;
    void handleTableExists(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const;
    void handleDropTable(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, const Poco::URI & uri, HTTPServerResponse & response, const Context & context) const;
    void handleUpdateTable(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerRequest & request, HTTPServerResponse & response, const Context & context) const;

    static void checkNotReadonly(const Context & context, const String & action);
    static void checkDDLAllowed(const Context & context, const String & action);

    static std::optional<IcebergNamespaceName> getNamespaceOrSendNotFound(
        const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response);

    Poco::JSON::Object::Ptr readTableMetadata(const IcebergRESTCatalogWarehouse & warehouse, const IcebergTablePointer & pointer) const;

    static void sendLoadTableResult(const String & metadata_location, const Poco::JSON::Object::Ptr & metadata, HTTPServerResponse & response);

    /// Reads the whole request body. Returns nullopt after answering 413 if the body exceeds max_size.
    static std::optional<String> readRequestBody(HTTPServerRequest & request, HTTPServerResponse & response, size_t max_size);

    static void sendJSON(HTTPServerResponse & response, const Poco::JSON::Object & json, Poco::Net::HTTPResponse::HTTPStatus status);
    static void sendNoContent(HTTPServerResponse & response);
    static void sendError(
        HTTPServerResponse & response, Poco::Net::HTTPResponse::HTTPStatus status, const String & type, const String & message);
    static void sendNoSuchNamespace(HTTPServerResponse & response, const IcebergNamespaceName & ns);
    static void sendNoSuchTable(HTTPServerResponse & response, const IcebergNamespaceName & ns, const String & table);

    LoggerPtr log;
    IServer & server;
    IcebergRESTCatalogWarehousesPtr warehouses;
};

}
