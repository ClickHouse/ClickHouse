#pragma once

#include <IO/CompressionMethod.h>
#include <Interpreters/Context_fwd.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseRegistry.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogRouter.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>
#include <Common/logger_useful.h>

#include <Poco/JSON/Object.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>

#include <optional>
#include <vector>

namespace DB
{

class IServer;
class Session;

/// Serves the Iceberg REST catalog v1 API (RFC: issue #114697).
/// Every request is authenticated with the regular HTTP credentials (Basic, `X-ClickHouse-User`, certificates).
class IcebergRESTCatalogHandler : public HTTPRequestHandler
{
public:
    IcebergRESTCatalogHandler(IServer & server_, IcebergRESTCatalogWarehouseRegistryPtr warehouses_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    /// Returns nullptr when the response has already been sent (401 with `WWW-Authenticate`).
    ContextMutablePtr authenticateUser(HTTPServerRequest & request, HTTPServerResponse & response, Session & session) const;

    void handleGetConfig(const Poco::URI & uri, HTTPServerResponse & response) const;
    void handleListNamespaces(const IcebergRESTCatalogWarehouse & warehouse, const Poco::URI & uri, HTTPServerResponse & response) const;
    void handleCreateNamespace(const IcebergRESTCatalogWarehouse & warehouse, const String & body, HTTPServerResponse & response, const Context & context) const;
    void handleLoadNamespace(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const;
    void handleNamespaceExists(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const;
    void handleUpdateNamespaceProperties(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, const String & body, HTTPServerResponse & response, const Context & context) const;

    void handleListTables(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const;
    void handleCreateTable(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, const String & body, HTTPServerResponse & response, const Context & context) const;
    void handleLoadTable(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const;
    void handleTableExists(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const;
    void handleDropTable(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, const Poco::URI & uri, HTTPServerResponse & response, const Context & context) const;
    void handleUpdateTable(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, const String & body, HTTPServerResponse & response, const Context & context) const;
    void handleCommitTransaction(const IcebergRESTCatalogWarehouse & warehouse, const String & body, HTTPServerResponse & response, const Context & context) const;
    void handleRenameTable(const IcebergRESTCatalogWarehouse & warehouse, const String & body, HTTPServerResponse & response, const Context & context) const;
    void handleReportMetrics(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response) const;

    /// Finishes a stage-create: a commit with `assert-create` to a table that does not exist yet.
    void handleStagedCreateCommit(
        const IcebergRESTCatalogWarehouse & warehouse,
        const IcebergNamespaceName & ns,
        const String & table,
        const Poco::JSON::Array & updates,
        HTTPServerResponse & response,
        const Context & context) const;

    /// Writes the first metadata file and registers the table. Removes the file if the table is not registered.
    [[nodiscard]] IcebergRESTCatalogWarehouse::CreateTableResult writeAndRegisterTable(
        const IcebergRESTCatalogWarehouse & warehouse,
        const IcebergNamespaceName & ns,
        const String & table,
        const IcebergTablePointer & pointer,
        const Poco::JSON::Object::Ptr & metadata,
        CompressionMethod compression_method) const;

    /// One table of a commit after its requirements passed and its updates were applied in memory.
    struct PreparedTableCommit
    {
        IcebergNamespaceName ns;
        String table;
        IcebergTablePointer current;
        Poco::JSON::Object::Ptr metadata;
        CompressionMethod compression_method = CompressionMethod::None;
        /// Empty when `updates` was empty: nothing to write, nothing to move.
        String new_location;
    };

    /// Checks requirements and applies updates in memory. Sends the error response and returns nullopt on a client error.
    std::optional<PreparedTableCommit> prepareTableCommit(
        const IcebergRESTCatalogWarehouse & warehouse,
        const IcebergNamespaceName & ns,
        const String & table,
        const IcebergTablePointer & pointer,
        const Poco::JSON::Array & requirements,
        const Poco::JSON::Array & updates,
        HTTPServerResponse & response) const;

    /// Writes the new metadata files, then moves every table pointer in one Keeper multi with a version check.
    /// Returns true when the commit was applied. Otherwise removes the written files, sends the error response and returns false.
    bool commitPreparedTables(
        const IcebergRESTCatalogWarehouse & warehouse, const std::vector<PreparedTableCommit> & prepared, HTTPServerResponse & response) const;
    static void sendUpdateTableError(
        HTTPServerResponse & response, IcebergRESTCatalogWarehouse::UpdateTableResult result, const IcebergNamespaceName & ns, const String & table);

    static void checkNotReadonly(const Context & context, const String & action);
    static void checkDDLAllowed(const Context & context, const String & action);

    static std::optional<IcebergNamespaceName> getNamespaceOrSendNotFound(
        const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, HTTPServerResponse & response);

    [[nodiscard]] Poco::JSON::Object::Ptr readTableMetadata(const IcebergRESTCatalogWarehouse & warehouse, const IcebergTablePointer & pointer) const;

    /// A staged table has no metadata file yet, so `metadata_location` is empty.
    static void sendLoadTableResult(const std::optional<String> & metadata_location, const Poco::JSON::Object::Ptr & metadata, HTTPServerResponse & response);
    static void sendCommitTableResult(const String & metadata_location, const Poco::JSON::Object::Ptr & metadata, HTTPServerResponse & response);
    /// Maps `NOT_IMPLEMENTED` to `UnsupportedOperationException` and other codes to `BadRequestException`.
    static void sendCommitRequestError(HTTPServerResponse & response, int code, const String & message);

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
    IcebergRESTCatalogWarehouseRegistryPtr warehouses;
};

}
