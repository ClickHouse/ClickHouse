#pragma once

#include <IO/CompressionMethod.h>
#include <Interpreters/Context_fwd.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogRouter.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseRegistry.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>
#include <Common/logger_useful.h>

#include <Poco/JSON/Object.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>

namespace DB
{

struct IcebergRESTResponse
{
    Poco::Net::HTTPResponse::HTTPStatus status = Poco::Net::HTTPResponse::HTTP_OK;
    /// JSON text. Empty for a response without content.
    String body;
};

/// The Iceberg REST catalog v1 API (RFC: issue #114697) without the transport.
/// The caller passes the request as plain values.
class IcebergRESTCatalogService : private WithContext
{
public:
    IcebergRESTCatalogService(ContextPtr global_context_, IcebergRESTCatalogWarehouseRegistryPtr warehouses_);

    /// Runs one request. `query_context` holds the authenticated user of an external client, and the catalog checks it.
    /// It is null for a caller inside the server, where ClickHouse has already checked the query.
    /// `request_uri` is the path with the query string, for example `/v1/config?warehouse=name`.
    /// A failed request is returned as an error response.
    IcebergRESTResponse execute(const String & method, const String & request_uri, const String & body, const Context * query_context) const;

    static IcebergRESTResponse makeError(Poco::Net::HTTPResponse::HTTPStatus status, const String & type, const String & message);

    /// Logs the current exception and maps it to an error response. Call it inside a `catch` block.
    IcebergRESTResponse makeErrorFromCurrentException() const;

private:
    IcebergRESTResponse handleGetConfig(const Poco::URI & uri) const;
    IcebergRESTResponse handleListNamespaces(const IcebergRESTCatalogWarehouse & warehouse, const Poco::URI & uri) const;
    IcebergRESTResponse handleCreateNamespace(const IcebergRESTCatalogWarehouse & warehouse, const String & body, const Context * query_context) const;
    IcebergRESTResponse handleLoadNamespace(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match) const;
    IcebergRESTResponse handleNamespaceExists(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match) const;
    IcebergRESTResponse handleUpdateNamespaceProperties(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, const String & body, const Context * query_context) const;

    IcebergRESTResponse handleListTables(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match) const;
    IcebergRESTResponse handleCreateTable(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, const String & body, const Context * query_context) const;
    IcebergRESTResponse handleLoadTable(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match) const;
    IcebergRESTResponse handleTableExists(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match) const;
    IcebergRESTResponse handleDropTable(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, const Poco::URI & uri, const Context * query_context) const;
    IcebergRESTResponse handleUpdateTable(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match, const String & body, const Context * query_context) const;
    IcebergRESTResponse handleRenameTable(const IcebergRESTCatalogWarehouse & warehouse, const String & body, const Context * query_context) const;
    IcebergRESTResponse handleReportMetrics(const IcebergRESTCatalogWarehouse & warehouse, const IcebergRESTRouteMatch & match) const;

    /// Writes the new metadata file and moves the table pointer to it with a version check.
    IcebergRESTResponse commitTableMetadata(
        const IcebergRESTCatalogWarehouse & warehouse,
        const IcebergNamespaceName & ns,
        const String & table,
        const IcebergTablePointer & pointer,
        const Poco::JSON::Object::Ptr & metadata,
        CompressionMethod compression_method) const;

    static void checkNotReadonly(const Context * query_context, const String & action);
    static void checkDDLAllowed(const Context * query_context, const String & action);

    Poco::JSON::Object::Ptr readTableMetadata(const IcebergRESTCatalogWarehouse & warehouse, const IcebergTablePointer & pointer) const;

    static IcebergRESTResponse makeLoadTableResult(const String & metadata_location, const Poco::JSON::Object::Ptr & metadata);
    static IcebergRESTResponse makeCommitTableResult(const String & metadata_location, const Poco::JSON::Object::Ptr & metadata);
    /// Maps `NOT_IMPLEMENTED` to `UnsupportedOperationException` and other codes to `BadRequestException`.
    static IcebergRESTResponse makeCommitRequestError(int code, const String & message);

    static IcebergRESTResponse makeJSON(const Poco::JSON::Object & json, Poco::Net::HTTPResponse::HTTPStatus status);
    static IcebergRESTResponse makeNoContent();
    static IcebergRESTResponse makeNoSuchNamespace(const IcebergNamespaceName & ns);
    static IcebergRESTResponse makeNoSuchTable(const IcebergNamespaceName & ns, const String & table);

    LoggerPtr log;
    IcebergRESTCatalogWarehouseRegistryPtr warehouses;
};

using IcebergRESTCatalogServicePtr = std::shared_ptr<const IcebergRESTCatalogService>;

}
