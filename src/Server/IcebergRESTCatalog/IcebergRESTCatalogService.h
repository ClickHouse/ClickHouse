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

struct IcebergRESTRequest
{
    String method;
    /// The path with the query string, for example `/v1/config?warehouse=name`.
    String uri;
    /// JSON text.
    String body;
};

struct IcebergRESTResponse
{
    Poco::Net::HTTPResponse::HTTPStatus status = Poco::Net::HTTPResponse::HTTP_OK;
    /// JSON text.
    String body;
};

/// The logic of the Iceberg REST catalog v1 API (RFC: issue #114697).
/// TODO: Call it directly from `catalog_type = 'clickhouse'`, without HTTP.
class IcebergRESTCatalogService : private WithContext
{
public:
    IcebergRESTCatalogService(ContextPtr global_context_, IcebergRESTCatalogWarehouseRegistryPtr warehouses_);

    /// Runs one request of an external client. The catalog checks the user of `user_context`.
    IcebergRESTResponse handleExternalRequest(const IcebergRESTRequest & request, const Context & user_context) const;

    static IcebergRESTResponse makeIcebergError(Poco::Net::HTTPResponse::HTTPStatus status, const String & type, const String & message);

    /// Call it inside a `catch` block.
    IcebergRESTResponse convertCurrentExceptionToIcebergError() const;

private:
    /// A request after routing. The path prefix selected `warehouse`.
    struct RoutedRequest
    {
        const IcebergRESTCatalogWarehouse & warehouse;
        const IcebergRESTRouteMatch & match;
        const Poco::URI & uri;
        const String & body;
        const Context & user_context;

        /// Names from the URL path: `/v1/{prefix}/namespaces/{namespace}/tables/{table}`.
        IcebergNamespaceName namespaceFromURL() const;
        const String & tableFromURL() const;
    };

    IcebergRESTResponse handleExternalRequestImpl(const IcebergRESTRequest & request, const Context & user_context) const;

    /// Config.
    IcebergRESTResponse handleGetConfig(const Poco::URI & uri) const;

    /// Namespaces.
    IcebergRESTResponse handleListNamespaces(const RoutedRequest & request) const;
    IcebergRESTResponse handleCreateNamespace(const RoutedRequest & request) const;
    IcebergRESTResponse handleLoadNamespace(const RoutedRequest & request) const;
    IcebergRESTResponse handleNamespaceExists(const RoutedRequest & request) const;
    IcebergRESTResponse handleUpdateNamespaceProperties(const RoutedRequest & request) const;

    /// Tables.
    IcebergRESTResponse handleListTables(const RoutedRequest & request) const;
    IcebergRESTResponse handleCreateTable(const RoutedRequest & request) const;
    IcebergRESTResponse handleLoadTable(const RoutedRequest & request) const;
    IcebergRESTResponse handleTableExists(const RoutedRequest & request) const;
    IcebergRESTResponse handleDropTable(const RoutedRequest & request) const;
    IcebergRESTResponse handleUpdateTable(const RoutedRequest & request) const;
    IcebergRESTResponse handleRenameTable(const RoutedRequest & request) const;
    IcebergRESTResponse handleReportMetrics(const RoutedRequest & request) const;

    /// Reads the specified `metadata.json` file from object storage.
    [[nodiscard]] Poco::JSON::Object::Ptr readTableMetadata(const IcebergRESTCatalogWarehouse & warehouse, const IcebergTablePointer & pointer) const;

    /// Writes the next `metadata.json` file to object storage. Then points the table to it in Keeper, with a version check.
    IcebergRESTResponse commitTableMetadata(
        const IcebergRESTCatalogWarehouse & warehouse,
        const IcebergNamespaceName & ns,
        const String & table,
        const IcebergTablePointer & pointer,
        const Poco::JSON::Object::Ptr & metadata,
        CompressionMethod compression_method) const;

    LoggerPtr log;
    IcebergRESTCatalogWarehouseRegistryPtr warehouses;
};

using IcebergRESTCatalogServicePtr = std::shared_ptr<const IcebergRESTCatalogService>;

}
