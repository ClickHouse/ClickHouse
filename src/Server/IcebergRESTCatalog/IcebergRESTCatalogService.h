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

#include <expected>
#include <optional>
#include <vector>

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
    [[nodiscard]] IcebergRESTResponse handleExternalRequest(const IcebergRESTRequest & request, const Context & user_context) const;

    static IcebergRESTResponse makeIcebergError(Poco::Net::HTTPResponse::HTTPStatus status, const String & type, const String & message);

    /// Call it inside a `catch` block.
    [[nodiscard]] IcebergRESTResponse convertCurrentExceptionToIcebergError() const;

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
        [[nodiscard]] IcebergNamespaceName namespaceFromURL() const;
        [[nodiscard]] const String & tableFromURL() const;
    };

    [[nodiscard]] IcebergRESTResponse handleExternalRequestImpl(const IcebergRESTRequest & request, const Context & user_context) const;

    /// Config.
    [[nodiscard]] IcebergRESTResponse handleGetConfig(const Poco::URI & uri) const;

    /// Namespaces.
    [[nodiscard]] IcebergRESTResponse handleListNamespaces(const RoutedRequest & request) const;
    [[nodiscard]] IcebergRESTResponse handleCreateNamespace(const RoutedRequest & request) const;
    [[nodiscard]] IcebergRESTResponse handleLoadNamespace(const RoutedRequest & request) const;
    [[nodiscard]] IcebergRESTResponse handleNamespaceExists(const RoutedRequest & request) const;
    [[nodiscard]] IcebergRESTResponse handleUpdateNamespaceProperties(const RoutedRequest & request) const;

    /// Tables.
    [[nodiscard]] IcebergRESTResponse handleListTables(const RoutedRequest & request) const;
    [[nodiscard]] IcebergRESTResponse handleCreateTable(const RoutedRequest & request) const;
    [[nodiscard]] IcebergRESTResponse handleLoadTable(const RoutedRequest & request) const;
    [[nodiscard]] IcebergRESTResponse handleTableExists(const RoutedRequest & request) const;
    [[nodiscard]] IcebergRESTResponse handleDropTable(const RoutedRequest & request) const;
    [[nodiscard]] IcebergRESTResponse handleUpdateTable(const RoutedRequest & request) const;
    [[nodiscard]] IcebergRESTResponse handleCommitTransaction(const RoutedRequest & request) const;
    [[nodiscard]] IcebergRESTResponse handleRenameTable(const RoutedRequest & request) const;
    [[nodiscard]] IcebergRESTResponse handleReportMetrics(const RoutedRequest & request) const;

    /// Warehouse management API (issue #127488). Each handler checks same grant as its `ICEBERG WAREHOUSE` statement.
    /// TODO(asya): Behavior change. A SQL-created user without these grants now gets 403. Confirm and mention in changelog.
    [[nodiscard]] IcebergRESTResponse handleCreateWarehouse(const String & body, const Context & user_context) const;
    [[nodiscard]] IcebergRESTResponse handleListWarehouses(const Context & user_context) const;
    [[nodiscard]] IcebergRESTResponse handleGetWarehouse(const IcebergRESTRouteMatch & match, const Context & user_context) const;
    [[nodiscard]] IcebergRESTResponse handleReplaceWarehouseCredential(
        const IcebergRESTRouteMatch & match, const String & body, const Context & user_context) const;
    [[nodiscard]] IcebergRESTResponse handleDeleteWarehouse(const IcebergRESTRouteMatch & match, const Context & user_context) const;

    /// Finishes a stage-create: a commit with `assert-create` to a table that does not exist yet.
    [[nodiscard]] IcebergRESTResponse handleStagedCreateCommit(
        const RoutedRequest & request, const IcebergNamespaceName & ns, const String & table, const Poco::JSON::Array & updates) const;

    /// Reads the specified `metadata.json` file from object storage.
    [[nodiscard]] Poco::JSON::Object::Ptr readTableMetadata(const IcebergRESTCatalogWarehouse & warehouse, const IcebergTablePointer & pointer) const;

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

    /// Checks requirements and applies updates in memory. Returns the error response on a client error.
    [[nodiscard]] std::expected<PreparedTableCommit, IcebergRESTResponse> prepareTableCommit(
        const IcebergRESTCatalogWarehouse & warehouse,
        const IcebergNamespaceName & ns,
        const String & table,
        const IcebergTablePointer & pointer,
        const Poco::JSON::Array & requirements,
        const Poco::JSON::Array & updates) const;

    /// Writes the new metadata files, then moves every table pointer in one Keeper multi with a version check.
    /// Returns nullopt when the commit was applied. Otherwise removes the written files and returns the error response.
    [[nodiscard]] std::optional<IcebergRESTResponse> commitPreparedTables(
        const IcebergRESTCatalogWarehouse & warehouse, const std::vector<PreparedTableCommit> & prepared) const;

    LoggerPtr log;
    IcebergRESTCatalogWarehouseRegistryPtr warehouses;
};

using IcebergRESTCatalogServicePtr = std::shared_ptr<const IcebergRESTCatalogService>;

}
