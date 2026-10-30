#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{

/// Operations of the Iceberg REST catalog v1 API (RFC: issue #114697).
enum class IcebergRESTOperation
{
    GetConfig,
    ListNamespaces,
    CreateNamespace,
    LoadNamespace,
    NamespaceExists,
    DropNamespace,
    UpdateNamespaceProperties,
    ListTables,
    CreateTable,
    LoadTable,
    TableExists,
    UpdateTable,
    DropTable,
    RenameTable,
    RegisterTable,
    UnregisterTable,
    ReportMetrics,
    CommitTransaction,
    /// The operations below are recognized so that they answer 406 instead of 404, but there are no plans to support them.
    GetToken,
    LoadCredentials,
    SignRequest,
    PlanTableScan,
    FetchPlanningResult,
    CancelPlanning,
    FetchScanTasks,
    ListViews,
    CreateView,
    LoadView,
    ReplaceView,
    DropView,
    ViewExists,
    RenameView,
    RegisterView,
    ListFunctions,
    LoadFunction,
    /// Warehouse management API (issue #127488). Not part of the Iceberg spec.
    CreateWarehouse,
    ListWarehouses,
    GetWarehouse,
    ReplaceWarehouseCredential,
    DeleteWarehouse,
};

std::string toString(IcebergRESTOperation operation);

struct IcebergRESTRoute
{
    /// HTTP_GET, HTTP_POST, etc.
    std::string method;
    /// Path segments
    std::vector<std::string> pattern;
    IcebergRESTOperation operation;
    bool implemented;
};

struct IcebergRESTRouteMatch
{
    const IcebergRESTRoute * route = nullptr;
    /// Captured placeholders: "prefix", "namespace", "table", "warehouse". Values are percent-decoded.
    std::unordered_map<std::string, std::string> path_params;
};

/// Routes of the Iceberg REST catalog v1 API. These are advertised in `GET /v1/config`.
const std::vector<IcebergRESTRoute> & getIcebergRESTRoutes();
/// Routes under `/management/v1`, served on the same port.
const std::vector<IcebergRESTRoute> & getIcebergRESTManagementRoutes();

/// Matches the catalog routes and the management routes. Returns nullopt when no route matches.
std::optional<IcebergRESTRouteMatch> matchIcebergRESTRoute(const std::string & method, const std::vector<std::string> & segments);

}
