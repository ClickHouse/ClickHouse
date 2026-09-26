#include <Server/IcebergRESTCatalog/IcebergRESTCatalogRouter.h>

#include <Poco/Net/HTTPRequest.h>

#include <base/EnumReflection.h>

namespace DB
{

std::string toString(IcebergRESTOperation operation)
{
    return std::string(magic_enum::enum_name(operation));
}

const std::vector<IcebergRESTRoute> & getIcebergRESTRoutes()
{
    using Poco::Net::HTTPRequest;

    /// Fully-literal routes go before parameterized siblings so that literal segments win.
    static const std::vector<IcebergRESTRoute> routes =
    {
        {HTTPRequest::HTTP_GET, {"v1", "config"}, IcebergRESTOperation::GetConfig, true},
        {HTTPRequest::HTTP_POST, {"v1", "oauth", "tokens"}, IcebergRESTOperation::GetToken, false},

        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "tables", "rename"}, IcebergRESTOperation::RenameTable, false},
        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "transactions", "commit"}, IcebergRESTOperation::CommitTransaction, false},
        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "views", "rename"}, IcebergRESTOperation::RenameView, false},

        {HTTPRequest::HTTP_GET, {"v1", "{prefix}", "namespaces"}, IcebergRESTOperation::ListNamespaces, true},
        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "namespaces"}, IcebergRESTOperation::CreateNamespace, true},

        {HTTPRequest::HTTP_GET, {"v1", "{prefix}", "namespaces", "{namespace}"}, IcebergRESTOperation::LoadNamespace, false},
        {HTTPRequest::HTTP_HEAD, {"v1", "{prefix}", "namespaces", "{namespace}"}, IcebergRESTOperation::NamespaceExists, true},
        {HTTPRequest::HTTP_DELETE, {"v1", "{prefix}", "namespaces", "{namespace}"}, IcebergRESTOperation::DropNamespace, false},
        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "namespaces", "{namespace}", "properties"}, IcebergRESTOperation::UpdateNamespaceProperties, false},

        {HTTPRequest::HTTP_GET, {"v1", "{prefix}", "namespaces", "{namespace}", "tables"}, IcebergRESTOperation::ListTables, false},
        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "namespaces", "{namespace}", "tables"}, IcebergRESTOperation::CreateTable, false},

        {HTTPRequest::HTTP_GET, {"v1", "{prefix}", "namespaces", "{namespace}", "tables", "{table}"}, IcebergRESTOperation::LoadTable, false},
        {HTTPRequest::HTTP_HEAD, {"v1", "{prefix}", "namespaces", "{namespace}", "tables", "{table}"}, IcebergRESTOperation::TableExists, false},
        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "namespaces", "{namespace}", "tables", "{table}"}, IcebergRESTOperation::UpdateTable, false},
        {HTTPRequest::HTTP_DELETE, {"v1", "{prefix}", "namespaces", "{namespace}", "tables", "{table}"}, IcebergRESTOperation::DropTable, false},

        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "namespaces", "{namespace}", "register"}, IcebergRESTOperation::RegisterTable, false},
        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "namespaces", "{namespace}", "tables", "{table}", "unregister"}, IcebergRESTOperation::UnregisterTable, false},
        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "namespaces", "{namespace}", "tables", "{table}", "metrics"}, IcebergRESTOperation::ReportMetrics, false},
        {HTTPRequest::HTTP_GET, {"v1", "{prefix}", "namespaces", "{namespace}", "tables", "{table}", "credentials"}, IcebergRESTOperation::LoadCredentials, false},
        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "namespaces", "{namespace}", "tables", "{table}", "sign"}, IcebergRESTOperation::SignRequest, false},

        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "namespaces", "{namespace}", "tables", "{table}", "plan"}, IcebergRESTOperation::PlanTableScan, false},
        {HTTPRequest::HTTP_GET, {"v1", "{prefix}", "namespaces", "{namespace}", "tables", "{table}", "plan", "{plan-id}"}, IcebergRESTOperation::FetchPlanningResult, false},
        {HTTPRequest::HTTP_DELETE, {"v1", "{prefix}", "namespaces", "{namespace}", "tables", "{table}", "plan", "{plan-id}"}, IcebergRESTOperation::CancelPlanning, false},
        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "namespaces", "{namespace}", "tables", "{table}", "tasks"}, IcebergRESTOperation::FetchScanTasks, false},

        {HTTPRequest::HTTP_GET, {"v1", "{prefix}", "namespaces", "{namespace}", "views"}, IcebergRESTOperation::ListViews, false},
        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "namespaces", "{namespace}", "views"}, IcebergRESTOperation::CreateView, false},
        {HTTPRequest::HTTP_GET, {"v1", "{prefix}", "namespaces", "{namespace}", "views", "{view}"}, IcebergRESTOperation::LoadView, false},
        {HTTPRequest::HTTP_HEAD, {"v1", "{prefix}", "namespaces", "{namespace}", "views", "{view}"}, IcebergRESTOperation::ViewExists, false},
        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "namespaces", "{namespace}", "views", "{view}"}, IcebergRESTOperation::ReplaceView, false},
        {HTTPRequest::HTTP_DELETE, {"v1", "{prefix}", "namespaces", "{namespace}", "views", "{view}"}, IcebergRESTOperation::DropView, false},
        {HTTPRequest::HTTP_POST, {"v1", "{prefix}", "namespaces", "{namespace}", "register-view"}, IcebergRESTOperation::RegisterView, false},

        {HTTPRequest::HTTP_GET, {"v1", "{prefix}", "namespaces", "{namespace}", "functions"}, IcebergRESTOperation::ListFunctions, false},
        {HTTPRequest::HTTP_GET, {"v1", "{prefix}", "namespaces", "{namespace}", "functions", "{function}"}, IcebergRESTOperation::LoadFunction, false},
    };
    return routes;
}

std::optional<IcebergRESTRouteMatch> matchIcebergRESTRoute(const std::string & method, const std::vector<std::string> & segments)
{
    for (const auto & route : getIcebergRESTRoutes())
    {
        if (method != route.method || segments.size() != route.pattern.size())
            continue;

        IcebergRESTRouteMatch match;
        match.route = &route;

        bool matched = true;
        for (size_t i = 0; i < segments.size(); ++i)
        {
            const auto & pattern_segment = route.pattern[i];
            if (pattern_segment.starts_with('{'))
            {
                match.path_params[pattern_segment.substr(1, pattern_segment.size() - 2)] = segments[i];
            }
            else if (pattern_segment != segments[i])
            {
                matched = false;
                break;
            }
        }

        if (matched)
            return match;
    }
    return std::nullopt;
}

}
