#include <Interpreters/InterpreterCreateClusterQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterFactory.h>
#include <Common/Clusters/SQLClusterCatalogPropertyValidation.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateClusterQuery.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CLUSTER_DEFINITION_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsInt64 distributed_ddl_task_timeout;
}

BlockIO InterpreterCreateClusterQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query->as<ASTCreateClusterQuery &>();

    current_context->checkAccess(AccessType::CREATE_CLUSTER);

    String cluster_secret;
    bool allow_distributed_ddl_queries = true;
    validateAndExtractClusterLevelProperties(query.cluster_properties, cluster_secret, allow_distributed_ddl_queries);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        ContextPtr ddl_context = current_context;
        ContextMutablePtr ddl_context_override;
        if (query.sync && current_context->getSettingsRef()[Setting::distributed_ddl_task_timeout] == 0)
        {
            ddl_context_override = Context::createCopy(current_context);
            ddl_context_override->setSetting("distributed_ddl_task_timeout", Field{Int64{180}});
            ddl_context = ddl_context_override;
        }
        return executeDDLQueryOnCluster(updated_query, ddl_context, params);
    }

    auto global_context = current_context->getGlobalContext();

    if (global_context->isClusterDefinedOnlyInRemoteServers(query.cluster_name))
    {
        if (!query.if_not_exists)
            throw Exception(
                ErrorCodes::CLUSTER_DEFINITION_ALREADY_EXISTS,
                "Cannot create SQL CLUSTER {} because a cluster with the same name is already defined in the server configuration (remote_servers)",
                backQuoteIfNeed(query.cluster_name));
        return {};
    }

    if (query.if_not_exists && ClusterFactory::instance().hasCluster(query.cluster_name))
        return {};

    ClusterFactory::instance().createCluster(
        query.cluster_name, query.members, cluster_secret, allow_distributed_ddl_queries);

    auto cluster = ClusterFactory::instance().tryMaterializeCluster(query.cluster_name, global_context);
    if (!cluster)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to materialize SQL CLUSTER `{}` after create", query.cluster_name);

    global_context->setCluster(query.cluster_name, cluster);
    return {};
}

void registerInterpreterCreateClusterQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterCreateClusterQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterCreateClusterQuery>(args.query, args.context); });
}

}
