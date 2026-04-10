#include <Interpreters/InterpreterCreateClusterQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterFactory.h>
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

BlockIO InterpreterCreateClusterQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query->as<ASTCreateClusterQuery &>();

    current_context->checkAccess(AccessType::CREATE_CLUSTER);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
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

    ClusterFactory::instance().createCluster(query.cluster_name, query.members);

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
