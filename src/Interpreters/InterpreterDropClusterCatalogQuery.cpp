#include <Interpreters/InterpreterDropClusterCatalogQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTDropClusterCatalogQuery.h>

namespace DB
{

BlockIO InterpreterDropClusterCatalogQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query->as<ASTDropClusterCatalogQuery &>();

    const bool is_cluster = query.kind == ASTDropClusterCatalogQuery::Kind::Cluster;
    current_context->checkAccess(is_cluster ? AccessType::DROP_CLUSTER : AccessType::DROP_SHARD);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    if (is_cluster)
        ClusterFactory::instance().dropCluster(query.name, query.if_exists);
    else
        ClusterFactory::instance().dropShard(query.name, query.if_exists);
    return {};
}

void registerInterpreterDropClusterCatalogQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterDropClusterCatalogQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterDropClusterCatalogQuery>(args.query, args.context); });
}

}
