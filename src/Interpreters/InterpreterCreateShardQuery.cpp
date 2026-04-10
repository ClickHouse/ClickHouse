#include <Interpreters/InterpreterCreateShardQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateShardQuery.h>

namespace DB
{

BlockIO InterpreterCreateShardQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query->as<ASTCreateShardQuery &>();

    current_context->checkAccess(AccessType::CREATE_SHARD);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    if (query.if_not_exists && ClusterFactory::instance().hasShard(query.shard_name))
        return {};

    ClusterFactory::instance().createShard(
        query.shard_name, query.replicas, query.weight, query.internal_replication);
    return {};
}

void registerInterpreterCreateShardQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterCreateShardQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterCreateShardQuery>(args.query, args.context); });
}

}
