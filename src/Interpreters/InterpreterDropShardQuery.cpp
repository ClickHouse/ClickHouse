#include <Interpreters/InterpreterDropShardQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTDropShardQuery.h>

namespace DB
{

BlockIO InterpreterDropShardQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query->as<ASTDropShardQuery &>();

    current_context->checkAccess(AccessType::DROP_SHARD);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    ClusterFactory::instance().dropShard(query.shard_name, query.if_exists);
    return {};
}

void registerInterpreterDropShardQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterDropShardQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterDropShardQuery>(args.query, args.context); });
}

}
