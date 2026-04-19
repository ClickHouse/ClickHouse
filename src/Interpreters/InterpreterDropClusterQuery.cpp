#include <Interpreters/InterpreterDropClusterQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/ClusterFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTDropClusterQuery.h>

namespace DB
{

BlockIO InterpreterDropClusterQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query->as<ASTDropClusterQuery &>();

    current_context->checkAccess(AccessType::DROP_CLUSTER);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    ClusterFactory::instance().dropCluster(query.cluster_name, query.if_exists);
    return {};
}

void registerInterpreterDropClusterQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterDropClusterQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterDropClusterQuery>(args.query, args.context); });
}

}
