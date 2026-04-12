#include <Interpreters/InterpreterCreateReplicaQuery.h>
#include <Interpreters/InterpreterCreateNamedCollectionQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Access/ContextAccess.h>
#include <Common/Clusters/SQLClusterCatalogPropertyValidation.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTCreateReplicaQuery.h>


namespace DB
{

BlockIO InterpreterCreateReplicaQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & replica_query = updated_query->as<const ASTCreateReplicaQuery &>();

    current_context->checkAccess(AccessType::CREATE_NAMED_COLLECTION, replica_query.replica_name);

    validateReplicaLevelPropertiesForSqlReplica(replica_query.properties);

    if (!replica_query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    auto named_collection_ast = make_intrusive<ASTCreateNamedCollectionQuery>();
    named_collection_ast->collection_name = replica_query.replica_name;
    named_collection_ast->changes = replica_query.properties;
    named_collection_ast->if_not_exists = replica_query.if_not_exists;

    return InterpreterCreateNamedCollectionQuery(named_collection_ast, getContext()).execute();
}

void registerInterpreterCreateReplicaQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterCreateReplicaQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterCreateReplicaQuery>(args.query, args.context); });
}

}
