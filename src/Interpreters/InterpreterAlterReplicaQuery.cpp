#include <Interpreters/InterpreterAlterReplicaQuery.h>

#include <Access/ContextAccess.h>
#include <Common/Clusters/SQLClusterCatalogPropertyValidation.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterAlterNamedCollectionQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTAlterReplicaQuery.h>
#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NAMED_COLLECTION_DOESNT_EXIST;
}

BlockIO InterpreterAlterReplicaQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTAlterReplicaQuery &>();

    current_context->checkAccess(AccessType::ALTER_NAMED_COLLECTION, query.replica_name);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    const auto collection = NamedCollectionFactory::instance().tryGet(query.replica_name);
    if (!collection)
    {
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "Cannot alter replica `{}`, because it does not exist",
            query.replica_name);
    }

    if (!boost::iequals(collection->getCollectionType(), "REPLICA"))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Named collection `{}` is not TYPE `REPLICA`, cannot use ALTER REPLICA",
            query.replica_name);
    }

    /// Enforce the same allowed-key set as `CREATE REPLICA` so `ALTER REPLICA` cannot smuggle keys that
    /// would be rejected at creation time. This preserves the SQL-replica property model invariants that
    /// shard materialisation relies on. Does not prevent `ALTER NAMED COLLECTION` with the same target —
    /// that path deliberately bypasses SQL-cluster semantics.
    validateReplicaLevelPropertyKeys(query.properties);

    auto alter_named_collection_ast = make_intrusive<ASTAlterNamedCollectionQuery>();
    alter_named_collection_ast->collection_name = query.replica_name;
    alter_named_collection_ast->changes = query.properties;
    return InterpreterAlterNamedCollectionQuery(alter_named_collection_ast, getContext()).execute();
}

void registerInterpreterAlterReplicaQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterAlterReplicaQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterAlterReplicaQuery>(args.query, args.context); });
}

}
