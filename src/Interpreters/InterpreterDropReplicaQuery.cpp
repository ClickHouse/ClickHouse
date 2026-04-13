#include <Interpreters/InterpreterDropReplicaQuery.h>

#include <Access/ContextAccess.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropNamedCollectionQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Parsers/ASTDropReplicaQuery.h>
#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NAMED_COLLECTION_DOESNT_EXIST;
}

BlockIO InterpreterDropReplicaQuery::execute()
{
    auto current_context = getContext();
    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTDropReplicaQuery &>();

    current_context->checkAccess(AccessType::DROP_NAMED_COLLECTION, query.replica_name);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    const auto collection = NamedCollectionFactory::instance().tryGet(query.replica_name);
    if (!collection)
    {
        if (query.if_exists)
            return {};
        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "Cannot drop replica `{}`, because it does not exist",
            query.replica_name);
    }

    if (!boost::iequals(collection->getCollectionType(), "REPLICA"))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Named collection `{}` is not TYPE `REPLICA`, cannot use DROP REPLICA",
            query.replica_name);
    }

    auto drop_named_collection_ast = make_intrusive<ASTDropNamedCollectionQuery>();
    drop_named_collection_ast->collection_name = query.replica_name;
    drop_named_collection_ast->if_exists = query.if_exists;
    return InterpreterDropNamedCollectionQuery(drop_named_collection_ast, getContext()).execute();
}

void registerInterpreterDropReplicaQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterDropReplicaQuery",
        [](const InterpreterFactory::Arguments & args)
        { return std::make_unique<InterpreterDropReplicaQuery>(args.query, args.context); });
}

}
