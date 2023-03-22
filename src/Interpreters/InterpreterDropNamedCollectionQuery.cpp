#include <Interpreters/InterpreterDropNamedCollectionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Storages/NamedCollectionUtils.h>


namespace DB
{

BlockIO InterpreterDropNamedCollectionQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::DROP_NAMED_COLLECTION);

    const auto & query = query_ptr->as<const ASTDropNamedCollectionQuery &>();
    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    if (query.if_exists)
        NamedCollectionUtils::removeIfExistsFromSQL(query.collection_name, current_context);
    else
        NamedCollectionUtils::removeFromSQL(query.collection_name, current_context);

    return {};
}

}
