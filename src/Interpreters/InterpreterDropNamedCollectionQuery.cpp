#include <Interpreters/InterpreterDropNamedCollectionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Common/NamedCollections/NamedCollectionUtils.h>


namespace DB
{

BlockIO InterpreterDropNamedCollectionQuery::execute()
{
    auto current_context = getContext();
    const auto & query = query_ptr->as<const ASTDropNamedCollectionQuery &>();

    current_context->checkAccess(AccessType::DROP_NAMED_COLLECTION, query.collection_name);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    NamedCollectionUtils::removeFromSQL(query, current_context);
    return {};
}

}
