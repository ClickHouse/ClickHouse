#include <Interpreters/InterpreterCreateNamedCollectionQuery.h>

#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Storages/NamedCollectionUtils.h>


namespace DB
{

BlockIO InterpreterCreateNamedCollectionQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::CREATE_NAMED_COLLECTION);

    const auto & query = query_ptr->as<const ASTCreateNamedCollectionQuery &>();

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    NamedCollectionUtils::createFromSQL(query, current_context);
    return {};
}

}
