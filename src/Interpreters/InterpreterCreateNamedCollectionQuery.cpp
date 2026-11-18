#include <Interpreters/InterpreterCreateNamedCollectionQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>


namespace DB
{

BlockIO InterpreterCreateNamedCollectionQuery::execute()
{
    auto current_context = getContext();

    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTCreateNamedCollectionQuery &>();

    current_context->checkAccess(AccessType::CREATE_NAMED_COLLECTION, query.collection_name);
    if (query.or_replace)
        current_context->checkAccess(AccessType::DROP_NAMED_COLLECTION, query.collection_name);

    /// `max_named_collection_num_to_throw` is enforced by `NamedCollectionFactory::createFromSQL`,
    /// under the factory mutex, so that every path adding a new collection is covered - in
    /// particular RESTORE, which calls the factory directly.

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    NamedCollectionFactory::instance().createFromSQL(query);

    return {};
}

void registerInterpreterCreateNamedCollectionQuery(InterpreterFactory & factory);
void registerInterpreterCreateNamedCollectionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateNamedCollectionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateNamedCollectionQuery", create_fn);
}

}
