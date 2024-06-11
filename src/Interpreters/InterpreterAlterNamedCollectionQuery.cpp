#include <Interpreters/InterpreterAlterNamedCollectionQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>


namespace DB
{

BlockIO InterpreterAlterNamedCollectionQuery::execute()
{
    auto current_context = getContext();
    const auto & query = query_ptr->as<const ASTAlterNamedCollectionQuery &>();

    current_context->checkAccess(AccessType::ALTER_NAMED_COLLECTION, query.collection_name);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }

    NamedCollectionFactory::instance().updateFromSQL(query);
    return {};
}

void registerInterpreterAlterNamedCollectionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterAlterNamedCollectionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterAlterNamedCollectionQuery", create_fn);
}

}
