#include <Interpreters/InterpreterDropHandlerQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTDropHandlerQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Common/SQLDefinedHandlers/SQLDefinedHandlersFactory.h>


namespace DB
{

BlockIO InterpreterDropHandlerQuery::execute()
{
    auto current_context = getContext();

    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTDropHandlerQuery &>();

    current_context->checkAccess(AccessType::DROP_HANDLER);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    SQLDefinedHandlersFactory::instance().removeFromSQL(query);

    return {};
}

void registerInterpreterDropHandlerQuery(InterpreterFactory & factory);
void registerInterpreterDropHandlerQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropHandlerQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropHandlerQuery", create_fn);
}

}
