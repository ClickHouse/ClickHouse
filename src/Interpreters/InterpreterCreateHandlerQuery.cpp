#include <Interpreters/InterpreterCreateHandlerQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTCreateHandlerQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Common/SQLDefinedHandlers/SQLDefinedHandlersFactory.h>


namespace DB
{

BlockIO InterpreterCreateHandlerQuery::execute()
{
    auto current_context = getContext();

    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTCreateHandlerQuery &>();

    current_context->checkAccess(query.is_alter ? AccessType::ALTER_HANDLER : AccessType::CREATE_HANDLER);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    if (query.is_alter)
        SQLDefinedHandlersFactory::instance().updateFromSQL(query);
    else
        SQLDefinedHandlersFactory::instance().createFromSQL(query);

    return {};
}

void registerInterpreterCreateHandlerQuery(InterpreterFactory & factory);
void registerInterpreterCreateHandlerQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateHandlerQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateHandlerQuery", create_fn);
}

}
