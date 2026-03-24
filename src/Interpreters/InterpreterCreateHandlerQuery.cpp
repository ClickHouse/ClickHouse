#include <Interpreters/InterpreterCreateHandlerQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTCreateHandlerQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Server/CustomHandlers/CustomHandlersFactory.h>
#include <Core/Settings.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_sql_handlers;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

BlockIO InterpreterCreateHandlerQuery::execute()
{
    auto current_context = getContext();

    if (!current_context->getSettingsRef()[Setting::allow_experimental_sql_handlers])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SQL-based HTTP handler management is experimental. Set `allow_experimental_sql_handlers = 1` to enable it");

    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTCreateHandlerQuery &>();

    current_context->checkAccess(AccessType::CREATE_HANDLER);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    CustomHandlersFactory::instance().create(query);

    return {};
}

void registerInterpreterCreateHandlerQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateHandlerQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateHandlerQuery", create_fn);
}

}
