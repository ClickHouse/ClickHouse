#include <Interpreters/InterpreterAlterHandlerQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTAlterHandlerQuery.h>
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

BlockIO InterpreterAlterHandlerQuery::execute()
{
    auto current_context = getContext();

    if (!current_context->getSettingsRef()[Setting::allow_experimental_sql_handlers])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SQL-based HTTP handler management is experimental. Set `allow_experimental_sql_handlers = 1` to enable it");

    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTAlterHandlerQuery &>();

    current_context->checkAccess(AccessType::ALTER_HANDLER);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    CustomHandlersFactory::instance().alter(query);
    return {};
}

void registerInterpreterAlterHandlerQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterAlterHandlerQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterAlterHandlerQuery", create_fn);
}

}
