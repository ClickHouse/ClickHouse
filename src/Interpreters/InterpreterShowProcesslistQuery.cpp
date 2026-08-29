#include <Core/Settings.h>

#include <IO/ReadBufferFromString.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterShowProcesslistQuery.h>

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool show_processlist_include_internal;
}

BlockIO InterpreterShowProcesslistQuery::execute()
{
    auto context = getContext();

    String query;
    if (context->getSettingsRef()[Setting::show_processlist_include_internal])
    {
        query = "SELECT * FROM system.processes ORDER BY elapsed DESC";
    }
    else
    {
        query = "SELECT * FROM system.processes WHERE is_internal = 0 ORDER BY elapsed DESC";
    }

    auto query_context = Context::createCopy(context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");

    return executeQuery(query, query_context, QueryFlags{ .internal = true }).second;
}

void registerInterpreterShowProcesslistQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowProcesslistQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowProcesslistQuery", create_fn);
}

}
