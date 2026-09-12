#include <IO/ReadBufferFromString.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterShowEngineQuery.h>

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

BlockIO InterpreterShowEnginesQuery::execute()
{
    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");

    return executeQuery("SELECT * FROM system.table_engines ORDER BY name", query_context, QueryFlags{ .internal = true }).second;
}

void registerInterpreterShowEnginesQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowEnginesQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowEnginesQuery", create_fn);
}

}
