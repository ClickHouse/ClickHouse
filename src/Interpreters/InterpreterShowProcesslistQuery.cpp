#include <IO/ReadBufferFromString.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterShowProcesslistQuery.h>

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

BlockIO InterpreterShowProcesslistQuery::execute()
{
    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");

    BlockIO io = executeQuery("SELECT * FROM system.processes ORDER BY elapsed DESC", query_context, QueryFlags{ .internal = true }).second;
    io.context_holder = std::move(query_context);
    return io;
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
