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
    return executeQuery("SELECT * FROM system.processes ORDER BY elapsed DESC", getContext(), QueryFlags{ .internal = true }).second;
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
