#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterLSQuery.h>

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

BlockIO InterpreterLSQuery::execute()
{
    return executeQuery("SELECT _file FROM file('*', 'RawBlob')", getContext(), QueryFlags{ .internal = true }).second;
}

void registerInterpreterLSQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterLSQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterLSQuery", create_fn);
}

}

