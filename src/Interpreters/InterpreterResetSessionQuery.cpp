#include <Interpreters/InterpreterResetSessionQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTResetSessionQuery.h>


namespace DB
{

BlockIO InterpreterResetSessionQuery::execute()
{
    getContext()->getSessionContext()->resetToUserDefaults();
    return {};
}

void registerInterpreterResetSessionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterResetSessionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterResetSessionQuery", create_fn);
}

}
