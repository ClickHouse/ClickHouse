#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterShowPrivilegesQuery.h>
#include <Interpreters/executeQuery.h>


namespace DB
{
InterpreterShowPrivilegesQuery::InterpreterShowPrivilegesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : query_ptr(query_ptr_), context(context_)
{
}


BlockIO InterpreterShowPrivilegesQuery::execute()
{
    return executeQuery("SELECT * FROM system.privileges", context, QueryFlags{ .internal = true }).second;
}

void registerInterpreterShowPrivilegesQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowPrivilegesQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowPrivilegesQuery", create_fn);
}


}
