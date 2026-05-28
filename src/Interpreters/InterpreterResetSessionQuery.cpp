#include <Interpreters/InterpreterResetSessionQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTResetSessionQuery.h>


namespace DB
{

BlockIO InterpreterResetSessionQuery::execute()
{
    /// gRPC requests without `session_id` (and other paths that build a
    /// query context directly from the global context) never create a
    /// session context: every bit of mutable state lives on the query
    /// context and dies with it. There is nothing to reset to a
    /// post-authentication baseline, so treat this as a no-op rather than
    /// throwing `THERE_IS_NO_SESSION`. This matches the documented
    /// "already-clean session" contract.
    if (!getContext()->hasSessionContext())
        return {};
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
