#include <Interpreters/InterpreterResetSessionQuery.h>

#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTResetSessionQuery.h>


namespace DB
{

BlockIO InterpreterResetSessionQuery::execute()
{
    auto query_context = getContext();

    /// gRPC requests without `session_id` build a query context straight from
    /// the global context — no session, nothing session-scoped to reset.
    /// Treat that one specific case as a no-op, matching the documented
    /// already-clean-session behaviour. Any other interface that lacks a
    /// session context here is a real bug (HTTP/TCP/MySQL/Postgres/Arrow
    /// Flight all call `makeSessionContext` before dispatching queries), so
    /// let the throwing `getSessionContext` accessor below fire.
    if (!query_context->hasSessionContext()
        && query_context->getClientInfo().interface == ClientInfo::Interface::GRPC)
        return {};

    query_context->getSessionContext()->resetToUserDefaults();
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
