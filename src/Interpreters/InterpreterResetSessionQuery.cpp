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
    const auto interface = query_context->getClientInfo().interface;

    /// No-op over the PostgreSQL protocol: the handler keeps the `pg_*`
    /// compatibility views as session temp tables, and a real reset would clear
    /// them without re-creating them (re-init lives in the handler), breaking
    /// later metadata queries. Skip entirely rather than half-reset. Documented.
    if (interface == ClientInfo::Interface::POSTGRESQL)
        return {};

    /// gRPC without `session_id` has no session context to reset, so no-op
    /// (matches already-clean-session behaviour). For any other interface a
    /// missing session context is a bug — let `getSessionContext` below throw.
    if (!query_context->hasSessionContext()
        && interface == ClientInfo::Interface::GRPC)
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
