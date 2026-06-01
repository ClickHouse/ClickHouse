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

    /// No-op over the PostgreSQL and MySQL wire protocols. PostgreSQL keeps the
    /// `pg_*` compatibility views as session temp tables that a real reset would
    /// clear without re-creating (re-init lives in the handler), breaking later
    /// metadata queries. MySQL keeps protocol-local state (e.g. the handshake
    /// database and prepared statements) the session context does not own, so a
    /// reset would move the session off its post-handshake baseline. Skip
    /// entirely on both rather than half-reset. Documented.
    if (interface == ClientInfo::Interface::POSTGRESQL
        || interface == ClientInfo::Interface::MYSQL)
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

/// Declared in `registerInterpreters.cpp`; repeated here so the definition has a
/// visible prototype when this file is compiled outside the unity build.
void registerInterpreterResetSessionQuery(InterpreterFactory & factory);

void registerInterpreterResetSessionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterResetSessionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterResetSessionQuery", create_fn);
}

}
