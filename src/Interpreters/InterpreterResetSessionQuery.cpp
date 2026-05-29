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

    /// `RESET SESSION` is a no-op over the PostgreSQL wire protocol. The
    /// PostgreSQL handler lazily creates the `pg_*` compatibility views as
    /// session temporary tables, and `resetToUserDefaults` would clear that
    /// session mapping without re-creating them (re-initialization lives in
    /// the handler, which this PR intentionally leaves untouched). A real
    /// reset would therefore leave later driver metadata queries such as
    /// `SELECT * FROM pg_type` failing with `UNKNOWN_TABLE`. Until per-protocol
    /// cleanup is implemented, skip the reset entirely on this interface rather
    /// than half-resetting it. Documented on the statement page.
    if (interface == ClientInfo::Interface::POSTGRESQL)
        return {};

    /// gRPC requests without `session_id` build a query context straight from
    /// the global context — no session, nothing session-scoped to reset.
    /// Treat that one specific case as a no-op, matching the documented
    /// already-clean-session behaviour. Any other interface that lacks a
    /// session context here is a real bug (HTTP/TCP/MySQL/Arrow Flight all
    /// call `makeSessionContext` before dispatching queries), so let the
    /// throwing `getSessionContext` accessor below fire.
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
