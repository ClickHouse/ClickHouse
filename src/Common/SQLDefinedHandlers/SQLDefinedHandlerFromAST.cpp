#include <Common/SQLDefinedHandlers/SQLDefinedHandlerFromAST.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateHandlerQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTParallelWithQuery.h>
#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/QueryParameterVisitor.h>

#include <algorithm>
#include <string_view>
#include <vector>

#include <fmt/ranges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_COMPILE_REGEXP;
}

namespace
{

SQLDefinedHandler::URLMatchType convertURLMatchType(ASTCreateHandlerQuery::URLMatchType type)
{
    switch (type)
    {
        case ASTCreateHandlerQuery::URLMatchType::Exact: return SQLDefinedHandler::URLMatchType::Exact;
        case ASTCreateHandlerQuery::URLMatchType::Prefix: return SQLDefinedHandler::URLMatchType::Prefix;
        case ASTCreateHandlerQuery::URLMatchType::Regexp: return SQLDefinedHandler::URLMatchType::Regexp;
    }
}

/// The statements a request to this handler actually executes.
///
/// Most queries are a single statement, but two of them are composite wrappers whose own `QueryKind` says nothing
/// about what runs:
/// - `statement1 PARALLEL WITH statement2 ...` is an `ASTParallelWithQuery` holding the statements as children, and
///   `InterpreterParallelWithQuery::executeSubquery` runs each of them under a copy of the handler's context;
/// - `EXECUTE AS <user> <statement>` is an `ASTExecuteAsQuery`, and `InterpreterExecuteAsQuery::execute` runs the
///   wrapped statement under an impersonated copy of the handler's context (which keeps the `readonly` setting,
///   because the copy re-applies the original context's settings changes).
/// Both wrappers must therefore be looked through, recursively, so that the method fences below follow the
/// statements that really run instead of the wrapper kind. A bare `EXECUTE AS <user>` wraps nothing - it is the
/// statement itself (and mutates the session, see `statementHasSideEffectsUnderReadonly`).
void collectExecutedStatements(const IAST & query, std::vector<const IAST *> & result)
{
    if (const auto * parallel_with = query.as<ASTParallelWithQuery>())
    {
        for (const auto & child : parallel_with->children)
            collectExecutedStatements(*child, result);
        return;
    }

    if (const auto * execute_as = query.as<ASTExecuteAsQuery>(); execute_as && execute_as->subquery)
    {
        /// The wrapper itself is also part of the list: impersonation has its own execution requirements
        /// (see `statementRequiresMutatingMethod`), independent of the wrapped statement's.
        result.push_back(&query);
        collectExecutedStatements(*execute_as->subquery, result);
        return;
    }

    result.push_back(&query);
}

std::vector<const IAST *> getExecutedStatements(const IAST & query)
{
    std::vector<const IAST *> result;
    collectExecutedStatements(query, result);
    return result;
}

/// Whether executing a query of this kind can modify data or server state and therefore requires the handler to
/// accept a mutating HTTP method. This mirrors the `readonly` enforcement in `ContextAccess`: the HTTP execution
/// path enables `readonly` for every non-mutating (safe) method, so a mutating query served only over such methods
/// would always be rejected at invocation time. The switch is exhaustive on purpose - adding a new `QueryKind`
/// forces an explicit decision here.
bool queryKindRequiresMutatingMethod(IAST::QueryKind kind)
{
    switch (kind)
    {
        /// Read-only: allowed to run under `readonly`, so it can be served over a safe method such as `GET`.
        case IAST::QueryKind::None: /// Unclassified queries have no known write; treat them as read-only.
        case IAST::QueryKind::Select:
        case IAST::QueryKind::Show:
        case IAST::QueryKind::Exists:
        case IAST::QueryKind::Describe:
        case IAST::QueryKind::Explain:
        case IAST::QueryKind::Check:
        /// Session- and transaction-mutating statements (`USE`, `SET` / `SET ROLE`, `BEGIN` / `COMMIT` / `ROLLBACK` /
        /// `SET TRANSACTION SNAPSHOT`) run under `readonly = 2` - the mode the HTTP execution path sets for safe
        /// methods such as `GET` - so this readonly-mirror predicate reports them as runnable over a safe method.
        /// Their session-visible side effects are fenced off separately by `queryKindHasSideEffectsUnderReadonly`,
        /// which requires *every* method of such a handler to be a mutating one.
        case IAST::QueryKind::Use:
        case IAST::QueryKind::Set:
        case IAST::QueryKind::Begin:
        case IAST::QueryKind::Commit:
        case IAST::QueryKind::Rollback:
        case IAST::QueryKind::SetTransactionSnapshot:
        /// BACKUP and RESTORE run under `readonly = 2` - the mode the HTTP execution path sets for safe methods
        /// such as `GET` - because `BackupsWorker` rejects them only under the strict, user-set `readonly = 1`.
        /// So this readonly-mirror predicate reports them as runnable over a safe method; their durable side
        /// effects are fenced off separately by `queryKindHasSideEffectsUnderReadonly`, which requires *every*
        /// method of such a handler to be a mutating one.
        case IAST::QueryKind::Backup:
        case IAST::QueryKind::Restore:
            return false;

        /// Mutating: rejected under `readonly`, so it needs a write-capable HTTP method.
        case IAST::QueryKind::Insert:
        case IAST::QueryKind::Delete:
        case IAST::QueryKind::Update:
        case IAST::QueryKind::Create:
        case IAST::QueryKind::Drop:
        case IAST::QueryKind::Undrop:
        case IAST::QueryKind::Rename:
        case IAST::QueryKind::Optimize:
        case IAST::QueryKind::Alter:
        case IAST::QueryKind::Grant:
        case IAST::QueryKind::Revoke:
        case IAST::QueryKind::Move:
        case IAST::QueryKind::System:
        case IAST::QueryKind::KillQuery:
        case IAST::QueryKind::ExternalDDL:
        case IAST::QueryKind::AsyncInsertFlush:
        /// A `PARALLEL WITH` query is looked through by `collectExecutedStatements`, so the fences never classify
        /// the wrapper itself; the entry stays here (and conservatively mutating) for exhaustiveness.
        case IAST::QueryKind::ParallelWithQuery:
        case IAST::QueryKind::Copy:
        case IAST::QueryKind::Snapshot:
            return true;
    }
}

/// Whether the concrete query requires the handler to accept a mutating HTTP method. This refines
/// `queryKindRequiresMutatingMethod` for the `Create` kind: `CREATE TEMPORARY TABLE` / `CREATE TEMPORARY VIEW`
/// need only the `CREATE_TEMPORARY_TABLE` access flag, which `ContextAccess` still allows under `readonly = 2`
/// (the mode a safe HTTP method such as `GET` sets); they are rejected only under `readonly = 1`. So a temporary
/// -object create is runnable over `GET` and must not require a mutating method here. Its session-visible side
/// effects are fenced off separately by `queryHasSideEffectsUnderReadonly`, which requires *every* method of
/// such a handler to be a mutating one.
bool statementRequiresMutatingMethod(const IAST & statement)
{
    if (const auto * create = statement.as<ASTCreateQuery>(); create && create->isTemporary())
        return false;
    /// `EXECUTE AS <user>` - bare or wrapping a statement - checks the `IMPERSONATE` privilege, which belongs
    /// to `ACCESS_MANAGEMENT` and is therefore denied under any `readonly` (see `ContextAccess`,
    /// `write_dcl_access` is part of `not_readonly_flags`). So impersonation can never run over a safe method
    /// and requires a mutating one, whatever the wrapped statement is. The session-visible effect of the bare
    /// form is additionally fenced off by `statementHasSideEffectsUnderReadonly`.
    if (statement.as<ASTExecuteAsQuery>())
        return true;
    return queryKindRequiresMutatingMethod(statement.getQueryKind());
}

/// Whether the handler's query requires a mutating HTTP method: it does if any of the statements it executes does
/// (see `collectExecutedStatements` for the composite `PARALLEL WITH` / `EXECUTE AS` wrappers).
bool queryRequiresMutatingMethod(const IAST & query)
{
    const auto statements = getExecutedStatements(query);
    return std::any_of(statements.begin(), statements.end(),
        [](const IAST * statement) { return statementRequiresMutatingMethod(*statement); });
}

/// Whether the query itself reads the HTTP request body as its data.
///
/// A plain `INSERT` takes the body as the data to insert. `INSERT ... SELECT` does not: it gets its data from the
/// `SELECT`, and `executeQuery` explicitly drops the request tail for it (see `executeQuery.cpp`, the
/// `insert_query->tail.reset()` branch). The exception to that exception is the `input` table function: an
/// `INSERT ... SELECT ... FROM input(...)` is fed from the request body, and `executeQuery` builds its source
/// pipe from the tail before dropping it.
///
/// This deliberately does not look through the composite `PARALLEL WITH` / `EXECUTE AS` wrappers: both re-format
/// their wrapped statements and execute them through a fresh `executeQuery` call with no request tail, so a
/// wrapped `INSERT` never receives the request body.
bool queryConsumesRequestBody(const IAST & query)
{
    const auto * insert = query.as<ASTInsertQuery>();
    if (!insert || insert->getQueryKind() != IAST::QueryKind::Insert)
        return false;

    if (!insert->select)
        return true;

    ASTPtr input_function;
    insert->tryFindInputFunction(input_function);
    return input_function != nullptr;
}

/// Whether the query wraps - inside `EXECUTE AS` or `PARALLEL WITH` - a statement that takes the HTTP request
/// body as its data. Such a handler can never work: both wrappers re-format their statements and run them
/// through `executeQuery(String, ...)` (see `InterpreterExecuteAsQuery::execute` and
/// `InterpreterParallelWithQuery::executeSubquery`), and that overload always passes `no_input_buffer`, so the
/// request tail is gone by the time the wrapped statement runs. A wrapped plain `INSERT` would silently insert
/// nothing, and a wrapped `INSERT ... SELECT ... FROM input(...)` could never be fed at all. There is nothing
/// to enforce at invocation time either - the data is simply dropped - so reject the handler at creation.
bool queryWrapsBodyConsumingStatement(const IAST & query)
{
    const auto statements = getExecutedStatements(query);
    return std::any_of(statements.begin(), statements.end(),
        [&](const IAST * statement) { return statement != &query && queryConsumesRequestBody(*statement); });
}

/// Whether `readonly = 2` (the mode the HTTP execution path sets for safe methods such as `GET`) still lets a
/// query of this kind produce side effects. Two groups:
/// - `BACKUP` writes an archive to disk or object storage and `RESTORE` writes data into tables, yet
///   `BackupsWorker` rejects them only under the strict, user-set `readonly = 1`.
/// - Session- and transaction-mutating statements: `SET` changes session settings (allowed under `readonly = 2`,
///   which forbids only changing `readonly` itself), `SET ROLE` changes the active roles, `USE` changes the
///   session database, and `BEGIN` / `COMMIT` / `ROLLBACK` / `SET TRANSACTION SNAPSHOT` mutate the current
///   transaction - none of which `readonly` blocks. When the client uses `session_id`, these effects persist
///   across requests, so a `GET` could invisibly commit a transaction or alter session state.
/// The runtime `readonly` enforcement cannot fence any of these off. HTTP requires safe methods to be
/// side-effect-free: `GET` is expected to have no effects, and a handler declared for `GET` is also served for
/// `HEAD` (see `HTTPHandlerFactory`), where the suppressed response body would hide the effect entirely. Such
/// queries therefore must not be reachable over safe methods at all.
bool queryKindHasSideEffectsUnderReadonly(IAST::QueryKind kind)
{
    switch (kind)
    {
        case IAST::QueryKind::Backup:
        case IAST::QueryKind::Restore:
        case IAST::QueryKind::Set:
        case IAST::QueryKind::Use:
        case IAST::QueryKind::Begin:
        case IAST::QueryKind::Commit:
        case IAST::QueryKind::Rollback:
        case IAST::QueryKind::SetTransactionSnapshot:
            return true;
        default:
            return false;
    }
}

/// Whether the query can mutate an *existing* session temporary table when run under `readonly = 2` (the mode
/// the HTTP execution path sets for safe methods such as `GET`). Access to temporary tables is not controlled
/// like access to normal tables: `ContextAccess` grants everything on the session's temporary database
/// unconditionally (see `ContextAccess::checkAccessImplHelper`), so `readonly` blocks none of these operations -
/// `00543_access_to_temporary_table_in_readonly_mode` pins `INSERT` into and `DROP TEMPORARY TABLE` of a
/// temporary table under `readonly = 2`. A temporary table is a session object that does not exist when the
/// handler is created, so whether an unqualified table name will resolve to one cannot be known here - the check
/// is over query *shapes* and fails close:
/// - an `INSERT` whose target table is not qualified with a database may resolve to a session temporary table
///   (temporary tables shadow tables of the current database);
/// - `DROP TEMPORARY TABLE` targets one explicitly;
/// - `DROP TABLE` / `TRUNCATE TABLE` of a table not qualified with a database resolves external-first
///   (see `InterpreterDropQuery::executeToTableImpl`), so it too may hit a session temporary table.
/// - an `ALTER` whose target table is not qualified with a database: `InterpreterAlterQuery::executeToTable`
///   calls `tryResolveStorageID` (external-first) and rewrites the AST database to the resolved temporary
///   database *before* `checkAccess`, so the unconditional grant on the temporary database applies and
///   `readonly = 2` does not stop the mutation. `ALTER TEMPORARY TABLE` parses to the same unqualified AST.
///   (`RENAME` and `OPTIMIZE` check access against the current database *before* resolving the table, so
///   `readonly` rejects them and they need no fence here.)
/// A database-qualified target can never be a temporary table, so such queries are not fenced - under
/// `readonly = 2` they are rejected at invocation time by the normal access checks. `DETACH` of temporary
/// tables is rejected outright by `InterpreterDropQuery`, so `DETACH` is not fenced either.
bool queryMayMutateTemporaryTable(const IAST & query)
{
    if (const auto * insert = query.as<ASTInsertQuery>(); insert && insert->getQueryKind() == IAST::QueryKind::Insert)
        return !insert->table_function && insert->table && insert->getDatabase().empty();

    if (const auto * drop = query.as<ASTDropQuery>())
    {
        if (drop->kind != ASTDropQuery::Kind::Drop && drop->kind != ASTDropQuery::Kind::Truncate)
            return false;

        if (drop->isTemporary())
            return true;

        /// `TRUNCATE ALL TABLES FROM db` always names a database.
        if (drop->has_all)
            return false;

        /// `DROP TABLE t1, t2, ...` - fenced if any of the targets is unqualified.
        if (drop->database_and_tables)
        {
            for (const auto & child : drop->database_and_tables->as<ASTExpressionList &>().children)
            {
                const auto * identifier = child->as<ASTTableIdentifier>();
                if (identifier && identifier->getTableId().database_name.empty())
                    return true;
            }
            return false;
        }

        return drop->table && drop->getDatabase().empty();
    }

    /// `ALTER DATABASE` has no table target; `ALTER TABLE t ...` with an unqualified `t` is fenced.
    if (const auto * alter = query.as<ASTAlterQuery>())
        return alter->table && alter->getDatabase().empty();

    return false;
}

/// Whether the concrete query still produces side effects under `readonly = 2`. This refines
/// `queryKindHasSideEffectsUnderReadonly` for the `Create` kind: `CREATE TEMPORARY TABLE` /
/// `CREATE TEMPORARY VIEW` need only the `CREATE_TEMPORARY_TABLE` access flag, which `ContextAccess` still
/// allows under `readonly = 2` (the mode a safe HTTP method such as `GET` sets), and the created object lives
/// in the session - with `session_id` it persists across requests, so a `GET` (or the implicitly served `HEAD`)
/// would invisibly mutate session state.
/// It also covers queries that can mutate an *existing* session temporary table, which `readonly = 2`
/// does not block either - see `queryMayMutateTemporaryTable`.
bool statementHasSideEffectsUnderReadonly(const IAST & statement)
{
    if (const auto * create = statement.as<ASTCreateQuery>(); create && create->isTemporary())
        return true;
    if (queryMayMutateTemporaryTable(statement))
        return true;
    /// A bare `EXECUTE AS <user>` (no wrapped statement) impersonates the user in the *session* context
    /// (`InterpreterExecuteAsQuery::execute` calls `impersonateSessionContext` on `getSessionContext()`), so with
    /// `session_id` in use every following request of that session runs as another user. `readonly` does not block
    /// it, exactly like `SET` or `USE`.
    if (const auto * execute_as = statement.as<ASTExecuteAsQuery>(); execute_as && !execute_as->subquery)
        return true;
    return queryKindHasSideEffectsUnderReadonly(statement.getQueryKind());
}

/// Whether the handler's query still produces side effects under `readonly = 2`: it does if any of the statements
/// it executes does (see `collectExecutedStatements` for the composite `PARALLEL WITH` / `EXECUTE AS` wrappers -
/// their children run under a copy of the handler's context, so a safe method would reach them all the same).
bool queryHasSideEffectsUnderReadonly(const IAST & query)
{
    const auto statements = getExecutedStatements(query);
    return std::any_of(statements.begin(), statements.end(),
        [](const IAST * statement) { return statementHasSideEffectsUnderReadonly(*statement); });
}

/// A short description of a side-effecting-under-readonly statement for the error message.
std::string_view describeStatementSideEffectsUnderReadonly(const IAST & statement)
{
    if (const auto * create = statement.as<ASTCreateQuery>(); create && create->isTemporary())
        return "a CREATE TEMPORARY query, which creates an object living in the session";

    if (queryMayMutateTemporaryTable(statement))
    {
        if (statement.as<ASTInsertQuery>())
            return "an INSERT into a table not qualified with a database, which may be a temporary table living in the session "
                   "(qualify the table with a database if a temporary table is not intended)";
        if (statement.as<ASTAlterQuery>())
            return "an ALTER of a table not qualified with a database, which may be a temporary table living in the session "
                   "(qualify the table with a database if a temporary table is not intended)";
        return "a DROP or TRUNCATE that may target a temporary table living in the session "
               "(qualify the table with a database if a temporary table is not intended)";
    }

    if (const auto * execute_as = statement.as<ASTExecuteAsQuery>(); execute_as && !execute_as->subquery)
        return "an EXECUTE AS query without a statement, which makes the session run as another user";

    switch (statement.getQueryKind())
    {
        case IAST::QueryKind::Backup: return "a BACKUP query, which writes a backup";
        case IAST::QueryKind::Restore: return "a RESTORE query, which writes data into tables";
        case IAST::QueryKind::Set: return "a SET or SET ROLE query, which changes session state";
        case IAST::QueryKind::Use: return "a USE query, which changes the session database";
        case IAST::QueryKind::Begin: return "a BEGIN TRANSACTION query, which changes transaction state";
        case IAST::QueryKind::Commit: return "a COMMIT query, which changes transaction state";
        case IAST::QueryKind::Rollback: return "a ROLLBACK query, which changes transaction state";
        case IAST::QueryKind::SetTransactionSnapshot: return "a SET TRANSACTION SNAPSHOT query, which changes transaction state";
        default: return "a query with side effects";
    }
}

/// A short description of the first side-effecting-under-readonly statement of the handler's query, for the error
/// message. `queryHasSideEffectsUnderReadonly` is what decides whether the handler is rejected, so this is only
/// called when at least one such statement exists.
std::string_view describeSideEffectsUnderReadonly(const IAST & query)
{
    for (const auto * statement : getExecutedStatements(query))
    {
        if (statementHasSideEffectsUnderReadonly(*statement))
            return describeStatementSideEffectsUnderReadonly(*statement);
    }
    return "a query with side effects";
}

/// The HTTP methods that are allowed to run modifying queries (see `setReadOnlyIfHTTPMethodIdempotent`).
bool isMutatingHTTPMethod(const String & method)
{
    return method == "POST" || method == "PUT" || method == "DELETE";
}

}

SQLDefinedHandlerPtr makeSQLDefinedHandler(const ASTCreateHandlerQuery & create)
{
    if (!create.has_url)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` must specify a URL", create.handler_name);

    if (!create.query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` must specify a query (the AS clause)", create.handler_name);

    const String type = create.handler_type.value_or("query");
    if (type != "query")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported handler type `{}`, only `query` is supported", type);

    /// The handler query is persisted as formatted text (see `handler->query` below), and
    /// `ASTInsertQuery::formatImpl` intentionally drops the inline payload that follows `VALUES` or the
    /// `FORMAT` clause - the stored handler would silently execute a different query (`INSERT INTO t VALUES`
    /// with no rows) than the one the user wrote, and would also be misclassified as reading the request body.
    /// An INSERT handler takes its data from the HTTP request body instead, so reject inline data outright.
    /// Today such a query cannot reach this point via SQL - `ParserInsertQuery` leaves the payload unconsumed,
    /// so the enclosing CREATE/ALTER HANDLER statement fails to parse (see
    /// `04823_handler_insert_inline_data`) - but the check keeps the lossy-canonicalization invariant explicit
    /// rather than dependent on that parser behavior.
    if (const auto * insert = create.query->as<ASTInsertQuery>(); insert && insert->hasInlinedData())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Handler `{}` query is an INSERT with inline data after the VALUES or FORMAT clause. "
            "Inline data cannot be preserved in a handler definition. "
            "Leave the data out and send it in the HTTP request body "
            "(for example, AS INSERT INTO t FORMAT TSV), or compute it in the query itself "
            "with INSERT ... SELECT.",
            create.handler_name);
    }

    auto handler = std::make_shared<SQLDefinedHandler>();
    handler->name = create.handler_name;
    handler->protocol = create.protocol;
    handler->url_match_type = convertURLMatchType(create.url_match_type);
    handler->url = create.url;
    handler->type = type;

    if (handler->url.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` URL cannot be empty", create.handler_name);

    if (handler->url_match_type == SQLDefinedHandler::URLMatchType::Regexp)
    {
        auto regex = std::make_shared<const re2::RE2>(handler->url);
        if (!regex->ok())
            throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                "Cannot compile re2 regexp for handler `{}`: {}, error: {}",
                create.handler_name, handler->url, regex->error());
        handler->url_regex = regex;
    }

    if (create.methods && !create.methods->empty())
        handler->methods = *create.methods;
    else
        handler->methods = {"GET"};

    /// A handler whose query modifies data must accept at least one mutating HTTP method. Otherwise, the HTTP
    /// execution path enables `readonly` for every allowed (safe) method, and the handler could never run its
    /// query - so reject it here with a clear error instead of silently creating a broken handler.
    if (queryRequiresMutatingMethod(*create.query)
        && std::none_of(handler->methods.begin(), handler->methods.end(), isMutatingHTTPMethod))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Handler `{}` runs a query that cannot execute in the readonly mode of safe HTTP methods "
            "(it modifies data, or, like EXECUTE AS, needs a privilege that readonly denies), "
            "but its allowed HTTP methods ({}) are all read-only. "
            "Add a mutating method (POST, PUT, or DELETE) to the METHODS clause.",
            create.handler_name, fmt::join(handler->methods, ", "));
    }

    /// The `readonly` enforcement above cannot fence off queries whose side effects survive `readonly = 2` - the
    /// mode that safe methods set: `BACKUP` / `RESTORE` write durable data, and `SET` / `SET ROLE` / `USE` /
    /// transaction control / `CREATE TEMPORARY TABLE` / `CREATE TEMPORARY VIEW` / an `INSERT`, `DROP` or
    /// `TRUNCATE` that may target an existing temporary table mutate session state that
    /// persists across requests when `session_id` is in use. A safe method must never trigger them - `GET` is
    /// expected to be side-effect-free, and a declared `GET` is also served for `HEAD` (see `HTTPHandlerFactory`),
    /// where the suppressed response body would hide the effect entirely. So require *every* allowed method of
    /// such a handler to be a mutating one.
    if (queryHasSideEffectsUnderReadonly(*create.query)
        && !std::all_of(handler->methods.begin(), handler->methods.end(), isMutatingHTTPMethod))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Handler `{}` runs {}. The read-only mode of safe HTTP methods does not prevent these side effects, "
            "but the handler's allowed HTTP methods ({}) include read-only ones. "
            "List only mutating methods (POST, PUT, or DELETE) in the METHODS clause.",
            create.handler_name,
            describeSideEffectsUnderReadonly(*create.query),
            fmt::join(handler->methods, ", "));
    }

    handler->query = create.query->formatWithSecretsOneLine();
    /// Precompute the set of query parameters once, so the per-request handler path does not re-parse the query.
    /// Collect them from the already-parsed AST rather than re-parsing the formatted string: re-parsing would
    /// apply the default parser depth/backtrack limits and could reject a query the user's parser settings accepted.
    handler->receive_params = analyzeReceiveQueryParams(create.query);

    /// A statement that takes the request body as its data must be the handler's own query: neither `EXECUTE AS`
    /// nor `PARALLEL WITH` forwards the request tail to the statements it runs, so a wrapped one would silently
    /// receive no data at all. Reject it here instead of creating a handler that discards every upload.
    if (queryWrapsBodyConsumingStatement(*create.query))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Handler `{}` wraps a query that takes its data from the HTTP request body (an INSERT, or an "
            "INSERT ... SELECT reading from `input`) inside EXECUTE AS or PARALLEL WITH. Those clauses run "
            "their statements without the request body, so the uploaded data would be silently discarded. "
            "Make the body-reading INSERT the handler's own query.",
            create.handler_name);
    }

    /// A query that takes the request body as its data cannot also use the `_request_body` parameter: there is a
    /// single non-rewindable body stream, and binding `_request_body` drains it (see
    /// `PredefinedQueryHandler::customizeContext`) before the query's input pipeline is built (see
    /// `HTTPHandler::processQuery`), so the `INSERT` (or its `input` function) would find the stream at EOF and
    /// silently insert nothing. Reject the handler here instead of creating one that discards every upload.
    if (queryConsumesRequestBody(*create.query) && handler->receive_params.contains("_request_body"))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Handler `{}` runs a query that takes its data from the HTTP request body (an INSERT, or an "
            "INSERT ... SELECT reading from `input`) and also uses the `_request_body` parameter. "
            "Binding `_request_body` consumes the request body before the query reads its input data, "
            "so the uploaded data would be silently lost. "
            "Use either the query's own body input or the `_request_body` parameter, not both.",
            create.handler_name);
    }

    /// An `INSERT` handler takes the request body as its data, and a query using `_request_body` reads the body
    /// explicitly. Only these handlers need `Content-Length` on a non-chunked request (see `SQLDefinedHandler`).
    handler->consumes_request_body = queryConsumesRequestBody(*create.query)
        || handler->receive_params.contains("_request_body");

    /// A handler whose query reads the HTTP request body can never receive one over a safe method:
    /// the HTTP layer gives a non-chunked `GET` an empty body stream (see `HTTPServerRequest`), so the
    /// query would silently bind an empty body instead of ever reading or rejecting the request. And a
    /// declared `GET` is also served for `HEAD` (see `HTTPHandlerFactory`), so allowing safe methods
    /// alongside body-carrying ones would keep those silent invocations reachable. The body-carrying
    /// methods are exactly the mutating ones (`POST`, `PUT`, `DELETE`), so require *every* allowed
    /// method to be one of them, and reject the handler with a clear error otherwise.
    if (handler->consumes_request_body
        && !std::all_of(handler->methods.begin(), handler->methods.end(), isMutatingHTTPMethod))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Handler `{}` reads the HTTP request body (an INSERT query taking its data from the body, "
            "or the `_request_body` parameter), "
            "but not all of its allowed HTTP methods ({}) carry a request body. "
            "List only body-carrying methods (POST, PUT, or DELETE) in the METHODS clause.",
            create.handler_name, fmt::join(handler->methods, ", "));
    }

    /// Build a normalized, complete CREATE HANDLER statement for persistence and introspection.
    auto normalized = create.clone();
    auto & normalized_create = normalized->as<ASTCreateHandlerQuery &>();
    normalized_create.is_alter = false;
    normalized_create.if_not_exists = false;
    normalized_create.cluster.clear();
    /// PROTOCOL ANY is the same as an omitted clause on CREATE, so it is normalized away.
    normalized_create.reset_protocol = false;
    normalized_create.methods = handler->methods;
    normalized_create.handler_type = type;
    handler->create_statement = normalized_create.formatWithSecretsOneLine();

    return handler;
}

void mergeAlterIntoCreateHandler(ASTCreateHandlerQuery & create, const ASTCreateHandlerQuery & alter)
{
    if (alter.protocol)
        create.protocol = alter.protocol;
    else if (alter.reset_protocol)
        create.protocol.reset();

    if (alter.has_url)
    {
        create.has_url = true;
        create.url_match_type = alter.url_match_type;
        create.url = alter.url;
    }

    if (alter.methods)
        create.methods = alter.methods;

    if (alter.handler_type)
        create.handler_type = alter.handler_type;

    if (alter.query)
    {
        /// Replace the query child. The query is the only child of ASTCreateHandlerQuery.
        create.children.clear();
        create.query = alter.query->clone();
        create.children.push_back(create.query);
    }
}

}
