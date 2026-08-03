#include <Common/SQLDefinedHandlers/SQLDefinedHandlerFromAST.h>

#include <Parsers/ASTCreateHandlerQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/QueryParameterVisitor.h>

#include <algorithm>

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
/// -object create is runnable over `GET` and must not require a mutating method here.
bool queryRequiresMutatingMethod(const IAST & query)
{
    if (const auto * create = query.as<ASTCreateQuery>(); create && create->isTemporary())
        return false;
    /// `ASTWatchQuery` reports `QueryKind::Create`, but WATCH is a read-only streaming query:
    /// `InterpreterWatchQuery` checks only `SELECT` access, so it is runnable under `readonly = 2`
    /// (the mode a safe HTTP method such as `GET` sets) and must not require a mutating method.
    if (query.as<ASTWatchQuery>())
        return false;
    return queryKindRequiresMutatingMethod(query.getQueryKind());
}

/// Whether the query itself reads the HTTP request body as its data.
///
/// A plain `INSERT` takes the body as the data to insert. `INSERT ... SELECT` does not: it gets its data from the
/// `SELECT`, and `executeQuery` explicitly drops the request tail for it (see `executeQuery.cpp`, the
/// `insert_query->tail.reset()` branch). The exception to that exception is the `input` table function: an
/// `INSERT ... SELECT ... FROM input(...)` is fed from the request body, and `executeQuery` builds its source
/// pipe from the tail before dropping it.
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

/// Whether `readonly = 2` (the mode the HTTP execution path sets for safe methods such as `GET`) still lets a
/// query of this kind produce durable side effects. `BACKUP` writes an archive to disk or object storage and
/// `RESTORE` writes data into tables, yet `BackupsWorker` rejects them only under the strict, user-set
/// `readonly = 1` - so the runtime `readonly` enforcement cannot fence them off. HTTP requires safe methods to be
/// side-effect-free: `GET` is expected to have no effects, and a handler declared for `GET` is also served for
/// `HEAD` (see `HTTPHandlerFactory`), where the suppressed response body would hide the effect entirely. Such
/// queries therefore must not be reachable over safe methods at all.
bool queryKindHasSideEffectsUnderReadonly(IAST::QueryKind kind)
{
    return kind == IAST::QueryKind::Backup || kind == IAST::QueryKind::Restore;
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
            "Handler `{}` runs a query that modifies data, but its allowed HTTP methods ({}) are all read-only. "
            "Add a mutating method (POST, PUT, or DELETE) to the METHODS clause.",
            create.handler_name, fmt::join(handler->methods, ", "));
    }

    /// The `readonly` enforcement above cannot fence off `BACKUP` / `RESTORE`: they run under the `readonly = 2`
    /// mode that safe methods set, yet they have durable side effects. A safe method must never trigger them -
    /// `GET` is expected to be side-effect-free, and a declared `GET` is also served for `HEAD` (see
    /// `HTTPHandlerFactory`), where the suppressed response body would hide the effect entirely. So require
    /// *every* allowed method of such a handler to be a mutating one.
    if (queryKindHasSideEffectsUnderReadonly(create.query->getQueryKind())
        && !std::all_of(handler->methods.begin(), handler->methods.end(), isMutatingHTTPMethod))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Handler `{}` runs a {} query, which has side effects that the read-only mode of safe HTTP methods "
            "does not prevent, but its allowed HTTP methods ({}) include read-only ones. "
            "List only mutating methods (POST, PUT, or DELETE) in the METHODS clause.",
            create.handler_name,
            create.query->getQueryKind() == IAST::QueryKind::Backup ? "BACKUP" : "RESTORE",
            fmt::join(handler->methods, ", "));
    }

    handler->query = create.query->formatWithSecretsOneLine();
    /// Precompute the set of query parameters once, so the per-request handler path does not re-parse the query.
    /// Collect them from the already-parsed AST rather than re-parsing the formatted string: re-parsing would
    /// apply the default parser depth/backtrack limits and could reject a query the user's parser settings accepted.
    handler->receive_params = analyzeReceiveQueryParams(create.query);

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
