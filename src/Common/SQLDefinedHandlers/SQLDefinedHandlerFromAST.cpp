#include <Common/SQLDefinedHandlers/SQLDefinedHandlerFromAST.h>

#include <Parsers/ASTCreateHandlerQuery.h>
#include <Parsers/ASTCreateQuery.h>
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
        case IAST::QueryKind::Backup:
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
        case IAST::QueryKind::Restore:
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
    return queryKindRequiresMutatingMethod(query.getQueryKind());
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

    handler->query = create.query->formatWithSecretsOneLine();
    /// Precompute the set of query parameters once, so the per-request handler path does not re-parse the query.
    /// Collect them from the already-parsed AST rather than re-parsing the formatted string: re-parsing would
    /// apply the default parser depth/backtrack limits and could reject a query the user's parser settings accepted.
    handler->receive_params = analyzeReceiveQueryParams(create.query);

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
