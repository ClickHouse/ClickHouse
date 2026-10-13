#include <Parsers/ParserCreateHandlerQuery.h>

#include <Parsers/ASTCreateHandlerQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>
#include <Common/StringUtils.h>
#include <Poco/String.h>

#include <unordered_set>


namespace DB
{

namespace
{

bool parseMethods(IParser::Pos & pos, Expected & expected, std::vector<String> & methods)
{
    if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
        return false;

    static const std::unordered_set<String> allowed_methods = {"GET", "POST", "PUT", "DELETE"};

    while (true)
    {
        ASTPtr method_ast;
        ParserIdentifier method_p;
        if (!method_p.parse(pos, method_ast, expected))
            return false;

        String method = Poco::toUpper(getIdentifierName(method_ast));
        if (!allowed_methods.contains(method))
            return false;

        methods.push_back(method);

        if (ParserToken(TokenType::Comma).ignore(pos, expected))
            continue;
        break;
    }

    return ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected);
}

}

bool ParserCreateHandlerQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_alter(Keyword::ALTER);
    ParserKeyword s_handler(Keyword::HANDLER);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_protocol(Keyword::PROTOCOL);
    ParserKeyword s_any(Keyword::ANY);
    ParserKeyword s_url(Keyword::URL);
    ParserKeyword s_prefix(Keyword::PREFIX);
    ParserKeyword s_regexp(Keyword::REGEXP);
    ParserKeyword s_methods(Keyword::METHODS);
    ParserKeyword s_type(Keyword::TYPE);
    ParserKeyword s_as(Keyword::AS);

    ParserIdentifier name_p;
    ParserStringLiteral string_literal_p;

    bool is_alter = false;
    if (s_create.ignore(pos, expected))
        is_alter = false;
    else if (s_alter.ignore(pos, expected))
        is_alter = true;
    else
        return false;

    if (!s_handler.ignore(pos, expected))
        return false;

    auto query = make_intrusive<ASTCreateHandlerQuery>();
    query->is_alter = is_alter;

    if (!is_alter && s_if_not_exists.ignore(pos, expected))
        query->if_not_exists = true;

    ASTPtr name_ast;
    if (!name_p.parse(pos, name_ast, expected))
        return false;
    query->handler_name = getIdentifierName(name_ast);

    if (s_on.ignore(pos, expected))
    {
        String cluster_str;
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
        query->cluster = std::move(cluster_str);
    }

    if (s_protocol.ignore(pos, expected))
    {
        /// PROTOCOL ANY resets the handler to the default "active on all HTTP endpoints" behavior.
        /// A composable protocol literally named "any" can still be referenced with back quotes.
        if (s_any.ignore(pos, expected))
        {
            query->reset_protocol = true;
        }
        else
        {
            ASTPtr protocol_ast;
            if (!name_p.parse(pos, protocol_ast, expected))
                return false;
            query->protocol = getIdentifierName(protocol_ast);
        }
    }

    if (s_url.ignore(pos, expected))
    {
        if (s_prefix.ignore(pos, expected))
            query->url_match_type = ASTCreateHandlerQuery::URLMatchType::Prefix;
        else if (s_regexp.ignore(pos, expected))
            query->url_match_type = ASTCreateHandlerQuery::URLMatchType::Regexp;
        else
            query->url_match_type = ASTCreateHandlerQuery::URLMatchType::Exact;

        ASTPtr url_ast;
        if (!string_literal_p.parse(pos, url_ast, expected))
            return false;
        query->url = url_ast->as<ASTLiteral &>().value.safeGet<String>();
        query->has_url = true;
    }

    if (s_methods.ignore(pos, expected))
    {
        std::vector<String> methods;
        if (!parseMethods(pos, expected, methods))
            return false;
        query->methods = std::move(methods);
    }

    if (s_type.ignore(pos, expected))
    {
        ASTPtr type_ast;
        if (!name_p.parse(pos, type_ast, expected))
            return false;
        query->handler_type = Poco::toLower(getIdentifierName(type_ast));
    }

    if (s_as.ignore(pos, expected))
    {
        ParserQuery query_p(end);
        ASTPtr inner_query;

        /// Parse the inner query directly. `ParserQuery` already accepts `SELECT` queries that begin with a
        /// parenthesis - in particular `UNION`/`INTERSECT`/`EXCEPT`, which are formatted with parenthesized
        /// operands like `(SELECT ...) EXCEPT (SELECT ...)`. So a leading parenthesis must not be stripped up
        /// front as a disambiguation wrapper, otherwise only the first operand would be consumed and the rest
        /// of the query left dangling - which also broke the format/parse round-trip for such handlers.
        if (!query_p.parse(pos, inner_query, expected))
        {
            /// Fall back to a single pair of wrapping parentheses for query kinds that `ParserQuery` does not
            /// accept inside parentheses on their own (for example `AS (SHOW DATABASES)`).
            if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
                return false;
            if (!query_p.parse(pos, inner_query, expected))
                return false;
            if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                return false;
        }

        query->query = inner_query;
        query->children.push_back(inner_query);
    }

    /// URL is mandatory for CREATE.
    if (!is_alter && !query->has_url)
        return false;

    node = query;
    return true;
}

void registerStatementCreateHandler(StatementFactory & factory)
{
    factory.registerStatement("CREATE HANDLER",
    {
        .description = R"DOCS_MD(
Creates a custom HTTP handler defined from SQL, without editing the server configuration file. SQL-defined handlers are an alternative to the configuration-based [HTTP interface handlers](/concepts/features/interfaces/http).

## Syntax {#syntax}

```sql
CREATE HANDLER [IF NOT EXISTS] name [ON CLUSTER cluster]
[PROTOCOL protocol_name|ANY]
URL [PREFIX|REGEXP] '/path'
[METHODS (GET, POST)]
[TYPE query]
AS [SELECT|INSERT|...] ...
```

Creates a handler with a specified `name`. The name is used for managing handlers with SQL queries, for diagnostic messages, and for ordering handlers.

## Clauses {#clauses}

- `PROTOCOL` — optional. If a protocol name is specified, the handler is active only for the specified [composable protocol](/concepts/features/configuration/server-config/composable-protocols). Otherwise, the handler is active on all HTTP endpoints: the built-in `http`/`https` ports and every HTTP-type [composable protocol](/concepts/features/configuration/server-config/composable-protocols) listener. `PROTOCOL ANY` explicitly selects the latter default behavior; in `ALTER HANDLER` it removes a previously set protocol restriction. A protocol literally named `any` can be referenced with back quotes: ``PROTOCOL `any` ``.
- `URL` — mandatory. Can be in the form of an exact URL, a `URL PREFIX`, or a `URL REGEXP`. For exact URLs and prefixes, ambiguity is checked at creation/alter time and an exception is thrown if there is ambiguity. For regexp, ambiguity cannot be checked. The URL is matched without the `?` query string and the `#` fragment identifier. A `URL PREFIX` is matched as a base path, on a path-segment boundary — the same semantics as the `url_prefix` rule of [configuration-defined handlers](/concepts/features/interfaces/http): `URL PREFIX '/api/v1'` matches `/api/v1`, `/api/v1/` and `/api/v1/write`, but not `/api/v1beta`. A trailing `/` in the prefix is ignored, so `'/api/v1/'` and `'/api/v1'` behave the same.
- `METHODS` — optional. The list of allowed HTTP methods. By default, it is only `GET`. The supported methods are `GET`, `POST`, `PUT` and `DELETE`. The mutating methods `POST`, `PUT` and `DELETE` are allowed to run modifying queries; the safe methods such as `GET` and `HEAD` are always executed in `readonly` mode. Consequently, a handler whose query modifies data (for example `INSERT` or DDL) must allow at least one mutating method - creating such a handler with only read-only methods (for example the default `GET`) throws an exception. Queries whose side effects survive the `readonly` mode are a special case: `BACKUP` and `RESTORE` have durable side effects, the session-mutating statements `SET`, `SET ROLE`, `USE`, `BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK` and `SET TRANSACTION SNAPSHOT` change session or transaction state that persists across requests when `session_id` is in use, and `CREATE TEMPORARY TABLE` / `CREATE TEMPORARY VIEW` create an object living in the session - yet the `readonly` mode of safe methods blocks none of them. Mutations of an *existing* temporary table are equally unblocked by the `readonly` mode, so queries that may target one are treated the same way: an `INSERT` whose target table is not qualified with a database (an unqualified name may resolve to a session temporary table), a `DROP TEMPORARY TABLE`, a `DROP TABLE` / `TRUNCATE TABLE` of a table not qualified with a database, and an `ALTER` of a table not qualified with a database (`ALTER TEMPORARY TABLE` is the same statement). A database-qualified target can never be a temporary table, so such queries are not subject to this rule. HTTP requires safe methods to be side-effect-free (a handler declared for `GET` is also served for `HEAD`, where the response body is suppressed and the effect would be invisible). So a handler running such a query must list *only* mutating methods - creating or altering it to include a safe method throws an exception. Composite statements are looked through: for `statement1 PARALLEL WITH statement2 ...` and `EXECUTE AS <user> <statement>` the rules above apply to the wrapped statements, because they are the ones that run (each under a copy of the handler's context, which keeps the `readonly` mode). A bare `EXECUTE AS <user>` makes the whole session run as another user, so it counts as session-mutating itself. In addition, any `EXECUTE AS` handler - bare or wrapping a statement - must allow at least one mutating method: impersonation needs the `IMPERSONATE` privilege, which the `readonly` mode of safe methods denies.
- `TYPE` — optional. The only supported type for now is `query`.
- `AS` — the SQL query that will be invoked by this handler. The query can be parameterized. The query is parsed for syntactic correctness during handler creation/alter, but not analyzed - for example, the tables referenced by the query can be missing at the time of the handler creation. The `FORMAT` and similar clauses belong to the query, not to the whole `CREATE`/`ALTER` statement. The query can be put in parentheses for disambiguation. An `INSERT` query must not contain inline data after the `VALUES` or `FORMAT` clause - creating or altering such a handler throws an exception, because the inline payload cannot be preserved in the handler definition; the data is expected to be provided in the HTTP body (or computed by an `INSERT ... SELECT`). A request to a handler whose query reads the body - an `INSERT` taking its data from the body, or a query using the `_request_body` parameter - must declare its length: a non-chunked request without a `Content-Length` header is answered with `411 Length Required`, because the body would otherwise be read until end of stream and a dropped connection would be accepted as a complete request. Every method of such a handler must also be body-carrying (`POST`, `PUT` or `DELETE`) - creating it with a safe method in the `METHODS` clause (for example the default `GET`) throws an exception, because a safe method never supplies a request body and the query would silently read an empty one; a declared `GET` is served for `HEAD` too, so mixing safe and body-carrying methods would keep those invocations reachable. An `INSERT ... SELECT` does not read the body (its data comes from the `SELECT`), so it is not subject to these requirements - unless its `SELECT` reads from the `input` table function, which is fed from the request body. A body-reading `INSERT` must be the handler's own query: `EXECUTE AS` and `PARALLEL WITH` run the statements they wrap without the request body, so wrapping one in them is rejected at creation instead of silently discarding every upload. A body-reading query also must not use the `_request_body` parameter: there is a single request body, and binding `_request_body` consumes it before the query reads its input data, so such a handler is rejected at creation instead of silently losing every upload - use either the query's own body input or `_request_body`, not both. Handlers that do not read the body have no such requirement; the body of a request to such a handler is ignored and is never appended to the handler's query. The stored query text is re-parsed by the server with unlimited parser depth and backtracks whenever the handler is reloaded or invoked, so a handler created in a session with raised `max_parser_depth` / `max_parser_backtracks` stays loadable and invokable under ordinary session limits.

## Priority {#priority}

Handlers defined in the server configuration have priority over SQL-defined handlers. SQL-defined handlers are matched in the lexicographical order of their names.

## Parameters {#parameters}

Query parameters for parameterized queries are supplied, just as with configuration-defined handlers, from:

- HTTP URL parameters in the query string, using the `param_<name>` convention (for example `?param_id=42` binds `{id:Type}`);
- named capture groups in a `URL REGEXP` (for example `URL REGEXP '/users/(?P<id>\d+)'` binds `{id:Type}`);
- form fields of the request body, for a handler whose query declares parameters: an `application/x-www-form-urlencoded` body (for example `curl -d 'param_id=42'`) and the fields of a `multipart/form-data` body bind `{name:Type}` parameters the same way as URL parameters, on the body-carrying methods `POST`, `PUT` and `DELETE`. A parameter present both in the URL and in the body takes its value from the URL. A body parsed as a form is consumed by the handler layer: it is not fed to the query as `INSERT` data. A handler whose only body use is `_request_body` gets the raw body instead of form parsing; a handler that declares `_request_body` alongside other parameters gets both - a copy of the raw, unparsed body is preserved in `_request_body` (subject to `http_max_request_param_data_size`) before the body is parsed as a form.

Standard ClickHouse HTTP headers (such as `X-ClickHouse-Database`, `X-ClickHouse-User`, `X-ClickHouse-Key`) are honored as usual when invoking a handler.

The functions [`currentHandler`](/reference/functions/regular-functions/other-functions#currentHandler) and [`currentRequestURL`](/reference/functions/regular-functions/other-functions#currentRequestURL) can be used to customize query behavior depending on the invoked handler and request URL.

## Access control {#access-control}

`CREATE HANDLER`, `DROP HANDLER` and `ALTER HANDLER` require the `CREATE HANDLER`, `DROP HANDLER` and `ALTER HANDLER` grants respectively.

Reading the [`system.handlers`](/reference/system-tables/handlers) table requires the `SHOW HANDLERS` grant. Secrets that may be embedded in a handler's query are masked there unless the user is additionally allowed to see secrets (see [`system.handlers`](/reference/system-tables/handlers)).

Invoking a handler does not require any separate grant, but grants are checked as usual during the query invocation, and authentication works in the usual way. To encapsulate access to certain queries, create a [`VIEW` with `SQL SECURITY DEFINER`](/reference/statements/create/view#sql_security) and define a handler that selects from that view.

## Storage {#storage}

Handlers are saved in a storage, which can be a local or Keeper storage, similarly to [named collections](/concepts/features/configuration/server-config/named-collections), configured in the `query_rules_storage` section of the configuration file:

```xml
<query_rules_storage>
    <type>local</type> <!-- or zookeeper -->
    <path>/var/lib/clickhouse/handlers/</path>
</query_rules_storage>
```

With Keeper storage, handlers are kept in sync across all replicas automatically, so an explicit `ON CLUSTER` clause is redundant and would make every replica try to create the same handler. Enable the `ignore_on_cluster_for_replicated_handler_queries` setting to make `CREATE`, `ALTER` and `DROP HANDLER` ignore `ON CLUSTER` when the storage is replicated, mirroring `ignore_on_cluster_for_replicated_named_collections_queries`.

## ALTER HANDLER {#alter-handler}

```sql
ALTER HANDLER name
[PROTOCOL protocol_name|ANY]
[URL [PREFIX|REGEXP] '/path']
[METHODS (GET, POST)]
[TYPE query]
[AS SELECT ...]
```

Replaces the handler with a new one. The `ALTER` query can include only a subset of clauses, e.g., it can be used to only change the URL or the query. The unspecified clauses keep their previous values. `PROTOCOL ANY` removes an existing protocol restriction, making the handler active on all HTTP endpoints again.

## DROP HANDLER {#drop-handler}

```sql
DROP HANDLER [IF EXISTS] name
```

Drops the handler with the specified name.

## Introspection {#introspection}

The [`system.handlers`](/reference/system-tables/handlers) table lists all SQL-defined handlers. The [`system.query_log`](/reference/system-tables/query_log) table records the handler name and the HTTP request path (without the query string) of each query in the `http_handler_name` and `http_request_url` columns.

## Example {#example}

```sql
CREATE HANDLER my_handler URL '/my_handler' AS SELECT version();
```

```bash
$ curl 'http://localhost:8123/my_handler'
```

A parameterized handler with a regexp URL:

```sql
CREATE HANDLER get_user URL REGEXP '/users/(?P<id>\d+)' AS SELECT * FROM users WHERE id = {id:UInt64};
```

```bash
$ curl 'http://localhost:8123/users/42'
```

## Related statements {#related-statements}

`CREATE HANDLER` is part of the `CREATE` statement family and is related to `ALTER` and `DROP`.
)DOCS_MD",
        .syntax = R"(
CREATE HANDLER [IF NOT EXISTS] name [ON CLUSTER cluster]
[PROTOCOL protocol_name|ANY]
URL [PREFIX|REGEXP] '/path'
[METHODS (GET, POST)]
[TYPE query]
AS [SELECT|INSERT|...] ...
)",
        .parent = "CREATE",
        .related = {"ALTER", "DROP"},
    });
}

}
