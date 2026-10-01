#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTExplainTextAction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTableOverrides.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ASTExplainQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ExplainQuery");
    w.writeString("kind", toString(kind));
    w.writeChild("settings", ast_settings);
    w.writeChild("query", query);
    w.writeChild("actions", actions);
    w.writeChild("table_function", table_function);
    w.writeChild("table_override", table_override);
    writeOutputOptionsJSON(w);
}

void ASTExplainQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    kind = fromString(r.getString("kind"));

    /// `InterpreterExplainQuery` reads `settings` as an `ASTSetQuery`, the table function as an
    /// `ASTFunction`, and the override as an `ASTTableOverride`; restore those with typed reads.
    auto settings_child = r.readChildOfType<ASTSetQuery>("settings");
    if (settings_child)
        setSettings(std::move(settings_child));

    auto query_child = r.readChild("query");
    if (query_child)
        setExplainedQuery(std::move(query_child));

    auto actions_child = r.readChildOfType<ASTExpressionList>("actions");
    if (actions_child)
    {
        const auto & action_list = actions_child->as<const ASTExpressionList &>();
        if (action_list.getSeparator() != ',' || action_list.children.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "EXPLAIN TEXT requires a non-empty comma-separated action list during AST JSON deserialization");
        for (const auto & action : action_list.children)
        {
            if (!action->as<ASTExplainTextAction>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "EXPLAIN TEXT action list can contain only ASTExplainTextAction nodes during AST JSON deserialization");
        }
        setActions(std::move(actions_child));
    }

    auto table_function_child = r.readScreenedChildOfType<ASTFunction>("table_function");

    if (table_function_child)
        setTableFunction(std::move(table_function_child));

    auto table_override_child = r.readChildOfType<ASTTableOverride>("table_override");
    if (table_override_child)
        setTableOverride(std::move(table_override_child));

    if (kind != ExplainKind::FormattedQuery && getActions())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "'actions' are only valid for EXPLAIN TEXT during AST JSON deserialization");

    /// Enforce the exact parser-produced child set per kind, rejecting forbidden extras as well:
    /// `EXPLAIN TABLE OVERRIDE` dereferences the table function and override but never parses an
    /// explained query, `EXPLAIN CURRENT TRANSACTION` explains nothing, and every other kind
    /// dereferences the explained query (e.g. `dumpAST(*getExplainedQuery())`) and never parses a
    /// table function or override. An extra child would format into parser-impossible SQL while
    /// `InterpreterExplainQuery` silently ignores it.
    switch (kind)
    {
        case ExplainKind::FormattedQuery:
            if (!getExplainedQuery() || getExplainedQuery()->getQueryKind() == QueryKind::None)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "EXPLAIN TEXT requires an explained query during AST JSON deserialization");
            /// `ParserQuery` never produces a top-level `ASTSelectIntersectExceptQuery` it only exists
            /// inside an `ASTSelectWithUnionQuery`, which is also what carries the output options that
            /// `MODIFY FORMAT` sets. A bare `ASTSelectQuery` stays accepted as a convenience for callers
            /// that build the JSON by hand
            if (getExplainedQuery()->as<ASTSelectIntersectExceptQuery>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "EXPLAIN TEXT requires INTERSECT and EXCEPT to be wrapped in 'SelectWithUnionQuery' during AST JSON deserialization");
            /// `ASTSetQuery` also represents embedded settings clauses, which are not source statements.
            if (const auto * set_query = getExplainedQuery()->as<ASTSetQuery>();
                set_query && (!set_query->is_standalone
                    || (set_query->changes.empty() && set_query->default_settings.empty() && set_query->query_parameters.empty())))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "EXPLAIN TEXT requires a non-empty standalone SET query during AST JSON deserialization");
            if (getSettings())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "EXPLAIN TEXT cannot carry leading kind-specific settings during AST JSON deserialization");
            if (getTableFunction() || getTableOverride())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "EXPLAIN TEXT cannot carry 'table_function' or 'table_override' during AST JSON deserialization");
            break;
        case ExplainKind::TableOverride:
            if (!getTableFunction() || !getTableOverride())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "EXPLAIN TABLE OVERRIDE requires 'table_function' and 'table_override' during AST JSON deserialization");
            if (getExplainedQuery())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "EXPLAIN TABLE OVERRIDE cannot carry an explained 'query' during AST JSON deserialization");
            /// `ParserExplainQuery` parses the override with `ParserTableOverrideDeclaration(false)`,
            /// so it is always the embedded form: not standalone and without a table name. A standalone
            /// override would format as `EXPLAIN TABLE OVERRIDE <function> TABLE OVERRIDE name ...`,
            /// which the SQL parser can never produce back.
            {
                const auto & override_ast = getTableOverride()->as<const ASTTableOverride &>();
                if (override_ast.is_standalone || !override_ast.table_name.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "EXPLAIN TABLE OVERRIDE requires an embedded table override (not standalone, without "
                        "'table_name') during AST JSON deserialization");
            }
            break;
        case ExplainKind::CurrentTransaction:
            if (getExplainedQuery() || getTableFunction() || getTableOverride())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "EXPLAIN CURRENT TRANSACTION cannot carry 'query', 'table_function', or 'table_override' during AST JSON deserialization");
            break;
        default:
        {
            const auto & explained = getExplainedQuery();
            if (!explained || explained->getQueryKind() == QueryKind::None)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} requires an explained query during AST JSON deserialization", toString(kind));

            /// `ASTSetQuery` also represents embedded settings clauses, which are not statements.
            if (const auto * set_query = explained->as<ASTSetQuery>();
                set_query && (!set_query->is_standalone || (set_query->changes.empty() && set_query->default_settings.empty() && set_query->query_parameters.empty())))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} requires a non-empty standalone SET query during AST JSON deserialization", toString(kind));

            /// `ParsedQuery` always wraps a top-level `SELECT` in `ASTSelectWithUnionQuery`, and the
            /// interpreters of these kinds check for that wrapper. Only `EXPLAIN TEXT` formats a bare
            /// `ASTSelectQuery`, which `forQueryFromJSON` callers may build directly.
            if (explained->getQueryKind() == QueryKind::Select && !explained->as<ASTSelectWithUnionQuery>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} requires the SELECT to be wrapped in 'SelectWithUnionQuery' during AST JSON deserialization", toString(kind));

            /// `ParserExplainQuery` hands `EXPLAIN AST` to the full `ParserQuery`. `EXPLAIN QUERY TREE` accepts
            /// only `SELECT`. Every other kind accepts `SELECT`, `CREATE TABLE`, `INSERT` and `SYSTEM`. a wider
            /// child would format into SQL the parser can never produce.
            if (kind != ExplainKind::ParsedAST)
            {
                const auto query_kind = explained->getQueryKind();
                const bool allowed = query_kind == QueryKind::Select || (kind != ExplainKind::QueryTree && (query_kind == QueryKind::Create || query_kind == QueryKind::Insert || query_kind == QueryKind::System));
                if (!allowed)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} cannot explain this statement during AST JSON deserialization: only SELECT{} is accepted", toString(kind), kind == ExplainKind::QueryTree ? "" : ", CREATE TABLE, INSERT and SYSTEM");
            }

            if (getTableFunction() || getTableOverride())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "'table_function' and 'table_override' are only valid for EXPLAIN TABLE OVERRIDE during AST JSON deserialization");
            break;
        }
    }

    readOutputOptionsJSON(r);
}

}
