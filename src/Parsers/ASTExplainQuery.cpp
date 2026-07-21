#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
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

    auto table_function_child = r.readChildOfType<ASTFunction>("table_function");
    if (table_function_child)
        setTableFunction(std::move(table_function_child));

    auto table_override_child = r.readChildOfType<ASTTableOverride>("table_override");
    if (table_override_child)
        setTableOverride(std::move(table_override_child));

    /// Enforce the exact parser-produced child set per kind, rejecting forbidden extras as well:
    /// `EXPLAIN TABLE OVERRIDE` dereferences the table function and override but never parses an
    /// explained query, `EXPLAIN CURRENT TRANSACTION` explains nothing, and every other kind
    /// dereferences the explained query (e.g. `dumpAST(*getExplainedQuery())`) and never parses a
    /// table function or override. An extra child would format into parser-impossible SQL while
    /// `InterpreterExplainQuery` silently ignores it.
    switch (kind)
    {
        case ExplainKind::TableOverride:
            if (!getTableFunction() || !getTableOverride())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "EXPLAIN TABLE OVERRIDE requires 'table_function' and 'table_override' during AST JSON deserialization");
            if (getExplainedQuery())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "EXPLAIN TABLE OVERRIDE cannot carry an explained 'query' during AST JSON deserialization");
            break;
        case ExplainKind::CurrentTransaction:
            if (getExplainedQuery() || getTableFunction() || getTableOverride())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "EXPLAIN CURRENT TRANSACTION cannot carry 'query', 'table_function', or 'table_override' during AST JSON deserialization");
            break;
        default:
            if (!getExplainedQuery())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "{} requires an explained query during AST JSON deserialization", toString(kind));
            if (getTableFunction() || getTableOverride())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "'table_function' and 'table_override' are only valid for EXPLAIN TABLE OVERRIDE during AST JSON deserialization");
            break;
    }

    readOutputOptionsJSON(r);
}

}
