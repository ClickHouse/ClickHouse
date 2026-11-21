#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ASTDescribeQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "DescribeQuery");
    w.writeBool("temporary", temporary);
    w.writeChild("table_expression", table_expression);
    writeOutputOptionsJSON(w);
}

void ASTDescribeQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    temporary = r.getBool("temporary", false);
    auto child = r.readChildOfType<ASTTableExpression>("table_expression");
    if (!child)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`DescribeQuery` must have a 'table_expression' during AST JSON deserialization");
    /// `ParserDescribeTableQuery` sets `temporary = true` only in the `DESCRIBE TEMPORARY TABLE <name>`
    /// branch, and only after confirming the table expression is a bare name (not a subquery or table
    /// function). Reject payloads that break that invariant — otherwise `formatImpl` would emit
    /// `DESCRIBE TEMPORARY TABLE numbers(1)` / `DESCRIBE TEMPORARY TABLE (SELECT ...)`, which the SQL
    /// parser cannot re-read.
    if (temporary && !child->as<ASTTableExpression>()->database_and_table_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`DescribeQuery` 'temporary' is valid only with a plain table name 'table_expression' during AST JSON deserialization");
    table_expression = child;
    children.push_back(table_expression);
    readOutputOptionsJSON(r);
}

}
