#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDescribeTableQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>

#include <Common/typeid_cast.h>


namespace DB
{

namespace
{

/// The input begins with `(`. Decide whether the leading parenthesis encloses only the first
/// element of a top-level set operation, e.g. `(SELECT 1) UNION ALL (SELECT 2)`, as opposed to a
/// single parenthesized subquery used as a table expression, e.g. `(SELECT 1 UNION ALL SELECT 2)`
/// or `(SELECT 1) AS source`. We skip past the parenthesis matching the leading `(` and check
/// whether it is immediately followed by a set-operation keyword.
bool isTopLevelSetOperation(IParser::Pos pos)
{
    int depth = 0;
    do
    {
        if (pos->type == TokenType::OpeningRoundBracket)
            ++depth;
        else if (pos->type == TokenType::ClosingRoundBracket)
            --depth;
        ++pos;
    } while (depth > 0 && pos.isValid());

    Expected expected;
    return ParserKeyword(Keyword::UNION).ignore(pos, expected)
        || ParserKeyword(Keyword::EXCEPT).ignore(pos, expected)
        || ParserKeyword(Keyword::INTERSECT).ignore(pos, expected);
}

}

bool ParserDescribeTableQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_describe(Keyword::DESCRIBE);
    ParserKeyword s_desc(Keyword::DESC);
    ParserKeyword s_temporary(Keyword::TEMPORARY);
    ParserKeyword s_table(Keyword::TABLE);
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserSetQuery parser_settings(true);

    ASTPtr select;

    if (!s_describe.ignore(pos, expected) && !s_desc.ignore(pos, expected))
        return false;

    auto query = make_intrusive<ASTDescribeQuery>();

    /// TEMPORARY is only recognized in the explicit "DESCRIBE TEMPORARY TABLE ..." form.
    /// This avoids breaking "DESCRIBE temporary" where `temporary` is an unquoted table name.
    bool temporary = false;
    {
        auto saved_pos = pos;
        if (s_temporary.ignore(pos, expected))
        {
            if (s_table.ignore(pos, expected))
            {
                temporary = true;
            }
            else
            {
                /// Not followed by TABLE — revert so `temporary` is parsed as a table name.
                pos = saved_pos;
                s_table.ignore(pos, expected);
            }
        }
        else
        {
            s_table.ignore(pos, expected);
        }
    }

    /// A SELECT can be the source of DESCRIBE in several forms:
    ///   - a bare SELECT:                 DESCRIBE SELECT 1
    ///   - a set operation:               DESCRIBE (SELECT 1) UNION ALL (SELECT 2)
    /// while a single parenthesized SELECT is a table expression that may carry an alias:
    ///   - a subquery with an alias:      DESCRIBE (SELECT 1) AS source
    ///   - a subquery containing a set operation with an alias:
    ///                                    DESCRIBE (SELECT 1 UNION ALL SELECT 2) AS source
    /// The latter must go through ParserTableExpression so the trailing alias is accepted.
    /// To tell them apart we speculatively parse a SELECT. We keep it when it is a bare SELECT or
    /// a genuine top-level set operation; a lone parenthesized SELECT (even one whose body is a set
    /// operation) is rolled back and handled as a table expression below. Note that
    /// ParserSelectWithUnionQuery lifts up a single parenthesized inner union, so the size of
    /// list_of_selects cannot distinguish `(SELECT 1) UNION ALL (SELECT 2)` from
    /// `(SELECT 1 UNION ALL SELECT 2)`; we instead inspect what follows the leading parenthesis.
    bool parsed_as_select = false;
    {
        auto saved_pos = pos;
        ASTPtr parsed_select;
        if (ParserSelectWithUnionQuery().parse(pos, parsed_select, expected))
        {
            const bool keep_select = saved_pos->type != TokenType::OpeningRoundBracket
                || isTopLevelSetOperation(saved_pos);

            if (keep_select)
            {
                select = std::move(parsed_select);
                parsed_as_select = true;
            }
            else
            {
                /// A single parenthesized SELECT: roll back and let ParserTableExpression pick up the alias.
                pos = saved_pos;
            }
        }
    }

    if (parsed_as_select)
    {
        /// TEMPORARY is only valid with a table name, not with a subquery or SELECT
        if (temporary)
            return false;

        auto table_expr = make_intrusive<ASTTableExpression>();
        /// Wrap SELECT in ASTSubquery, as expected by the rest of the codebase
        auto subquery = make_intrusive<ASTSubquery>(std::move(select));
        table_expr->subquery = subquery;
        table_expr->children.push_back(table_expr->subquery);
        query->table_expression = table_expr;
    }
    else if (!ParserTableExpression().parse(pos, query->table_expression, expected))
    {
        return false;
    }
    else if (temporary)
    {
        /// TEMPORARY is only valid with a table name, not with a table function or subquery
        auto * table_expr = query->table_expression->as<ASTTableExpression>();
        if (!table_expr || !table_expr->database_and_table_name)
            return false;
        query->temporary = true;
    }

    /// For compatibility with SELECTs, where SETTINGS can be in front of FORMAT
    ASTPtr settings;
    if (s_settings.ignore(pos, expected))
    {
        if (!parser_settings.parse(pos, query->settings_ast, expected))
            return false;
    }

    query->children.push_back(query->table_expression);

    if (query->settings_ast)
        query->children.push_back(query->settings_ast);

    node = query;

    return true;
}

}

namespace DB
{

void registerStatementDescribeTable(StatementFactory & factory)
{
    factory.registerStatement("DESCRIBE TABLE",
    {
        .description = R"DOCS_MD(
Returns information about table columns.

**Syntax**

```sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

The `DESCRIBE` statement returns a row for each table column with the following [String](/reference/data-types/string) values:

- `name` — A column name.
- `type` — A column type.
- `default_type` — A clause that is used in the column [default expression](/reference/statements/create/table): `DEFAULT`, `MATERIALIZED` or `ALIAS`. If there is no default expression, then empty string is returned.
- `default_expression` — An expression specified after the `DEFAULT` clause.
- `comment` — A [column comment](/reference/statements/alter/column#comment-column).
- `codec_expression` — A [codec](/reference/statements/create/table/codec) that is applied to the column.
- `ttl_expression` — A [TTL](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl) expression.
- `is_subcolumn` — A flag that equals `1` for internal subcolumns. It is included into the result only if subcolumn description is enabled by the [describe_include_subcolumns](/reference/settings/session-settings/describe-include#describe_include_subcolumns) setting.

All columns in [Nested](/reference/data-types/nested-data-structures/index) data structures are described separately. The name of each column is prefixed with a parent column name and a dot.

To show internal subcolumns of other data types, use the [describe_include_subcolumns](/reference/settings/session-settings/describe-include#describe_include_subcolumns) setting.

**Example**

```sql title="Query"
CREATE TABLE describe_example (
    id UInt64, text String DEFAULT 'unknown' CODEC(ZSTD),
    user Tuple (name String, age UInt8)
) ENGINE = MergeTree() ORDER BY id;

DESCRIBE TABLE describe_example;
DESCRIBE TABLE describe_example SETTINGS describe_include_subcolumns=1;
```

```text title="Response"
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id   │ UInt64                        │              │                    │         │                  │                │
│ text │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │
│ user │ Tuple(name String, age UInt8) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

The second query additionally shows subcolumns:

```text title="Response"
┌─name──────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┬─is_subcolumn─┐
│ id        │ UInt64                        │              │                    │         │                  │                │            0 │
│ text      │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │            0 │
│ user      │ Tuple(name String, age UInt8) │              │                    │         │                  │                │            0 │
│ user.name │ String                        │              │                    │         │                  │                │            1 │
│ user.age  │ UInt8                         │              │                    │         │                  │                │            1 │
└───────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┴──────────────┘
```

The DESCRIBE statement can also be used with subqueries or scalar expressions:

``` SQL
DESCRIBE SELECT 1 FORMAT TSV;
```

or

``` SQL
DESCRIBE (SELECT 1) FORMAT TSV;
```

``` text title="Response"
1       UInt8
```

This usage returns metadata about the result columns of the specified query or subquery. It is useful for understanding the structure of complex queries before execution.

**See Also**

- [describe_include_subcolumns](/reference/settings/session-settings/describe-include#describe_include_subcolumns) setting.
)DOCS_MD",
        .syntax = R"(
DESC|DESCRIBE [TABLE] [db.]table | (subquery) | table_function [INTO OUTFILE filename] [FORMAT format]
)",
        .related = {"SHOW", "EXISTS", "CREATE TABLE"},
    });
}

}
