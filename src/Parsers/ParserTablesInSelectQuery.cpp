#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserStreamSettings.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Core/Joins.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


bool ParserTableExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto res = make_intrusive<ASTTableExpression>();

    if (!ParserWithOptionalAlias(std::make_unique<ParserSubquery>(), allow_alias_without_as_keyword).parse(pos, res->subquery, expected)
        && !ParserWithOptionalAlias(std::make_unique<ParserFunction>(false, true), allow_alias_without_as_keyword).parse(pos, res->table_function, expected)
        && !ParserWithOptionalAlias(std::make_unique<ParserCompoundIdentifier>(true, true), allow_alias_without_as_keyword)
                .parse(pos, res->database_and_table_name, expected)
        && !ParserWithOptionalAlias(std::make_unique<ParserTableAsStringLiteralIdentifier>(), allow_alias_without_as_keyword)
                .parse(pos, res->database_and_table_name, expected))
    {
        /// Parenthesized table join expression: (t1 JOIN t2 ON ...) → SELECT * FROM t1 JOIN t2 ON ...
        /// Standard SQL allows parentheses around joined table expressions in FROM clauses.
        if (pos->type == TokenType::OpeningRoundBracket)
        {
            auto open_paren = pos;
            ++pos;

            ASTPtr tables_in_select;
            if (ParserTablesInSelectQuery(false).parse(pos, tables_in_select, expected)
                && pos->type == TokenType::ClosingRoundBracket
                && tables_in_select->as<ASTTablesInSelectQuery &>().children.size() > 1)
            {
                ++pos;

                /// Build: SELECT * FROM <parsed_tables>
                auto select_ast = make_intrusive<ASTSelectQuery>();
                select_ast->setExpression(ASTSelectQuery::Expression::SELECT, make_intrusive<ASTExpressionList>());
                select_ast->select()->children.push_back(make_intrusive<ASTAsterisk>());
                select_ast->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables_in_select));

                auto list_of_selects = make_intrusive<ASTExpressionList>();
                list_of_selects->children.push_back(select_ast);

                auto select_with_union = make_intrusive<ASTSelectWithUnionQuery>();
                select_with_union->children.push_back(std::move(list_of_selects));
                select_with_union->list_of_selects = select_with_union->children.back();

                res->subquery = make_intrusive<ASTSubquery>(std::move(select_with_union));

                /// Parse optional alias: (t1 CROSS JOIN t2) AS j
                ParserAlias alias_parser(allow_alias_without_as_keyword);
                ASTPtr alias_node;
                if (alias_parser.parse(pos, alias_node, expected))
                    res->subquery->setAlias(getIdentifierName(alias_node));
            }
            else
            {
                pos = open_paren;
                return false;
            }
        }
        else
        {
            return false;
        }
    }

    /// parse column aliases `AS alias(col1, col2, ...)`, check for (col1, col2, ...)
    if (pos->type == TokenType::OpeningRoundBracket)
    {
        ++pos;
        ParserAliasesExpressionList column_aliases_parser;
        if (!column_aliases_parser.parse(pos, res->column_aliases, expected))
            return false;

        if (pos->type != TokenType::ClosingRoundBracket)
            return false;
        ++pos;
    }

    /// FINAL
    if (ParserKeyword(Keyword::FINAL).ignore(pos, expected))
        res->final = true;

    /// SAMPLE number
    if (ParserKeyword(Keyword::SAMPLE).ignore(pos, expected))
    {
        ParserSampleRatio ratio;

        if (!ratio.parse(pos, res->sample_size, expected))
            return false;

        /// OFFSET number
        if (ParserKeyword(Keyword::OFFSET).ignore(pos, expected))
        {
            if (!ratio.parse(pos, res->sample_offset, expected))
                return false;
        }
    }

    /// STREAM [CURSOR '{...}']
    if (ParserKeyword(Keyword::STREAM).ignore(pos, expected))
    {
        ParserStreamSettings stream_settings_p;

        if (!stream_settings_p.parse(pos, res->stream_settings, expected))
            return false;
    }

    if (res->database_and_table_name)
        res->children.emplace_back(res->database_and_table_name);
    if (res->table_function)
        res->children.emplace_back(res->table_function);
    if (res->subquery)
        res->children.emplace_back(res->subquery);
    if (res->sample_size)
        res->children.emplace_back(res->sample_size);
    if (res->sample_offset)
        res->children.emplace_back(res->sample_offset);
    if (res->stream_settings)
        res->children.emplace_back(res->stream_settings);
    if (res->column_aliases)
        res->children.emplace_back(res->column_aliases);

    chassert(res->database_and_table_name || res->table_function || res->subquery);

    node = res;
    return true;
}


bool ParserArrayJoin::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto res = make_intrusive<ASTArrayJoin>();

    /// [LEFT] ARRAY JOIN expr list
    Pos saved_pos = pos;
    bool has_array_join = false;

    if (ParserKeyword(Keyword::LEFT_ARRAY_JOIN).ignore(pos, expected))
    {
        res->kind = ASTArrayJoin::Kind::Left;
        has_array_join = true;
    }
    else
    {
        pos = saved_pos;

        /// INNER may be specified explicitly, otherwise it is assumed as default.
        ParserKeyword(Keyword::INNER).ignore(pos, expected);

        if (ParserKeyword(Keyword::ARRAY_JOIN).ignore(pos, expected))
        {
            res->kind = ASTArrayJoin::Kind::Inner;
            has_array_join = true;
        }
    }

    if (!has_array_join)
        return false;

    /// An empty expression list is not a valid ARRAY JOIN clause: the analyzer rejects it, and the
    /// formatter would emit a dangling `ARRAY JOIN` keyword that cannot be parsed back, because inside
    /// a set operation it swallows the next branch's SELECT.
    if (!ParserNotEmptyExpressionList(false).parse(pos, res->expression_list, expected))
        return false;

    if (res->expression_list)
        res->children.emplace_back(res->expression_list);

    node = res;
    return true;
}


static void parseJoinStrictness(IParser::Pos & pos, ASTTableJoin & table_join, Expected & expected)
{
    if (ParserKeyword(Keyword::ANY).ignore(pos, expected))
        table_join.strictness = JoinStrictness::Any;
    else if (ParserKeyword(Keyword::ALL).ignore(pos, expected))
        table_join.strictness = JoinStrictness::All;
    else if (ParserKeyword(Keyword::ASOF).ignore(pos, expected))
        table_join.strictness = JoinStrictness::Asof;
    else if (ParserKeyword(Keyword::SEMI).ignore(pos, expected))
        table_join.strictness = JoinStrictness::Semi;
    else if (ParserKeyword(Keyword::ANTI).ignore(pos, expected) || ParserKeyword(Keyword::ONLY).ignore(pos, expected))
        table_join.strictness = JoinStrictness::Anti;
}

bool ParserTablesInSelectQueryElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto res = make_intrusive<ASTTablesInSelectQueryElement>();

    if (is_first)
    {
        if (!ParserTableExpression(allow_alias_without_as_keyword).parse(pos, res->table_expression, expected))
            return false;
    }
    else if (ParserArrayJoin().parse(pos, res->array_join, expected))
    {
    }
    else
    {
        auto table_join = make_intrusive<ASTTableJoin>();

        if (pos->type == TokenType::Comma)
        {
            ++pos;
            table_join->kind = JoinKind::Comma;
        }
        else
        {
            if (ParserKeyword(Keyword::GLOBAL).ignore(pos, expected))
                table_join->locality = JoinLocality::Global;
            else if (ParserKeyword(Keyword::LOCAL).ignore(pos, expected))
                table_join->locality = JoinLocality::Local;

            bool is_natural = ParserKeyword(Keyword::NATURAL).ignore(pos, expected);

            table_join->strictness = JoinStrictness::Unspecified;

            /// Legacy: allow JOIN type before JOIN kind
            parseJoinStrictness(pos, *table_join, expected);

            bool no_kind = false;
            if (ParserKeyword(Keyword::INNER).ignore(pos, expected))
                table_join->kind = JoinKind::Inner;
            else if (ParserKeyword(Keyword::LEFT).ignore(pos, expected))
                table_join->kind = JoinKind::Left;
            else if (ParserKeyword(Keyword::RIGHT).ignore(pos, expected))
                table_join->kind = JoinKind::Right;
            else if (ParserKeyword(Keyword::FULL).ignore(pos, expected))
                table_join->kind = JoinKind::Full;
            else if (ParserKeyword(Keyword::CROSS).ignore(pos, expected))
                table_join->kind = JoinKind::Cross;
            else if (ParserKeyword(Keyword::PASTE).ignore(pos, expected))
                table_join->kind = JoinKind::Paste;
            else
                no_kind = true;

            /// Standard position: JOIN type after JOIN kind
            parseJoinStrictness(pos, *table_join, expected);

            /// Optional OUTER keyword for outer joins.
            if (table_join->kind == JoinKind::Left
                || table_join->kind == JoinKind::Right
                || table_join->kind == JoinKind::Full)
            {
                ParserKeyword(Keyword::OUTER).ignore(pos, expected);
            }

            if (no_kind)
            {
                /// Use INNER by default as in another DBMS.
                if (table_join->strictness == JoinStrictness::Semi ||
                    table_join->strictness == JoinStrictness::Anti)
                    table_join->kind = JoinKind::Left;
                else
                    table_join->kind = JoinKind::Inner;
            }

            if (table_join->strictness != JoinStrictness::Unspecified
                && (table_join->kind == JoinKind::Cross || table_join->kind == JoinKind::Paste))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "You must not specify ANY or ALL for {} JOIN.", toString(table_join->kind));

            if ((table_join->strictness == JoinStrictness::Semi || table_join->strictness == JoinStrictness::Anti) &&
                (table_join->kind != JoinKind::Left && table_join->kind != JoinKind::Right))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "SEMI|ANTI JOIN should be LEFT or RIGHT.");

            if (is_natural && table_join->strictness != JoinStrictness::Unspecified)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "NATURAL JOIN cannot be combined with ANY/ALL/ASOF/SEMI/ANTI modifiers.");

            if (is_natural && (table_join->kind == JoinKind::Cross || table_join->kind == JoinKind::Paste))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "NATURAL JOIN cannot be used with CROSS or PASTE join.");

            table_join->is_natural = is_natural;

            if (!ParserKeyword(Keyword::JOIN).ignore(pos, expected))
                return false;
        }

        /// `LATERAL` is only a keyword when it is followed by a subquery, the only supported lateral shape.
        /// Otherwise it is left to `ParserTableExpression`, so a table, view or table function named `lateral`
        /// (`JOIN lateral ON ...`, `JOIN lateral(...) ON ...`) still parses.
        Pos before_lateral = pos;
        bool is_lateral = false;
        if (ParserKeyword(Keyword::LATERAL).ignore(pos, expected) && pos->type == TokenType::OpeningRoundBracket)
        {
            is_lateral = ParserTableExpression(allow_alias_without_as_keyword).parse(pos, res->table_expression, expected)
                && res->table_expression->as<ASTTableExpression &>().subquery;
        }

        if (is_lateral)
        {
            table_join->lateral = true;
        }
        else
        {
            pos = before_lateral;
            res->table_expression = nullptr;
            if (!ParserTableExpression(allow_alias_without_as_keyword).parse(pos, res->table_expression, expected))
                return false;
        }

        if (table_join->kind != JoinKind::Comma
            && table_join->kind != JoinKind::Cross && table_join->kind != JoinKind::Paste)
        {
            if (table_join->is_natural)
            {
                /// NATURAL JOIN: the USING columns are derived automatically from common column names during analysis.
            }
            else if (ParserKeyword(Keyword::USING).ignore(pos, expected))
            {
                /// Expression for USING could be in parentheses or not.
                bool in_parens = pos->type == TokenType::OpeningRoundBracket;
                if (in_parens)
                    ++pos;

                if (!ParserExpressionList(false).parse(pos, table_join->using_expression_list, expected))
                    return false;

                if (table_join->using_expression_list->children.empty())
                {
                    expected.variants.clear();
                    expected.add(pos, "column identifier for USING");
                    return false;
                }

                if (in_parens)
                {
                    if (pos->type != TokenType::ClosingRoundBracket)
                        return false;
                    ++pos;
                }
            }
            else if (ParserKeyword(Keyword::ON).ignore(pos, expected))
            {
                if (!ParserExpression().parse(pos, table_join->on_expression, expected))
                    return false;
            }
            else
            {
                return false;
            }
        }

        if (table_join->using_expression_list)
            table_join->children.emplace_back(table_join->using_expression_list);
        if (table_join->on_expression)
            table_join->children.emplace_back(table_join->on_expression);

        res->table_join = table_join;
    }

    if (res->table_expression)
        res->children.emplace_back(res->table_expression);
    if (res->table_join)
        res->children.emplace_back(res->table_join);
    if (res->array_join)
        res->children.emplace_back(res->array_join);

    node = res;
    return true;
}


bool ParserTablesInSelectQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto res = make_intrusive<ASTTablesInSelectQuery>();

    ASTPtr child;

    if (ParserTablesInSelectQueryElement(true, allow_alias_without_as_keyword).parse(pos, child, expected))
        res->children.emplace_back(child);
    else
        return false;

    while (true)
    {
        /// A comma (cross) join right after an ARRAY JOIN is not supported: reject it
        /// instead of misparsing the item after the comma as a table.
        const auto * prev = res->children.back()->as<ASTTablesInSelectQueryElement>();
        if (prev && prev->array_join && pos->type == TokenType::Comma)
            break;

        if (!ParserTablesInSelectQueryElement(false, allow_alias_without_as_keyword).parse(pos, child, expected))
            break;
        res->children.emplace_back(child);
    }

    node = res;
    return true;
}

std::map<String, Documentation> ParserTablesInSelectQuery::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["FROM"] =
    {
        .description = R"DOCS_MD(
The `FROM` clause specifies the source to read data from:

- [Table](/reference/engines/table-engines/index)
- [Subquery](/reference/statements/select/index)
- [Table function](/reference/functions/table-functions/index)

[JOIN](/reference/statements/select/join) and [ARRAY JOIN](/reference/statements/select/array-join) clauses may also be used to extend the functionality of the `FROM` clause.

Subquery is another `SELECT` query that may be specified in parenthesis inside `FROM` clause.

A SQL standard `VALUES` clause can also be used as a table expression:

```sql
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

See [Values table function](/reference/functions/table-functions/values#sql-standard-values-clause) for more details.

The `FROM` can contain multiple data sources, separated by commas, which is equivalent of performing [CROSS JOIN](/reference/statements/select/join) on them.

`FROM` can optionally appear before a `SELECT` clause. This is a ClickHouse-specific extension of standard SQL which makes `SELECT` statements easier to read. Example:

```sql
FROM table
SELECT *
```

In a query that starts with `FROM`, the `SELECT` clause is optional - when it is omitted, the query works as if `SELECT *` was written:

```sql
FROM table
WHERE x > 1
```

This form is mostly used in queries with [pipe operators](/reference/statements/select/pipe-operators):

```sql
FROM table
|> WHERE x > 1
|> AGGREGATE count() AS c GROUP BY y
```

## FINAL Modifier {#final-modifier}

When `FINAL` is specified, ClickHouse fully merges the data before returning the result. This also performs all data transformations that happen during merges for the given table engine.

It is applicable when selecting data from tables using the following table engines:
- `ReplacingMergeTree`
- `SummingMergeTree`
- `AggregatingMergeTree`
- `CollapsingMergeTree`
- `VersionedCollapsingMergeTree`

`SELECT` queries with `FINAL` are executed in parallel. The [max_final_threads](/reference/settings/session-settings/max#max_final_threads) setting limits the number of threads used.

### Drawbacks {#drawbacks}

Queries that use `FINAL` execute slightly slower than similar queries that do not use `FINAL` because:

- Data is merged during query execution.
- Queries with `FINAL` may read primary key columns in addition to the columns specified in the query.

`FINAL` requires additional compute and memory resources because the processing that normally would occur at merge time must occur in memory at the time of the query. However, using FINAL is sometimes necessary in order to produce accurate results (as data may not yet be fully merged). It is less expensive than running `OPTIMIZE` to force a merge.

As an alternative to using `FINAL`, it is sometimes possible to use different queries that assume the background processes of the `MergeTree` engine have not yet occurred and deal with it by applying an aggregation (for example, to discard duplicates). If you need to use `FINAL` in your queries in order to get the required results, it is okay to do so but be aware of the additional processing required.

`FINAL` can be applied automatically using [FINAL](/reference/settings/session-settings/other#final) setting to all tables in a query using a session or a user profile.

### Example Usage {#example-usage}

Using the `FINAL` keyword

```sql
SELECT x, y FROM mytable FINAL WHERE x > 1;
```

Using `FINAL` as a query-level setting

```sql
SELECT x, y FROM mytable WHERE x > 1 SETTINGS final = 1;
```

Using `FINAL` as a session-level setting

```sql
SET final = 1;
SELECT x, y FROM mytable WHERE x > 1;
```

### Aliases and FINAL {#aliases-and-final}

When a table has an alias, `FINAL` comes after the alias. This is most visible in [`JOIN`](/reference/statements/select/join) queries, where tables are usually aliased:

```sql
SELECT t1.id, t2.name
FROM table1 AS t1 FINAL
INNER JOIN table2 AS t2 FINAL ON t1.id = t2.id;
```

`FINAL` is a modifier on the table reference, so it must follow the full `table [AS alias]` expression. Placing it before the alias (`FROM table1 FINAL AS t1`) is a syntax error.

## Implementation Details {#implementation-details}

If the `FROM` clause is omitted, data will be read from the `system.one` table.
The `system.one` table contains exactly one row (this table fulfills the same purpose as the DUAL table found in other DBMSs).

To execute a query, all the columns listed in the query are extracted from the appropriate table. Any columns not needed for the external query are thrown out of the subqueries.
If a query does not list any columns (for example, `SELECT count() FROM t`), some column is extracted from the table anyway (the smallest one is preferred), in order to calculate the number of rows.
)DOCS_MD",
        .syntax = R"(
SELECT ... FROM [db.]table | (subquery) | table_function | VALUES (...) [FINAL] [SAMPLE ...] ...
FROM [db.]table SELECT ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "JOIN", "ARRAY JOIN", "SAMPLE", "WHERE"},
    };

    documentation["JOIN"] =
    {
        .description = R"DOCS_MD(
The `JOIN` clause produces a new table by combining columns from one or multiple tables by using values common to each. It is a common operation in databases with SQL support, which corresponds to [relational algebra](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators) join. The special case of one table join is often referred to as a "self-join".

**Syntax**

```sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ALL|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

Expressions from the `ON` clause and columns from the `USING` clause are called "JOIN keys". Unless otherwise stated, a `JOIN` produces a [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) from rows with matching JOIN keys, which might produce results with many more rows than the source tables.

This page describes the syntax and semantics of the `JOIN` clause. To learn about the execution of JOINs in ClickHouse, JOIN types, settings, memory considerations, and optimizations, refer to [Related content](#related-content).

## Supported types of JOIN {#supported-types-of-join}

All standard [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) types are supported:

| Type              | Description                                                                   |
|-------------------|-------------------------------------------------------------------------------|
| `INNER JOIN`      | Only matching rows are returned.                                              |
| `LEFT OUTER JOIN` | Non-matching rows from the left table are returned in addition to matching rows.  |
| `RIGHT OUTER JOIN`| Non-matching rows from the right table are returned in addition to matching rows. |
| `FULL OUTER JOIN` | Non-matching rows from both tables are returned in addition to matching rows. |
| `CROSS JOIN`      | Produces the Cartesian product of whole tables. JOIN keys are **not** specified.|
| `NATURAL JOIN`    | Automatically joins on all columns with the same name in both tables. Each common column appears once in the result. Supports `INNER` (default), `LEFT`, `RIGHT`, and `FULL` variants. Equivalent to `JOIN ... USING (col1, col2, ...)` where the column list is derived automatically. |

- `JOIN` without a type specified implies `INNER`.
- The keyword `OUTER` can be omitted.
- An alternative syntax for `CROSS JOIN` is specifying multiple tables in the [`FROM` clause](/reference/statements/select/from) separated by commas.
- If there are no matching columns for a `NATURAL JOIN`, it functions like a `CROSS JOIN`.

Additional JOIN types available in ClickHouse are:

| Type                                        | Description                                                                                                                               |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `LEFT SEMI JOIN`, `RIGHT SEMI JOIN`         | An allowlist on JOIN keys, without producing a Cartesian product.                                                                        |
| `LEFT ANTI JOIN`, `RIGHT ANTI JOIN`         | A denylist on JOIN keys, without producing a Cartesian product.                                                                        |
| `LEFT ANY JOIN`, `RIGHT ANY JOIN`, `INNER ANY JOIN` | Partially (for the opposite side of `LEFT` and `RIGHT`) or completely (for `INNER`) disables the Cartesian product for standard `JOIN` types. `ANY FULL JOIN` is not supported. |
| `ASOF JOIN`, `LEFT ASOF JOIN`               | Joining sequences with a non-exact match. See [ASOF JOIN usage](#asof-join-usage).                                                           |
| `PASTE JOIN`                                | Performs a horizontal concatenation of two tables. See [PASTE JOIN usage](#paste-join-usage).                                                                                          |
| `INNER JOIN LATERAL`, `LEFT JOIN LATERAL`, `CROSS JOIN LATERAL`, `, LATERAL` | Joins a subquery that references columns of the tables on its left side. Experimental, requires [`enable_lateral_join`](/reference/settings/session-settings/enable#enable_lateral_join). See [LATERAL JOIN](#lateral-join). |

Diagrams of each JOIN type and strictness are shown in [Using JOINs in ClickHouse](/concepts/features/operations/select/joining-tables#join-types-and-strictness).

<Note>
Disabling the `semi_join_include_columns_from_both_sides` or `anti_join_include_columns_from_both_sides` setting makes the corresponding join expose only its preserved side to expressions resolved after the join result is formed.

- `LEFT SEMI JOIN` and `LEFT ANTI JOIN` expose only left-side columns.
- `RIGHT SEMI JOIN` and `RIGHT ANTI JOIN` expose only right-side columns.
- This affects clauses such as `SELECT`, `PREWHERE`, `WHERE`, `GROUP BY`, `HAVING`, `QUALIFY`, `ORDER BY`, and `LIMIT BY`, including qualified wildcards like `t1.*`.
- The `ON` expression of the same `JOIN` can still reference both sides.

When these settings are enabled (the default), ClickHouse keeps the legacy behavior, where both sides remain accessible and `SELECT *` expands columns from both tables.
</Note>

Not every JOIN algorithm supports every combination of JOIN type and strictness. The supported combinations are listed in [Supported JOIN types per algorithm](/concepts/features/operations/select/join-algorithms#supported-join-types-per-algorithm).

### Rewriting `CROSS JOIN` as `INNER JOIN` {#cross-join-rewrite}

When the `WHERE` clause of a query that uses a `CROSS JOIN` or a comma-separated list of tables contains equality conditions between the tables, ClickHouse can rewrite the JOIN as an `INNER JOIN` that uses these conditions as JOIN keys. The [`cross_to_inner_join_rewrite`](/reference/settings/session-settings/other#cross_to_inner_join_rewrite) setting controls this rewrite:

- `0`: The rewrite is disabled.
- `1`: A comma-separated list of tables or a `CROSS JOIN` is rewritten when the `WHERE` clause contains an equality condition between the tables. Otherwise, the JOIN is executed as a `CROSS JOIN`.
- `2`: As with `1`, but a comma-separated list of tables that cannot be rewritten makes the query fail with the `INCORRECT_QUERY` error if the query has a `WHERE` clause. A JOIN written with the `CROSS JOIN` keywords that cannot be rewritten is executed as a `CROSS JOIN`.

With the value `0`, the query plan optimization controlled by [`query_plan_merge_filter_into_join_condition`](/reference/settings/session-settings/query-plan#query_plan_merge_filter_into_join_condition) can still merge an equality filter into the JOIN and execute it as an `INNER JOIN`.

## ON section conditions {#on-section-conditions}

An `ON` section can contain several conditions combined using the `AND` and `OR` operators. Conditions specifying JOIN keys must:
- reference both left and right tables
- use the equality operator

Other conditions may use other operators and may reference either the left or the right table of a query, or both.

Rows are joined if the whole complex condition is met. If the conditions are not met, rows may still be included in the result depending on the `JOIN` type. If the same conditions are placed in a `WHERE` section and they are not met, then rows are always filtered out from the result.

JOIN keys combined with `OR` are supported only by the [`hash`](/concepts/features/operations/select/join-algorithms#hash) algorithm, which builds a separate hash table for each `OR` argument that contains JOIN keys.

<Note>
Only the equality operator (`=`) makes a condition a JOIN key. Other operators between columns of different tables are supported, but without any equality the JOIN has to examine every pair of rows; see [JOIN with an arbitrary ON condition](#join-with-an-arbitrary-on-condition).
</Note>

**Example**

Consider `table_1` and `table_2`:

```response
┌─Id─┬─name─┐     ┌─Id─┬─text───────────┬─scores─┐
│  1 │ A    │     │  1 │ Text A         │     10 │
│  2 │ B    │     │  1 │ Another text A │     12 │
│  3 │ C    │     │  2 │ Text B         │     15 │
└────┴──────┘     └────┴────────────────┴────────┘
```

Query with one JOIN key condition and an additional condition for `table_2`:

```sql title="Query"
SELECT name, text FROM table_1 LEFT OUTER JOIN table_2
    ON table_1.Id = table_2.Id AND startsWith(table_2.text, 'Text');
```

The result contains the row with the name `C` and the empty text column. It is included into the result because an `OUTER` type of a join is used.

```response title="Response"
┌─name─┬─text───┐
│ A    │ Text A │
│ B    │ Text B │
│ C    │        │
└──────┴────────┘
```

Query with `INNER` type of a join and multiple conditions:

```sql title="Query"
SELECT name, text, scores FROM table_1 INNER JOIN table_2
    ON table_1.Id = table_2.Id AND table_2.scores > 10 AND startsWith(table_2.text, 'Text');
```

```response title="Response"
┌─name─┬─text───┬─scores─┐
│ B    │ Text B │     15 │
└──────┴────────┴────────┘
```

Query with `INNER` type of a join and condition with `OR`:

```sql title="Query"
CREATE TABLE t1 (`a` Int64, `b` Int64) ENGINE = MergeTree() ORDER BY a;

CREATE TABLE t2 (`key` Int32, `val` Int64) ENGINE = MergeTree() ORDER BY key;

INSERT INTO t1 SELECT number as a, -a as b from numbers(5);

INSERT INTO t2 SELECT if(number % 2 == 0, toInt64(number), -number) as key, number as val from numbers(5);

SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 1 │ -1 │   1 │
│ 2 │ -2 │   2 │
│ 3 │ -3 │   3 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

Query with `INNER` type of a join and conditions with `OR` and `AND`:

<Note>

A non-equal condition that uses columns from a single table, such as `t1.a = t2.key AND t1.b > 0 AND t2.b > t2.c`, is applied to that table alone: `t1.b > 0` uses columns only from `t1` and `t2.b > t2.c` uses columns only from `t2`.
A non-equal condition that compares columns of different tables, such as `t1.a = t2.key AND t1.b > t2.key`, is also supported; see the sections below.
</Note>

```sql title="Query"
SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key AND t2.val > 3;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 2 │ -2 │   2 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

## JOIN with inequality conditions for columns from different tables {#join-with-inequality-conditions-for-columns-from-different-tables}

In addition to equality conditions, the `ON` section can contain conditions that compare columns of different tables with other operators. Such conditions are supported with `ALL`, `ANY`, `SEMI`, and `ANTI` strictness for every JOIN type that the strictness allows. They are not supported with `ASOF` strictness.

When the `ON` section contains an equality between the two tables next to the inequality, the equality is the JOIN key and the inequality is checked on the rows it matched. Such a mixed condition is executed by the [`hash`](/concepts/features/operations/select/join-algorithms#hash), [`parallel_hash`](/concepts/features/operations/select/join-algorithms#parallel-hash), and [`grace_hash`](/concepts/features/operations/select/join-algorithms#grace-hash) algorithms. When [`ie_join`](/concepts/features/operations/select/join-algorithms#ie-join) is listed ahead of those algorithms in [`join_algorithm`](/reference/settings/session-settings/join#join_algorithm) and the `ON` section contains two inequality conditions, `ie_join` executes the JOIN instead and applies the equality as a filter.

When the `ON` section contains no equality between the two tables, there is no JOIN key to match on. Such a JOIN is executed by [`ie_join`](/concepts/features/operations/select/join-algorithms#ie-join) when the condition is a pair of inequalities, or otherwise as a [block nested loop join](#join-with-an-arbitrary-on-condition).

A condition evaluated during the join may not contain `arrayJoin`, because it must preserve the number of rows; use `ARRAY JOIN` in a subquery instead.

**Example**

Table `t1`:

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ a    │ 1 │ 1 │ 2 │
│ key1 │ b    │ 2 │ 3 │ 2 │
│ key1 │ c    │ 3 │ 2 │ 1 │
│ key1 │ d    │ 4 │ 7 │ 2 │
│ key1 │ e    │ 5 │ 5 │ 5 │
│ key2 │ a2   │ 1 │ 1 │ 1 │
│ key4 │ f    │ 2 │ 3 │ 4 │
└──────┴──────┴───┴───┴───┘
```

Table `t2`

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ A    │ 1 │ 2 │ 1 │
│ key1 │ B    │ 2 │ 1 │ 2 │
│ key1 │ C    │ 3 │ 4 │ 5 │
│ key1 │ D    │ 4 │ 1 │ 6 │
│ key3 │ a3   │ 1 │ 1 │ 1 │
│ key4 │ F    │ 1 │ 1 │ 1 │
└──────┴──────┴───┴───┴───┘
```

```sql
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.key = t2.key AND (t1.a < t2.a) ORDER BY (t1.key, t1.attr, t2.key, t2.attr);
```

```response
key1    a    1    1    2    key1    B    2    1    2
key1    a    1    1    2    key1    C    3    4    5
key1    a    1    1    2    key1    D    4    1    6
key1    b    2    3    2    key1    C    3    4    5
key1    b    2    3    2    key1    D    4    1    6
key1    c    3    2    1    key1    D    4    1    6
key1    d    4    7    2            0    0    \N
key1    e    5    5    5            0    0    \N
key2    a2    1    1    1            0    0    \N
key4    f    2    3    4            0    0    \N
```

## JOIN with an arbitrary ON condition {#join-with-an-arbitrary-on-condition}

The `ON` section may be an arbitrary boolean expression over the columns of both tables, such as a range check, an arithmetic comparison, or a call to a scalar function. When it contains no equality between the two tables, there is no JOIN key to match rows on. A pair of inequality conditions is executed by `ie_join` whenever that algorithm is listed in [`join_algorithm`](/reference/settings/session-settings/join#join_algorithm), with no requirement that it come first, because no other `join_algorithm` value can execute such a condition. The default value of `join_algorithm` includes `ie_join` since version 26.8. Any other condition is executed as a [block nested loop join](https://en.wikipedia.org/wiki/Nested_loop_join): the right table is materialized and the condition is evaluated on every pair of rows. An `ALL INNER JOIN` takes the equivalent form of a `CROSS JOIN` with the condition as a filter.

The block nested loop join supports every JOIN type and strictness except `ASOF JOIN`, `PASTE JOIN` and `ANY FULL JOIN`. It is not one of the `join_algorithm` values: it is the last resort, used only when no other algorithm can execute the condition. In `EXPLAIN` output it appears as a `BlockNestedLoopJoin` step. When [`allow_block_nested_loop_join`](/reference/settings/session-settings/allow#allow_block_nested_loop_join) is disabled, a query that would need it is rejected with `INVALID_JOIN_ON_EXPRESSION`.

**Example**

```sql title="Query"
CREATE TABLE orders (id UInt32, amount UInt32) ENGINE = Memory;
CREATE TABLE discounts (min_amount UInt32, max_amount UInt32, pct UInt32) ENGINE = Memory;

INSERT INTO orders VALUES (1, 50), (2, 150), (3, 300);
INSERT INTO discounts VALUES (100, 199, 5), (200, 1000, 10);

SELECT o.id, o.amount, d.pct
FROM orders AS o
LEFT JOIN discounts AS d ON o.amount BETWEEN d.min_amount AND d.max_amount
ORDER BY o.id;
```

```response title="Response"
┌─id─┬─amount─┬─pct─┐
│  1 │     50 │   0 │
│  2 │    150 │   5 │
│  3 │    300 │  10 │
└────┴────────┴─────┘
```

The row that matched nothing is padded according to [`join_use_nulls`](/reference/settings/session-settings/join#join_use_nulls), as in any other JOIN: with a default value above, and with `NULL` when the setting is enabled.

**Strictness**

`ANY` and `SEMI` normally keep one row per group of rows that share a JOIN key. There is no such group here, so they keep one row per row of the table that drives the JOIN: `LEFT ANY` and `LEFT SEMI` emit each left row at most once, `RIGHT ANY` and `RIGHT SEMI` each right row at most once. `ANY INNER` limits both sides at once: each row of either table is used at most once, so the result has at most as many rows as the smaller table. Which pairs make up such a result is arbitrary, exactly as `ANY` implies, and it may differ between two runs of the same query. For `ANY INNER` this extends to the number of rows: pairing each row of both sides greedily, in whatever order the rows are examined, may leave a different number of rows unpaired, so the result of `count()` over an `ANY INNER` JOIN with no JOIN key is not reproducible and depends on the number of threads and on the physical order of the right table.

**Performance**

A block nested loop join examines every pair of rows, so its cost grows with the product of the row counts of both tables, as described in [Block nested loop fallback](/concepts/features/operations/select/join-algorithms#block-nested-loop-fallback). The materialized right table is subject to the hard limits and spill thresholds described in [JOIN memory management and spilling](/concepts/features/operations/select/join-memory-management).

## NULL and NaN values in JOIN keys {#null-values-in-join-keys}

`NULL` is not equal to any value, including itself. If a JOIN key has a `NULL` value in one table, it does not match a `NULL` value in the other table. If the JOIN keys are [Nullable](/reference/data-types/nullable) columns, a row in which at least one of the keys is [NULL](/reference/syntax#null) is not joined.

**Example**

Table `A`:

```response
┌───id─┬─name────┐
│    1 │ Alice   │
│    2 │ Bob     │
│ ᴺᵁᴸᴸ │ Charlie │
└──────┴─────────┘
```

Table `B`:

```response
┌───id─┬─score─┐
│    1 │    90 │
│    3 │    85 │
│ ᴺᵁᴸᴸ │    88 │
└──────┴───────┘
```

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON A.id = B.id
```

```response
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │     0 │
└─────────┴───────┘
```

The row with `Charlie` from table `A` and the row with score 88 from table `B` are not matched because of the `NULL` value in the JOIN key.

The rows without a match get the value `0` in the `score` column, as described in [Processing of empty or NULL cells](#processing-of-empty-or-null-cells).

To match `NULL` values, compare the JOIN keys with the `isNotDistinctFrom` function:

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON isNotDistinctFrom(A.id, B.id)
```

```response
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │    88 │
└─────────┴───────┘
```

`NaN` values in float JOIN keys do not follow the `NULL` rule above.
A scalar comparison of two `NaN` values (`NaN = NaN`) is `0`. However, JOIN keys are not compared using scalar semantics, and `NaN` keys may match.
Whether they match is not specified: it depends on the JOIN algorithm, the key type, and the session settings.
To exclude `NaN` keys from matching, map them to `NULL` values, for example `ON if(isNaN(A.id), NULL, A.id) = B.id`.
In an `ASOF JOIN`, the closest-match column is compared by ordering, which is not defined for `NaN` values.

## Implicit type conversion {#implicit-type-conversion}

When the JOIN keys of the left and the right table have different types, ClickHouse converts them to their least common supertype before it compares them. This applies to every JOIN type and strictness:

- With `ON`, the conversion applies only to the comparison. The key columns keep their original types in the result.
- With `USING`, the merged column has the common type in the result. When `semi_join_include_columns_from_both_sides` or `anti_join_include_columns_from_both_sides` is disabled, the merged column of a `SEMI JOIN` or an `ANTI JOIN` keeps the type of the preserved table.

If the key types have no common supertype and both are integer types or tuples of integer types, as is the case for `UInt64` and `Int64`, ClickHouse converts both keys to a type that holds the values that both key types have in common. For `UInt64` and `Int64`, this type is `UInt64`. A key value that this type cannot represent does not match any value. This conversion is used for the equality conditions in `ON`, except `isNotDistinctFrom`, and for `USING` in an `INNER JOIN` or a `SEMI JOIN`. It is not used for the closest-match condition of an `ASOF JOIN`, for the inequality conditions that `ie_join` executes, or for `USING` in other JOINs, because their result column must also hold the values without a match. This conversion is available since version 26.9.

If the key types have no common supertype and this conversion does not apply, as is the case for `String` and `Int32` or for `UInt64` and `Float64`, the query fails with an exception, such as `NO_COMMON_TYPE`.

**Example**

Consider the table `t_1`:
```response
┌─a─┬─b─┬─toTypeName(a)─┬─toTypeName(b)─┐
│ 1 │ 1 │ UInt16        │ UInt8         │
│ 2 │ 2 │ UInt16        │ UInt8         │
└───┴───┴───────────────┴───────────────┘
```
and the table `t_2`:
```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│ -1 │    1 │ Int16         │ Nullable(Int64) │
│  1 │   -1 │ Int16         │ Nullable(Int64) │
│  1 │    1 │ Int16         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

The query
```sql
SELECT a, b, toTypeName(a), toTypeName(b) FROM t_1 FULL JOIN t_2 USING (a, b) ORDER BY ALL;
```
returns the set:
```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│ -1 │    1 │ Int32         │ Nullable(Int64) │
│  1 │   -1 │ Int32         │ Nullable(Int64) │
│  1 │    1 │ Int32         │ Nullable(Int64) │
│  2 │    2 │ Int32         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

## ASOF JOIN usage {#asof-join-usage}

`ASOF JOIN` joins records that have no exact match. `ASOF` is a JOIN strictness that is combined with an `INNER` or `LEFT` JOIN. It is not a JOIN algorithm.

An `ASOF JOIN` requires a closest-match column in both tables. This column:

- Must have a type with a defined order.
- With the [`hash`](/concepts/features/operations/select/join-algorithms#hash) algorithm, can be of the following types: [Int, UInt](/reference/data-types/int-uint), [Float](/reference/data-types/float), [Decimal](/reference/data-types/decimal), [Date](/reference/data-types/date), [Date32](/reference/data-types/date32), [DateTime](/reference/data-types/datetime), [DateTime64](/reference/data-types/datetime64), [Time](/reference/data-types/time), [Enum](/reference/data-types/enum), [Bool](/reference/data-types/boolean), and [Nullable](/reference/data-types/nullable) of these types. Other types, such as `String` and `Time64`, are rejected with the `BAD_TYPE_OF_FIELD` error.
- With the [`full_sorting_merge`](/concepts/features/operations/select/join-algorithms#full-sorting-merge) algorithm, can also be of other comparable types, such as `String`.
- With the `hash` algorithm, cannot be the only column in the `JOIN` clause. At least one equality condition is required. The `full_sorting_merge` algorithm does not have this requirement.

Syntax `ASOF JOIN ... ON`:

```sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

You can use any number of equality conditions and exactly one closest match condition. For example, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

Conditions supported for the closest match: `>`, `>=`, `<`, `<=`.

Syntax `ASOF JOIN ... USING`:

```sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` uses `equi_columnX` for joining on equality and `asof_column` for joining on the closest match with the `table_1.asof_column >= table_2.asof_column` condition. The `asof_column` column is always the last one in the `USING` clause.

For example, consider the following tables:

```text
         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...
```

`ASOF JOIN` can take the timestamp of a user event from `table_1` and find an event in `table_2` where the timestamp is closest to the timestamp of the event from `table_1` corresponding to the closest match condition. Equal timestamp values are the closest if available. Here, the `user_id` column can be used for joining on equality and the `ev_time` column can be used for joining on the closest match. In our example, `event_1_1` can be joined with `event_2_1` and `event_1_2` can be joined with `event_2_3`, but `event_2_2` can't be joined.

<Note>
Of the JOIN algorithms, only [`hash`](/concepts/features/operations/select/join-algorithms#hash) and [`full_sorting_merge`](/concepts/features/operations/select/join-algorithms#full-sorting-merge) support `ASOF` strictness. See [Supported JOIN types per algorithm](/concepts/features/operations/select/join-algorithms#supported-join-types-per-algorithm).
`ASOF` is **not** supported in the [Join](/reference/engines/table-engines/special/join) table engine.
</Note>

## PASTE JOIN usage {#paste-join-usage}

The result of `PASTE JOIN` is a table that contains all columns from left subquery followed by all columns from the right subquery.
The rows are matched based on their positions in the original tables (the order of rows should be defined).
If the subqueries return a different number of rows, extra rows will be cut.

Example:
```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers(2)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(2)
    ORDER BY a DESC
) AS t2

┌─a─┬─t2.a─┐
│ 0 │    1 │
│ 1 │    0 │
└───┴──────┘
```

Note: in this case result can be nondeterministic if the reading is parallel. For example:

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers_mt(5)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(10)
    ORDER BY a DESC
) AS t2
SETTINGS max_block_size = 2;

┌─a─┬─t2.a─┐
│ 2 │    9 │
│ 3 │    8 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 0 │    7 │
│ 1 │    6 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 4 │    5 │
└───┴──────┘
```

## LATERAL JOIN {#lateral-join}

`JOIN LATERAL` lets the subquery on the right side of a join reference columns of the table
expressions on its left side. The result is as if the subquery were evaluated for each distinct combination
of the left-side column values it references, and its result were joined to every left row with that combination:

```sql
SELECT ...
FROM <left_table>
[INNER|LEFT] JOIN LATERAL (SELECT ... WHERE <expr referencing left_table>) AS <alias> ON <condition>
```

`CROSS JOIN LATERAL (...) AS <alias>` and `<left_table>, LATERAL (...) AS <alias>` mean the same as
`INNER JOIN LATERAL (...) AS <alias> ON true`.

`ON` is required for `INNER` and `LEFT` (use `ON true` when the subquery's `WHERE` already relates the two sides)
and is evaluated for every pair of a left row and a lateral row, like for any other join; for `LEFT JOIN LATERAL`
a left row without such a pair gets default values (`NULL` with `join_use_nulls`).

It is available since version 26.10. It is experimental and disabled by default; enable it with the
[`enable_lateral_join`](/reference/settings/session-settings/enable#enable_lateral_join) setting.

The lateral subquery is not executed once per left row or once per distinct combination. Like other correlated
subqueries, it is decorrelated into a single query over all left rows: the left-side columns it references are
added to its `GROUP BY` keys if it aggregates, its `LIMIT` (if any) becomes `LIMIT ... BY` these columns, and its
result is joined to the left rows on them. When the correlation is a simple equality, such as `orders.user_id = u.id`,
the left-side column can be replaced by the equal column of the subquery
(`correlated_subqueries_substitute_equivalent_expressions`, enabled by default), which can let the subquery avoid
reading the left rows. This is not guaranteed: for example, an aggregate without `GROUP BY` (such as `count`) still
reads the distinct left values to produce its result for left values with no matching rows. Whenever the distinct
left values are passed to the subquery, with the default settings this goes through an in-memory buffer, and only
when such a buffer is actually used the join is always a hash join. In all other cases, such as when the equality
substitution avoids reading the left rows or the buffer is disabled
(`correlated_subqueries_use_in_memory_buffer = 0` or `correlated_subqueries_default_join_kind = 'left'`), the
algorithm is chosen by the `join_algorithm` setting. For example, `EXPLAIN` of a lateral subquery with `LIMIT`
shows a `Join` step over a `LimitBy (LIMIT BY for decorrelated correlated subquery)` step; a subquery without
`LIMIT` has no such step.

Only the following subset is supported so far; anything else is rejected with an error:

- `INNER`, `LEFT`, `CROSS` and comma forms only; `RIGHT`, `FULL`, `PASTE` and `NATURAL` joins are not supported.
- The default `ALL` strictness only; `ANY`, `SEMI`, `ANTI` and `ASOF` are not supported.
- `USING` is not supported.
- The `GLOBAL` and `LOCAL` join modifiers are not supported.
- The lateral subquery must reference at least one column of the left side. Use a regular join for a
  non-correlated subquery.
- Because the lateral subquery is decorrelated rather than evaluated separately for each left value, it
  must not contain functions that are non-deterministic within a query, such as
  `rand` or `generateUUIDv4`, or table functions that generate random rows, such as `generateRandom`.
  Functions that are constant within a query, such as `now`, are allowed, and so is `arrayJoin` (`unnest`),
  because it returns the same rows for the same argument.
- Only a subquery is supported as the lateral table expression. The PostgreSQL table-source forms
  `LATERAL unnest(...)` and `CROSS JOIN UNNEST(...)` are not supported. Use the
  [`ARRAY JOIN`](/reference/statements/select/array-join) clause instead.
- The `GROUP BY` and `ORDER BY` of the lateral subquery run once for all left values together, so the
  `max_rows_to_group_by`, `max_rows_to_sort` and `max_bytes_to_sort` limits count the rows for all left values,
  not for one. They are only supported with the `throw` overflow mode; `any` and `break` are rejected.
- The result of the decorrelated subquery is matched to the left rows by a single join. As for any hash join,
  `max_rows_in_join` and `max_bytes_in_join` limit the side of this join that is kept in memory, and the
  planner chooses that side: when the in-memory buffer described above is actually used, it is the left side
  of `JOIN LATERAL`, because the left rows must be fully read before the lateral subquery is evaluated; otherwise it can be the result of the decorrelated subquery. The limits are always
  enforced as if `join_overflow_mode` were `throw`: with `break`, the join would silently drop unrelated left rows.

**Example**

```sql
SELECT u.id, o.total
FROM users AS u
LEFT JOIN LATERAL
(
    SELECT total
    FROM orders
    WHERE orders.user_id = u.id
    ORDER BY total DESC
    LIMIT 1
) AS o ON true
SETTINGS enable_lateral_join = 1;
```

## Distributed JOIN {#distributed-join}

There are two ways to execute a JOIN involving distributed tables:

- When using a normal `JOIN`, the query is sent to remote servers. Subqueries are run on each of them in order to make the right table, and the join is performed with this table. In other words, the right table is formed on each server separately.
- When using `GLOBAL ... JOIN`, first the requestor server runs a subquery to calculate one side of the join and collects the result into a temporary table. This temporary table is then passed to each remote server, and queries are run on them using the temporary data that was transmitted. For `LEFT` and `INNER` joins, the right table is calculated as the subquery.

For a `GLOBAL RIGHT JOIN` with `ALL`, `ANY`, `SEMI`, or `ANTI` strictness whose left table is a `Distributed` table with more than one shard, ClickHouse swaps the two sides and executes the JOIN as a `LEFT JOIN`, so that the rows of the preserved right table are returned once. The left table is then calculated as the subquery, and the right table is read from its shards if it is a `Distributed` table with several shards, or on the requestor server otherwise. A `GLOBAL RIGHT JOIN` or `GLOBAL FULL JOIN` that cannot be swapped this way is executed on the requestor server. This behavior applies since version 26.9.

Be careful when using `GLOBAL`. For more information, see the [Distributed subqueries](/reference/statements/in#distributed-subqueries) section.

## Processing of empty or NULL cells {#processing-of-empty-or-null-cells}

In the result of an outer JOIN, the columns of the other table are empty for a row without a match. The [`join_use_nulls`](/reference/settings/session-settings/join#join_use_nulls) setting defines how ClickHouse fills these cells:

- `0`: The cells are filled with the default value of the column type.
- `1`: The cells are filled with `NULL`, and the types of these columns are converted to [Nullable](/reference/data-types/nullable). A column of a type that cannot be inside `Nullable`, such as `Array`, keeps its type and is filled with the default value. In a `FULL` JOIN, the merged `USING` column is also converted to `Nullable`.

A row in which at least one of the JOIN keys is `NULL` is not joined, as described in [NULL and NaN values in JOIN keys](#null-values-in-join-keys).

## USING clause {#using-clause}

The `USING` clause specifies one or more columns to join on, and establishes the equality of these columns:

- Each column must exist under the same name in both the left and the right table expression. Aliases in subqueries can give columns the same name.
- The `USING` clause accepts only column names. Expressions are not supported.
- The column list can be written with or without parentheses.
- Each column of the `USING` clause appears once in the result. Its type is described in [Implicit type conversion](#implicit-type-conversion).
- Other columns can have the same name in both tables. `SELECT *` qualifies the column of the right table with the table name or alias, and an unqualified name resolves to the column of the left table when [`single_join_prefer_left_table`](/reference/settings/session-settings/other#single_join_prefer_left_table) is enabled.
- When [`analyzer_compatibility_join_using_top_level_identifier`](/reference/settings/session-settings/analyzer-compatibility#analyzer_compatibility_join_using_top_level_identifier) is enabled, a column name in `USING` is resolved from an alias of the same name in the query, such as an alias in the `SELECT` list.

## Examples {#examples}

The following query joins the number of hits per counter with the number of visits per counter. The `ANY LEFT JOIN` returns each row of the left subquery once, and a counter without visits gets the default value `0`:

```sql title="Query"
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM VALUES('CounterID UInt32', 1, 1, 1, 2, 2, 3)
    GROUP BY CounterID
) AS h
ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM VALUES('CounterID UInt32, Sign Int8', (1, 1), (1, 1), (1, -1), (2, 1), (4, 1))
    GROUP BY CounterID
) AS v USING (CounterID)
ORDER BY hits DESC;
```

```response title="Response"
┌─CounterID─┬─hits─┬─visits─┐
│         1 │    3 │      1 │
│         2 │    2 │      1 │
│         3 │    1 │      0 │
└───────────┴──────┴────────┘
```

## Settings {#settings}

The session settings that affect the semantics, planning, execution, and memory use of JOINs are listed in [JOIN Settings](/concepts/features/operations/select/join-settings).

## Related content {#related-content}

- [Using JOINs in ClickHouse](/concepts/features/operations/select/joining-tables) — JOIN types and strictness, with diagrams
- [How ClickHouse executes JOINs](/concepts/features/operations/select/join-execution) — query planning
- [JOIN algorithms](/concepts/features/operations/select/join-algorithms) — how each algorithm runs, and which JOIN types it supports
- [JOIN memory management and spilling](/concepts/features/operations/select/join-memory-management) — memory limits and spilling to disk
- [JOIN Settings](/concepts/features/operations/select/join-settings) — the session settings that affect JOINs
- [Minimize and optimize JOINs](/concepts/best-practices/minimize-optimize-joins) — best practices, with benchmarked examples
- [`IN` operator](/reference/statements/in) — tests whether a value is in the result of a subquery
- [Dictionaries](/concepts/features/dictionaries) — an alternative to joining a small dimension table
- [`Join` table engine](/reference/engines/table-engines/special/join) — keeps a prepared right-hand side in memory
- Blog: [ClickHouse: A Blazingly Fast DBMS with Full SQL Join Support - Part 1](https://clickhouse.com/blog/clickhouse-fully-supports-joins)
- Blog: [ClickHouse: A Blazingly Fast DBMS with Full SQL Join Support - Under the Hood - Part 2](https://clickhouse.com/blog/clickhouse-fully-supports-joins-hash-joins-part2)
- Blog: [ClickHouse: A Blazingly Fast DBMS with Full SQL Join Support - Under the Hood - Part 3](https://clickhouse.com/blog/clickhouse-fully-supports-joins-full-sort-partial-merge-part3)
- Blog: [ClickHouse: A Blazingly Fast DBMS with Full SQL Join Support - Under the Hood - Part 4](https://clickhouse.com/blog/clickhouse-fully-supports-joins-direct-join-part4)
)DOCS_MD",
        .syntax = R"(
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ALL|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
)",
        .parent = "SELECT",
        .related = {"SELECT", "FROM", "ARRAY JOIN", "IN", "UNION"},
    };

    documentation["ARRAY JOIN"] =
    {
        .description = R"DOCS_MD(
It is a common operation for tables that contain an array column to produce a new table that has a row with each individual array element of that initial column, while values of other columns are duplicated. This is the basic case of what `ARRAY JOIN` clause does.

Its name comes from the fact that it can be looked at as executing `JOIN` with an array or nested data structure. The intent is similar to the [arrayJoin](/reference/functions/regular-functions/array-join) function, but the clause functionality is broader.

<Note>
PostgreSQL's `FROM unnest(...)`, `CROSS JOIN UNNEST(...)` and `LATERAL unnest(...)` table-source forms are not supported. Use `ARRAY JOIN` instead. The `unnest` name (since version 26.5) is a function-call alias of `arrayJoin` (`SELECT unnest(arr)`), not a table function. `JOIN LATERAL <subquery>` over a correlated subquery is supported separately as an experimental feature, see [`JOIN`](/reference/statements/select/join#lateral-join).
</Note>

Syntax:

```sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

Supported types of `ARRAY JOIN` are listed below:

- `ARRAY JOIN` - In base case, empty arrays are not included in the result of `JOIN`.
- `LEFT ARRAY JOIN` - The result of `JOIN` contains rows with empty arrays. The value for an empty array is set to the default value for the array element type (usually 0, empty string or NULL).

## Basic ARRAY JOIN Examples {#basic-array-join-examples}

### ARRAY JOIN and LEFT ARRAY JOIN {#array-join-left-array-join-examples}

The examples below demonstrate the usage of the `ARRAY JOIN` and `LEFT ARRAY JOIN` clauses. Let's create a table with an [Array](/reference/data-types/array) type column and insert values into it:

```sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

```response
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

The example below uses the `ARRAY JOIN` clause:

```sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

```response
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

The next example uses the `LEFT ARRAY JOIN` clause:

```sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

```response
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

### ARRAY JOIN and arrayEnumerate function {#array-join-arrayEnumerate}

This function is normally used with `ARRAY JOIN`. It allows counting something just once for each array after applying `ARRAY JOIN`. Example:

```sql
SELECT
    count() AS Reaches,
    countIf(num = 1) AS Hits
FROM test.hits
ARRAY JOIN
    GoalsReached,
    arrayEnumerate(GoalsReached) AS num
WHERE CounterID = 160656
LIMIT 10
```

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

In this example, Reaches is the number of conversions (the strings received after applying `ARRAY JOIN`), and Hits is the number of pageviews (strings before `ARRAY JOIN`). In this particular case, you can get the same result in an easier way:

```sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

```text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

### ARRAY JOIN and arrayEnumerateUniq {#array_join_arrayEnumerateUniq}

This function is useful when using `ARRAY JOIN` and aggregating array elements.

In this example, each goal ID has a calculation of the number of conversions (each element in the Goals nested data structure is a goal that was reached, which we refer to as a conversion) and the number of sessions. Without `ARRAY JOIN`, we would have counted the number of sessions as sum(Sign). But in this particular case, the rows were multiplied by the nested Goals structure, so in order to count each session one time after this, we apply a condition to the value of the `arrayEnumerateUniq(Goals.ID)` function.

```sql
SELECT
    Goals.ID AS GoalID,
    sum(Sign) AS Reaches,
    sumIf(Sign, num = 1) AS Visits
FROM test.visits
ARRAY JOIN
    Goals,
    arrayEnumerateUniq(Goals.ID) AS num
WHERE CounterID = 160656
GROUP BY GoalID
ORDER BY Reaches DESC
LIMIT 10
```

```text
┌──GoalID─┬─Reaches─┬─Visits─┐
│   53225 │    3214 │   1097 │
│ 2825062 │    3188 │   1097 │
│   56600 │    2803 │    488 │
│ 1989037 │    2401 │    365 │
│ 2830064 │    2396 │    910 │
│ 1113562 │    2372 │    373 │
│ 3270895 │    2262 │    812 │
│ 1084657 │    2262 │    345 │
│   56599 │    2260 │    799 │
│ 3271094 │    2256 │    812 │
└─────────┴─────────┴────────┘
```

## Using Aliases {#using-aliases}

An alias can be specified for an array in the `ARRAY JOIN` clause. In this case, an array item can be accessed by this alias, but the array itself is accessed by the original name. Example:

```sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

```response
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

Using aliases, you can perform `ARRAY JOIN` with an external array. For example:

```sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

```response
┌─s───────────┬─arr_external─┐
│ Hello       │            1 │
│ Hello       │            2 │
│ Hello       │            3 │
│ World       │            1 │
│ World       │            2 │
│ World       │            3 │
│ Goodbye     │            1 │
│ Goodbye     │            2 │
│ Goodbye     │            3 │
└─────────────┴──────────────┘
```

Multiple arrays can be comma-separated in the `ARRAY JOIN` clause. In this case, `JOIN` is performed with them simultaneously (the direct sum, not the cartesian product). Note that all the arrays must have the same size by default. Example:

```sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

```response
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

The example below uses the [arrayEnumerate](/reference/functions/regular-functions/array-functions#arrayEnumerate) function:

```sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

```response
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

Multiple arrays with different sizes can be joined by using: `SETTINGS enable_unaligned_array_join = 1`. Example:

```sql
SELECT s, arr, a, b
FROM arrays_test ARRAY JOIN arr AS a, [['a','b'],['c']] AS b
SETTINGS enable_unaligned_array_join = 1;
```

```response
┌─s───────┬─arr─────┬─a─┬─b─────────┐
│ Hello   │ [1,2]   │ 1 │ ['a','b'] │
│ Hello   │ [1,2]   │ 2 │ ['c']     │
│ World   │ [3,4,5] │ 3 │ ['a','b'] │
│ World   │ [3,4,5] │ 4 │ ['c']     │
│ World   │ [3,4,5] │ 5 │ []        │
│ Goodbye │ []      │ 0 │ ['a','b'] │
│ Goodbye │ []      │ 0 │ ['c']     │
└─────────┴─────────┴───┴───────────┘
```

## ARRAY JOIN with Nested Data Structure {#array-join-with-nested-data-structure}

`ARRAY JOIN` also works with [nested data structures](/reference/data-types/nested-data-structures/index):

```sql
CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory;

INSERT INTO nested_test
VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
```

```response
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

```response
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

When specifying names of nested data structures in `ARRAY JOIN`, the meaning is the same as `ARRAY JOIN` with all the array elements that it consists of. Examples are listed below:

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

```response
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

This variation also makes sense:

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

```response
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

An alias may be used for a nested data structure, in order to select either the `JOIN` result or the source array. Example:

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

```response
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

Example of using the [arrayEnumerate](/reference/functions/regular-functions/array-functions#arrayEnumerate) function:

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

```response
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

## Row Order and Paging {#row-order-and-paging}

`ARRAY JOIN` keeps the order of the rows that it receives. For each row, it produces the elements in their order in the array. If a query has more than one `ARRAY JOIN` clause, the first clause is the outer loop:

```sql
SELECT x, y
FROM (SELECT ['a', 'b'] AS xs, [1, 2] AS ys)
ARRAY JOIN xs AS x
ARRAY JOIN ys AS y;
```

```response
┌─x─┬─y─┐
│ a │ 1 │
│ a │ 2 │
│ b │ 1 │
│ b │ 2 │
└───┴───┘
```

If one `ARRAY JOIN` clause has more than one array, for example `ARRAY JOIN xs AS x, ys AS y`, the elements at the same position go to the same row. In this example, the result is the two rows `('a', 1)` and `('b', 2)`.

Without `ORDER BY`, the order of the rows that `ARRAY JOIN` receives is not guaranteed. For example, a query can read a table in parallel streams. To get a stable order, add `ORDER BY` with the columns that identify the source row and the position of the element. You must do this to read the result in pages with `LIMIT` and `OFFSET`:

```sql
SELECT id, num, x
FROM t
ARRAY JOIN xs AS x, arrayEnumerate(xs) AS num
ORDER BY id, num
LIMIT 10 OFFSET 20;
```

If `ORDER BY` starts with the columns of the sorting key of a `MergeTree` table, the query reads the table in the order of the key. In the example, this is true when the table has `ORDER BY id`. The query then stops when it has the rows for `LIMIT`, and it does not expand the arrays of the other rows. For more information, see [Optimization of Data Reading](/reference/statements/select/order-by#optimization-of-data-reading) and the setting [optimize_read_in_order](/reference/settings/session-settings/optimize#optimize_read_in_order). If `ORDER BY` starts with an element of the array, for example `ORDER BY num, id`, the query expands and sorts all the rows.

The [arrayJoin](/reference/functions/regular-functions/array-join) function also produces the elements of each array in their order in the array. But if a query has more than one `arrayJoin` call, the first call is not always the outer loop. For more information, see [Row order](/reference/functions/regular-functions/array-join#row-order).

## Implementation Details {#implementation-details}

The query execution order is optimized when running `ARRAY JOIN`. Although `ARRAY JOIN` must always be specified before the [WHERE](/reference/statements/select/where)/[PREWHERE](/reference/statements/select/prewhere) clause in a query, technically they can be performed in any order, unless result of `ARRAY JOIN` is used for filtering. The processing order is controlled by the query optimizer.

### Incompatibility with short-circuit function evaluation {#incompatibility-with-short-circuit-function-evaluation}

[Short-circuit function evaluation](/reference/settings/session-settings/short-circuit-function-evaluation#short_circuit_function_evaluation) is a feature that optimizes the execution of complex expressions in specific functions such as `if`, `multiIf`, `and`, and `or`. It prevents potential exceptions, such as division by zero, from occurring during the execution of these functions.

`arrayJoin` is always executed and not supported for short circuit function evaluation. That's because it's a unique function processed separately from all other functions during query analysis and execution and requires additional logic that doesn't work with short circuit function execution. The reason is that the number of rows in the result depends on the arrayJoin result, and it's too complex and expensive to implement lazy execution of `arrayJoin`.

## Related content {#related-content}

- Blog: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
)DOCS_MD",
        .syntax = R"(
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
)",
        .parent = "SELECT",
        .related = {"SELECT", "JOIN", "FROM"},
    };

    documentation["SAMPLE"] =
    {
        .description = R"DOCS_MD(
The `SAMPLE` clause allows for approximated `SELECT` query processing.

When data sampling is enabled, the query is not performed on all the data, but only on a certain fraction of data (sample). For example, if you need to calculate statistics for all the visits, it is enough to execute the query on the 1/10 fraction of all the visits and then multiply the result by 10.

Approximated query processing can be useful in the following cases:

- When you have strict latency requirements (like below 100ms) but you can't justify the cost of additional hardware resources to meet them.
- When your raw data is not accurate, so approximation does not noticeably degrade the quality.
- Business requirements target approximate results (for cost-effectiveness, or to market exact results to premium users).

<Note>
You can only use sampling with the tables in the [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) family, and only if the sampling expression was specified during table creation (see [MergeTree engine](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-creating-a-table)).
</Note>

The features of data sampling are listed below:

- Data sampling is a deterministic mechanism. The result of the same `SELECT .. SAMPLE` query is always the same.
- Sampling works consistently for different tables. For tables with a single sampling key, a sample with the same coefficient always selects the same subset of possible data. For example, a sample of user IDs takes rows with the same subset of all the possible user IDs from different tables. This means that you can use the sample in subqueries in the [IN](/reference/statements/in) clause. Also, you can join samples using the [JOIN](/reference/statements/select/join) clause.
- Sampling allows reading less data from a disk. Note that you must specify the sampling key correctly. For more information, see [Creating a MergeTree Table](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-creating-a-table).

For the `SAMPLE` clause the following syntax is supported:

| SAMPLE Clause Syntax | Description                                                                                                                                                                                                                                    |
|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SAMPLE k`   | Here `k` is the number from 0 to 1. The query is executed on `k` fraction of data. For example, `SAMPLE 0.1` runs the query on 10% of data. [Read more](#sample-k)                                                                             |
| `SAMPLE n`    | Here `n` is a sufficiently large integer. The query is executed on a sample of at least `n` rows (but not significantly more than this). For example, `SAMPLE 10000000` runs the query on a minimum of 10,000,000 rows. [Read more](#sample-n) |
| `SAMPLE k OFFSET m`  | Here `k` and `m` are the numbers from 0 to 1. The query is executed on a sample of `k` fraction of the data. The data used for the sample is offset by `m` fraction. [Read more](#sample-k-offset-m)                                           |

## SAMPLE K {#sample-k}

Here `k` is the number from 0 to 1 (both fractional and decimal notations are supported). For example, `SAMPLE 1/2` or `SAMPLE 0.5`.

In a `SAMPLE k` clause, the sample is taken from the `k` fraction of data. The example is shown below:

```sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

In this example, the query is executed on a sample from 0.1 (10%) of data. Values of aggregate functions are not corrected automatically, so to get an approximate result, the value `count()` is manually multiplied by 10.

## SAMPLE N {#sample-n}

Here `n` is a sufficiently large integer. For example, `SAMPLE 10000000`.

In this case, the query is executed on a sample of at least `n` rows (but not significantly more than this). For example, `SAMPLE 10000000` runs the query on a minimum of 10,000,000 rows.

Since the minimum unit for data reading is one granule (its size is set by the `index_granularity` setting), it makes sense to set a sample that is much larger than the size of the granule.

When using the `SAMPLE n` clause, you do not know which relative percent of data was processed. So you do not know the coefficient the aggregate functions should be multiplied by. Use the `_sample_factor` virtual column to get the approximate result.

The `_sample_factor` column contains relative coefficients that are calculated dynamically. This column is created automatically when you [create](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-creating-a-table) a table with the specified sampling key. The usage examples of the `_sample_factor` column are shown below.

Let's consider the table `visits`, which contains the statistics about site visits. The first example shows how to calculate the number of page views:

```sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

The next example shows how to calculate the total number of visits:

```sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

The example below shows how to calculate the average session duration. Note that you do not need to use the relative coefficient to calculate the average values.

```sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

## SAMPLE K OFFSET M {#sample-k-offset-m}

Here `k` and `m` are numbers from 0 to 1. Examples are shown below.

**Example 1**

```sql
SAMPLE 1/10
```

In this example, the sample is 1/10th of all data:

`[++------------]`

**Example 2**

```sql
SAMPLE 1/10 OFFSET 1/2
```

Here, a sample of 10% is taken from the second half of the data.

`[------++------]`
)DOCS_MD",
        .syntax = R"(
SELECT ... FROM table SAMPLE k
SELECT ... FROM table SAMPLE n
SELECT ... FROM table SAMPLE k OFFSET m
)",
        .parent = "SELECT",
        .related = {"SELECT", "FROM", "CREATE TABLE", "ALTER TABLE ... MODIFY SAMPLE BY"},
    };

    return documentation;
}

}
