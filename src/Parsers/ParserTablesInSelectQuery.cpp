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
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>
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

        if (!ParserTableExpression(allow_alias_without_as_keyword).parse(pos, res->table_expression, expected))
            return false;

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

}

namespace DB
{

void registerStatementTablesInSelect(StatementFactory & factory)
{
    factory.registerStatement("FROM",
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
    });

    factory.registerStatement("JOIN",
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

Expressions from the `ON` clause and columns from the `USING` clause are called "join keys". Unless otherwise stated, a `JOIN` produces a [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) from rows with matching "join keys", which might produce results with many more rows than the source tables.

## Supported types of JOIN {#supported-types-of-join}

All standard [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) types are supported:

| Type              | Description                                                                   |
|-------------------|-------------------------------------------------------------------------------|
| `INNER JOIN`      | only matching rows are returned.                                              |
| `LEFT OUTER JOIN` | non-matching rows from left table are returned in addition to matching rows.  |
| `RIGHT OUTER JOIN`| non-matching rows from right table are returned in addition to matching rows. |
| `FULL OUTER JOIN` | non-matching rows from both tables are returned in addition to matching rows. |
| `CROSS JOIN`      | produces cartesian product of whole tables, "join keys" are **not** specified.|
| `NATURAL JOIN`    | automatically joins on all columns with the same name in both tables; each common column appears once in the result. Supports `INNER` (default), `LEFT`, `RIGHT`, and `FULL` variants. Equivalent to `JOIN ... USING (col1, col2, ...)` where the column list is derived automatically. |

- `JOIN` without a type specified implies `INNER`.
- The keyword `OUTER` can be safely omitted.
- An alternative syntax for `CROSS JOIN` is specifying multiple tables in the [`FROM` clause](/reference/statements/select/from) separated by commas.
- If there are no matching columns for a `NATURAL JOIN`, it functions like a `CROSS JOIN`.

Additional join types available in ClickHouse are:

| Type                                        | Description                                                                                                                               |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `LEFT SEMI JOIN`, `RIGHT SEMI JOIN`         | An allowlist on "join keys", without producing a cartesian product.                                                                        |
| `LEFT ANTI JOIN`, `RIGHT ANTI JOIN`         | A denylist on "join keys", without producing a cartesian product.                                                                        |
| `LEFT ANY JOIN`, `RIGHT ANY JOIN`, `INNER ANY JOIN` | Partially (for opposite side of `LEFT` and `RIGHT`) or completely (for `INNER` and `FULL`) disables the cartesian product for standard `JOIN` types. |
| `ASOF JOIN`, `LEFT ASOF JOIN`               | Joining sequences with a non-exact match. `ASOF JOIN` usage is described below.                                                           |
| `PASTE JOIN`                                | Performs a horizontal concatenation of two tables.                                                                                          |

<Note>
When [join_algorithm](/reference/settings/session-settings/join#join_algorithm) is set to `partial_merge`, `RIGHT JOIN` and `FULL JOIN` are supported only with `ALL` strictness (`SEMI`, `ANTI`, `ANY`, and `ASOF` are not supported).

`INNER ANY JOIN` is supported, but the partial merge algorithm keeps the legacy behavior of [`any_join_distinct_right_table_keys`](/reference/settings/session-settings/other#any_join_distinct_right_table_keys): it returns every matching row from whichever input the plan uses as its left side, rather than one row per key. This applies whenever the partial merge algorithm ends up running, not only when `join_algorithm` is spelled `partial_merge`. At the default `any_join_distinct_right_table_keys = 0`, the `hash`, `full_sorting_merge` and `grace_hash` algorithms return one row per key.
</Note>

## Settings {#settings}

The default join type can be overridden using [`join_default_strictness`](/reference/settings/session-settings/join#join_default_strictness) setting.

The behavior of the ClickHouse server for `ANY JOIN` operations depends on the [`any_join_distinct_right_table_keys`](/reference/settings/session-settings/other#any_join_distinct_right_table_keys) setting.

**See also**

- [`join_algorithm`](/reference/settings/session-settings/join#join_algorithm)
- [`join_any_take_last_row`](/reference/settings/session-settings/join#join_any_take_last_row)
- [`join_use_nulls`](/reference/settings/session-settings/join#join_use_nulls)
- [`partial_merge_join_rows_in_right_blocks`](/reference/settings/session-settings/partial-merge#partial_merge_join_rows_in_right_blocks)
- [`join_on_disk_max_files_to_merge`](/reference/settings/session-settings/join#join_on_disk_max_files_to_merge)
- [`any_join_distinct_right_table_keys`](/reference/settings/session-settings/other#any_join_distinct_right_table_keys)

Use the `cross_to_inner_join_rewrite` setting to define the behavior when ClickHouse fails to rewrite a `CROSS JOIN` as an `INNER JOIN`. The default value is `1`, which  allows the join to continue but it will be slower. Set `cross_to_inner_join_rewrite` to `0` if you want an error to be thrown, and set it to `2` to not run the cross joins but instead force a rewrite of all comma/cross joins. If the rewriting fails when the value is `2`, you will receive an error message stating "Please, try to simplify `WHERE` section".

## ON section conditions {#on-section-conditions}

An `ON` section can contain several conditions combined using the `AND` and `OR` operators. Conditions specifying join keys must:
- reference both left and right tables
- use the equality operator

Other conditions may use other logical operators but they must reference either the left or the right table of a query.

Rows are joined if the whole complex condition is met. If the conditions are not met, rows may still be included in the result depending on the `JOIN` type. Note that if the same conditions are placed in a `WHERE` section and they are not met, then rows are always filtered out from the result.

The `OR` operator inside the `ON` clause works using the hash join algorithm — for each `OR` argument with join keys for `JOIN`, a separate hash table is created, so memory consumption and query execution time grow linearly with an increase in the number of expressions `OR` of the `ON` clause.

<Note>
If a condition references columns from different tables, then only the equality operator (`=`) is supported so far.
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

Query with one join key condition and an additional condition for `table_2`:

```sql title="Query"
SELECT name, text FROM table_1 LEFT OUTER JOIN table_2
    ON table_1.Id = table_2.Id AND startsWith(table_2.text, 'Text');
```

Note that the result contains the row with the name `C` and the empty text column. It is included into the result because an `OUTER` type of a join is used.

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

```sql title="Response"
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

By default, non-equal conditions are supported as long as they use columns from the same table.
For example, `t1.a = t2.key AND t1.b > 0 AND t2.b > t2.c`, because `t1.b > 0` uses columns only from `t1` and `t2.b > t2.c` uses columns only from `t2`.
However, you can try experimental support for conditions like `t1.a = t2.key AND t1.b > t2.key`, check out the section below for more details.
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

ClickHouse currently supports `ALL/ANY/SEMI/ANTI INNER/LEFT/RIGHT/FULL JOIN` with inequality conditions in addition to equality conditions. The inequality conditions are supported only for `hash`, `parallel_hash` and `grace_hash` join algorithms. A non equi condition that is evaluated during the join may not contain `arrayJoin`, because such a condition must preserve the number of rows; a condition that applies to one side only, and an equality key over `arrayJoin`, are extracted before the join and are unaffected; a non-disjunctive `ALL INNER JOIN` condition is also unaffected, because there the condition is applied after the join instead. Where the expansion depends on one side only, move it into an `ARRAY JOIN` in a subquery before the join; a condition whose `arrayJoin` argument reads columns from both sides has to be restructured.

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

## NULL and NaN values in JOIN keys {#null-values-in-join-keys}

`NULL` is not equal to any value, including itself. This means that if a `JOIN` key has a `NULL` value in one table, it won't match a `NULL` value in the other table.

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

Notice that the row with `Charlie` from table `A` and the row with score 88 from table `B` are not in the result because of the `NULL` value in the `JOIN` key.

In case you want to match `NULL` values, use the `isNotDistinctFrom` function to compare the `JOIN` keys.

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON isNotDistinctFrom(A.id, B.id)
```

```markdown
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │    88 │
└─────────┴───────┘
```

`NaN` values in float `JOIN` keys do not follow the `NULL` rule above.
A scalar comparison of two `NaN` values (`NaN = NaN`) is `0`, however, `JOIN` keys are not compared using scalar semantics - `NaN` keys may match.
Whether they actually do is an implementation detail and depends on the join algorithm, the key type, and the session settings.
Do not rely on a specific behavior.
If you require that `NaN` rows do not match, map them to `NULL` values: `ON if(isNaN(A.id), NULL, A.id) = B.id`.
In an `ASOF JOIN`, the closest-match column is compared by ordering, which `NaN` does not support — filter such rows out on both sides.

## ASOF JOIN usage {#asof-join-usage}

`ASOF JOIN` is useful when you need to join records that have no exact match.

This JOIN algorithm requires a special column in tables. This column:

- Must contain an ordered sequence.
- Can be one of the following types: [Int, UInt](/reference/data-types/int-uint), [Float](/reference/data-types/float), [Date](/reference/data-types/date), [DateTime](/reference/data-types/datetime), [Decimal](/reference/data-types/decimal).
- For the `hash` join algorithm it can't be the only column in the `JOIN` clause.

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
`ASOF JOIN` is supported only by `hash` and `full_sorting_merge` join algorithms.
It's **not** supported in the [Join](/reference/engines/table-engines/special/join) table engine.
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

## Distributed JOIN {#distributed-join}

There are two ways to execute a JOIN involving distributed tables:

- When using a normal `JOIN`, the query is sent to remote servers. Subqueries are run on each of them in order to make the right table, and the join is performed with this table. In other words, the right table is formed on each server separately.
- When using `GLOBAL ... JOIN`, first the requestor server runs a subquery to calculate one side of the join and collects the result into a temporary table. This temporary table is then passed to each remote server, and queries are run on them using the temporary data that was transmitted. For `LEFT` and `INNER` joins, the right table is calculated as the subquery. For `RIGHT` joins, the left table is calculated instead, since the right table is the one being preserved and should be read from shards.

Be careful when using `GLOBAL`. For more information, see the [Distributed subqueries](/reference/statements/in#distributed-subqueries) section.

## Implicit type conversion {#implicit-type-conversion}

`INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, and `FULL JOIN` queries support the implicit type conversion for "join keys". However the query can not be executed, if join keys from the left and the right tables cannot be converted to a single type (for example, there is no data type that can hold all values from both `UInt64` and `Int64`, or `String` and `Int32`).

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
SELECT a, b, toTypeName(a), toTypeName(b) FROM t_1 FULL JOIN t_2 USING (a, b);
```
returns the set:
```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│  1 │    1 │ Int32         │ Nullable(Int64) │
│  2 │    2 │ Int32         │ Nullable(Int64) │
│ -1 │    1 │ Int32         │ Nullable(Int64) │
│  1 │   -1 │ Int32         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

## Usage recommendations {#usage-recommendations}

### Processing of empty or NULL cells {#processing-of-empty-or-null-cells}

While joining tables, the empty cells may appear. The setting [join_use_nulls](/reference/settings/session-settings/join#join_use_nulls) define how ClickHouse fills these cells.

If the `JOIN` keys are [Nullable](/reference/data-types/nullable) fields, the rows where at least one of the keys has the value [NULL](/reference/syntax#null) are not joined.

### Syntax {#syntax}

The columns specified in `USING` must have the same names in both subqueries, and the other columns must be named differently. You can use aliases to change the names of columns in subqueries.

The `USING` clause specifies one or more columns to join, which establishes the equality of these columns. The list of columns is set without brackets. More complex join conditions are not supported.

### Syntax Limitations {#syntax-limitations}

For multiple `JOIN` clauses in a single `SELECT` query:

- Taking all the columns via `*` is available only if tables are joined, not subqueries.
- The `PREWHERE` clause is not available.
- The `USING` clause is not available.

For `ON`, `WHERE`, and `GROUP BY` clauses:

- Arbitrary expressions cannot be used in `ON`, `WHERE`, and `GROUP BY` clauses, but you can define an expression in a `SELECT` clause and then use it in these clauses via an alias.

### Performance {#performance}

When running a `JOIN`, there is no optimization of the order of execution in relation to other stages of the query. The join (a search in the right table) is run before filtering in `WHERE` and before aggregation.

Each time a query is run with the same `JOIN`, the subquery is run again because the result is not cached. To avoid this, use the special [Join](/reference/engines/table-engines/special/join) table engine, which is a prepared array for joining that is always in RAM.

In some cases, it is more efficient to use [IN](/reference/statements/in) instead of `JOIN`.

If you need a `JOIN` for joining with dimension tables (these are relatively small tables that contain dimension properties, such as names for advertising campaigns), a `JOIN` might not be very convenient due to the fact that the right table is re-accessed for every query. For such cases, there is a "dictionaries" feature that you should use instead of `JOIN`. For more information, see the [Dictionaries](/reference/statements/create/dictionary) section.

### Memory limitations {#memory-limitations}

By default, ClickHouse uses the [hash join](https://en.wikipedia.org/wiki/Hash_join) algorithm. ClickHouse takes the right_table and creates a hash table for it in RAM. If `join_algorithm = 'auto'` is enabled, then after some threshold of memory consumption, ClickHouse falls back to [merge](https://en.wikipedia.org/wiki/Sort-merge_join) join algorithm. For `JOIN` algorithms description see the [join_algorithm](/reference/settings/session-settings/join#join_algorithm) setting.

If you need to restrict `JOIN` operation memory consumption use the following settings:

- [max_rows_in_join](/reference/settings/session-settings/max-rows#max_rows_in_join) — Limits number of rows in the hash table.
- [max_bytes_in_join](/reference/settings/session-settings/max-bytes#max_bytes_in_join) — Limits size of the hash table.

When any of these limits is reached, ClickHouse acts as the [join_overflow_mode](/reference/settings/session-settings/join#join_overflow_mode)
setting instructs.

## Examples {#examples}

Example:

```sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

```text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```

## Related content {#related-content}

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
    });

    factory.registerStatement("ARRAY JOIN",
    {
        .description = R"DOCS_MD(
It is a common operation for tables that contain an array column to produce a new table that has a row with each individual array element of that initial column, while values of other columns are duplicated. This is the basic case of what `ARRAY JOIN` clause does.

Its name comes from the fact that it can be looked at as executing `JOIN` with an array or nested data structure. The intent is similar to the [arrayJoin](/reference/functions/regular-functions/array-join) function, but the clause functionality is broader.

<Note>
PostgreSQL `FROM unnest(...)`, `CROSS JOIN UNNEST(...)`, and `LATERAL` are not supported. Use `ARRAY JOIN` instead. The `unnest` name (since version 26.5) is a function-call alias of `arrayJoin` (`SELECT unnest(arr)`), not a table function.
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
    });

    factory.registerStatement("SAMPLE",
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
    });
}

}
