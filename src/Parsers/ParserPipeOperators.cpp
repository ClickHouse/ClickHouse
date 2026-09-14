#include <Parsers/ParserPipeOperators.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserUnionQueryElement.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>

#include <optional>


namespace DB
{

namespace
{

/// An expression list with a single asterisk: `*`, optionally with a transformer, e.g. `* EXCEPT (a, b)`.
ASTPtr makeAsteriskList(ASTPtr transformer = nullptr)
{
    auto asterisk = make_intrusive<ASTAsterisk>();
    if (transformer)
    {
        auto transformer_list = make_intrusive<ASTColumnsTransformerList>();
        transformer_list->children.push_back(std::move(transformer));
        asterisk->transformers = transformer_list;
        asterisk->children.push_back(transformer_list);
    }

    auto list = make_intrusive<ASTExpressionList>();
    list->children.push_back(std::move(asterisk));
    return list;
}

/// Wrap a SELECT query into a single-element ASTSelectWithUnionQuery, as ParserSelectWithUnionQuery does.
ASTPtr wrapIntoUnion(ASTPtr query)
{
    auto res = make_intrusive<ASTSelectWithUnionQuery>();
    res->list_of_selects = make_intrusive<ASTExpressionList>();
    res->list_of_selects->children.push_back(std::move(query));
    res->children.push_back(res->list_of_selects);
    return res;
}

/// If the position is at a set operation (UNION/INTERSECT/EXCEPT with an optional ALL/DISTINCT modifier),
/// consume it and return the mode.
std::optional<SelectUnionMode> parseSetOperationMode(IParser::Pos & pos, Expected & expected)
{
    ParserKeyword s_union(Keyword::UNION);
    ParserKeyword s_intersect(Keyword::INTERSECT);
    ParserKeyword s_except(Keyword::EXCEPT);
    ParserKeyword s_all(Keyword::ALL);
    ParserKeyword s_distinct(Keyword::DISTINCT);

    if (s_union.ignore(pos, expected))
    {
        if (s_all.ignore(pos, expected))
            return SelectUnionMode::UNION_ALL;
        if (s_distinct.ignore(pos, expected))
            return SelectUnionMode::UNION_DISTINCT;
        return SelectUnionMode::UNION_DEFAULT;
    }

    if (s_intersect.ignore(pos, expected))
    {
        if (s_all.ignore(pos, expected))
            return SelectUnionMode::INTERSECT_ALL;
        if (s_distinct.ignore(pos, expected))
            return SelectUnionMode::INTERSECT_DISTINCT;
        return SelectUnionMode::INTERSECT_DEFAULT;
    }

    if (s_except.ignore(pos, expected))
    {
        if (s_all.ignore(pos, expected))
            return SelectUnionMode::EXCEPT_ALL;
        if (s_distinct.ignore(pos, expected))
            return SelectUnionMode::EXCEPT_DISTINCT;
        return SelectUnionMode::EXCEPT_DEFAULT;
    }

    return std::nullopt;
}

/// The first SELECT of a query, possibly nested in unions: its WITH clause defines
/// the query-scoped aliases and CTEs that must stay visible across pipe operators.
const ASTSelectQuery * getFirstSelect(const IAST & query)
{
    if (const auto * select = query.as<ASTSelectQuery>())
        return select;
    if (const auto * union_query = query.as<ASTSelectWithUnionQuery>())
        if (union_query->list_of_selects && !union_query->list_of_selects->children.empty())
            return getFirstSelect(*union_query->list_of_selects->children.front());
    return nullptr;
}

/// Create `SELECT <select_list> FROM (query) [AS alias]` - the input query becomes a subquery in the FROM clause.
/// If `select_list` is nullptr, `SELECT *` is used. The alias is consumed and cleared.
/// The WITH clause of the query is cloned onto the wrapping SELECT, so that query-scoped aliases and CTEs
/// remain visible in the following pipe operators: WITH 10 AS threshold FROM t |> WHERE x < threshold.
/// The CTE_ALIASES sidecar is cloned together with WITH: it keeps the CTE column alias lists
/// (WITH t(a) AS ...) in the children, so the tree hash of the reparsed formatted query matches.
boost::intrusive_ptr<ASTSelectQuery> wrapQueryIntoSelect(ASTPtr query, String & pending_alias, ASTPtr select_list = nullptr)
{
    ASTPtr with_clause;
    ASTPtr cte_aliases;
    bool recursive_with = false;
    if (const auto * inner_select = getFirstSelect(*query))
    {
        if (auto inner_with = inner_select->with())
        {
            with_clause = inner_with->clone();
            recursive_with = inner_select->recursive_with;
            if (auto inner_cte_aliases = inner_select->cteAliases())
                cte_aliases = inner_cte_aliases->clone();
        }
    }

    auto subquery = make_intrusive<ASTSubquery>(std::move(query));
    if (!pending_alias.empty())
    {
        subquery->setAlias(pending_alias);
        pending_alias.clear();
    }

    auto table_expression = make_intrusive<ASTTableExpression>();
    table_expression->subquery = subquery;
    table_expression->children.push_back(std::move(subquery));

    auto element = make_intrusive<ASTTablesInSelectQueryElement>();
    element->table_expression = table_expression;
    element->children.push_back(std::move(table_expression));

    auto tables = make_intrusive<ASTTablesInSelectQuery>();
    tables->children.push_back(std::move(element));

    auto select = make_intrusive<ASTSelectQuery>();
    /// The children are added in the same order as in ParserSelectQuery,
    /// because it is checked when the formatted AST is parsed back.
    if (with_clause)
    {
        select->setExpression(ASTSelectQuery::Expression::WITH, std::move(with_clause));
        select->recursive_with = recursive_with;
    }
    select->setExpression(ASTSelectQuery::Expression::SELECT, select_list ? std::move(select_list) : makeAsteriskList());
    select->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
    if (cte_aliases)
        select->setExpression(ASTSelectQuery::Expression::CTE_ALIASES, std::move(cte_aliases));
    return select;
}

}


bool parsePipeOperators(IParser::Pos & pos, ASTPtr & query, Expected & expected)
{
    ParserKeyword s_where(Keyword::WHERE);
    ParserKeyword s_select(Keyword::SELECT);
    ParserKeyword s_distinct(Keyword::DISTINCT);
    ParserKeyword s_extend(Keyword::EXTEND);
    ParserKeyword s_set(Keyword::SET);
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_as(Keyword::AS);
    ParserKeyword s_aggregate(Keyword::AGGREGATE);
    ParserKeyword s_group_by(Keyword::GROUP_BY);
    ParserKeyword s_order_by(Keyword::ORDER_BY);
    ParserKeyword s_limit(Keyword::LIMIT);
    ParserKeyword s_offset(Keyword::OFFSET);
    ParserKeyword s_settings(Keyword::SETTINGS);

    ParserExpressionWithOptionalAlias exp_elem(false);
    ParserExpression exp;
    ParserNotEmptyExpressionList exp_list_with_aliases(true);
    /// The select-list-like operators accept exactly the same syntax as the `SELECT` clause of an ordinary query,
    /// including a trailing comma, e.g. `FROM t |> SELECT a, b,`.
    ParserNotEmptyExpressionList exp_list_for_select_clause(/*allow_alias_without_as_keyword*/ true, /*allow_trailing_commas*/ true);
    ParserIdentifier identifier;

    /// The alias set by the AS operator; it is applied to the subquery when the next operator wraps the query.
    String pending_alias;

    auto current_depth = pos.depth;

    while (pos->type == TokenType::PipeOperator)
    {
        ++pos;

        /// The AST grows by one nesting level with every pipe operator, same as with explicitly written subqueries.
        /// The parsing depth is limited here, and the depth of the resulting AST is checked once again after parsing.
        pos.increaseDepth();

        if (s_where.ignore(pos, expected))
        {
            /// |> WHERE condition
            ASTPtr expr;
            if (!exp_elem.parse(pos, expr, expected))
                return false;

            auto select = wrapQueryIntoSelect(std::move(query), pending_alias);
            select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(expr));
            query = wrapIntoUnion(std::move(select));
        }
        else if (s_select.ignore(pos, expected))
        {
            /// |> SELECT [DISTINCT] expr1 [AS alias1], ...
            bool distinct = s_distinct.ignore(pos, expected);

            ASTPtr select_list;
            if (!exp_list_for_select_clause.parse(pos, select_list, expected))
                return false;

            auto select = wrapQueryIntoSelect(std::move(query), pending_alias, std::move(select_list));
            select->distinct = distinct;
            query = wrapIntoUnion(std::move(select));
        }
        else if (s_extend.ignore(pos, expected))
        {
            /// |> EXTEND expr1 [AS alias1], ...   ->   SELECT *, expr1 AS alias1, ...
            ASTPtr extend_list;
            if (!exp_list_for_select_clause.parse(pos, extend_list, expected))
                return false;

            ASTPtr select_list = makeAsteriskList();
            for (auto & child : extend_list->children)
                select_list->children.push_back(std::move(child));

            query = wrapIntoUnion(wrapQueryIntoSelect(std::move(query), pending_alias, std::move(select_list)));
        }
        else if (s_set.ignore(pos, expected))
        {
            /// |> SET column1 = expr1, ...   ->   SELECT * REPLACE (expr1 AS column1, ...)
            ASTs replacements;

            auto parse_replacement = [&]
            {
                ASTPtr column;
                if (!identifier.parse(pos, column, expected))
                    return false;

                if (pos->type != TokenType::Equals)
                    return false;
                ++pos;

                ASTPtr expr;
                if (!exp.parse(pos, expr, expected))
                    return false;

                auto replacement = make_intrusive<ASTColumnsReplaceTransformer::Replacement>();
                replacement->name = getIdentifierName(column);
                replacement->children.push_back(std::move(expr));
                replacements.emplace_back(std::move(replacement));
                return true;
            };

            if (!parse_replacement())
                return false;

            while (pos->type == TokenType::Comma)
            {
                IParser::Pos saved_pos = pos;
                ++pos;
                if (!parse_replacement())
                {
                    /// The comma belongs to an outer list, e.g. a list of table function arguments.
                    pos = saved_pos;
                    break;
                }
            }

            auto transformer = make_intrusive<ASTColumnsReplaceTransformer>();
            transformer->children = std::move(replacements);

            query = wrapIntoUnion(wrapQueryIntoSelect(std::move(query), pending_alias, makeAsteriskList(std::move(transformer))));
        }
        else if (s_drop.ignore(pos, expected))
        {
            /// |> DROP column1, ...   ->   SELECT * EXCEPT (column1, ...)
            ASTs columns;

            auto parse_column = [&]
            {
                ASTPtr column;
                if (!ParserIdentifier(true).parse(pos, column, expected))
                    return false;

                columns.emplace_back(std::move(column));
                return true;
            };

            if (!parse_column())
                return false;

            while (pos->type == TokenType::Comma)
            {
                IParser::Pos saved_pos = pos;
                ++pos;
                if (!parse_column())
                {
                    /// The comma belongs to an outer list, e.g. a list of table function arguments.
                    pos = saved_pos;
                    break;
                }
            }

            auto transformer = make_intrusive<ASTColumnsExceptTransformer>();
            transformer->children = std::move(columns);

            query = wrapIntoUnion(wrapQueryIntoSelect(std::move(query), pending_alias, makeAsteriskList(std::move(transformer))));
        }
        else if (s_as.ignore(pos, expected))
        {
            /// |> AS alias
            /// The alias is applied to the subquery when the next pipe operator wraps the query,
            /// so it can be referenced in that operator: FROM t1 |> AS lhs |> JOIN t2 ON lhs.x = t2.x
            ASTPtr alias;
            if (!identifier.parse(pos, alias, expected))
                return false;

            pending_alias = getIdentifierName(alias);
        }
        else if (s_aggregate.ignore(pos, expected))
        {
            /// |> AGGREGATE agg1 [AS alias1], ... [GROUP BY expr1 [AS alias1], ...]
            /// -> SELECT expr1 AS alias1, ..., agg1 AS alias1, ... GROUP BY expr1, ...
            /// The output columns are the grouping columns followed by the aggregate columns.
            /// Without GROUP BY, it is an aggregation of the whole table.
            ASTPtr aggregate_list;
            if (!exp_list_for_select_clause.parse(pos, aggregate_list, expected))
                return false;

            ASTPtr group_list;
            if (s_group_by.ignore(pos, expected))
            {
                if (!exp_list_with_aliases.parse(pos, group_list, expected))
                    return false;
            }

            auto select_list = make_intrusive<ASTExpressionList>();
            ASTPtr group_by_list;

            if (group_list)
            {
                group_by_list = make_intrusive<ASTExpressionList>();
                for (auto & group_elem : group_list->children)
                {
                    /// The GROUP BY clause contains the same expressions, but without aliases.
                    auto group_by_elem = group_elem->clone();
                    if (auto * with_alias = dynamic_cast<ASTWithAlias *>(group_by_elem.get()))
                        with_alias->alias.clear();
                    group_by_list->children.push_back(std::move(group_by_elem));

                    select_list->children.push_back(std::move(group_elem));
                }
            }

            for (auto & aggregate_elem : aggregate_list->children)
                select_list->children.push_back(std::move(aggregate_elem));

            auto select = wrapQueryIntoSelect(std::move(query), pending_alias, std::move(select_list));
            if (group_by_list)
                select->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_by_list));
            query = wrapIntoUnion(std::move(select));
        }
        else if (s_distinct.ignore(pos, expected))
        {
            /// |> DISTINCT   ->   SELECT DISTINCT *
            auto select = wrapQueryIntoSelect(std::move(query), pending_alias);
            select->distinct = true;
            query = wrapIntoUnion(std::move(select));
        }
        else if (s_order_by.ignore(pos, expected))
        {
            /// |> ORDER BY expr1 [ASC/DESC] [WITH FILL], ... [INTERPOLATE (...)], or ORDER BY ALL.
            /// The clause body is parsed by the same function as in the ordinary SELECT query.
            ASTPtr order_expression_list;
            ASTPtr interpolate_expression_list;
            bool order_by_all = false;
            if (!parseOrderByClauseBody(pos, expected, order_expression_list, interpolate_expression_list, order_by_all))
                return false;

            auto select = wrapQueryIntoSelect(std::move(query), pending_alias);
            select->order_by_all = order_by_all;
            select->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_expression_list));
            select->setExpression(ASTSelectQuery::Expression::INTERPOLATE, std::move(interpolate_expression_list));
            query = wrapIntoUnion(std::move(select));
        }
        else if (s_limit.ignore(pos, expected))
        {
            /// |> LIMIT length [OFFSET offset]
            ASTPtr length;
            if (!exp_elem.parse(pos, length, expected))
                return false;

            ASTPtr offset;
            if (s_offset.ignore(pos, expected))
            {
                if (!exp_elem.parse(pos, offset, expected))
                    return false;
            }

            auto select = wrapQueryIntoSelect(std::move(query), pending_alias);
            /// The offset is set first, matching the order of children in ParserSelectQuery,
            /// because it is checked when the formatted AST is parsed back.
            if (offset)
                select->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(offset));
            select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(length));
            query = wrapIntoUnion(std::move(select));
        }
        else if (s_offset.ignore(pos, expected))
        {
            /// |> OFFSET offset
            ASTPtr offset;
            if (!exp_elem.parse(pos, offset, expected))
                return false;

            auto select = wrapQueryIntoSelect(std::move(query), pending_alias);
            select->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(offset));
            query = wrapIntoUnion(std::move(select));
        }
        else if (auto set_operation_mode = parseSetOperationMode(pos, expected))
        {
            /// |> UNION [ALL/DISTINCT] query1 [, query2, ...]
            /// |> INTERSECT [ALL/DISTINCT] query1 [, query2, ...]
            /// |> EXCEPT [ALL/DISTINCT] query1 [, query2, ...]
            /// Every query is typically enclosed in parentheses (mandatory for the comma-separated list form).
            SelectUnionMode mode = *set_operation_mode;

            ParserUnionQueryElement element_parser;

            ASTs elements;
            bool last_element_parenthesized = pos->type == TokenType::OpeningRoundBracket;
            ASTPtr element;
            if (!element_parser.parse(pos, element, expected))
                return false;
            elements.push_back(std::move(element));

            while (pos->type == TokenType::Comma)
            {
                IParser::Pos saved_pos = pos;
                ++pos;

                bool next_element_parenthesized = pos->type == TokenType::OpeningRoundBracket;
                ASTPtr next_element;
                if (!element_parser.parse(pos, next_element, expected))
                {
                    /// The comma belongs to an outer list, e.g. a list of table function arguments.
                    pos = saved_pos;
                    break;
                }
                last_element_parenthesized = next_element_parenthesized;
                elements.push_back(std::move(next_element));
            }

            /// A pipe operator after an unparenthesized operand is ambiguous: it is unclear whether it
            /// applies to the last operand or to the whole set operation, so parentheses are required:
            /// FROM lhs |> UNION ALL (FROM rhs) |> WHERE ... or FROM lhs |> UNION ALL (FROM rhs |> WHERE ...)
            if (!last_element_parenthesized && pos->type == TokenType::PipeOperator)
            {
                expected.add(pos, "parenthesized set operation operand before a pipe operator");
                return false;
            }

            /// The alias would be lost otherwise, so give the aliased query its own nesting level.
            if (!pending_alias.empty())
                query = wrapIntoUnion(wrapQueryIntoSelect(std::move(query), pending_alias));

            /// Flatten the query if it is a single SELECT, as ParserUnionQueryElement does.
            if (const auto * query_union = query->as<ASTSelectWithUnionQuery>();
                query_union && query_union->list_of_selects->children.size() == 1)
                query = query_union->list_of_selects->children.at(0);

            auto res = make_intrusive<ASTSelectWithUnionQuery>();
            res->list_of_selects = make_intrusive<ASTExpressionList>();
            res->list_of_selects->children.push_back(std::move(query));
            for (auto & elem : elements)
                res->list_of_selects->children.push_back(std::move(elem));
            res->children.push_back(res->list_of_selects);
            res->list_of_modes = SelectUnionModes(elements.size(), mode);

            query = res;
        }
        else
        {
            /// Operators of the FROM clause: |> [LEFT/RIGHT/...] JOIN table ON/USING ..., |> ARRAY JOIN expr
            /// -> SELECT * FROM (query) JOIN table ON/USING ...
            /// A single operator can contain several JOINs, like a FROM clause. The comma (cross) join
            /// spelling is allowed as well, with the input of the operator as its left side:
            /// FROM lhs |> , rhs   ->   SELECT * FROM (SELECT * FROM lhs), rhs
            auto select = wrapQueryIntoSelect(std::move(query), pending_alias);
            auto & tables = select->refTables()->children;

            ASTPtr element;
            if (!ParserTablesInSelectQueryElement(false).parse(pos, element, expected))
                return false;
            tables.push_back(std::move(element));

            while (true)
            {
                /// A comma (cross) join is not supported right after an ARRAY JOIN, same as in
                /// ParserTablesInSelectQuery. ARRAY JOIN greedily consumes its comma-separated
                /// expression list, so a leftover comma means the next item failed to parse as an
                /// expression; consuming it as a comma-joined table would break the format/parse
                /// round-trip (the ARRAY JOIN absorbs the reformatted subquery on reparse). Stop
                /// instead so the query fails as a syntax error.
                const auto * prev = tables.back()->as<ASTTablesInSelectQueryElement>();
                if (prev && prev->array_join && pos->type == TokenType::Comma)
                    break;

                IParser::Pos saved_pos = pos;
                ASTPtr next_element;
                if (!ParserTablesInSelectQueryElement(false).parse(pos, next_element, expected))
                {
                    pos = saved_pos;
                    break;
                }
                tables.push_back(std::move(next_element));
            }

            query = wrapIntoUnion(std::move(select));
        }

        /// Like any SELECT query, the query generated by a pipe operator can end with a SETTINGS
        /// clause: FROM t |> LIMIT 1 SETTINGS max_threads = 1. It is attached to the generated
        /// wrapper, so the AST is the same as for the equivalent hand-written nested form
        /// SELECT * FROM (SELECT * FROM t) LIMIT 1 SETTINGS max_threads = 1. Without this,
        /// a trailing SETTINGS would be a syntax error in the contexts that have no separate
        /// pass for query settings, such as a subquery, CREATE VIEW, or the view table function.
        /// After a set operation the SETTINGS is not consumed, because the equivalent query
        /// with subqueries cannot have a SETTINGS clause in that position either.
        if (s_settings.checkWithoutMoving(pos, expected))
        {
            const auto * query_union = query->as<ASTSelectWithUnionQuery>();
            if (query_union && query_union->list_of_selects->children.size() == 1)
            {
                if (auto * select = query_union->list_of_selects->children.front()->as<ASTSelectQuery>();
                    select && !select->settings())
                {
                    s_settings.ignore(pos, expected);

                    ASTPtr settings;
                    ParserSetQuery parser_settings(true);
                    if (!parser_settings.parse(pos, settings, expected))
                        return false;

                    /// SETTINGS goes before INTERPOLATE in the order of children of ParserSelectQuery,
                    /// which is checked when the formatted AST is parsed back.
                    ASTPtr interpolate = select->interpolate();
                    if (interpolate)
                        select->setExpression(ASTSelectQuery::Expression::INTERPOLATE, {});
                    select->setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings));
                    if (interpolate)
                        select->setExpression(ASTSelectQuery::Expression::INTERPOLATE, std::move(interpolate));
                }
            }
        }
    }

    /// If AS was the last operator in the chain, the alias is applied to a wrap with no other effect.
    if (!pending_alias.empty())
        query = wrapIntoUnion(wrapQueryIntoSelect(std::move(query), pending_alias));

    pos.depth = current_depth;
    return true;
}

}

namespace DB
{

void registerStatementPipeOperators(StatementFactory & factory)
{
    factory.registerStatement("PIPE OPERATORS",
    {
        .description = R"DOCS_MD(
Pipe operators allow writing queries as a linear chain of transformations that reads from top to bottom, similar to the [pipe syntax of GoogleSQL](https://research.google/pubs/sql-has-problems-we-can-fix-them-pipe-syntax-in-sql/):

```sql
FROM orders
|> WHERE cancelled = 0
|> AGGREGATE sum(amount) AS total GROUP BY customer
|> ORDER BY total DESC
|> LIMIT 3
```

Any `SELECT` query can be followed by a chain of pipe operators. Each operator starts with the `|>` token, takes the result of the query before it as input, and applies one more transformation to it. Inside every operator, the regular ClickHouse syntax is used.

Pipe operators are a syntax extension: every operator wraps the query before it into a subquery, so the resulting AST is the same as the AST of the equivalent query written with nested subqueries, and the query above is equivalent to:

```sql
SELECT * FROM
(
    SELECT customer, sum(amount) AS total FROM
    (
        SELECT * FROM
        (
            SELECT * FROM orders
        )
        WHERE cancelled = 0
    )
    GROUP BY customer
)
ORDER BY total DESC
LIMIT 3
```

## FROM queries {#from-queries}

A query can start with the `FROM` clause, and the `SELECT` clause is optional in such queries - when it is omitted, the query works as if `SELECT *` was written:

```sql
FROM orders;
FROM orders WHERE amount > 100;
FROM orders |> WHERE amount > 100;
```

Table aliases can be written with or without the `AS` keyword, as in the `FROM` clause of an ordinary `SELECT` query: `FROM orders o WHERE o.amount > 100`. The only exception is an alias written as the bare word `select`: after the tables it starts the explicit `SELECT` clause instead of being treated as an alias. A table named `select` is unaffected and keeps its own alias: `FROM select s WHERE s.id = 1`.

The `SELECT` clause cannot be omitted when the sample offset of the last table could also be read as a query-level `OFFSET`, because in `FROM t SAMPLE 1/10 OFFSET 5` the `OFFSET` belongs to `SAMPLE`, while in `FROM t SAMPLE 1/10 SELECT * OFFSET 5` it is a query-level `OFFSET` - the explicit `SELECT` is required to disambiguate the two. When the query continues with a clause that a query-level `OFFSET` cannot precede, there is no ambiguity and the `SELECT` clause is optional as usual: `FROM t SAMPLE 1/10 OFFSET 5 WHERE x > 0`, `FROM t SAMPLE 1/10 OFFSET 5 JOIN dim USING (id)`.

## Operators {#operators}

### WHERE {#where}

`|> WHERE condition` filters the input rows. When it is applied after an aggregation, it works like `HAVING`:

```sql
FROM orders
|> AGGREGATE sum(amount) AS total GROUP BY customer
|> WHERE total > 100
```

### SELECT {#select}

`|> SELECT [DISTINCT] expr1 [AS alias1], ...` leaves only the listed expressions as the output columns:

```sql
FROM orders |> SELECT customer, amount * 2 AS doubled
```

A trailing comma is allowed at the end of the list of expressions in the same positions as in the `SELECT` clause of an ordinary query - here it can be followed by the end of the query or by the next `|>` operator: `FROM orders |> SELECT customer, amount, |> LIMIT 1`. The same applies to the `EXTEND` and `AGGREGATE` operators.

### EXTEND {#extend}

`|> EXTEND expr1 [AS alias1], ...` appends the listed expressions to the input columns; it is equivalent to `SELECT *, expr1 AS alias1, ...`:

```sql
FROM orders |> EXTEND amount * 10 AS big
```

### SET {#set}

`|> SET column1 = expr1, ...` replaces the values of the listed columns; it is equivalent to `SELECT * REPLACE (expr1 AS column1, ...)`:

```sql
FROM orders |> SET amount = amount + 1000
```

### DROP {#drop}

`|> DROP column1, ...` removes the listed columns; it is equivalent to `SELECT * EXCEPT (column1, ...)`:

```sql
FROM orders |> DROP cancelled
```

### AS {#as}

`|> AS alias` gives an alias to the input of the next operator, so it can be referenced in that operator, which is mostly useful for joins:

```sql
FROM orders
|> AGGREGATE sum(amount) AS total GROUP BY customer
|> AS agg
|> JOIN orders AS o ON agg.customer = o.customer
```

### AGGREGATE {#aggregate}

`|> AGGREGATE agg1 [AS alias1], ... [GROUP BY expr1 [AS alias1], ...]` aggregates the input rows. The output columns are the grouping columns followed by the aggregate columns. Without `GROUP BY`, the whole input is aggregated to a single row:

```sql
FROM orders |> AGGREGATE count() AS c, sum(amount) AS total GROUP BY customer;
FROM orders |> AGGREGATE count() AS c;
```

### DISTINCT {#distinct}

`|> DISTINCT` removes duplicate rows; it is equivalent to `SELECT DISTINCT *`.

### ORDER BY {#order-by}

`|> ORDER BY expr1 [ASC/DESC], ...` sorts the input rows. The full `ORDER BY` clause syntax is supported, including `ORDER BY ALL`, `WITH FILL`, and `INTERPOLATE`:

```sql
FROM orders |> ORDER BY amount DESC;
FROM orders |> SELECT customer, amount |> ORDER BY ALL;
FROM points |> ORDER BY x WITH FILL FROM 1 TO 10 INTERPOLATE (y AS y + 1)
```

### LIMIT and OFFSET {#limit-and-offset}

`|> LIMIT length [OFFSET offset]` and `|> OFFSET offset` limit the number of rows:

```sql
FROM orders |> ORDER BY amount DESC |> LIMIT 3 OFFSET 1
```

### JOIN and ARRAY JOIN {#join-and-array-join}

`|> [GLOBAL] [ANY/ALL/ASOF/SEMI/ANTI] [INNER/LEFT/RIGHT/FULL/CROSS] JOIN table [ON expr | USING (columns)]` joins the input with another table, subquery, or table function. All kinds of [JOIN](/reference/statements/select/join) and [ARRAY JOIN](/reference/statements/select/array-join) are supported, and a single operator can contain several joins, like a `FROM` clause:

```sql
FROM customers
|> AS c
|> LEFT JOIN orders AS o ON c.name = o.customer
|> ARRAY JOIN tags
```

Since every operator is a new subquery scope, table aliases are visible only inside the same operator (in the `ON` condition). The following operators see the combined columns of the join result, as after `SELECT *`.

The comma spelling of a cross join is supported as well, with the input of the operator as the left side: `FROM customers |> AS c |> , orders`. As with the other joins, the input needs an alias when the `joined_subquery_requires_alias` setting is enabled (it is by default).

As in the `FROM` clause of an ordinary query, a comma (cross) join is not supported right after an `ARRAY JOIN`: a comma after the `ARRAY JOIN` always belongs to its expression list.

### UNION, INTERSECT, and EXCEPT {#union-intersect-and-except}

`|> UNION [ALL/DISTINCT] (query1) [, (query2), ...]`, `|> INTERSECT [ALL/DISTINCT] ...`, and `|> EXCEPT [ALL/DISTINCT] ...` combine the input with the results of other queries:

```sql
FROM orders
|> SELECT customer
|> UNION ALL (FROM customers |> SELECT name)
|> DISTINCT
```

The parentheses around an operand are optional for a single query, but they are required when the chain continues with another pipe operator after the set operation - otherwise it would be unclear whether the next operator applies to the last operand or to the whole result.

## Notes {#notes}

- The `WITH` clause of the query stays visible in all following pipe operators, both for scalar aliases and for CTEs: `WITH 10 AS threshold FROM t |> WHERE x < threshold`.
- In `INSERT ... SELECT`, a `WITH` clause written before `INSERT` is attached to the outermost generated `SELECT`, and it reaches the inner pipe stages during interpretation via the `enable_global_with_statement` setting (enabled by default) — the same way it reaches a hand-written nested subquery. If that setting is disabled, aliases and CTEs from an `INSERT`-scoped `WITH` are not visible inside the pipe stages, exactly as they are not visible inside a hand-written subquery.
- Like any `SELECT` query, the query generated by a pipe operator can end with a `SETTINGS` clause, which is attached to that generated query: `FROM t |> LIMIT 1 SETTINGS max_threads = 1` is the same as `SELECT * FROM (SELECT * FROM t) LIMIT 1 SETTINGS max_threads = 1`. This also works where there is no separate pass for query settings, such as in a subquery, in `CREATE VIEW`, or in the `view` table function. A `SETTINGS` clause in the middle of a chain stays on its stage, which becomes a subquery of the next operator. After a set operation with a parenthesized operand, a trailing `SETTINGS` is not accepted - the equivalent query with subqueries cannot have a `SETTINGS` clause in that position either.
- A `SETTINGS` clause of the query before the first pipe operator stays on that query, which becomes a subquery of the generated wrapper. Ordinary settings keep working, because settings of a subquery are applied when that subquery is interpreted. The only exception is the pair of settings that select the query analyzer, `enable_analyzer` and its alias `allow_experimental_analyzer`: changing them in a subquery is not allowed, so `SELECT number FROM numbers(1) SETTINGS enable_analyzer = 0 |> LIMIT 1` throws `INCORRECT_QUERY` — exactly as the equivalent hand-written `SELECT * FROM (SELECT number FROM numbers(1) SETTINGS enable_analyzer = 0) LIMIT 1` does. Write these two settings after the last pipe operator, or pass them outside of the query.
- Pipe operators bind to the whole query before them, including set operations: in `SELECT 1 UNION ALL SELECT 2 |> AGGREGATE count()`, the aggregation is applied to the result of the `UNION ALL`. To continue a query with `UNION` after a pipe operator, use the `|> UNION` operator or parentheses.
- Pipe operators can be used everywhere a `SELECT` query is expected: in subqueries, in `INSERT ... SELECT` (including the form `INSERT INTO t FROM src |> ...`), in `CREATE VIEW`, in the `view` table function, and so on.
- The renaming of columns in place is not provided as a separate operator; use `|> SELECT * EXCEPT (old_name), old_name AS new_name` or the `SET` and `DROP` operators.
)DOCS_MD",
        .syntax = R"(
FROM table
|> SELECT ... | WHERE ... | ORDER BY ... | LIMIT ... | AGGREGATE ... [GROUP BY ...] | EXTEND ... | SET ... | DROP ... | RENAME ... | DISTINCT | UNION ... | INTERSECT ... | EXCEPT ... | JOIN ... | ARRAY JOIN ... | CALL ...
[|> ...]
)",
        .parent = "SELECT",
        .related = {"SELECT", "FROM", "WHERE", "ORDER BY", "LIMIT"},
    });
}

}
