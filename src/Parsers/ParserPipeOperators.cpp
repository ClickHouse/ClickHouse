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
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserUnionQueryElement.h>

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

    ParserExpressionWithOptionalAlias exp_elem(false);
    ParserExpression exp;
    ParserNotEmptyExpressionList exp_list_with_aliases(true);
    ParserOrderByExpressionList order_list;
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
            if (!exp_list_with_aliases.parse(pos, select_list, expected))
                return false;

            auto select = wrapQueryIntoSelect(std::move(query), pending_alias, std::move(select_list));
            select->distinct = distinct;
            query = wrapIntoUnion(std::move(select));
        }
        else if (s_extend.ignore(pos, expected))
        {
            /// |> EXTEND expr1 [AS alias1], ...   ->   SELECT *, expr1 AS alias1, ...
            ASTPtr extend_list;
            if (!exp_list_with_aliases.parse(pos, extend_list, expected))
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
            if (!exp_list_with_aliases.parse(pos, aggregate_list, expected))
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
            /// |> ORDER BY expr1 [ASC/DESC], ...
            ASTPtr order_expression_list;
            if (!order_list.parse(pos, order_expression_list, expected))
                return false;

            auto select = wrapQueryIntoSelect(std::move(query), pending_alias);
            select->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_expression_list));
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
            ASTPtr element;
            if (!element_parser.parse(pos, element, expected))
                return false;
            elements.push_back(std::move(element));

            while (pos->type == TokenType::Comma)
            {
                IParser::Pos saved_pos = pos;
                ++pos;

                ASTPtr next_element;
                if (!element_parser.parse(pos, next_element, expected))
                {
                    /// The comma belongs to an outer list, e.g. a list of table function arguments.
                    pos = saved_pos;
                    break;
                }
                elements.push_back(std::move(next_element));
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
            /// A single operator can contain several JOINs, like a FROM clause.
            if (pos->type == TokenType::Comma)
                return false;

            auto select = wrapQueryIntoSelect(std::move(query), pending_alias);
            auto & tables = select->refTables()->children;

            ASTPtr element;
            if (!ParserTablesInSelectQueryElement(false).parse(pos, element, expected))
                return false;
            tables.push_back(std::move(element));

            while (true)
            {
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
    }

    /// If AS was the last operator in the chain, the alias is applied to a wrap with no other effect.
    if (!pending_alias.empty())
        query = wrapIntoUnion(wrapQueryIntoSelect(std::move(query), pending_alias));

    pos.depth = current_depth;
    return true;
}

}
