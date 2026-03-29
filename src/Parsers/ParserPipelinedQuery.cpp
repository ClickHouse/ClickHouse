#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserPipeOperators.h>
#include <Parsers/ParserPipelinedQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>

#include <Common/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace
{
ASTPtr wrapInSubquery(ASTPtr current_query, size_t seqno)
{
    auto subquery = make_intrusive<ASTSubquery>();
    auto inner_union = make_intrusive<ASTSelectWithUnionQuery>();
    auto inner_list = make_intrusive<ASTExpressionList>();

    inner_list->children.push_back(current_query);

    inner_union->list_of_selects = inner_list;
    inner_union->children.push_back(inner_list);

    subquery->children.push_back(inner_union);
    subquery->setAlias(fmt::format("_pipe_subquery_{}", seqno));

    auto table_expr = make_intrusive<ASTTableExpression>();
    table_expr->subquery = subquery;
    table_expr->children.push_back(subquery);

    auto tables_element = make_intrusive<ASTTablesInSelectQueryElement>();
    tables_element->table_expression = table_expr;
    tables_element->children.push_back(table_expr);

    auto tables_in_select = make_intrusive<ASTTablesInSelectQuery>();
    tables_in_select->children.push_back(tables_element);

    auto outer_query = make_intrusive<ASTSelectQuery>();

    auto select_list = make_intrusive<ASTExpressionList>();
    select_list->children.push_back(make_intrusive<ASTAsterisk>());
    outer_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list));
    outer_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables_in_select));

    return outer_query;
}

bool startsWithJoinClause(const IParser::Pos & pos)
{
    auto starts_with_keyword = [&](Keyword keyword)
    {
        IParser::Pos lookahead = pos;
        Expected expected;
        return ParserKeyword(keyword).ignore(lookahead, expected);
    };

    return starts_with_keyword(Keyword::JOIN) || starts_with_keyword(Keyword::GLOBAL) || starts_with_keyword(Keyword::LOCAL)
        || starts_with_keyword(Keyword::NATURAL) || starts_with_keyword(Keyword::ANY) || starts_with_keyword(Keyword::ALL)
        || starts_with_keyword(Keyword::ASOF) || starts_with_keyword(Keyword::SEMI) || starts_with_keyword(Keyword::ANTI)
        || starts_with_keyword(Keyword::ONLY) || starts_with_keyword(Keyword::INNER) || starts_with_keyword(Keyword::LEFT)
        || starts_with_keyword(Keyword::RIGHT) || starts_with_keyword(Keyword::FULL) || starts_with_keyword(Keyword::CROSS)
        || starts_with_keyword(Keyword::PASTE);
}
}

bool ParserPipelinedQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!allow_pipe_syntax)
    {
        return false;
    }

    ParserKeyword s_from(Keyword::FROM);
    ParserKeyword s_where(Keyword::WHERE);
    ParserKeyword s_order_by(Keyword::ORDER_BY);
    ParserKeyword s_aggregate(Keyword::AGGREGATE);
    ParserKeyword s_limit(Keyword::LIMIT);

    ParserToken s_pipe_arrow(TokenType::PipeArrow);

    if (!s_from.ignore(pos, expected))
        return false;

    ASTPtr tables;
    if (!ParserTableExpression().parse(pos, tables, expected))
        return false;

    auto current_query = make_intrusive<ASTSelectQuery>();
    auto tables_in_select = make_intrusive<ASTTablesInSelectQuery>();
    auto tables_element = make_intrusive<ASTTablesInSelectQueryElement>();

    tables_element->table_expression = tables;
    tables_element->children.push_back(tables);

    tables_in_select->children.push_back(tables_element);

    auto select_expr_list = make_intrusive<ASTExpressionList>();
    select_expr_list->children.push_back(make_intrusive<ASTAsterisk>());

    current_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expr_list));
    current_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables_in_select));

    size_t subquery_seqno = 0;
    bool has_where = false;
    bool has_aggregate = false;
    bool has_order_by = false;
    bool has_limit = false;

    auto wrap_current_query = [&]()
    {
        current_query = wrapInSubquery(current_query, subquery_seqno++)->as<ASTSelectQuery>();
        has_where = false;
        has_aggregate = false;
        has_order_by = false;
        has_limit = false;
    };

    ParserPipeWhere pipe_where_parser;
    ParserPipeOrderBy pipe_order_by_parser;
    ParserPipeLimit pipe_limit_parser;
    ParserPipeJoin pipe_join_parser;
    ParserPipeAggregate pipe_aggregate_parser;

    while (s_pipe_arrow.ignore(pos, expected))
    {
        if (s_where.ignore(pos, expected))
        {
            if (has_limit || has_aggregate || has_order_by)
                wrap_current_query();

            if (!pipe_where_parser.parse(pos, current_query->as<ASTSelectQuery &>(), expected))
                return false;

            has_where = true;
        }
        else if (s_order_by.ignore(pos, expected))
        {
            if (has_limit || has_order_by)
                wrap_current_query();

            if (!pipe_order_by_parser.parse(pos, current_query->as<ASTSelectQuery &>(), expected))
                return false;

            has_order_by = true;
        }
        else if (s_limit.ignore(pos, expected))
        {
            if (has_limit)
                wrap_current_query();

            if (!pipe_limit_parser.parse(pos, current_query->as<ASTSelectQuery &>(), expected))
                return false;

            has_limit = true;
        }
        else if (s_aggregate.ignore(pos, expected))
        {
            wrap_current_query();

            if (!pipe_aggregate_parser.parse(pos, current_query->as<ASTSelectQuery &>(), expected))
                return false;

            has_aggregate = true;
        }
        else if (startsWithJoinClause(pos))
        {
            if (has_where || has_aggregate || has_order_by || has_limit)
                wrap_current_query();

            if (!pipe_join_parser.parse(pos, current_query->as<ASTSelectQuery &>(), expected))
                return false;
        }
        else
        {
            return false;
        }
    }

    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(current_query);

    auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
    union_query->list_of_selects = list_of_selects;
    union_query->children.push_back(list_of_selects);

    node = std::move(union_query);
    return true;
}
}
