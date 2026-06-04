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

namespace ErrorCodes
{
extern const int WITH_TIES_WITHOUT_ORDER_BY;
}

namespace
{

enum SQLClauseOrder : int
{
    NONE = 0,
    JOIN = 1,
    WHERE = 2,
    GROUP_BY = 3,
    AGGREGATE = 4,
    ORDER_BY = 5,
    LIMIT_OFFSET = 6
};

void checkConstraints(ASTSelectQuery & query)
{
    if (query.limit_with_ties && !query.orderBy())
    {
        throw Exception(ErrorCodes::WITH_TIES_WITHOUT_ORDER_BY, "Can not use WITH TIES without ORDER BY");
    }
}

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

/// Whether `pos` points at a position where a query may legitimately end, i.e.
/// nothing the regular SELECT parser could consume follows. Used to make sure a
/// bare `FROM` pipelined query does not shadow existing syntax such as
/// `FROM t SELECT c` or `FROM a JOIN b SELECT c`.
bool atQueryEnd(IParser::Pos pos)
{
    if (pos->isEnd() || pos->type == TokenType::Semicolon || pos->type == TokenType::ClosingRoundBracket)
        return true;

    Expected expected;
    return ParserKeyword(Keyword::FORMAT).ignore(pos, expected)
        || ParserKeyword(Keyword::SETTINGS).ignore(pos, expected)
        || ParserKeyword(Keyword::INTO_OUTFILE).ignore(pos, expected);
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
    ParserKeyword s_offset(Keyword::OFFSET);

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
    SQLClauseOrder highest_op = SQLClauseOrder::NONE;

    auto wrap_current_query = [&]()
    {
        checkConstraints(current_query->as<ASTSelectQuery &>());

        current_query = wrapInSubquery(current_query, subquery_seqno++)->as<ASTSelectQuery>();
        highest_op = SQLClauseOrder::NONE;
    };

    ParserPipeWhere pipe_where_parser;
    ParserPipeOrderBy pipe_order_by_parser;
    ParserPipeLimit pipe_limit_parser;
    ParserPipeOffset pipe_offset_parser;
    ParserPipeJoin pipe_join_parser;
    ParserPipeAggregate pipe_aggregate_parser;

    bool consumed_pipe = false;

    while (s_pipe_arrow.ignore(pos, expected))
    {
        consumed_pipe = true;

        if (s_where.ignore(pos, expected))
        {
            if (SQLClauseOrder::WHERE < highest_op)
                wrap_current_query();

            if (!pipe_where_parser.parse(pos, current_query->as<ASTSelectQuery &>(), expected))
                return false;

            highest_op = std::max(highest_op, SQLClauseOrder::WHERE);
        }
        else if (s_order_by.ignore(pos, expected))
        {
            if (SQLClauseOrder::ORDER_BY <= highest_op)
                wrap_current_query();

            if (!pipe_order_by_parser.parse(pos, current_query->as<ASTSelectQuery &>(), expected))
                return false;

            highest_op = std::max(highest_op, SQLClauseOrder::ORDER_BY);
        }
        else if (s_limit.ignore(pos, expected))
        {
            if (SQLClauseOrder::LIMIT_OFFSET <= highest_op)
                wrap_current_query();

            if (!pipe_limit_parser.parse(pos, current_query->as<ASTSelectQuery &>(), expected))
                return false;

            highest_op = std::max(highest_op, SQLClauseOrder::LIMIT_OFFSET);
        }
        else if (s_offset.ignore(pos, expected))
        {
            if (SQLClauseOrder::LIMIT_OFFSET <= highest_op)
                wrap_current_query();

            if (!pipe_offset_parser.parse(pos, current_query->as<ASTSelectQuery &>(), expected))
                return false;

            highest_op = std::max(highest_op, SQLClauseOrder::LIMIT_OFFSET);
        }
        else if (s_aggregate.ignore(pos, expected))
        {
            if (SQLClauseOrder::GROUP_BY <= highest_op)
                wrap_current_query();

            if (!pipe_aggregate_parser.parse(pos, current_query->as<ASTSelectQuery &>(), expected))
                return false;

            highest_op = std::max(highest_op, SQLClauseOrder::AGGREGATE);
            if (current_query->groupBy())
            {
                highest_op = std::max(highest_op, SQLClauseOrder::GROUP_BY);
            }
        }
        else if (startsWithJoinClause(pos))
        {
            if (SQLClauseOrder::JOIN < highest_op)
                wrap_current_query();

            if (!pipe_join_parser.parse(pos, current_query->as<ASTSelectQuery &>(), expected))
                return false;

            highest_op = std::max(highest_op, SQLClauseOrder::JOIN);
        }
        else
        {
            return false;
        }
    }

    /// A bare `FROM <table>` query (without any pipe operator) must not shadow the
    /// existing `FROM ... SELECT ...` syntax handled by the regular SELECT parser,
    /// which runs after this one. Reject it unless the table expression is followed
    /// by a valid end of query, so that the regular parser gets a chance.
    if (!consumed_pipe && !atQueryEnd(pos))
        return false;

    checkConstraints(current_query->as<ASTSelectQuery &>());

    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(current_query);

    auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
    union_query->list_of_selects = list_of_selects;
    union_query->children.push_back(list_of_selects);

    node = std::move(union_query);
    return true;
}
}
