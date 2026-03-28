#include <algorithm>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserPipeOperators.h>
#include <Parsers/ParserTablesInSelectQuery.h>

namespace DB
{

bool ParserPipeWhere::parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const
{
    ASTPtr condition;
    if (!ParserExpressionWithOptionalAlias(false).parse(pos, condition, expected))
        return false;

    if (query.where())
        condition = makeASTFunction("and", query.where()->clone(), condition);

    query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(condition));
    return true;
}

bool ParserPipeOrderBy::parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const
{
    auto saved_pos = pos;
    bool is_order_by_all = false;

    ASTPtr order_expression_list;
    ASTPtr interpolate_expression_list;

    ParserKeyword s_all(Keyword::ALL);
    ParserKeyword s_interpolate(Keyword::INTERPOLATE);

    ParserToken s_open_bracket(TokenType::OpeningRoundBracket);
    ParserToken s_closing_bracket(TokenType::ClosingRoundBracket);

    if (s_all.ignore(pos, expected))
    {
        is_order_by_all = true;

        ParserKeyword s_desc(Keyword::DESC);
        ParserKeyword s_descending(Keyword::DESCENDING);
        ParserKeyword s_asc(Keyword::ASC);
        ParserKeyword s_ascending(Keyword::ASCENDING);
        ParserKeyword s_nulls(Keyword::NULLS);
        ParserKeyword s_last(Keyword::LAST);

        int direction = 1;
        int nulls_direction = 1;
        bool nulls_direction_was_explicitly_specified = false;

        if (s_desc.ignore(pos, expected) || s_descending.ignore(pos, expected))
        {
            direction = -1;
            nulls_direction = -1;
        }
        else
        {
            s_asc.ignore(pos, expected) || s_ascending.ignore(pos, expected);
        }

        if (s_nulls.ignore(pos, expected))
        {
            nulls_direction_was_explicitly_specified = true;
            if (ParserKeyword(Keyword::FIRST).ignore(pos, expected))
                nulls_direction = -direction;
            else if (s_last.ignore(pos, expected))
                ;
            else
                return false;
        }

        if (pos->type == TokenType::Comma)
        {
            pos = saved_pos;
            is_order_by_all = false;
        }
        else
        {
            query.order_by_all = true;

            auto elem = make_intrusive<ASTOrderByElement>();
            elem->direction = direction;
            elem->nulls_direction = nulls_direction;
            elem->nulls_direction_was_explicitly_specified = nulls_direction_was_explicitly_specified;
            elem->children.push_back(make_intrusive<ASTIdentifier>("all"));

            order_expression_list = make_intrusive<ASTExpressionList>();
            order_expression_list->children.push_back(std::move(elem));
        }
    }

    if (!is_order_by_all)
    {
        if (!ParserOrderByExpressionList().parse(pos, order_expression_list, expected))
            return false;

        if (std::any_of(
                order_expression_list->children.begin(),
                order_expression_list->children.end(),
                [](auto & child) { return child->template as<ASTOrderByElement>()->with_fill; }))
        {
            if (s_interpolate.ignore(pos, expected))
            {
                if (s_open_bracket.ignore(pos, expected))
                {
                    if (!ParserInterpolateExpressionList().parse(pos, interpolate_expression_list, expected))
                        return false;
                    if (!s_closing_bracket.ignore(pos, expected))
                        return false;
                }
                else
                {
                    interpolate_expression_list = make_intrusive<ASTExpressionList>();
                }
            }
        }
    }

    query.setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_expression_list));
    query.setExpression(ASTSelectQuery::Expression::INTERPOLATE, std::move(interpolate_expression_list));
    return true;
}

bool ParserPipeLimit::parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const
{
    ParserExpressionWithOptionalAlias exp_elem(false);
    ParserToken s_comma(TokenType::Comma);
    ParserKeyword s_offset(Keyword::OFFSET);

    ASTPtr limit_length;
    ASTPtr limit_offset;

    if (!exp_elem.parse(pos, limit_length, expected))
        return false;

    if (s_comma.ignore(pos, expected))
    {
        limit_offset = limit_length;
        if (!exp_elem.parse(pos, limit_length, expected))
            return false;
    }
    else if (s_offset.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, limit_offset, expected))
            return false;
    }

    query.setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(limit_offset));
    query.setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit_length));
    return true;
}

bool ParserPipeJoin::parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const
{
    auto tables_ast = query.tables();
    if (!tables_ast)
        return false;

    auto * tables = tables_ast->as<ASTTablesInSelectQuery>();
    if (!tables)
        return false;

    ASTPtr join_element;
    if (!ParserTablesInSelectQueryElement(false).parse(pos, join_element, expected))
        return false;

    auto * element = join_element->as<ASTTablesInSelectQueryElement>();
    if (!element || !element->table_join)
        return false;

    tables->children.emplace_back(std::move(join_element));
    return true;
}

bool ParserPipeAggregate::parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const
{
    ParserKeyword s_group_by(Keyword::GROUP_BY);

    ASTPtr select_expression_list;
    ASTPtr group_expression_list;

    if (!ParserNotEmptyExpressionList(true, true).parse(pos, select_expression_list, expected))
        return false;

    if (s_group_by.ignore(pos, expected))
    {
        if (!ParserNotEmptyExpressionList(false).parse(pos, group_expression_list, expected))
            return false;
    }

    query.setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));
    query.setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_expression_list));
    return true;
}

}
