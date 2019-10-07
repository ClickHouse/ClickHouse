#include <memory>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int TOP_AND_LIMIT_TOGETHER;
}


bool ParserSelectQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto select_query = std::make_shared<ASTSelectQuery>();
    node = select_query;

    ParserKeyword s_select("SELECT");
    ParserKeyword s_distinct("DISTINCT");
    ParserKeyword s_from("FROM");
    ParserKeyword s_prewhere("PREWHERE");
    ParserKeyword s_where("WHERE");
    ParserKeyword s_group_by("GROUP BY");
    ParserKeyword s_with("WITH");
    ParserKeyword s_totals("TOTALS");
    ParserKeyword s_having("HAVING");
    ParserKeyword s_order_by("ORDER BY");
    ParserKeyword s_limit("LIMIT");
    ParserKeyword s_settings("SETTINGS");
    ParserKeyword s_by("BY");
    ParserKeyword s_rollup("ROLLUP");
    ParserKeyword s_cube("CUBE");
    ParserKeyword s_top("TOP");
    ParserKeyword s_offset("OFFSET");

    ParserNotEmptyExpressionList exp_list(false);
    ParserNotEmptyExpressionList exp_list_for_with_clause(false, true); /// Set prefer_alias_to_column_name for each alias.
    ParserNotEmptyExpressionList exp_list_for_select_clause(true);    /// Allows aliases without AS keyword.
    ParserExpressionWithOptionalAlias exp_elem(false);
    ParserOrderByExpressionList order_list;

    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);

    ASTPtr with_expression_list;
    ASTPtr select_expression_list;
    ASTPtr tables;
    ASTPtr prewhere_expression;
    ASTPtr where_expression;
    ASTPtr group_expression_list;
    ASTPtr having_expression;
    ASTPtr order_expression_list;
    ASTPtr limit_by_length;
    ASTPtr limit_by_offset;
    ASTPtr limit_by_expression_list;
    ASTPtr limit_offset;
    ASTPtr limit_length;
    ASTPtr settings;

    /// WITH expr list
    {
        if (s_with.ignore(pos, expected))
        {
            if (!exp_list_for_with_clause.parse(pos, with_expression_list, expected))
                return false;
        }
    }

    /// SELECT [DISTINCT] [TOP N] expr list
    {
        if (!s_select.ignore(pos, expected))
            return false;

        if (s_distinct.ignore(pos, expected))
            select_query->distinct = true;

        if (s_top.ignore(pos, expected))
        {
            ParserNumber num;

            if (open_bracket.ignore(pos, expected))
            {
                if (!num.parse(pos, limit_length, expected))
                    return false;
                if (!close_bracket.ignore(pos, expected))
                    return false;
            }
            else
            {
                if (!num.parse(pos, limit_length, expected))
                    return false;
            }
        }

        if (!exp_list_for_select_clause.parse(pos, select_expression_list, expected))
            return false;
    }

    /// FROM database.table or FROM table or FROM (subquery) or FROM tableFunction(...)
    if (s_from.ignore(pos, expected))
    {
        if (!ParserTablesInSelectQuery().parse(pos, tables, expected))
            return false;
    }

    /// PREWHERE expr
    if (s_prewhere.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, prewhere_expression, expected))
            return false;
    }

    /// WHERE expr
    if (s_where.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, where_expression, expected))
            return false;
    }

    /// GROUP BY expr list
    if (s_group_by.ignore(pos, expected))
    {
        if (s_rollup.ignore(pos, expected))
            select_query->group_by_with_rollup = true;
        else if (s_cube.ignore(pos, expected))
            select_query->group_by_with_cube = true;

        if ((select_query->group_by_with_rollup || select_query->group_by_with_cube) && !open_bracket.ignore(pos, expected))
            return false;

        if (!exp_list.parse(pos, group_expression_list, expected))
            return false;

        if ((select_query->group_by_with_rollup || select_query->group_by_with_cube) && !close_bracket.ignore(pos, expected))
            return false;
    }

    /// WITH ROLLUP, CUBE or TOTALS
    if (s_with.ignore(pos, expected))
    {
        if (s_rollup.ignore(pos, expected))
            select_query->group_by_with_rollup = true;
        else if (s_cube.ignore(pos, expected))
            select_query->group_by_with_cube = true;
        else if (s_totals.ignore(pos, expected))
            select_query->group_by_with_totals = true;
        else
            return false;
    }

    /// WITH TOTALS
    if (s_with.ignore(pos, expected))
    {
        if (select_query->group_by_with_totals || !s_totals.ignore(pos, expected))
            return false;

        select_query->group_by_with_totals = true;
    }

    /// HAVING expr
    if (s_having.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, having_expression, expected))
            return false;
    }

    /// ORDER BY expr ASC|DESC COLLATE 'locale' list
    if (s_order_by.ignore(pos, expected))
    {
        if (!order_list.parse(pos, order_expression_list, expected))
            return false;
    }

    /// LIMIT length | LIMIT offset, length | LIMIT count BY expr-list | LIMIT offset, length BY expr-list
    if (s_limit.ignore(pos, expected))
    {
        if (limit_length)
            throw Exception("Can not use TOP and LIMIT together", ErrorCodes::TOP_AND_LIMIT_TOGETHER);

        ParserToken s_comma(TokenType::Comma);

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
        if (s_by.ignore(pos, expected))
        {
            limit_by_length = limit_length;
            limit_by_offset = limit_offset;
            limit_length = nullptr;
            limit_offset = nullptr;

            if (!exp_list.parse(pos, limit_by_expression_list, expected))
                return false;
        }
    }

    /// LIMIT length | LIMIT offset, length
    if (s_limit.ignore(pos, expected))
    {
        if (!limit_by_length|| limit_length)
            return false;

        ParserToken s_comma(TokenType::Comma);

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
    }

    /// SETTINGS key1 = value1, key2 = value2, ...
    if (s_settings.ignore(pos, expected))
    {
        ParserSetQuery parser_settings(true);

        if (!parser_settings.parse(pos, settings, expected))
            return false;
    }

    select_query->setExpression(ASTSelectQuery::Expression::WITH, std::move(with_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
    select_query->setExpression(ASTSelectQuery::Expression::PREWHERE, std::move(prewhere_expression));
    select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_expression));
    select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::HAVING, std::move(having_expression));
    select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::LIMIT_BY_OFFSET, std::move(limit_by_offset));
    select_query->setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, std::move(limit_by_length));
    select_query->setExpression(ASTSelectQuery::Expression::LIMIT_BY, std::move(limit_by_expression_list));
    select_query->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, std::move(limit_offset));
    select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit_length));
    select_query->setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings));
    return true;
}

}
