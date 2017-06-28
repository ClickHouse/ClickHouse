#include <memory>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>

#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


bool ParserSelectQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    auto select_query = std::make_shared<ASTSelectQuery>();
    node = select_query;

    ParserWhitespaceOrComments ws;
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

    ParserNotEmptyExpressionList exp_list(false);
    ParserNotEmptyExpressionList exp_list_for_select_clause(true);    /// Allows aliases without AS keyword.
    ParserExpressionWithOptionalAlias exp_elem(false);
    ParserOrderByExpressionList order_list;

    ws.ignore(pos, end);

    /// SELECT [DISTINCT] expr list
    {
        if (!s_select.ignore(pos, end, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        if (s_distinct.ignore(pos, end, max_parsed_pos, expected))
        {
            select_query->distinct = true;
            ws.ignore(pos, end);
        }

        if (!exp_list_for_select_clause.parse(pos, end, select_query->select_expression_list, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);
    }

    /// FROM database.table or FROM table or FROM (subquery) or FROM tableFunction
    if (s_from.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        if (!ParserTablesInSelectQuery().parse(pos, end, select_query->tables, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);
    }

    /// PREWHERE expr
    if (s_prewhere.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        if (!exp_elem.parse(pos, end, select_query->prewhere_expression, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);
    }

    /// WHERE expr
    if (s_where.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        if (!exp_elem.parse(pos, end, select_query->where_expression, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);
    }

    /// GROUP BY expr list
    if (s_group_by.ignore(pos, end, max_parsed_pos, expected))
    {
        if (!exp_list.parse(pos, end, select_query->group_expression_list, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);
    }

    /// WITH TOTALS
    if (s_with.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);
        if (!s_totals.ignore(pos, end, max_parsed_pos, expected))
            return false;

        select_query->group_by_with_totals = true;

        ws.ignore(pos, end);
    }

    /// HAVING expr
    if (s_having.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        if (!exp_elem.parse(pos, end, select_query->having_expression, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);
    }

    /// ORDER BY expr ASC|DESC COLLATE 'locale' list
    if (s_order_by.ignore(pos, end, max_parsed_pos, expected))
    {
        if (!order_list.parse(pos, end, select_query->order_expression_list, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);
    }

    /// LIMIT length | LIMIT offset, length | LIMIT count BY expr-list
    if (s_limit.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        ParserString s_comma(",");
        ParserNumber num;

        if (!num.parse(pos, end, select_query->limit_length, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        if (s_comma.ignore(pos, end, max_parsed_pos, expected))
        {
            select_query->limit_offset = select_query->limit_length;
            if (!num.parse(pos, end, select_query->limit_length, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);
        }
        else if (s_by.ignore(pos, end, max_parsed_pos, expected))
        {
            select_query->limit_by_value = select_query->limit_length;
            select_query->limit_length = nullptr;

            ws.ignore(pos, end);

            if (!exp_list.parse(pos, end, select_query->limit_by_expression_list, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);
        }
    }

    /// LIMIT length | LIMIT offset, length
    if (s_limit.ignore(pos, end, max_parsed_pos, expected))
    {
        if (!select_query->limit_by_value || select_query->limit_length)
            return false;

        ws.ignore(pos, end);

        ParserString s_comma(",");
        ParserNumber num;

        if (!num.parse(pos, end, select_query->limit_length, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);

        if (s_comma.ignore(pos, end, max_parsed_pos, expected))
        {
            select_query->limit_offset = select_query->limit_length;
            if (!num.parse(pos, end, select_query->limit_length, max_parsed_pos, expected))
                return false;

            ws.ignore(pos, end);
        }
    }

    /// SETTINGS key1 = value1, key2 = value2, ...
    if (s_settings.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        ParserSetQuery parser_settings(true);

        if (!parser_settings.parse(pos, end, select_query->settings, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end);
    }

    // UNION ALL select query
    if (ParserKeyword("UNION ALL").ignore(pos, end, max_parsed_pos, expected))
    {
        ParserSelectQuery select_p;
        if (!select_p.parse(pos, end, select_query->next_union_all, max_parsed_pos, expected))
            return false;
        auto next_select_query = static_cast<ASTSelectQuery *>(&*select_query->next_union_all);
        next_select_query->prev_union_all = node.get();

        ws.ignore(pos, end);
    }

    select_query->range = StringRange(begin, pos);

    select_query->children.push_back(select_query->select_expression_list);
    if (select_query->tables)
        select_query->children.push_back(select_query->tables);
    if (select_query->prewhere_expression)
        select_query->children.push_back(select_query->prewhere_expression);
    if (select_query->where_expression)
        select_query->children.push_back(select_query->where_expression);
    if (select_query->group_expression_list)
        select_query->children.push_back(select_query->group_expression_list);
    if (select_query->having_expression)
        select_query->children.push_back(select_query->having_expression);
    if (select_query->order_expression_list)
        select_query->children.push_back(select_query->order_expression_list);
    if (select_query->limit_by_value)
        select_query->children.push_back(select_query->limit_by_value);
    if (select_query->limit_by_expression_list)
        select_query->children.push_back(select_query->limit_by_expression_list);
    if (select_query->limit_offset)
        select_query->children.push_back(select_query->limit_offset);
    if (select_query->limit_length)
        select_query->children.push_back(select_query->limit_length);
    if (select_query->settings)
        select_query->children.push_back(select_query->settings);

    if (select_query->next_union_all)
        select_query->children.push_back(select_query->next_union_all);

    return true;
}

}
