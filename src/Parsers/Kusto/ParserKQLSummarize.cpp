#include <memory>
#include <queue>
#include <vector>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLSummarize.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserWithElement.h>

namespace DB
{

bool ParserKQLSummarize::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr select_expression_list;
    ASTPtr group_expression_list;

    String expr_aggregation;
    String expr_groupby;
    String expr_columns;
    bool groupby = false;

    auto begin = pos;
    auto pos_groupby = pos;

    while (!pos->isEnd() && pos->type != TokenType::PipeMark && pos->type != TokenType::Semicolon)
    {
        if (String(pos->begin, pos->end) == "by")
        {
            groupby = true;
            auto end = pos;
            --end;
            expr_aggregation = begin <= end ? String(begin->begin, end->end) : "";
            pos_groupby = pos;
            ++pos_groupby;
        }
        ++pos;
    }
    --pos;
    if (groupby)
        expr_groupby = String(pos_groupby->begin, pos->end);
    else
        expr_aggregation = begin <= pos ? String(begin->begin, pos->end) : "";

    if (!expr_groupby.empty())
        expr_columns = expr_groupby;

    if (!expr_aggregation.empty())
    {
        if (expr_columns.empty())
            expr_columns = expr_aggregation;
        else
            expr_columns =  expr_columns + "," + expr_aggregation;
    }

    String converted_columns =  getExprFromToken(expr_columns, pos.max_depth);

    Tokens token_converted_columns(converted_columns.c_str(), converted_columns.c_str() + converted_columns.size());
    IParser::Pos pos_converted_columns(token_converted_columns, pos.max_depth);

    if (!ParserNotEmptyExpressionList(true).parse(pos_converted_columns, select_expression_list, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));

    if (groupby)
    {
        String converted_groupby =  getExprFromToken(expr_groupby, pos.max_depth);

        Tokens token_converted_groupby(converted_groupby.c_str(), converted_groupby.c_str() + converted_groupby.size());
        IParser::Pos postoken_converted_groupby(token_converted_groupby, pos.max_depth);

        if (!ParserNotEmptyExpressionList(false).parse(postoken_converted_groupby, group_expression_list, expected))
            return false;
        node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_expression_list));
    }

    return true;
}
}
