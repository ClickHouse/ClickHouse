#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLSort.h>
#include <Parsers/Kusto/Utilities.h>

namespace DB
{

bool ParserKQLSort::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool has_dir = false;
    std::vector<bool> has_directions;
    ParserOrderByExpressionList order_list;
    ASTPtr order_expression_list;

    auto expr = getExprFromToken(pos);

    Tokens tokens(expr.data(), expr.data() + expr.size(), 0, true);
    IParser::Pos new_pos(tokens, pos.max_depth, pos.max_backtracks);

    auto pos_backup = new_pos;
    if (!order_list.parse(pos_backup, order_expression_list, expected))
        return false;

    while (isValidKQLPos(new_pos) && new_pos->type != TokenType::PipeMark && new_pos->type != TokenType::Semicolon)
    {
        String tmp(new_pos->begin, new_pos->end);
        if (tmp == "desc" || tmp == "asc")
            has_dir = true;

        if (new_pos->type == TokenType::Comma)
        {
            has_directions.push_back(has_dir);
            has_dir = false;
        }

        ++new_pos;
    }
    has_directions.push_back(has_dir);

    for (uint64_t i = 0; i < order_expression_list->children.size(); ++i)
    {
        if (!has_directions[i])
        {
            auto * order_expr = order_expression_list->children[i]->as<ASTOrderByElement>();
            order_expr->direction = -1; // default desc
            if (!order_expr->nulls_direction_was_explicitly_specified)
                order_expr->nulls_direction = -1;
            else
                order_expr->nulls_direction = order_expr->nulls_direction == 1 ? -1 : 1;
        }
    }

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_expression_list));
    return true;
}

}
