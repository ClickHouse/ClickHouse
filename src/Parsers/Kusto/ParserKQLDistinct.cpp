#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLDistinct.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
namespace DB
{

bool ParserKQLDistinct::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr select_expression_list;
    String expr;

    expr = getExprFromToken(pos);

    Tokens tokens(expr.data(), expr.data() + expr.size(), 0, true);
    IParser::Pos new_pos(tokens, pos.max_depth, pos.max_backtracks);

    if (!ParserNotEmptyExpressionList(false).parse(new_pos, select_expression_list, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));
    node->as<ASTSelectQuery>()->distinct = true;

    return true;
}

}
