#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLPrint.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
namespace DB
{

bool ParserKQLPrint::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr select_expression_list;
    const String expr = getExprFromToken(pos);

    Tokens tokens(expr.c_str(), expr.c_str() + expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth, pos.max_backtracks);

    if (!ParserNotEmptyExpressionList(true).parse(new_pos, select_expression_list, expected))
        return false;
    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));

    return true;
}

}
