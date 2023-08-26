#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLProject.h>
namespace DB
{

bool ParserKQLProject :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr select_expression_list;
    String expr;

    expr = getExprFromToken(pos);

    Tokens tokens(expr.c_str(), expr.c_str()+expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth);

    if (!ParserNotEmptyExpressionList(true).parse(new_pos, select_expression_list, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));

    return true;
}

}
