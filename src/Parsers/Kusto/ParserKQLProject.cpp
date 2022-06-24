#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLProject.h>
namespace DB
{

bool ParserKQLProject :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto begin = pos;
    String expr;
    if (op_pos.empty())
        expr = "*";
    else
        expr = getExprFromToken(op_pos.back());

    Tokens tokens(expr.c_str(), expr.c_str()+expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth);

    if (!ParserNotEmptyExpressionList(true).parse(new_pos, node, expected))
        return false;

    pos = begin;

    return true;
}

}
