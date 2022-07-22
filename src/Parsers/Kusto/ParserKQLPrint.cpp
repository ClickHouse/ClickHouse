#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLPrint.h>
namespace DB
{

bool ParserKQLPrint::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    const String expr = getExprFromToken(pos);

    Tokens tokens(expr.c_str(), expr.c_str() + expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth);

    if (!ParserNotEmptyExpressionList(true).parse(new_pos, node, expected))
        return false;

    return true;
}

}
