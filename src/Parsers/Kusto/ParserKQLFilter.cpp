#include <Parsers/ASTLiteral.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLFilter.h>
#include <Parsers/Kusto/ParserKQLOperators.h>

namespace DB
{

bool ParserKQLFilter :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (op_pos.empty())
        return true;
    Pos begin = pos;
    String expr;

    KQLOperators convetor;

    for (auto op_po : op_pos)
    {
        if (expr.empty())
            expr = "(" + convetor.getExprFromToken(op_po) +")";
        else
            expr = expr + " and (" + convetor.getExprFromToken(op_po) +")";
    }

    Tokens token_filter(expr.c_str(), expr.c_str()+expr.size());
    IParser::Pos pos_filter(token_filter, pos.max_depth);
    if (!ParserExpressionWithOptionalAlias(false).parse(pos_filter, node, expected))
        return false;

    pos = begin;

    return true;
}

}
