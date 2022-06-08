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

    for (auto it = op_pos.begin(); it != op_pos.end(); ++it)
    {
        pos = *it;
        if (expr.empty())
            expr = "(" + convetor.getExprFromToken(pos) +")";
        else
            expr = expr + " and (" + convetor.getExprFromToken(pos) +")";
    }

    Tokens tokenFilter(expr.c_str(), expr.c_str()+expr.size());
    IParser::Pos pos_filter(tokenFilter, pos.max_depth);
    if (!ParserExpressionWithOptionalAlias(false).parse(pos_filter, node, expected))
        return false;

    pos = begin;

    return true;
}

}
