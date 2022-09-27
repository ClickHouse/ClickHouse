#include <Parsers/ASTLiteral.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLMakeSeries.h>
#include <Parsers/Kusto/ParserKQLOperators.h>
#include <Parsers/Kusto/ParserKQLExtend.h>
#include <Parsers/Kusto/ParserKQLProject.h>
#include <Parsers/ParserSelectQuery.h>
#include <format>

namespace DB
{
bool ParserKQLExtend :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr select_query;
    String expr = getExprFromToken(pos);

    expr = std::format("SELECT *, {} from prev", expr);
    Tokens tokens(expr.c_str(), expr.c_str() + expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth);

    if (!ParserSelectQuery().parse(new_pos, select_query, expected))
        return false;
    if (!setSubQuerySource(select_query, node, false, false))
        return false;

    node = select_query;
    return true;
}

}
