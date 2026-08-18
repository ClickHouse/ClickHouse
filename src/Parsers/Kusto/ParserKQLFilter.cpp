#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLFilter.h>
#include <Parsers/Kusto/ParserKQLOperators.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

bool ParserKQLFilter::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    String expr = getExprFromToken(pos);
    ASTPtr where_expression;

    Tokens token_filter(expr.data(), expr.data() + expr.size(), 0, true);
    IParser::Pos pos_filter(token_filter, pos.max_depth, pos.max_backtracks);
    if (!ParserExpressionWithOptionalAlias(false).parse(pos_filter, where_expression, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_expression));

    return true;
}

}
