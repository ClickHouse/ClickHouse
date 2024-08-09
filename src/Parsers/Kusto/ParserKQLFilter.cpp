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
    String expr = getExprFromToken(pos);
    ASTPtr where_expression;

    Tokens token_filter(expr.c_str(), expr.c_str()+expr.size());
    IParser::Pos pos_filter(token_filter, pos.max_depth);
    if (!ParserExpressionWithOptionalAlias(false).parse(pos_filter, where_expression, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_expression));

    return true;
}

}
