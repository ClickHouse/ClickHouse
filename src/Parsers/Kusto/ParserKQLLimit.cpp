#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLLimit.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <cstdlib>
#include <format>

namespace DB
{

bool ParserKQLLimit :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr limit_length;

    auto expr = getExprFromToken(pos);

    Tokens tokens(expr.c_str(), expr.c_str() + expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth);

    if (!ParserExpressionWithOptionalAlias(false).parse(new_pos, limit_length, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit_length));

    return true;
}

}
