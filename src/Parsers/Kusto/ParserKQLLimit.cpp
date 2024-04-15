#include <cstdlib>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLLimit.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB
{

bool ParserKQLLimit::parseImpl(KQLPos & pos, ASTPtr & node, [[maybe_unused]] KQLExpected & expected)
{
    ASTPtr limit_length;

    auto expr = getExprFromToken(pos);

    Tokens tokens(expr.data(), expr.data() + expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth, pos.max_backtracks);
    Expected sql_expected;
    if (!ParserExpressionWithOptionalAlias(false).parse(new_pos, limit_length, sql_expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(limit_length));

    return true;
}

}
