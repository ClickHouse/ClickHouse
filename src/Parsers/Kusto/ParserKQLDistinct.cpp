#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLDistinct.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
namespace DB
{

bool ParserKQLDistinct::parseImpl(KQLPos & pos, ASTPtr & node, [[maybe_unused]] KQLExpected & expected)
{
    ASTPtr select_expression_list;
    String expr;

    expr = getExprFromToken(pos);

    Tokens tokens(expr.data(), expr.data() + expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth, pos.max_backtracks);
    Expected sql_expected;

    if (!ParserNotEmptyExpressionList(false).parse(new_pos, select_expression_list, sql_expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));
    node->as<ASTSelectQuery>()->distinct = true;

    return true;
}

}
