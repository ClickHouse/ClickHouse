#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Kusto/ParserKQLPrint.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
namespace DB
{

bool ParserKQLPrint::parseImpl(KQLPos & pos, ASTPtr & node, [[maybe_unused]] KQLExpected & expected)
{
    ASTPtr select_expression_list;
    const String expr = getExprFromToken(pos);

    Tokens tokens(expr.data(), expr.data() + expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth, pos.max_backtracks);
    Expected sql_expected;
    if (!ParserNotEmptyExpressionList(true).parse(new_pos, select_expression_list, sql_expected))
        return false;
    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));

    return true;
}

}
