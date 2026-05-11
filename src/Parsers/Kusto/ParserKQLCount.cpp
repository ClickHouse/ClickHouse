#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLCount.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/Utilities.h>

#include <Poco/String.h>
#include <fmt/format.h>

namespace DB
{

bool ParserKQLCount::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// count operator: produces a single row with the count of rows
    /// Optionally: count as <alias>
    String alias = "Count";

    /// `pos` may point at the `count` keyword itself: the operator-dispatch path in
    /// `ParserKQLQuery` stores the keyword position rather than what follows, so that
    /// bare `T | count` still produces a valid token even at end-of-stream. Skip the
    /// keyword here so the optional `as <alias>` is parsed correctly.
    if (isValidKQLPos(pos) && Poco::toLower(String(pos->begin, pos->end)) == "count")
        ++pos;

    if (isValidKQLPos(pos) && pos->type != TokenType::Semicolon && pos->type != TokenType::PipeMark)
    {
        String token(pos->begin, pos->end);
        if (Poco::toLower(token) == "as")
        {
            ++pos;
            if (isValidKQLPos(pos) && pos->type == TokenType::BareWord)
            {
                alias = String(pos->begin, pos->end);
                ++pos;
            }
        }
    }

    String expr = fmt::format("count() AS {}", alias);
    Tokens tokens(expr.data(), expr.data() + expr.size(), 0, true);
    IParser::Pos new_pos(tokens, pos.max_depth, pos.max_backtracks);

    ASTPtr select_expression_list;
    if (!ParserNotEmptyExpressionList(true).parse(new_pos, select_expression_list, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));
    return true;
}

}
