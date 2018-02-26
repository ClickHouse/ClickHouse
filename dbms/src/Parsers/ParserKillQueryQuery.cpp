#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ASTKillQueryQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>


namespace DB
{


bool ParserKillQueryQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTKillQueryQuery>();

    if (!ParserKeyword{"KILL QUERY"}.ignore(pos, expected))
        return false;

    if (!ParserKeyword{"WHERE"}.ignore(pos, expected))
        return false;

    ParserExpression p_where_expression;
    if (!p_where_expression.parse(pos, query->where_expression, expected))
        return false;

    if (ParserKeyword{"SYNC"}.ignore(pos))
        query->sync = true;
    else if (ParserKeyword{"ASYNC"}.ignore(pos))
        query->sync = false;
    else if (ParserKeyword{"TEST"}.ignore(pos))
        query->test = true;

    node = std::move(query);

    return true;
}

}
